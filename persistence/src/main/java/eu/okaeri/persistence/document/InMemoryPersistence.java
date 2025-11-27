package eu.okaeri.persistence.document;

import eu.okaeri.configs.configurer.InMemoryConfigurer;
import eu.okaeri.configs.serdes.OkaeriSerdesPack;
import eu.okaeri.persistence.*;
import eu.okaeri.persistence.document.index.IndexExtractor;
import eu.okaeri.persistence.document.index.IndexProperty;
import eu.okaeri.persistence.document.index.PropertyIndex;
import eu.okaeri.persistence.filter.*;
import eu.okaeri.persistence.filter.condition.Condition;
import eu.okaeri.persistence.filter.operation.UpdateOperation;
import lombok.Getter;
import lombok.NonNull;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * In-memory persistence backend with full filtering and update support.
 * Documents are stored in ConcurrentHashMaps with per-document locking.
 */
public class InMemoryPersistence implements Persistence, FilterablePersistence, UpdatablePersistence {

    @Getter
    private final PersistencePath basePath = PersistencePath.of("memory");

    private final @Getter DocumentSerializer serializer;
    private final IndexExtractor indexExtractor;
    private final InMemoryFilterEvaluator filterEvaluator;
    private final InMemoryUpdateEvaluator updateEvaluator;
    private final IndexQueryOptimizer queryOptimizer = new IndexQueryOptimizer();

    // Data storage
    private final Map<String, PersistenceCollection> knownCollections = new ConcurrentHashMap<>();
    private final Map<String, Map<String, PropertyIndex>> indexMap = new ConcurrentHashMap<>();
    private final Map<String, Map<PersistencePath, Document>> documents = new ConcurrentHashMap<>();
    private final Map<String, Map<PersistencePath, Object>> documentLocks = new ConcurrentHashMap<>();

    public InMemoryPersistence(@NonNull OkaeriSerdesPack... serdesPacks) {
        this.serializer = new DocumentSerializer(InMemoryConfigurer::new, serdesPacks);
        this.indexExtractor = new IndexExtractor(this.serializer.getSimplifier());
        this.filterEvaluator = new InMemoryFilterEvaluator(this.serializer.getSimplifier());
        this.updateEvaluator = new InMemoryUpdateEvaluator(InMemoryConfigurer::new, this.serializer.getSerdesRegistry());
    }

    /**
     * Get the in-memory indexes for a collection.
     * Exposed for testing and advanced use cases.
     */
    public Map<String, PropertyIndex> getIndexes(@NonNull PersistenceCollection collection) {
        return this.indexMap.get(collection.getValue());
    }

    // ==================== COLLECTION MANAGEMENT ====================

    @Override
    public void registerCollection(@NonNull PersistenceCollection collection) {
        this.knownCollections.put(collection.getValue(), collection);
        this.documents.put(collection.getValue(), new ConcurrentHashMap<>());
        this.documentLocks.put(collection.getValue(), new ConcurrentHashMap<>());

        // Create PropertyIndex for each indexed property
        Map<String, PropertyIndex> indexes = this.indexMap.computeIfAbsent(
            collection.getValue(), col -> new ConcurrentHashMap<>());
        collection.getIndexes().forEach(index ->
            indexes.put(index.getValue(), new PropertyIndex()));
    }

    private void checkCollectionRegistered(@NonNull PersistenceCollection collection) {
        if (!this.knownCollections.containsKey(collection.getValue())) {
            throw new IllegalArgumentException("Collection not registered: " + collection.getValue());
        }
    }

    /**
     * Get a lock object for a specific document.
     * Uses per-document locking to ensure atomicity without blocking the entire collection.
     */
    private Object getLockFor(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {
        Map<PersistencePath, Object> collectionLocks = this.documentLocks.get(collection.getValue());
        if (collectionLocks == null) {
            throw new IllegalStateException("Collection not registered: " + collection.getValue());
        }
        return collectionLocks.computeIfAbsent(path, k -> new Object());
    }

    // ==================== READ OPERATIONS ====================

    @Override
    public boolean exists(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {
        this.checkCollectionRegistered(collection);
        return this.documents.get(collection.getValue()).containsKey(path);
    }

    @Override
    public long count(@NonNull PersistenceCollection collection) {
        this.checkCollectionRegistered(collection);
        return this.documents.get(collection.getValue()).size();
    }

    @Override
    public Optional<Document> read(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {
        this.checkCollectionRegistered(collection);
        Document doc = this.documents.get(collection.getValue()).get(path);
        return Optional.ofNullable(doc);
    }

    @Override
    public Map<PersistencePath, Document> read(@NonNull PersistenceCollection collection, @NonNull Collection<PersistencePath> paths) {
        this.checkCollectionRegistered(collection);
        Map<PersistencePath, Document> collectionDocs = this.documents.get(collection.getValue());
        Map<PersistencePath, Document> result = new LinkedHashMap<>();

        for (PersistencePath path : paths) {
            Document doc = collectionDocs.get(path);
            if (doc != null) {
                result.put(path, doc);
            }
        }
        return result;
    }

    @Override
    public Map<PersistencePath, Document> readAll(@NonNull PersistenceCollection collection) {
        this.checkCollectionRegistered(collection);
        return new LinkedHashMap<>(this.documents.get(collection.getValue()));
    }

    // ==================== STREAMING ====================

    @Override
    public Stream<PersistenceEntity<Document>> streamAll(@NonNull PersistenceCollection collection) {
        this.checkCollectionRegistered(collection);
        return this.documents.get(collection.getValue()).values().stream()
            .map(doc -> new PersistenceEntity<>(doc.getPath(), doc));
    }


    // ==================== FILTERING ====================

    @Override
    public Stream<PersistenceEntity<Document>> find(@NonNull PersistenceCollection collection, @NonNull FindFilter filter) {
        this.checkCollectionRegistered(collection);

        // Start with candidates from index or full scan
        Stream<PersistenceEntity<Document>> candidates;
        Condition where = filter.getWhere();

        if (where != null) {
            Map<String, PropertyIndex> indexes = this.indexMap.get(collection.getValue());
            IndexQueryOptimizer.IndexResult optimized = this.queryOptimizer.optimize(where, indexes);

            if (!optimized.requiresFullScan()) {
                // Index provides at least partial coverage
                Map<PersistencePath, Document> collectionDocs = this.documents.get(collection.getValue());
                candidates = optimized.getDocIds().stream()
                    .map(docId -> {
                        PersistencePath path = PersistencePath.of(docId);
                        Document doc = collectionDocs.get(path);
                        return (doc != null) ? new PersistenceEntity<>(path, doc) : null;
                    })
                    .filter(Objects::nonNull);

                // Apply remaining filter if index only partially covered
                if (optimized.hasRemainingCondition()) {
                    Condition remaining = optimized.getRemainingCondition();
                    candidates = candidates.filter(entity ->
                        this.filterEvaluator.evaluateCondition(remaining, entity.getValue()));
                }
            } else {
                // Full scan with WHERE filter
                candidates = this.streamAll(collection)
                    .filter(entity -> this.filterEvaluator.evaluateCondition(where, entity.getValue()));
            }
        } else {
            // No WHERE - start with all documents
            candidates = this.streamAll(collection);
        }

        // Apply ORDER BY / SKIP / LIMIT
        return this.filterEvaluator.applyFilter(candidates, filter);
    }

    @Override
    public long delete(@NonNull PersistenceCollection collection, @NonNull DeleteFilter filter) {
        this.checkCollectionRegistered(collection);

        if (filter.getWhere() == null) {
            throw new IllegalArgumentException("DeleteFilter requires WHERE condition - use deleteAll() instead");
        }

        // Find matching documents using optimized find (uses indexes)
        FindFilter findFilter = FindFilter.builder().where(filter.getWhere()).build();
        List<PersistencePath> toDelete = this.find(collection, findFilter)
            .map(PersistenceEntity::getPath)
            .collect(Collectors.toList());

        // Delete them
        return this.delete(collection, toDelete);
    }

    // ==================== UPDATES ====================

    @Override
    public boolean updateOne(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull List<UpdateOperation> operations) {
        synchronized (this.getLockFor(collection, path)) {
            Optional<Document> docOpt = this.read(collection, path);
            if (!docOpt.isPresent()) {
                return false;
            }

            Document document = docOpt.get();
            boolean modified = this.updateEvaluator.applyUpdate(document, operations);

            if (modified) {
                this.updateIndexes(collection, path, document);
            }
            return true;
        }
    }

    @Override
    public Optional<Document> updateOneAndGet(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull List<UpdateOperation> operations) {
        synchronized (this.getLockFor(collection, path)) {
            Optional<Document> docOpt = this.read(collection, path);
            if (!docOpt.isPresent()) {
                return Optional.empty();
            }

            Document document = docOpt.get();
            boolean modified = this.updateEvaluator.applyUpdate(document, operations);

            if (modified) {
                this.updateIndexes(collection, path, document);
            }
            return Optional.of(document);
        }
    }

    @Override
    public Optional<Document> getAndUpdateOne(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull List<UpdateOperation> operations) {
        synchronized (this.getLockFor(collection, path)) {
            Optional<Document> docOpt = this.read(collection, path);
            if (!docOpt.isPresent()) {
                return Optional.empty();
            }

            Document document = docOpt.get();
            Document oldVersion = this.serializer.deepCopy(document);

            boolean modified = this.updateEvaluator.applyUpdate(document, operations);
            if (modified) {
                this.updateIndexes(collection, path, document);
            }
            return Optional.of(oldVersion);
        }
    }

    @Override
    public long update(@NonNull PersistenceCollection collection, @NonNull UpdateFilter filter) {
        this.checkCollectionRegistered(collection);

        Stream<PersistenceEntity<Document>> stream = this.streamAll(collection);

        if (filter.getWhere() != null) {
            stream = stream.filter(entity ->
                this.filterEvaluator.evaluateCondition(filter.getWhere(), entity.getValue()));
        }

        List<PersistencePath> pathsToUpdate = stream
            .map(PersistenceEntity::getPath)
            .collect(Collectors.toList());

        long count = 0;
        for (PersistencePath path : pathsToUpdate) {
            if (this.updateOne(collection, path, filter.getOperations())) {
                count++;
            }
        }
        return count;
    }

    // ==================== WRITE OPERATIONS ====================

    @Override
    public boolean write(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull Document document) {
        synchronized (this.getLockFor(collection, path)) {
            this.checkCollectionRegistered(collection);
            this.serializer.setupDocument(document, collection, path);
            this.updateIndexes(collection, path, document);
            return this.documents.get(collection.getValue()).put(path, document) != null;
        }
    }

    @Override
    public long write(@NonNull PersistenceCollection collection, @NonNull Map<PersistencePath, Document> documents) {
        return documents.entrySet().stream()
            .map(entry -> this.write(collection, entry.getKey(), entry.getValue()))
            .filter(Predicate.isEqual(true))
            .count();
    }

    // ==================== DELETE OPERATIONS ====================

    @Override
    public boolean delete(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {
        synchronized (this.getLockFor(collection, path)) {
            this.checkCollectionRegistered(collection);
            boolean removed = this.documents.get(collection.getValue()).remove(path) != null;
            if (removed) {
                this.dropIndex(collection, path);
                this.documentLocks.get(collection.getValue()).remove(path);
            }
            return removed;
        }
    }

    @Override
    public long delete(@NonNull PersistenceCollection collection, @NonNull Collection<PersistencePath> paths) {
        return paths.stream()
            .map(path -> this.delete(collection, path))
            .filter(Predicate.isEqual(true))
            .count();
    }

    @Override
    public boolean deleteAll(@NonNull PersistenceCollection collection) {
        this.checkCollectionRegistered(collection);
        Map<PersistencePath, Document> data = this.documents.get(collection.getValue());
        Map<PersistencePath, Object> locks = this.documentLocks.get(collection.getValue());
        Map<String, PropertyIndex> indexes = this.indexMap.get(collection.getValue());

        synchronized (data) {
            boolean changed = !data.isEmpty();
            data.clear();
            if (locks != null) {
                locks.clear();
            }
            if (indexes != null) {
                indexes.values().forEach(PropertyIndex::clear);
            }
            return changed;
        }
    }

    @Override
    public long deleteAll() {
        long count = this.documents.values().stream()
            .peek(Map::clear)
            .count();
        this.documentLocks.values().forEach(Map::clear);
        this.indexMap.values().forEach(indexes -> indexes.values().forEach(PropertyIndex::clear));
        return count;
    }

    @Override
    public void close() throws IOException {
        // Nothing to close for in-memory storage
    }

    // ==================== INDEX MANAGEMENT (INTERNAL) ====================

    private void updateIndexes(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull Document document) {
        Map<String, PropertyIndex> indexes = this.indexMap.get(collection.getValue());
        if ((indexes == null) || indexes.isEmpty()) {
            return;
        }

        Map<IndexProperty, Object> indexValues = this.indexExtractor.extract(collection, document);

        for (IndexProperty indexProp : collection.getIndexes()) {
            PropertyIndex index = indexes.get(indexProp.getValue());
            if (index == null) continue;

            Object value = indexValues.get(indexProp);
            index.put(path.getValue(), value);
        }
    }

    private void dropIndex(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {
        Map<String, PropertyIndex> indexes = this.indexMap.get(collection.getValue());
        if (indexes == null) return;

        for (PropertyIndex index : indexes.values()) {
            index.remove(path.getValue());
        }
    }
}
