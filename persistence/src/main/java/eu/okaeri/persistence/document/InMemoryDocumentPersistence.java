package eu.okaeri.persistence.document;

import eu.okaeri.configs.configurer.InMemoryConfigurer;
import eu.okaeri.configs.serdes.OkaeriSerdesPack;
import eu.okaeri.persistence.PersistenceCollection;
import eu.okaeri.persistence.PersistenceEntity;
import eu.okaeri.persistence.PersistencePath;
import eu.okaeri.persistence.document.index.IndexProperty;
import eu.okaeri.persistence.document.index.PropertyIndex;
import eu.okaeri.persistence.filter.FindFilter;
import eu.okaeri.persistence.filter.IndexQueryOptimizer;
import eu.okaeri.persistence.filter.OrderBy;
import eu.okaeri.persistence.filter.condition.Condition;
import eu.okaeri.persistence.filter.operation.UpdateOperation;
import eu.okaeri.persistence.raw.PersistenceIndexMode;
import eu.okaeri.persistence.raw.PersistencePropertyMode;
import eu.okaeri.persistence.raw.RawPersistence;
import lombok.Getter;
import lombok.NonNull;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static eu.okaeri.persistence.document.DocumentValueUtils.extractValue;

public class InMemoryDocumentPersistence extends DocumentPersistence {

    private final @Getter PersistencePath basePath = PersistencePath.of("memory");
    private final Map<String, Map<String, PropertyIndex>> indexMap = new ConcurrentHashMap<>();
    private final Map<String, Map<PersistencePath, Document>> documents = new ConcurrentHashMap<>();
    private final Map<String, Map<PersistencePath, Object>> documentLocks = new ConcurrentHashMap<>();
    private final IndexQueryOptimizer queryOptimizer = new IndexQueryOptimizer();

    private static class DelegatingRawPersistence extends RawPersistence {
        private InMemoryDocumentPersistence delegate;

        DelegatingRawPersistence() {
            super(PersistencePath.of("memory"), PersistencePropertyMode.NATIVE, PersistenceIndexMode.EMULATED);
        }

        void setDelegate(InMemoryDocumentPersistence delegate) {
            this.delegate = delegate;
        }

        @Override
        public long count(PersistenceCollection collection) {
            return this.delegate.count(collection);
        }

        @Override
        public boolean exists(PersistenceCollection collection, PersistencePath path) {
            return this.delegate.exists(collection, path);
        }

        @Override
        public Optional<String> read(PersistenceCollection collection, PersistencePath path) {
            throw new UnsupportedOperationException("read() should be overridden in InMemoryDocumentPersistence");
        }

        @Override
        public Map<PersistencePath, String> read(PersistenceCollection collection, Collection<PersistencePath> paths) {
            throw new UnsupportedOperationException("read(paths) should be overridden in InMemoryDocumentPersistence");
        }

        @Override
        public Map<PersistencePath, String> readAll(PersistenceCollection collection) {
            throw new UnsupportedOperationException("readAll() should be overridden in InMemoryDocumentPersistence");
        }

        @Override
        public Stream<PersistenceEntity<String>> streamAll(PersistenceCollection collection) {
            throw new UnsupportedOperationException("streamAll() should be overridden in InMemoryDocumentPersistence");
        }

        @Override
        public Stream<PersistenceEntity<String>> stream(PersistenceCollection collection, int batchSize) {
            throw new UnsupportedOperationException("stream() should be overridden in InMemoryDocumentPersistence");
        }

        @Override
        public boolean write(PersistenceCollection collection, PersistencePath path, String entity) {
            throw new UnsupportedOperationException("write(entity) should be overridden in InMemoryDocumentPersistence");
        }

        @Override
        public boolean delete(PersistenceCollection collection, PersistencePath path) {
            return this.delegate.delete(collection, path);
        }

        @Override
        public long delete(PersistenceCollection collection, Collection<PersistencePath> paths) {
            return this.delegate.delete(collection, paths);
        }

        @Override
        public boolean deleteAll(PersistenceCollection collection) {
            return this.delegate.deleteAll(collection);
        }

        @Override
        public long deleteAll() {
            return this.delegate.deleteAll();
        }

        @Override
        public void close() throws IOException {
        }
    }

    public InMemoryDocumentPersistence(@NonNull OkaeriSerdesPack... serdesPacks) {
        super(createRawPersistence(), InMemoryConfigurer::new, serdesPacks);
        ((DelegatingRawPersistence) this.getWrite()).setDelegate(this);
    }

    private static RawPersistence createRawPersistence() {
        return new DelegatingRawPersistence();
    }

    @Override
    public void setFlushOnWrite(boolean state) {
    }

    @Override
    public void flush() {
    }

    /**
     * Get the indexes for a collection.
     * Exposed for testing and advanced use cases.
     */
    public Map<String, PropertyIndex> getIndexes(@NonNull PersistenceCollection collection) {
        return this.indexMap.get(collection.getValue());
    }

    /**
     * Get a lock object for a specific document (collection + path).
     * Uses per-document locking to ensure atomicity without blocking the entire collection.
     */
    private Object getLockFor(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {
        Map<PersistencePath, Object> collectionLocks = this.documentLocks.get(collection.getValue());
        if (collectionLocks == null) {
            throw new IllegalStateException("Collection not registered: " + collection.getValue());
        }
        return collectionLocks.computeIfAbsent(path, k -> new Object());
    }

    @Override
    public void registerCollection(@NonNull PersistenceCollection collection) {
        this.getRead().getKnownCollections().put(collection.getValue(), collection);
        this.getRead().getKnownIndexes().put(collection.getValue(), collection.getIndexes());
        this.getWrite().getKnownCollections().put(collection.getValue(), collection);
        this.getWrite().getKnownIndexes().put(collection.getValue(), collection.getIndexes());
        this.documents.put(collection.getValue(), new ConcurrentHashMap<>());
        this.documentLocks.put(collection.getValue(), new ConcurrentHashMap<>());

        // Create PropertyIndex for each indexed property
        Map<String, PropertyIndex> indexes = this.indexMap.computeIfAbsent(collection.getValue(), col -> new ConcurrentHashMap<>());
        collection.getIndexes().forEach(index -> indexes.put(index.getValue(), new PropertyIndex()));
    }

    @Override
    public boolean updateIndex(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull IndexProperty property, String identity) {

        this.getWrite().checkCollectionRegistered(collection);
        Map<String, PropertyIndex> indexes = this.indexMap.get(collection.getValue());
        if (indexes == null) return false;

        PropertyIndex index = indexes.get(property.getValue());
        if (index == null) {
            throw new IllegalArgumentException("non-indexed property used: " + property);
        }

        return index.put(path.getValue(), identity);
    }

    /**
     * Update index with typed value (enables range queries).
     * Called from updateIndex(collection, path, document) in parent class.
     */
    public boolean updateIndexTyped(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull IndexProperty property, Object value) {

        this.getWrite().checkCollectionRegistered(collection);
        Map<String, PropertyIndex> indexes = this.indexMap.get(collection.getValue());
        if (indexes == null) return false;

        PropertyIndex index = indexes.get(property.getValue());
        if (index == null) {
            throw new IllegalArgumentException("non-indexed property used: " + property);
        }

        return index.put(path.getValue(), value);
    }

    @Override
    public boolean updateIndex(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull Document document) {

        Set<IndexProperty> collectionIndexes = this.getRead().getKnownIndexes().get(collection.getValue());
        if (collectionIndexes == null) {
            return false;
        }

        Map<String, Object> documentMap = document.asMap(this.simplifier, true);
        int changes = 0;

        for (IndexProperty index : collectionIndexes) {
            Object value = extractValue(documentMap, index.toParts());

            // If field is null (unset), drop the index entry
            if (value == null) {
                boolean changed = this.dropIndex(collection, path, index);
                if (changed) changes++;
                continue;
            }

            // Use typed update for proper range query support
            boolean changed = this.updateIndexTyped(collection, path, index, value);
            if (changed) changes++;
        }

        return changes > 0;
    }

    @Override
    public boolean dropIndex(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull IndexProperty property) {
        this.getWrite().checkCollectionRegistered(collection);
        Map<String, PropertyIndex> indexes = this.indexMap.get(collection.getValue());
        if (indexes == null) return false;

        PropertyIndex index = indexes.get(property.getValue());
        if (index == null) {
            throw new IllegalArgumentException("non-indexed property used: " + property);
        }

        return index.remove(path.getValue());
    }

    @Override
    public boolean dropIndex(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {
        return this.getWrite().getKnownIndexes().getOrDefault(collection.getValue(), Collections.emptySet()).stream()
            .map(index -> this.dropIndex(collection, path, index))
            .anyMatch(Predicate.isEqual(true));
    }

    @Override
    public boolean dropIndex(@NonNull PersistenceCollection collection, @NonNull IndexProperty property) {

        this.getWrite().checkCollectionRegistered(collection);
        Map<String, PropertyIndex> indexes = this.indexMap.get(collection.getValue());
        if (indexes == null) return false;

        PropertyIndex removed = indexes.remove(property.getValue());
        return removed != null;
    }

    @Override
    public Set<PersistencePath> findMissingIndexes(@NonNull PersistenceCollection collection, @NonNull Set<IndexProperty> indexProperties) {

        this.getRead().checkCollectionRegistered(collection);
        Map<String, PropertyIndex> collectionIndexes = this.indexMap.get(collection.getValue());
        if ((collectionIndexes == null) || collectionIndexes.isEmpty()) {
            return Collections.emptySet();
        }

        return this.streamAll(collection)
            .map(PersistenceEntity::getValue)
            .map(entity -> collectionIndexes.values().stream()
                .allMatch(index -> index.containsDoc(entity.getPath().getValue()))
                ? null : entity.getPath())
            .filter(Objects::nonNull)
            .collect(Collectors.toSet());
    }

    @Override
    public Document readOrEmpty(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {
        return this.read(collection, path).orElse(this.createDocument(collection, path));
    }

    @Override
    public Optional<Document> read(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {
        synchronized (this.getLockFor(collection, path)) {
            this.getRead().checkCollectionRegistered(collection);
            return Optional.ofNullable(this.documents.get(collection.getValue()).get(path));
        }
    }

    @Override
    public Map<PersistencePath, Document> readOrEmpty(@NonNull PersistenceCollection collection, @NonNull Collection<PersistencePath> paths) {
        this.getRead().checkCollectionRegistered(collection);
        Map<PersistencePath, Document> map = new LinkedHashMap<>();
        Map<PersistencePath, Document> data = this.read(collection, paths);

        for (PersistencePath path : paths) {
            map.put(path, data.getOrDefault(path, this.createDocument(collection, path)));
        }

        return map;
    }

    @Override
    public Map<PersistencePath, Document> read(@NonNull PersistenceCollection collection, @NonNull Collection<PersistencePath> paths) {
        this.getRead().checkCollectionRegistered(collection);
        return paths.stream()
            .map(path -> this.read(collection, path))
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.toMap(Document::getPath, Function.identity()));
    }

    @Override
    public Map<PersistencePath, Document> readAll(@NonNull PersistenceCollection collection) {
        return new HashMap<>(this.documents.get(collection.getValue()));
    }

    @Override
    public Stream<PersistenceEntity<Document>> readByProperty(@NonNull PersistenceCollection collection, @NonNull PersistencePath property, Object propertyValue) {

        this.getRead().checkCollectionRegistered(collection);
        Map<String, PropertyIndex> indexes = this.indexMap.get(collection.getValue());
        if (indexes == null) {
            return this.streamAll(collection);
        }

        PropertyIndex index = indexes.get(property.getValue());
        if (index == null) {
            return this.streamAll(collection);
        }

        // Use index for equality lookup
        Set<String> keys = index.findEquals(propertyValue);
        if (keys.isEmpty()) {
            return Stream.empty();
        }

        // Direct map access - no lock overhead per document
        Map<PersistencePath, Document> collectionDocs = this.documents.get(collection.getValue());
        return keys.stream()
            .map(key -> {
                PersistencePath path = PersistencePath.of(key);
                Document doc = collectionDocs.get(path);
                if (doc == null) {
                    // Stale index entry - clean up
                    this.dropIndex(collection, path);
                    return null;
                }
                return new PersistenceEntity<>(path, doc);
            })
            .filter(Objects::nonNull);
    }

    @Override
    public Stream<PersistenceEntity<Document>> readByFilter(@NonNull PersistenceCollection collection, @NonNull FindFilter filter) {
        this.getRead().checkCollectionRegistered(collection);

        // Try to use indexes for WHERE clause acceleration
        Stream<PersistenceEntity<Document>> stream = this.tryIndexedQuery(collection, filter);

        // Apply ORDER BY
        if (filter.hasOrderBy()) {
            stream = this.filterEvaluator.applyFilter(stream, FindFilter.builder()
                .orderBy(filter.getOrderBy().toArray(new OrderBy[0]))
                .build());
        }

        // Apply SKIP
        if (filter.hasSkip()) {
            stream = stream.skip(filter.getSkip());
        }

        // Apply LIMIT
        if (filter.hasLimit()) {
            stream = stream.limit(filter.getLimit());
        }

        return stream;
    }

    /**
     * Try to use indexes for WHERE clause optimization.
     * Falls back to full scan with filter if indexes can't help.
     */
    private Stream<PersistenceEntity<Document>> tryIndexedQuery(@NonNull PersistenceCollection collection, @NonNull FindFilter filter) {

        if (filter.getWhere() == null) {
            return this.streamAll(collection);
        }

        Condition where = filter.getWhere();
        Map<String, PropertyIndex> indexes = this.indexMap.get(collection.getValue());

        // Use query optimizer for complex condition analysis
        IndexQueryOptimizer.IndexResult optimized = this.queryOptimizer.optimize(where, indexes);

        if (!optimized.requiresFullScan()) {
            // Got results from index - load documents directly (no lock overhead)
            Map<PersistencePath, Document> collectionDocs = this.documents.get(collection.getValue());
            Set<String> docIds = optimized.getDocIds();
            Stream<PersistenceEntity<Document>> stream = docIds.stream()
                .map(docId -> {
                    PersistencePath path = PersistencePath.of(docId);
                    Document doc = collectionDocs.get(path);
                    return (doc != null) ? new PersistenceEntity<>(path, doc) : null;
                })
                .filter(Objects::nonNull);

            // Apply remaining conditions if index only partially covered the query
            if (optimized.hasRemainingCondition()) {
                Condition remaining = optimized.getRemainingCondition();
                stream = stream.filter(entity -> this.filterEvaluator.evaluateCondition(remaining, entity.getValue()));
            }

            return stream;
        }

        // Fall back to full scan with in-memory filtering
        return this.streamAll(collection)
            .filter(entity -> this.filterEvaluator.evaluateCondition(where, entity.getValue()));
    }

    @Override
    public Stream<PersistenceEntity<Document>> streamAll(@NonNull PersistenceCollection collection) {
        this.getRead().checkCollectionRegistered(collection);
        Collection<Document> docList = this.documents.get(collection.getValue()).values();
        return docList.stream().map(document -> new PersistenceEntity<>(document.getPath(), document));
    }

    @Override
    public Stream<PersistenceEntity<Document>> stream(@NonNull PersistenceCollection collection, int batchSize) {
        // In-memory implementation ignores batch size - all data already in memory
        return this.streamAll(collection);
    }

    @Override
    public long count(@NonNull PersistenceCollection collection) {
        this.getRead().checkCollectionRegistered(collection);
        return this.documents.get(collection.getValue()).size();
    }

    @Override
    public boolean exists(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {
        this.getRead().checkCollectionRegistered(collection);
        return this.documents.get(collection.getValue()).containsKey(path);
    }

    @Override
    public boolean write(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull Document document) {
        synchronized (this.getLockFor(collection, path)) {
            this.getWrite().checkCollectionRegistered(collection);
            Document updated = this.update(document, collection);
            this.updateIndex(collection, path, updated);
            return this.documents.get(collection.getValue()).put(path, updated) != null;
        }
    }

    @Override
    public long write(@NonNull PersistenceCollection collection, @NonNull Map<PersistencePath, Document> entities) {
        return entities.entrySet().stream()
            .map(entity -> this.write(collection, entity.getKey(), entity.getValue()))
            .filter(Predicate.isEqual(true))
            .count();
    }

    @Override
    public boolean delete(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {
        synchronized (this.getLockFor(collection, path)) {
            this.getWrite().checkCollectionRegistered(collection);
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
        return paths.stream().map(path -> this.delete(collection, path)).filter(Predicate.isEqual(true)).count();
    }

    @Override
    public boolean deleteAll(@NonNull PersistenceCollection collection) {

        this.getWrite().checkCollectionRegistered(collection);
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
    protected boolean updateOneInMemory(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull List<UpdateOperation> operations) {
        synchronized (this.getLockFor(collection, path)) {
            return super.updateOneInMemory(collection, path, operations);
        }
    }

    @Override
    protected Optional<Document> updateOneAndGetInMemory(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull List<UpdateOperation> operations) {
        synchronized (this.getLockFor(collection, path)) {
            return super.updateOneAndGetInMemory(collection, path, operations);
        }
    }

    @Override
    protected Optional<Document> getAndUpdateOneInMemory(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull List<UpdateOperation> operations) {
        synchronized (this.getLockFor(collection, path)) {
            return super.getAndUpdateOneInMemory(collection, path, operations);
        }
    }
}
