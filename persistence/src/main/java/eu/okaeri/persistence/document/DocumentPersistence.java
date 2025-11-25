package eu.okaeri.persistence.document;

import eu.okaeri.configs.ConfigManager;
import eu.okaeri.configs.configurer.Configurer;
import eu.okaeri.configs.serdes.OkaeriSerdesPack;
import eu.okaeri.configs.serdes.SerdesRegistry;
import eu.okaeri.configs.serdes.commons.SerdesCommons;
import eu.okaeri.configs.serdes.commons.serializer.InstantSerializer;
import eu.okaeri.configs.serdes.standard.StandardSerdes;
import eu.okaeri.persistence.Persistence;
import eu.okaeri.persistence.PersistenceCollection;
import eu.okaeri.persistence.PersistenceEntity;
import eu.okaeri.persistence.PersistencePath;
import eu.okaeri.persistence.document.index.IndexProperty;
import eu.okaeri.persistence.document.ref.EagerRefSerializer;
import eu.okaeri.persistence.document.ref.LazyRefSerializer;
import eu.okaeri.persistence.filter.*;
import eu.okaeri.persistence.filter.operation.UpdateOperation;
import eu.okaeri.persistence.filter.operation.UpdateOperationType;
import eu.okaeri.persistence.raw.PersistenceIndexMode;
import eu.okaeri.persistence.raw.PersistencePropertyMode;
import eu.okaeri.persistence.raw.RawPersistence;
import eu.okaeri.persistence.repository.DocumentRepository;
import eu.okaeri.persistence.repository.RepositoryDeclaration;
import lombok.Getter;
import lombok.NonNull;

import java.io.IOException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static eu.okaeri.persistence.document.DocumentValueUtils.compareEquals;
import static eu.okaeri.persistence.document.DocumentValueUtils.extractValue;

public class DocumentPersistence implements Persistence<Document> {

    private static final Logger LOGGER = Logger.getLogger(DocumentPersistence.class.getSimpleName());

    @Getter protected final ConfigurerProvider configurerProvider;
    @Getter protected final OkaeriSerdesPack[] serdesPacks;

    protected RawPersistence read;
    protected RawPersistence write;

    protected SerdesRegistry serdesRegistry;
    protected Configurer simplifier;
    protected InMemoryFilterEvaluator filterEvaluator;
    protected InMemoryUpdateEvaluator updateEvaluator;

    // Per-document locks for atomic in-memory update fallbacks (H2, Redis, Flat Files)
    private final Map<String, Map<String, Object>> fallbackLocks = new ConcurrentHashMap<>();

    /**
     * @param configurerProvider Okaeri Config's provider (mostly json)
     * @param serdesPacks        Additional serdes packs for the configurerProvider
     */
    public DocumentPersistence(@NonNull ConfigurerProvider configurerProvider, @NonNull OkaeriSerdesPack... serdesPacks) {
        this.configurerProvider = configurerProvider;
        this.serdesPacks = serdesPacks;
        // shared transform registry for faster transformations
        this.serdesRegistry = new SerdesRegistry();
        Stream.concat(
                Stream.of(
                    new StandardSerdes(),
                    new SerdesCommons(),
                    registry -> registry.registerExclusive(Instant.class, new InstantSerializer(true))
                ),
                Stream.of(this.serdesPacks)
            )
            .forEach(pack -> pack.register(this.serdesRegistry));
        this.serdesRegistry.register(new LazyRefSerializer(this));
        this.serdesRegistry.register(new EagerRefSerializer(this));
        // simplifier for document mappings
        this.simplifier = configurerProvider.get();
        this.simplifier.setRegistry(this.serdesRegistry);
        // in-memory filter evaluator for backends without native query support
        this.filterEvaluator = new InMemoryFilterEvaluator(this.simplifier);
        // in-memory update evaluator for backends without native update support
        this.updateEvaluator = new InMemoryUpdateEvaluator(this.configurerProvider, this.serdesRegistry);
    }

    /**
     * @param readPersistence    Base persistence provider for read operations
     * @param writePersistence   Base persistence provider for write operations
     * @param configurerProvider Okaeri Config's provider (mostly json)
     * @param serdesPacks        Additional serdes packs for the configurerProvider
     */
    public DocumentPersistence(@NonNull RawPersistence readPersistence, @NonNull RawPersistence writePersistence, @NonNull ConfigurerProvider configurerProvider, @NonNull OkaeriSerdesPack... serdesPacks) {
        this(configurerProvider, serdesPacks);
        this.read = readPersistence;
        this.write = writePersistence;
    }

    /**
     * @param rawPersistence     Base persistence provider
     * @param configurerProvider Okaeri Config's provider (mostly json)
     * @param serdesPacks        Additional serdes packs for the configurerProvider
     */
    public DocumentPersistence(@NonNull RawPersistence rawPersistence, @NonNull ConfigurerProvider configurerProvider, @NonNull OkaeriSerdesPack... serdesPacks) {
        this(rawPersistence, rawPersistence, configurerProvider, serdesPacks);
    }

    public RawPersistence getRead() {
        if (this.read == null) {
            throw new IllegalArgumentException("This persistence instance does not provide raw access.");
        }
        return this.read;
    }

    public RawPersistence getWrite() {
        if (this.write == null) {
            throw new IllegalArgumentException("This persistence instance does not provide raw access.");
        }
        return this.write;
    }

    @Deprecated
    public RawPersistence getRaw() {
        if (!this.getRead().equals(this.getWrite())) {
            throw new IllegalArgumentException("Cannot use #getRaw() with DocumentPersistence using separate instances for read and write");
        }
        return this.getRead();
    }

    @Override
    public void setFlushOnWrite(boolean state) {
        this.getWrite().setFlushOnWrite(state);
    }

    @Override
    public void flush() {
        this.getWrite().flush();
    }

    @Override
    public PersistencePath getBasePath() {
        if (!this.getRead().getBasePath().equals(this.getWrite().getBasePath())) {
            throw new IllegalArgumentException("Cannot use #getBasePath() with DocumentPersistence using different paths for read and write");
        }
        return this.getWrite().getBasePath();
    }

    @Override
    public void registerCollection(@NonNull PersistenceCollection collection) {

        this.getRead().registerCollection(collection);

        if (!this.getRead().equals(this.getWrite())) {
            this.getWrite().registerCollection(collection);
        }

        if (!collection.isAutofixIndexes()) {
            return;
        }

        this.fixIndexes(collection);
    }

    @Override
    public long fixIndexes(@NonNull PersistenceCollection collection) {

        if (this.getWrite().getIndexMode() != PersistenceIndexMode.EMULATED) {
            return 0;
        }

        Set<IndexProperty> indexes = this.getRead().getKnownIndexes().getOrDefault(collection.getValue(), new HashSet<>());
        Set<PersistencePath> withMissingIndexes = this.findMissingIndexes(collection, indexes);

        if (withMissingIndexes.isEmpty()) {
            return 0;
        }

        int total = withMissingIndexes.size();
        long start = System.currentTimeMillis();
        long lastInfo = System.currentTimeMillis();
        int updated = 0;
        LOGGER.warning("[" + this.getBasePath().sub(collection).getValue() + "] Found " + total + " entries with missing indexes, updating..");
        this.setFlushOnWrite(false);

        for (PersistencePath key : withMissingIndexes) {

            this.updateIndex(collection, key);
            updated++;

            if ((System.currentTimeMillis() - lastInfo) <= 5_000) {
                continue;
            }

            int percent = (int) (((double) updated / (double) total) * 100);
            LOGGER.warning("[" + this.getBasePath().sub(collection).getValue() + "] " + updated + " already done (" + percent + "%)");
            lastInfo = System.currentTimeMillis();
        }

        this.setFlushOnWrite(true);
        this.flush();
        LOGGER.warning("[" + this.getBasePath().sub(collection).getValue() + "] Finished creating indexes! (took: " + (System.currentTimeMillis() - start) + " ms)");
        return updated;
    }

    @Override
    public boolean updateIndex(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull IndexProperty property, String identity) {
        return (this.getWrite().getIndexMode() == PersistenceIndexMode.EMULATED) && this.getWrite().updateIndex(collection, path, property, identity);
    }

    @Override
    public boolean updateIndex(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull Document document) {

        if (this.getWrite().getIndexMode() != PersistenceIndexMode.EMULATED) {
            return false;
        }

        Set<IndexProperty> collectionIndexes = this.getRead().getKnownIndexes().get(collection.getValue());
        if (collectionIndexes == null) {
            return false;
        }

        Map<String, Object> documentMap = document.asMap(this.simplifier, true);
        int changes = 0;

        for (IndexProperty index : collectionIndexes) {
            Object value = extractValue(documentMap, index.toParts());

            // If field is null (unset), drop the index entry instead of updating with null
            if (value == null) {
                boolean changed = this.dropIndex(collection, path, index);
                if (changed) changes++;
                continue;
            }

            // Use typed index update to preserve value types (enables range queries)
            boolean changed = this.getWrite().updateIndexTyped(collection, path, index, value);
            if (changed) changes++;
        }

        return changes > 0;
    }

    @Override
    public boolean updateIndex(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {

        if (this.getWrite().getIndexMode() != PersistenceIndexMode.EMULATED) {
            return false;
        }

        Optional<Document> document = this.read(collection, path);
        return document.map(value -> this.updateIndex(collection, path, value)).isPresent();
    }

    @Override
    public boolean dropIndex(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull IndexProperty property) {
        return (this.getWrite().getIndexMode() == PersistenceIndexMode.EMULATED) && this.getWrite().dropIndex(collection, path, property);
    }

    @Override
    public boolean dropIndex(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {
        return (this.getWrite().getIndexMode() == PersistenceIndexMode.EMULATED) && this.getWrite().dropIndex(collection, path);
    }

    @Override
    public boolean dropIndex(@NonNull PersistenceCollection collection, @NonNull IndexProperty property) {
        return (this.getWrite().getIndexMode() == PersistenceIndexMode.EMULATED) && this.getWrite().dropIndex(collection, property);
    }

    @Override
    public Set<PersistencePath> findMissingIndexes(@NonNull PersistenceCollection collection, @NonNull Set<IndexProperty> indexProperties) {
        return this.getWrite().findMissingIndexes(collection, indexProperties);
    }

    @Override
    public Document readOrEmpty(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {
        return this.read(collection, path).orElse(this.createDocument(collection, path));
    }

    @Override
    public Optional<Document> read(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {

        Optional<String> data = this.getRead().read(collection, path);
        if (!data.isPresent()) {
            return Optional.empty();
        }

        Document document = this.createDocument(collection, path);
        return Optional.of((Document) document.load(data.get()));
    }

    @Override
    public Map<PersistencePath, Document> readOrEmpty(@NonNull PersistenceCollection collection, @NonNull Collection<PersistencePath> paths) {

        if (paths.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<PersistencePath, Document> map = new LinkedHashMap<>();
        Map<PersistencePath, Document> data = this.read(collection, paths);

        for (PersistencePath path : paths) {
            map.put(path, data.getOrDefault(path, this.createDocument(collection, path)));
        }

        return map;
    }

    @Override
    public Map<PersistencePath, Document> read(@NonNull PersistenceCollection collection, @NonNull Collection<PersistencePath> paths) {
        return paths.isEmpty() ? Collections.emptyMap() : this.getRead().read(collection, paths).entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> {
                Document document = this.createDocument(collection, entry.getKey());
                return (Document) document.load(entry.getValue());
            }));

    }

    @Override
    public Map<PersistencePath, Document> readAll(@NonNull PersistenceCollection collection) {
        return this.getRead().readAll(collection).entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> {
                Document document = this.createDocument(collection, entry.getKey());
                return (Document) document.load(entry.getValue());
            }));
    }

    @Override
    public Stream<PersistenceEntity<Document>> readByFilter(@NonNull PersistenceCollection collection, @NonNull FindFilter filter) {
        try {
            return this.getRead().readByFilter(collection, filter).map(this.entityToDocumentMapper(collection));
        } catch (PartialIndexResultException e) {
            // Index narrowed down candidates, but remaining filter needed
            LOGGER.fine("Partial index coverage, applying remaining filter for collection: " + collection.getValue());
            return this.filterEvaluator.applyFilter(
                e.getPartialResults().map(this.entityToDocumentMapper(collection)),
                filter
            );
        } catch (UnsupportedOperationException e) {
            LOGGER.fine("Backend doesn't support native find(), using in-memory filtering for collection: " + collection.getValue());
            return this.filterEvaluator.applyFilter(this.streamAll(collection), filter);
        }
    }

    @Override
    public Stream<PersistenceEntity<Document>> streamAll(@NonNull PersistenceCollection collection) {
        return this.getRead().streamAll(collection).map(this.entityToDocumentMapper(collection));
    }

    @Override
    public Stream<PersistenceEntity<Document>> stream(@NonNull PersistenceCollection collection, int batchSize) {
        try {
            return this.getRead().stream(collection, batchSize).map(this.entityToDocumentMapper(collection));
        } catch (UnsupportedOperationException e) {
            LOGGER.fine("Backend doesn't support native stream(), falling back to streamAll() for collection: " + collection.getValue());
            return this.streamAll(collection);
        }
    }

    @Override
    public long count(@NonNull PersistenceCollection collection) {
        return this.getRead().count(collection);
    }

    @Override
    public boolean exists(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {
        return this.getRead().exists(collection, path);
    }

    @Override
    public boolean write(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull Document document) {
        this.updateIndex(collection, path, document);
        return this.getWrite().write(collection, path, this.update(document, collection).saveToString());
    }

    @Override
    public long write(@NonNull PersistenceCollection collection, @NonNull Map<PersistencePath, Document> entities) {

        if (entities.isEmpty()) {
            return 0;
        }

        Map<PersistencePath, String> rawMap = new LinkedHashMap<>();

        for (Map.Entry<PersistencePath, Document> entry : entities.entrySet()) {
            this.updateIndex(collection, entry.getKey(), entry.getValue());
            rawMap.put(entry.getKey(), this.update(entry.getValue(), collection).saveToString());
        }

        return this.getWrite().write(collection, rawMap);
    }

    @Override
    public boolean delete(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {
        return this.getWrite().delete(collection, path);
    }

    @Override
    public long delete(@NonNull PersistenceCollection collection, @NonNull Collection<PersistencePath> paths) {
        return this.getWrite().delete(collection, paths);
    }

    @Override
    public boolean deleteAll(@NonNull PersistenceCollection collection) {
        return this.getWrite().deleteAll(collection);
    }

    @Override
    public long deleteAll() {
        return this.getWrite().deleteAll();
    }

    @Override
    public long deleteByFilter(@NonNull PersistenceCollection collection, @NonNull DeleteFilter filter) {
        try {
            return this.getWrite().deleteByFilter(collection, filter);
        } catch (UnsupportedOperationException e) {
            LOGGER.fine("Backend doesn't support native delete(), using in-memory filtering for collection: " + collection.getValue());
            // Search for matching documents, then delete them
            Stream<PersistenceEntity<Document>> stream = this.streamAll(collection);
            if (filter.getWhere() != null) {
                stream = stream.filter(entity -> this.filterEvaluator.evaluateCondition(filter.getWhere(), entity.getValue()));
            }
            List<PersistencePath> pathsToDelete = stream.map(PersistenceEntity::getPath).collect(Collectors.toList());
            return this.getWrite().delete(collection, pathsToDelete);
        }
    }

    public Document createDocument(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {
        Document config = this.update(ConfigManager.create(Document.class), collection);
        config.setPath(path);
        return config;
    }

    protected Function<PersistenceEntity<String>, PersistenceEntity<Document>> entityToDocumentMapper(PersistenceCollection collection) {
        return entity -> {
            Document document = this.createDocument(collection, entity.getPath());
            document.load(entity.getValue());
            return entity.into(document);
        };
    }

    public Document update(Document document, PersistenceCollection collection) {

        // OkaeriConfig
        if (document.getDeclaration() == null) {
            document.updateDeclaration();
        }

        if (document.getConfigurer() == null) {
            document.setConfigurer(this.configurerProvider.get());
            document.getConfigurer().setRegistry(this.serdesRegistry);
        }

        // Document
        if (document.getPersistence() == null) {
            document.setPersistence(this);
        }

        if (document.getCollection() == null) {
            document.setCollection(collection);
        }

        return document;
    }

    /**
     * Convenience method to create and register a repository in one call.
     * Combines collection creation, registration, and proxy instantiation.
     *
     * @param repositoryClass the repository interface class
     * @param <T>             the repository type
     * @return a proxy instance of the repository
     */
    public <T extends DocumentRepository<?, ?>> T createRepository(@NonNull Class<T> repositoryClass) {
        PersistenceCollection collection = PersistenceCollection.of(repositoryClass);
        this.registerCollection(collection);
        return RepositoryDeclaration.of(repositoryClass)
            .newProxy(this, collection, repositoryClass.getClassLoader());
    }

    // ===== UPDATE OPERATIONS =====

    @Override
    public boolean updateOne(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull List<UpdateOperation> operations) {
        // Validate: detect field conflicts (same field with different operation types)
        this.validateNoFieldConflicts(operations);

        // Try to use native update if available (MongoDB, MariaDB, PostgreSQL, etc.)
        try {
            return this.getWrite().updateOne(collection, path, operations);
        } catch (UnsupportedOperationException e) {
            LOGGER.fine("Backend doesn't support native updateOne(), using in-memory update for path: " + path.getValue());
            return this.updateOneInMemory(collection, path, operations);
        }
    }

    protected boolean updateOneInMemory(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull List<UpdateOperation> operations) {
        Optional<Document> docOpt = this.read(collection, path);
        if (!docOpt.isPresent()) {
            return false; // Document not found
        }

        Document document = docOpt.get();
        boolean modified = this.updateEvaluator.applyUpdate(document, operations);

        if (modified) {
            this.write(collection, path, document);
        }

        return true; // Document found and operations applied
    }

    /**
     * Validates that no field has multiple operations.
     * Each field can only appear once in an update operation, even with the same operation type.
     * This ensures consistent behavior across all backends (MongoDB, MariaDB, PostgreSQL, etc.)
     *
     * @param operations List of update operations to validate
     * @throws IllegalArgumentException if conflicts are detected
     */
    private void validateNoFieldConflicts(@NonNull List<UpdateOperation> operations) {
        Map<String, UpdateOperationType> fieldToFirstOp = new HashMap<>();
        Map<String, Integer> fieldCounts = new HashMap<>();

        for (UpdateOperation op : operations) {
            String field = op.getField();
            fieldCounts.put(field, fieldCounts.getOrDefault(field, 0) + 1);
            fieldToFirstOp.putIfAbsent(field, op.getType());
        }

        List<String> conflicts = new ArrayList<>();
        for (Map.Entry<String, Integer> entry : fieldCounts.entrySet()) {
            if (entry.getValue() > 1) {
                String field = entry.getKey();
                UpdateOperationType opType = fieldToFirstOp.get(field);
                conflicts.add(String.format("Field '%s' appears %d times (operation: %s)",
                    field, entry.getValue(), opType));
            }
        }

        if (!conflicts.isEmpty()) {
            throw new IllegalArgumentException(
                "Cannot execute update: multiple operations on the same field(s) in a single update. " +
                    "Each field can only be modified once per update. " +
                    "Conflicts: " + String.join(", ", conflicts) + ". " +
                    "Split into multiple sequential update calls if you need to apply the same operation multiple times."
            );
        }
    }

    @Override
    public Optional<Document> updateOneAndGet(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull List<UpdateOperation> operations) {
        this.validateNoFieldConflicts(operations);

        try {
            Optional<String> result = this.getWrite().updateOneAndGet(collection, path, operations);
            return result.map(json -> {
                Document document = this.createDocument(collection, path);
                document.load(json);
                return document;
            });
        } catch (UnsupportedOperationException e) {
            LOGGER.fine("Backend doesn't support native updateOneAndGet(), using in-memory update for path: " + path.getValue());
            return this.updateOneAndGetInMemory(collection, path, operations);
        }
    }

    protected Optional<Document> updateOneAndGetInMemory(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull List<UpdateOperation> operations) {
        Optional<Document> docOpt = this.read(collection, path);
        if (!docOpt.isPresent()) {
            return Optional.empty();
        }

        Document document = docOpt.get();
        boolean modified = this.updateEvaluator.applyUpdate(document, operations);

        if (modified) {
            this.write(collection, path, document);
        }

        return Optional.of(document);
    }

    @Override
    public Optional<Document> getAndUpdateOne(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull List<UpdateOperation> operations) {
        this.validateNoFieldConflicts(operations);

        try {
            Optional<String> result = this.getWrite().getAndUpdateOne(collection, path, operations);
            return result.map(json -> {
                Document document = this.createDocument(collection, path);
                document.load(json);
                return document;
            });
        } catch (UnsupportedOperationException e) {
            LOGGER.fine("Backend doesn't support native getAndUpdateOne(), using in-memory update for path: " + path.getValue());
            return this.getAndUpdateOneInMemory(collection, path, operations);
        }
    }

    protected Optional<Document> getAndUpdateOneInMemory(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull List<UpdateOperation> operations) {
        Optional<Document> docOpt = this.read(collection, path);
        if (!docOpt.isPresent()) {
            return Optional.empty();
        }

        Document document = docOpt.get();
        Configurer oldConfigurer = this.configurerProvider.get();
        oldConfigurer.setRegistry(this.serdesRegistry);
        Document oldVersion = ConfigManager.deepCopy(document, oldConfigurer, Document.class);
        oldVersion.setPath(document.getPath());
        oldVersion.setCollection(document.getCollection());
        oldVersion.setPersistence(document.getPersistence());

        boolean modified = this.updateEvaluator.applyUpdate(document, operations);
        if (modified) {
            this.write(collection, path, document);
        }

        return Optional.of(oldVersion);
    }

    @Override
    public long update(@NonNull PersistenceCollection collection, @NonNull UpdateFilter filter) {
        this.validateNoFieldConflicts(filter.getOperations());

        try {
            return this.getWrite().update(collection, filter);
        } catch (UnsupportedOperationException e) {
            LOGGER.fine("Backend doesn't support native update(), using in-memory update");
            return this.updateInMemory(collection, filter);
        }
    }

    protected long updateInMemory(@NonNull PersistenceCollection collection, @NonNull UpdateFilter filter) {
        // Stream all documents, filter by WHERE clause, apply updates
        Stream<PersistenceEntity<Document>> stream = this.streamAll(collection);

        // Apply WHERE filter if present
        if (filter.getWhere() != null) {
            stream = stream.filter(entity -> this.filterEvaluator.evaluateCondition(filter.getWhere(), entity.getValue()));
        }

        // Collect paths to update (need to materialize to avoid concurrent modification)
        List<PersistencePath> pathsToUpdate = stream
            .map(PersistenceEntity::getPath)
            .collect(Collectors.toList());

        // Apply updates to each document
        long count = 0;
        for (PersistencePath path : pathsToUpdate) {
            if (this.updateOneInMemory(collection, path, filter.getOperations())) {
                count++;
            }
        }

        return count;
    }

    @Override
    public void close() throws IOException {

        this.getRead().close();

        if (this.getRead().equals(this.getWrite())) {
            return;
        }

        this.getWrite().close();
    }
}
