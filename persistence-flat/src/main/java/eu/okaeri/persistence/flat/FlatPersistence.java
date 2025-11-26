package eu.okaeri.persistence.flat;

import eu.okaeri.configs.serdes.OkaeriSerdesPack;
import eu.okaeri.persistence.*;
import eu.okaeri.persistence.document.ConfigurerProvider;
import eu.okaeri.persistence.document.Document;
import eu.okaeri.persistence.document.DocumentSerializer;
import eu.okaeri.persistence.document.index.IndexExtractor;
import eu.okaeri.persistence.document.index.IndexProperty;
import eu.okaeri.persistence.document.index.PropertyIndex;
import eu.okaeri.persistence.filter.DeleteFilter;
import eu.okaeri.persistence.filter.FindFilter;
import eu.okaeri.persistence.filter.InMemoryFilterEvaluator;
import eu.okaeri.persistence.filter.IndexQueryOptimizer;
import eu.okaeri.persistence.filter.condition.Condition;
import lombok.Cleanup;
import lombok.Getter;
import lombok.NonNull;
import lombok.SneakyThrows;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * File-based persistence backend with in-memory indexing.
 * Each document is stored as a separate file in the filesystem.
 */
public class FlatPersistence implements Persistence, FilterablePersistence, StreamablePersistence {

    private static final boolean DEBUG = Boolean.parseBoolean(System.getProperty("okaeri.platform.debug", "false"));
    private static final Logger LOGGER = Logger.getLogger(FlatPersistence.class.getSimpleName());

    // Core configuration
    private final @Getter PersistencePath basePath;
    private final @Getter String fileSuffix;
    private final @Getter DocumentSerializer serializer;
    private final IndexExtractor indexExtractor;
    private final InMemoryFilterEvaluator filterEvaluator;

    // Collection and index tracking
    private final Map<String, PersistenceCollection> knownCollections = new ConcurrentHashMap<>();
    private final Map<String, Map<String, PropertyIndex>> indexMap = new ConcurrentHashMap<>();
    private final IndexQueryOptimizer queryOptimizer = new IndexQueryOptimizer();

    // File path helpers
    private final Function<Path, String> fileToKeyMapper = path -> {
        String name = path.getFileName().toString();
        return name.substring(0, name.length() - FlatPersistence.this.fileSuffix.length());
    };

    public FlatPersistence(@NonNull File storageDir, @NonNull ConfigurerProvider configurerProvider, @NonNull OkaeriSerdesPack... serdesPacks) {
        this(PersistencePath.of(storageDir), resolveFileSuffix(configurerProvider), configurerProvider, serdesPacks);
    }

    private FlatPersistence(
        @NonNull PersistencePath basePath,
        @NonNull String fileSuffix,
        @NonNull ConfigurerProvider configurerProvider,
        @NonNull OkaeriSerdesPack[] serdesPacks
    ) {
        this.basePath = basePath;
        this.fileSuffix = fileSuffix;
        this.serializer = new DocumentSerializer(configurerProvider, serdesPacks);
        this.indexExtractor = new IndexExtractor(this.serializer.getSimplifier());
        this.filterEvaluator = new InMemoryFilterEvaluator(this.serializer.getSimplifier());
    }

    private static String resolveFileSuffix(@NonNull ConfigurerProvider configurerProvider) {
        List<String> extensions = configurerProvider.get().getExtensions();
        if (extensions.isEmpty()) {
            throw new IllegalArgumentException("Configurer has no extensions - use builder with explicit suffix()");
        }
        return "." + extensions.get(0);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private PersistencePath storageDir;
        private String fileSuffix;
        private ConfigurerProvider configurerProvider;
        private OkaeriSerdesPack[] serdesPacks = new OkaeriSerdesPack[0];

        public Builder storageDir(@NonNull File dir) {
            this.storageDir = PersistencePath.of(dir);
            return this;
        }

        public Builder storageDir(@NonNull Path dir) {
            this.storageDir = PersistencePath.of(dir.toFile());
            return this;
        }

        public Builder suffix(@NonNull String suffix) {
            this.fileSuffix = suffix;
            return this;
        }

        public Builder extension(@NonNull String extension) {
            this.fileSuffix = "." + extension;
            return this;
        }

        public Builder configurer(@NonNull ConfigurerProvider configurerProvider) {
            this.configurerProvider = configurerProvider;
            return this;
        }

        public Builder serdes(@NonNull OkaeriSerdesPack... packs) {
            this.serdesPacks = packs;
            return this;
        }

        public FlatPersistence build() {
            if (this.storageDir == null) {
                throw new IllegalStateException("storageDir is required");
            }
            if (this.configurerProvider == null) {
                throw new IllegalStateException("configurer is required");
            }
            String suffix = (this.fileSuffix != null) ? this.fileSuffix : resolveFileSuffix(this.configurerProvider);
            return new FlatPersistence(this.storageDir, suffix, this.configurerProvider, this.serdesPacks);
        }
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
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public void registerCollection(@NonNull PersistenceCollection collection) {
        // Create collection directory
        PersistencePath collectionPath = this.basePath.sub(collection);
        File collectionFile = collectionPath.toFile();
        collectionFile.mkdirs();

        // Create PropertyIndex for each indexed property
        Map<String, PropertyIndex> indexes = this.indexMap.computeIfAbsent(
            collection.getValue(), col -> new ConcurrentHashMap<>());
        collection.getIndexes().forEach(index ->
            indexes.put(index.getValue(), new PropertyIndex()));

        // Track collection
        this.knownCollections.put(collection.getValue(), collection);
    }

    // ==================== READ OPERATIONS ====================

    @Override
    public boolean exists(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {
        this.checkCollectionRegistered(collection);
        return this.toFile(collection, path).exists();
    }

    @Override
    @SneakyThrows
    public long count(@NonNull PersistenceCollection collection) {
        this.checkCollectionRegistered(collection);
        Path collectionPath = this.basePath.sub(collection).toPath();
        try (Stream<Path> paths = this.scanCollection(collectionPath)) {
            return paths.count();
        }
    }

    @Override
    public Optional<Document> read(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {
        this.checkCollectionRegistered(collection);
        File file = this.toFile(collection, path);
        if (!file.exists()) {
            return Optional.empty();
        }
        String json = this.fileToString(file);
        if (json == null) {
            return Optional.empty();
        }
        return Optional.of(this.serializer.deserialize(collection, path, json));
    }

    @Override
    public Map<PersistencePath, Document> read(@NonNull PersistenceCollection collection, @NonNull Collection<PersistencePath> paths) {
        return paths.stream()
            .distinct()
            .map(path -> this.read(collection, path)
                .map(doc -> new PersistenceEntity<>(path, doc))
                .orElse(null))
            .filter(Objects::nonNull)
            .collect(Collectors.toMap(PersistenceEntity::getPath, PersistenceEntity::getValue));
    }

    @Override
    public Map<PersistencePath, Document> readAll(@NonNull PersistenceCollection collection) {
        return this.streamAll(collection)
            .collect(Collectors.toMap(PersistenceEntity::getPath, PersistenceEntity::getValue));
    }

    // ==================== STREAMING ====================

    @Override
    @SneakyThrows
    public Stream<PersistenceEntity<Document>> stream(@NonNull PersistenceCollection collection, int batchSize) {
        this.checkCollectionRegistered(collection);
        Path collectionPath = this.basePath.sub(collection).toPath();

        // Returns lazy stream - caller MUST close it
        return this.scanCollection(collectionPath)
            .map(filePath -> {
                String key = this.fileToKeyMapper.apply(filePath);
                PersistencePath path = PersistencePath.of(key);
                String json = this.fileToString(filePath.toFile());
                if (json == null) return null;
                Document doc = this.serializer.deserialize(collection, path, json);
                return new PersistenceEntity<>(path, doc);
            })
            .filter(Objects::nonNull);
    }

    @Override
    public Stream<PersistenceEntity<Document>> streamAll(@NonNull PersistenceCollection collection) {
        try (Stream<PersistenceEntity<Document>> stream = this.stream(collection, Integer.MAX_VALUE)) {
            return stream.collect(Collectors.toList()).stream();
        }
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
                candidates = optimized.getDocIds().stream()
                    .map(docId -> {
                        PersistencePath path = PersistencePath.of(docId);
                        return this.read(collection, path)
                            .map(doc -> new PersistenceEntity<>(path, doc))
                            .orElseGet(() -> {
                                // Stale index entry - clean up
                                this.dropIndex(collection, path);
                                return null;
                            });
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

        // Find matching documents
        List<PersistencePath> toDelete = this.streamAll(collection)
            .filter(entity -> this.filterEvaluator.evaluateCondition(filter.getWhere(), entity.getValue()))
            .map(PersistenceEntity::getPath)
            .collect(Collectors.toList());

        // Delete them
        return this.delete(collection, toDelete);
    }

    // ==================== WRITE OPERATIONS ====================

    @Override
    @SneakyThrows
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public boolean write(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull Document document) {
        this.checkCollectionRegistered(collection);

        // Setup document
        this.serializer.setupDocument(document, collection, path);

        // Update indexes
        this.updateIndexes(collection, path, document);

        // Write to file
        File file = this.toFile(collection, path);
        File parentFile = file.getParentFile();
        if (parentFile != null) parentFile.mkdirs();

        String json = this.serializer.serialize(document);
        this.writeToFile(file, json);
        return true;
    }

    @Override
    public long write(@NonNull PersistenceCollection collection, @NonNull Map<PersistencePath, Document> documents) {
        long count = 0;
        for (Map.Entry<PersistencePath, Document> entry : documents.entrySet()) {
            if (this.write(collection, entry.getKey(), entry.getValue())) {
                count++;
            }
        }
        return count;
    }

    // ==================== DELETE OPERATIONS ====================

    @Override
    public boolean delete(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {
        this.checkCollectionRegistered(collection);
        this.dropIndex(collection, path);
        return this.toFile(collection, path).delete();
    }

    @Override
    public long delete(@NonNull PersistenceCollection collection, @NonNull Collection<PersistencePath> paths) {
        return paths.stream()
            .map(path -> this.delete(collection, path))
            .filter(Predicate.isEqual(true))
            .count();
    }

    @Override
    @SneakyThrows
    public boolean deleteAll(@NonNull PersistenceCollection collection) {
        this.checkCollectionRegistered(collection);
        File collectionFile = this.basePath.sub(collection).toFile();

        // Clear indexes
        Map<String, PropertyIndex> indexes = this.indexMap.get(collection.getValue());
        if (indexes != null) {
            indexes.values().forEach(PropertyIndex::clear);
        }

        return collectionFile.exists() && (this.deleteRecursive(collectionFile) > 0);
    }

    @Override
    public long deleteAll() {
        File[] files = this.basePath.toFile().listFiles();
        if (files == null) {
            return 0;
        }

        // Clear all indexes
        this.indexMap.values().forEach(indexes -> indexes.values().forEach(PropertyIndex::clear));

        return Arrays.stream(files)
            .filter(file -> this.knownCollections.containsKey(file.getName()))
            .map(this::deleteRecursive)
            .filter(deleted -> deleted > 0)
            .count();
    }

    @Override
    public void close() throws IOException {
        // Nothing to close
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
            if (value != null) {
                index.put(path.getValue(), value);
            } else {
                index.remove(path.getValue());
            }
        }
    }

    private void dropIndex(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {
        Map<String, PropertyIndex> indexes = this.indexMap.get(collection.getValue());
        if (indexes == null) return;

        for (PropertyIndex index : indexes.values()) {
            index.remove(path.getValue());
        }
    }

    // ==================== HELPERS ====================

    private void checkCollectionRegistered(@NonNull PersistenceCollection collection) {
        if (!this.knownCollections.containsKey(collection.getValue())) {
            throw new IllegalArgumentException("Collection not registered: " + collection.getValue());
        }
    }

    private File toFile(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {
        return this.basePath.sub(collection).sub(path.append(this.fileSuffix)).toFile();
    }

    @SneakyThrows
    private Stream<Path> scanCollection(@NonNull Path collectionPath) {
        if (!Files.exists(collectionPath)) {
            return Stream.empty();
        }
        return Files.list(collectionPath)
            .filter(path -> {
                boolean endsWithSuffix = path.toString().endsWith(this.fileSuffix);
                if (DEBUG && !endsWithSuffix) {
                    LOGGER.log(Level.WARNING, "Unexpected file in " + collectionPath + ": " + path);
                }
                return endsWithSuffix;
            });
    }

    private String fileToString(@NonNull File file) {
        try {
            return new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8);
        } catch (IOException e) {
            if (DEBUG) {
                LOGGER.log(Level.WARNING, "Failed to read file: " + file, e);
            }
            return null;
        }
    }

    @SneakyThrows
    private void writeToFile(@NonNull File file, @NonNull String content) {
        @Cleanup BufferedWriter writer = new BufferedWriter(new FileWriter(file));
        writer.write(content);
    }

    @SneakyThrows
    private long deleteRecursive(@NonNull File file) {
        @Cleanup Stream<Path> walk = Files.walk(file.toPath());
        return walk.sorted(Comparator.reverseOrder())
            .map(Path::toFile)
            .map(File::delete)
            .filter(Predicate.isEqual(true))
            .count();
    }
}
