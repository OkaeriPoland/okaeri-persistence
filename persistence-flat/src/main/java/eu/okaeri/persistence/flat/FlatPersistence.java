package eu.okaeri.persistence.flat;

import eu.okaeri.persistence.PersistenceCollection;
import eu.okaeri.persistence.PersistenceEntity;
import eu.okaeri.persistence.PersistencePath;
import eu.okaeri.persistence.document.ConfigurerProvider;
import eu.okaeri.persistence.document.index.IndexProperty;
import eu.okaeri.persistence.document.index.PropertyIndex;
import eu.okaeri.persistence.filter.FindFilter;
import eu.okaeri.persistence.filter.IndexQueryOptimizer;
import eu.okaeri.persistence.filter.PartialIndexResultException;
import eu.okaeri.persistence.filter.condition.Condition;
import eu.okaeri.persistence.raw.PersistenceIndexMode;
import eu.okaeri.persistence.raw.PersistencePropertyMode;
import eu.okaeri.persistence.raw.RawPersistence;
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

public class FlatPersistence extends RawPersistence {

    private static final boolean DEBUG = Boolean.parseBoolean(System.getProperty("okaeri.platform.debug", "false"));
    private static final Logger LOGGER = Logger.getLogger(FlatPersistence.class.getSimpleName());

    private final Function<Path, String> fileToKeyMapper = path -> {
        String name = path.getFileName().toString();
        return name.substring(0, name.length() - FlatPersistence.this.getFileSuffix().length());
    };

    private final Map<String, Map<String, PropertyIndex>> indexMap = new ConcurrentHashMap<>();
    private final IndexQueryOptimizer queryOptimizer = new IndexQueryOptimizer();
    private @Getter final PersistencePath basePath;
    private @Getter final String fileSuffix;

    public FlatPersistence(@NonNull File basePath, @NonNull String fileSuffix) {
        super(PersistencePath.of(basePath), PersistencePropertyMode.TOSTRING, PersistenceIndexMode.EMULATED);
        this.basePath = PersistencePath.of(basePath);
        this.fileSuffix = fileSuffix;
    }

    /**
     * @deprecated The indexProvider parameter is no longer used. Indexes are now in-memory only.
     */
    @Deprecated
    public FlatPersistence(@NonNull File basePath, @NonNull String fileSuffix, @NonNull ConfigurerProvider indexProvider) {
        this(basePath, fileSuffix);
    }

    /**
     * @deprecated The indexProvider and saveIndex parameters are no longer used. Indexes are now in-memory only.
     */
    @Deprecated
    public FlatPersistence(@NonNull File basePath, @NonNull String fileSuffix, @NonNull ConfigurerProvider indexProvider, boolean saveIndex) {
        this(basePath, fileSuffix);
    }

    /**
     * Get the indexes for a collection.
     * Exposed for testing and advanced use cases.
     */
    public Map<String, PropertyIndex> getIndexes(@NonNull PersistenceCollection collection) {
        return this.indexMap.get(collection.getValue());
    }

    @Override
    public void flush() {
        // PropertyIndex doesn't persist to disk - indexes are rebuilt on startup
    }

    @Override
    public boolean updateIndex(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull IndexProperty property, String identity) {
        Map<String, PropertyIndex> indexes = this.indexMap.get(collection.getValue());
        if (indexes == null) return false;

        PropertyIndex index = indexes.get(property.getValue());
        return index != null && index.put(path.getValue(), identity);

    }

    /**
     * Update index with typed value (enables range queries).
     */
    @Override
    public boolean updateIndexTyped(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull IndexProperty property, Object value) {
        Map<String, PropertyIndex> indexes = this.indexMap.get(collection.getValue());
        if (indexes == null) return false;

        PropertyIndex index = indexes.get(property.getValue());
        return index != null && index.put(path.getValue(), value);

    }

    @Override
    public boolean dropIndex(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull IndexProperty property) {
        Map<String, PropertyIndex> indexes = this.indexMap.get(collection.getValue());
        if (indexes == null) return false;

        PropertyIndex index = indexes.get(property.getValue());
        return index != null && index.remove(path.getValue());

    }

    @Override
    public boolean dropIndex(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {
        return this.getKnownIndexes().getOrDefault(collection.getValue(), Collections.emptySet()).stream()
            .map(index -> this.dropIndex(collection, path, index))
            .anyMatch(Predicate.isEqual(true));
    }

    @Override
    public boolean dropIndex(@NonNull PersistenceCollection collection, @NonNull IndexProperty property) {
        Map<String, PropertyIndex> indexes = this.indexMap.get(collection.getValue());
        if (indexes == null) return false;

        PropertyIndex removed = indexes.remove(property.getValue());
        return removed != null;
    }

    @Override
    public Set<PersistencePath> findMissingIndexes(@NonNull PersistenceCollection collection, @NonNull Set<IndexProperty> indexProperties) {
        Map<String, PropertyIndex> collectionIndexes = this.indexMap.get(collection.getValue());
        if ((collectionIndexes == null) || collectionIndexes.isEmpty()) {
            return Collections.emptySet();
        }

        Path collectionFile = this.getBasePath().sub(collection).toPath();
        return this.scanCollection(collectionFile)
            .map(this.fileToKeyMapper)
            .map(key -> collectionIndexes.values().stream()
                .allMatch(index -> index.containsDoc(key))
                ? null : PersistencePath.of(key))
            .filter(Objects::nonNull)
            .collect(Collectors.toSet());
    }

    @Override
    public Stream<PersistenceEntity<String>> readByProperty(@NonNull PersistenceCollection collection, @NonNull PersistencePath property, @NonNull Object propertyValue) {
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

        return keys.stream()
            .map(key -> {
                PersistencePath path = PersistencePath.of(key);
                return this.read(collection, path)
                    .map(data -> new PersistenceEntity<>(path, data))
                    .orElseGet(() -> {
                        this.dropIndex(collection, path);
                        return null;
                    });
            })
            .filter(Objects::nonNull);
    }

    /**
     * Use indexes to optimize WHERE-only queries.
     * Throws UnsupportedOperationException for queries with ORDER BY/SKIP/LIMIT
     * (those fall back to DocumentPersistence full filtering which can handle them).
     */
    @Override
    public Stream<PersistenceEntity<String>> readByFilter(@NonNull PersistenceCollection collection, @NonNull FindFilter filter) {
        // Can't handle ORDER BY/SKIP/LIMIT at raw string level - need Document parsing
        if (filter.hasOrderBy() || filter.hasSkip() || filter.hasLimit()) {
            throw new UnsupportedOperationException("FlatPersistence can't handle ORDER BY/SKIP/LIMIT");
        }

        Condition where = filter.getWhere();
        if (where == null) {
            // No WHERE clause, no ORDER BY/SKIP/LIMIT - return all
            return this.streamAll(collection);
        }

        Map<String, PropertyIndex> indexes = this.indexMap.get(collection.getValue());
        IndexQueryOptimizer.IndexResult optimized = this.queryOptimizer.optimize(where, indexes);

        if (!optimized.requiresFullScan()) {
            // Index provides at least partial coverage - load only indexed candidates
            Stream<PersistenceEntity<String>> candidates = optimized.getDocIds().stream()
                .map(docId -> {
                    PersistencePath path = PersistencePath.of(docId);
                    return this.read(collection, path)
                        .map(data -> new PersistenceEntity<>(path, data))
                        .orElseGet(() -> {
                            this.dropIndex(collection, path);
                            return null;
                        });
                })
                .filter(Objects::nonNull);

            if (!optimized.hasRemainingCondition()) {
                // Index fully covers WHERE - return as-is
                return candidates;
            }

            // Partial coverage - signal DocumentPersistence to apply remaining filter
            throw new PartialIndexResultException(candidates);
        }

        // Index can't help at all - full scan needed
        throw new UnsupportedOperationException("Query requires full scan");
    }

    @Override
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public void registerCollection(@NonNull PersistenceCollection collection) {
        PersistencePath collectionPath = this.getBasePath().sub(collection);
        File collectionFile = collectionPath.toFile();
        collectionFile.mkdirs();

        // Create PropertyIndex for each indexed property
        Map<String, PropertyIndex> indexes = this.indexMap.computeIfAbsent(collection.getValue(), col -> new ConcurrentHashMap<>());
        collection.getIndexes().forEach(index -> indexes.put(index.getValue(), new PropertyIndex()));

        super.registerCollection(collection);
    }

    @Override
    public Optional<String> read(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {
        this.checkCollectionRegistered(collection);
        File file = this.toFullPath(collection, path).toFile();
        return file.exists() ? Optional.ofNullable(this.fileToString(file)) : Optional.empty();
    }

    @Override
    public Map<PersistencePath, String> read(@NonNull PersistenceCollection collection, @NonNull Collection<PersistencePath> paths) {
        return paths.stream()
            .distinct()
            .map(path -> new PersistenceEntity<>(path, this.read(collection, path).orElse(null)))
            .filter(entity -> entity.getValue() != null)
            .collect(Collectors.toMap(PersistenceEntity::getPath, PersistenceEntity::getValue));
    }

    @Override
    public Map<PersistencePath, String> readOrEmpty(@NonNull PersistenceCollection collection, @NonNull Collection<PersistencePath> paths) {
        return paths.stream()
            .distinct()
            .collect(Collectors.toMap(path -> path, path -> this.readOrEmpty(collection, path)));
    }

    @Override
    public Map<PersistencePath, String> readAll(@NonNull PersistenceCollection collection) {
        return this.streamAll(collection).collect(Collectors.toMap(PersistenceEntity::getPath, PersistenceEntity::getValue));
    }

    @Override
    @SneakyThrows
    public Stream<PersistenceEntity<String>> streamAll(@NonNull PersistenceCollection collection) {
        this.checkCollectionRegistered(collection);
        Path collectionFile = this.getBasePath().sub(collection).toPath();

        return this.scanCollection(collectionFile)
            .map(path -> {
                PersistencePath persistencePath = PersistencePath.of(this.fileToKeyMapper.apply(path));
                return new PersistenceEntity<>(persistencePath, this.fileToString(path.toFile()));
            });
    }

    @Override
    @SneakyThrows
    public long count(@NonNull PersistenceCollection collection) {
        this.checkCollectionRegistered(collection);
        Path collectionFile = this.getBasePath().sub(collection).toPath();

        return this.scanCollection(collectionFile).count();
    }

    @Override
    public boolean exists(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {
        this.checkCollectionRegistered(collection);
        return this.toFullPath(collection, path).toFile().exists();
    }

    @Override
    @SneakyThrows
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public boolean write(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull String raw) {
        this.checkCollectionRegistered(collection);
        File file = this.toFullPath(collection, path).toFile();
        File parentFile = file.getParentFile();
        if (parentFile != null) parentFile.mkdirs();
        this.writeToFile(file, raw);
        return true;
    }

    @Override
    public PersistencePath convertPath(@NonNull PersistencePath path) {
        return path.append(this.fileSuffix);
    }

    @Override
    public boolean delete(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {
        this.checkCollectionRegistered(collection);
        this.dropIndex(collection, path);
        return this.toFullPath(collection, path).toFile().delete();
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
        File collectionFile = this.getBasePath().sub(collection).toFile();

        // Clear indexes
        Map<String, PropertyIndex> indexes = this.indexMap.get(collection.getValue());
        if (indexes != null) {
            indexes.values().forEach(PropertyIndex::clear);
        }

        return collectionFile.exists() && (this.delete(collectionFile) > 0);
    }

    @Override
    public long deleteAll() {
        File[] files = this.getBasePath().toFile().listFiles();
        if (files == null) {
            return 0;
        }

        // Clear all indexes
        this.indexMap.values().forEach(indexes -> indexes.values().forEach(PropertyIndex::clear));

        return Arrays.stream(files)
            .filter(file -> this.getKnownCollections().keySet().contains(file.getName()))
            .map(this::delete)
            .filter(deleted -> deleted > 0)
            .count();
    }

    @Override
    public void close() throws IOException {
    }

    @SneakyThrows
    private long delete(@NonNull File file) {
        @Cleanup Stream<Path> walk = Files.walk(file.toPath());
        return walk.sorted(Comparator.reverseOrder())
            .map(Path::toFile)
            .map(File::delete)
            .filter(Predicate.isEqual(true))
            .count();
    }

    @SneakyThrows
    private Stream<Path> scanCollection(@NonNull Path collectionFile) {
        // Return empty stream if collection directory doesn't exist yet
        if (!Files.exists(collectionFile)) {
            return Stream.empty();
        }

        return Files.list(collectionFile)
            .filter(path -> {
                boolean endsWithSuffix = path.toString().endsWith(this.getFileSuffix());
                if (DEBUG && !endsWithSuffix) {
                    LOGGER.log(Level.WARNING, "Possibly bogus file found in " + collectionFile + ": " + path + " (not ending with '" + this.getFileSuffix() + "')");
                }
                return endsWithSuffix;
            });
    }

    private String fileToString(File file) {
        try {
            return new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8);
        } catch (IOException exception) {
            if (DEBUG) {
                LOGGER.log(Level.WARNING, "Returning empty data for " + file, exception);
            }
            return null;
        }
    }

    @SneakyThrows
    private void writeToFile(File file, String text) {
        @Cleanup BufferedWriter writer = new BufferedWriter(new FileWriter(file));
        writer.write(text);
    }
}
