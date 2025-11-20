package eu.okaeri.persistence.flat;

import eu.okaeri.configs.ConfigManager;
import eu.okaeri.configs.configurer.InMemoryConfigurer;
import eu.okaeri.persistence.PersistenceCollection;
import eu.okaeri.persistence.PersistenceEntity;
import eu.okaeri.persistence.PersistencePath;
import eu.okaeri.persistence.document.ConfigurerProvider;
import eu.okaeri.persistence.document.index.InMemoryIndex;
import eu.okaeri.persistence.document.index.IndexProperty;
import eu.okaeri.persistence.raw.RawPersistence;
import eu.okaeri.persistence.raw.PersistenceIndexMode;
import eu.okaeri.persistence.raw.PersistencePropertyMode;
import lombok.*;

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

    private final Map<String, Map<String, InMemoryIndex>> indexMap = new ConcurrentHashMap<>();
    private @Getter final PersistencePath basePath;
    private @Getter final String fileSuffix;
    private @Getter final ConfigurerProvider indexProvider;
    private @Getter @Setter boolean saveIndex;

    public FlatPersistence(@NonNull File basePath, @NonNull String fileSuffix) {
        this(basePath, fileSuffix, InMemoryConfigurer::new, false);
    }

    public FlatPersistence(@NonNull File basePath, @NonNull String fileSuffix, @NonNull ConfigurerProvider indexProvider) {
        this(basePath, fileSuffix, indexProvider, true);
    }

    public FlatPersistence(@NonNull File basePath, @NonNull String fileSuffix, @NonNull ConfigurerProvider indexProvider, boolean saveIndex) {
        super(PersistencePath.of(basePath), PersistencePropertyMode.TOSTRING, PersistenceIndexMode.EMULATED);
        this.basePath = PersistencePath.of(basePath);
        this.fileSuffix = fileSuffix;
        this.indexProvider = indexProvider;
        this.saveIndex = saveIndex;
    }

    @Override
    public void flush() {
        if (!this.isSaveIndex()) return;
        this.indexMap.forEach((collection, indexes) -> indexes.values().forEach(InMemoryIndex::save));
    }

    @Override
    public boolean updateIndex(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull IndexProperty property, String identity) {

        // get index - return false if not registered (FlatFile uses in-memory filtering, indexes optional)
        Map<String, InMemoryIndex> indexes = this.indexMap.get(collection.getValue());
        if (indexes == null) return false;

        InMemoryIndex flatIndex = indexes.get(property.getValue());
        if (flatIndex == null) return false;

        // get current value by key and remove from mapping
        String currentValue = flatIndex.getKeyToValue().remove(path.getValue());

        // remove from old set value_to_keys
        if (currentValue != null) {
            flatIndex.getValueToKeys().get(currentValue).remove(path.getValue());
        }

        // add to new value_to_keys
        Set<String> keys = flatIndex.getValueToKeys().computeIfAbsent(identity, s -> new HashSet<>());
        boolean changed = keys.add(path.getValue());

        // update key to value
        changed = (flatIndex.getKeyToValue().put(path.getValue(), identity) != null) || changed;

        // save index
        if (this.isSaveIndex() && this.isFlushOnWrite()) {
            flatIndex.save();
        }

        // return changes
        return changed;
    }

    @Override
    public boolean dropIndex(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull IndexProperty property) {

        // get index - return false if not registered (FlatFile uses in-memory filtering, indexes optional)
        Map<String, InMemoryIndex> indexes = this.indexMap.get(collection.getValue());
        if (indexes == null) return false;

        InMemoryIndex flatIndex = indexes.get(property.getValue());
        if (flatIndex == null) return false;

        // get current value by key and remove from mapping
        String currentValue = flatIndex.getKeyToValue().remove(path.getValue());

        // delete from value to set
        boolean changed = (currentValue != null) && flatIndex.getValueToKeys().get(currentValue).remove(path.getValue());

        // save index
        if (this.isSaveIndex() && this.isFlushOnWrite()) {
            flatIndex.save();
        }

        // return changes
        return changed;
    }

    @Override
    public boolean dropIndex(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {
        return this.getKnownIndexes().getOrDefault(collection.getValue(), Collections.emptySet()).stream()
            .map(index -> this.dropIndex(collection, path, index))
            .anyMatch(Predicate.isEqual(true));
    }

    @Override
    @SneakyThrows
    public boolean dropIndex(@NonNull PersistenceCollection collection, @NonNull IndexProperty property) {

        // remove from list and get index
        InMemoryIndex flatIndex = this.indexMap.get(collection.getValue()).remove(property.getValue());

        // delete index file
        return (flatIndex != null) && Files.deleteIfExists(flatIndex.getBindFile());
    }

    @Override
    @SneakyThrows
    public Set<PersistencePath> findMissingIndexes(@NonNull PersistenceCollection collection, @NonNull Set<IndexProperty> indexProperties) {

        Map<String, InMemoryIndex> collectionIndexes = this.indexMap.get(collection.getValue());
        if (collectionIndexes.isEmpty()) {
            return Collections.emptySet();
        }

        Path collectionFile = this.getBasePath().sub(collection).toPath();
        return this.scanCollection(collectionFile)
            .map(this.fileToKeyMapper)
            .map(key -> collectionIndexes.values().stream()
                .allMatch(flatIndex -> flatIndex.getKeyToValue().containsKey(key))
                ? null : PersistencePath.of(key))
            .filter(Objects::nonNull)
            .collect(Collectors.toSet());
    }

    @Override
    public Stream<PersistenceEntity<String>> readByProperty(@NonNull PersistenceCollection collection, @NonNull PersistencePath property, @NonNull Object propertyValue) {

        if (!this.canUseToString(propertyValue)) {
            return this.streamAll(collection);
        }

        InMemoryIndex flatIndex = this.indexMap.get(collection.getValue()).get(property.getValue());
        if (flatIndex == null) return this.streamAll(collection);

        Set<String> keys = flatIndex.getValueToKeys().get(String.valueOf(propertyValue));
        if ((keys == null) || keys.isEmpty()) {
            return Stream.of();
        }

        return new ArrayList<>(keys).stream()
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

    @Override
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public void registerCollection(@NonNull PersistenceCollection collection) {

        PersistencePath collectionPath = this.getBasePath().sub(collection);
        File collectionFile = collectionPath.toFile();
        collectionFile.mkdirs();

        Map<String, InMemoryIndex> indexes = this.indexMap.computeIfAbsent(collection.getValue(), col -> new ConcurrentHashMap<>());

        for (IndexProperty index : collection.getIndexes()) {

            InMemoryIndex flatIndex = ConfigManager.create(InMemoryIndex.class);
            flatIndex.setConfigurer(this.indexProvider.get());

            Path path = collectionPath.append("_").append(index.toSafeFileName()).append(".index").toPath();
            flatIndex.withBindFile(path);

            if (this.isSaveIndex() && Files.exists(path)) {
                flatIndex.load(path);
            }

            indexes.put(index.getValue(), flatIndex);
        }

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
        Set<IndexProperty> collectionIndexes = this.getKnownIndexes().get(collection.getValue());

        if (collectionIndexes != null) {
            collectionIndexes.forEach(index -> this.dropIndex(collection, path));
        }

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
        this.checkCollectionRegistered(collection);
        Set<IndexProperty> collectionIndexes = this.getKnownIndexes().get(collection.getValue());

        if (collectionIndexes != null) {
            collectionIndexes.forEach(index -> this.dropIndex(collection, index));
        }

        return collectionFile.exists() && (this.delete(collectionFile) > 0);
    }

    @Override
    public long deleteAll() {

        File[] files = this.getBasePath().toFile().listFiles();
        if (files == null) {
            return 0;
        }

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

    @Data
    @AllArgsConstructor
    private class Pair<L, R> {
        private L left;
        private R right;
    }
}
