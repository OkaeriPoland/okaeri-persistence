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
import lombok.*;

public class FlatPersistence extends RawPersistence {

  private static final boolean DEBUG =
      Boolean.parseBoolean(System.getProperty("okaeri.platform.debug", "false"));
  private static final Logger LOGGER = Logger.getLogger(FlatPersistence.class.getSimpleName());
  private final Map<String, Map<String, InMemoryIndex>> indexMap = new ConcurrentHashMap<>();
  private @Getter final PersistencePath basePath;
  private @Getter final String fileSuffix;
  private final Function<Path, String> fileToKeyMapper;
  private @Getter final ConfigurerProvider indexProvider;
  private @Getter @Setter boolean saveIndex;

  public FlatPersistence(@NonNull final File basePath, @NonNull final String fileSuffix) {
    this(basePath, fileSuffix, InMemoryConfigurer::new, false);
  }

  public FlatPersistence(
      @NonNull final File basePath,
      @NonNull final String fileSuffix,
      @NonNull final ConfigurerProvider indexProvider) {
    this(basePath, fileSuffix, indexProvider, true);
  }

  public FlatPersistence(
      @NonNull final File basePath,
      @NonNull final String fileSuffix,
      @NonNull final ConfigurerProvider indexProvider,
      final boolean saveIndex) {
    super(PersistencePath.of(basePath), true, true, false, true, true);
    this.basePath = PersistencePath.of(basePath);
    this.fileSuffix = fileSuffix;
    this.indexProvider = indexProvider;
    this.saveIndex = saveIndex;
    this.fileToKeyMapper =
        path -> {
          final String name = path.getFileName().toString();
          return name.substring(0, name.length() - this.fileSuffix.length());
        };
  }

  @Override
  public void flush() {
    if (!this.saveIndex) return;
    this.indexMap.forEach((collection, indexes) -> indexes.values().forEach(InMemoryIndex::save));
  }

  @Override
  public boolean updateIndex(
      @NonNull final PersistenceCollection collection,
      @NonNull final PersistencePath path,
      @NonNull final IndexProperty property,
      final String identity) {

    // get index
    final InMemoryIndex flatIndex =
        this.indexMap.get(collection.getValue()).get(property.getValue());
    if (flatIndex == null)
      throw new IllegalArgumentException("non-indexed property used: " + property);

    // get current value by key and remove from mapping
    final String currentValue = flatIndex.getKeyToValue().remove(path.getValue());

    // remove from old set value_to_keys
    if (currentValue != null) {
      flatIndex.getValueToKeys().get(currentValue).remove(path.getValue());
    }

    // add to new value_to_keys
    final Set<String> keys =
        flatIndex.getValueToKeys().computeIfAbsent(identity, s -> new HashSet<>());
    boolean changed = keys.add(path.getValue());

    // update key to value
    changed = (flatIndex.getKeyToValue().put(path.getValue(), identity) != null) || changed;

    // save index
    if (this.saveIndex && this.isAutoFlush()) {
      flatIndex.save();
    }

    // return changes
    return changed;
  }

  @Override
  public boolean dropIndex(
      @NonNull final PersistenceCollection collection,
      @NonNull final PersistencePath path,
      @NonNull final IndexProperty property) {

    // get index
    final InMemoryIndex flatIndex =
        this.indexMap.get(collection.getValue()).get(property.getValue());
    if (flatIndex == null)
      throw new IllegalArgumentException("non-indexed property used: " + property);

    // get current value by key and remove from mapping
    final String currentValue = flatIndex.getKeyToValue().remove(path.getValue());

    // delete from value to set
    final boolean changed =
        (currentValue != null)
            && flatIndex.getValueToKeys().get(currentValue).remove(path.getValue());

    // save index
    if (this.saveIndex && this.isAutoFlush()) {
      flatIndex.save();
    }

    // return changes
    return changed;
  }

  @Override
  public boolean dropIndex(
      @NonNull final PersistenceCollection collection, @NonNull final PersistencePath path) {
    return this.getKnownIndexes()
        .getOrDefault(collection.getValue(), Collections.emptySet())
        .stream()
        .map(index -> this.dropIndex(collection, path, index))
        .anyMatch(Predicate.isEqual(true));
  }

  @Override
  @SneakyThrows
  public boolean dropIndex(
      @NonNull final PersistenceCollection collection, @NonNull final IndexProperty property) {

    // remove from list and get index
    final InMemoryIndex flatIndex =
        this.indexMap.get(collection.getValue()).remove(property.getValue());

    // delete index file
    return (flatIndex != null) && Files.deleteIfExists(flatIndex.getBindFile());
  }

  @Override
  @SneakyThrows
  public Set<PersistencePath> findMissingIndexes(
      @NonNull final PersistenceCollection collection,
      @NonNull final Set<IndexProperty> indexProperties) {

    final Map<String, InMemoryIndex> collectionIndexes = this.indexMap.get(collection.getValue());
    if (collectionIndexes.isEmpty()) {
      return Collections.emptySet();
    }

    final Path collectionFile = this.basePath.sub(collection).toPath();
    return this.scanCollection(collectionFile)
        .map(this.fileToKeyMapper)
        .map(
            key ->
                collectionIndexes.values().stream()
                        .allMatch(flatIndex -> flatIndex.getKeyToValue().containsKey(key))
                    ? null
                    : PersistencePath.of(key))
        .filter(Objects::nonNull)
        .collect(Collectors.toSet());
  }

  @Override
  public Stream<PersistenceEntity<String>> readByProperty(
      @NonNull final PersistenceCollection collection,
      @NonNull final PersistencePath property,
      @NonNull final Object propertyValue) {

    if (!this.canUseToString(propertyValue)) {
      return this.streamAll(collection);
    }

    final InMemoryIndex flatIndex =
        this.indexMap.get(collection.getValue()).get(property.getValue());
    if (flatIndex == null) return this.streamAll(collection);

    final Set<String> keys = flatIndex.getValueToKeys().get(String.valueOf(propertyValue));
    if ((keys == null) || keys.isEmpty()) {
      return Stream.of();
    }

    return new ArrayList<>(keys)
        .stream()
            .map(
                key -> {
                  final PersistencePath path = PersistencePath.of(key);
                  return this.read(collection, path)
                      .map(data -> new PersistenceEntity<>(path, data))
                      .orElseGet(
                          () -> {
                            this.dropIndex(collection, path);
                            return null;
                          });
                })
            .filter(Objects::nonNull);
  }

  @Override
  public Stream<PersistenceEntity<String>> readByPropertyIgnoreCase(
      @NonNull final PersistenceCollection collection,
      @NonNull final PersistencePath property,
      @NonNull final String propertyValue) {

    if (!this.canUseToString(propertyValue)) {
      return this.streamAll(collection);
    }

    final InMemoryIndex flatIndex =
        this.indexMap.get(collection.getValue()).get(property.getValue());
    if (flatIndex == null) return this.streamAll(collection);

    final String propertyValueStr = propertyValue.toLowerCase(Locale.ROOT);
    final Set<String> keys =
        flatIndex.getValueToKeys().entrySet().stream()
            .filter(entry -> entry.getKey().toLowerCase(Locale.ROOT).equals(propertyValueStr))
            .flatMap(entry -> entry.getValue().stream())
            .collect(Collectors.toSet());

    if (keys.isEmpty()) {
      return Stream.of();
    }

    return new ArrayList<>(keys)
        .stream()
            .map(
                key -> {
                  final PersistencePath path = PersistencePath.of(key);
                  return this.read(collection, path)
                      .map(data -> new PersistenceEntity<>(path, data))
                      .orElseGet(
                          () -> {
                            this.dropIndex(collection, path);
                            return null;
                          });
                })
            .filter(Objects::nonNull);
  }

  @Override
  @SuppressWarnings("ResultOfMethodCallIgnored")
  public void registerCollection(@NonNull final PersistenceCollection collection) {

    final PersistencePath collectionPath = this.basePath.sub(collection);
    final File collectionFile = collectionPath.toFile();
    collectionFile.mkdirs();

    final Map<String, InMemoryIndex> indexes =
        this.indexMap.computeIfAbsent(collection.getValue(), col -> new ConcurrentHashMap<>());

    for (final IndexProperty index : collection.getIndexes()) {

      final InMemoryIndex flatIndex = ConfigManager.create(InMemoryIndex.class);
      flatIndex.setConfigurer(this.indexProvider.get());

      final Path path =
          collectionPath.append("_").append(index.toSafeFileName()).append(".index").toPath();
      flatIndex.withBindFile(path);

      if (this.saveIndex && Files.exists(path)) {
        flatIndex.load(path);
      }

      indexes.put(index.getValue(), flatIndex);
    }

    super.registerCollection(collection);
  }

  @Override
  public Optional<String> read(
      @NonNull final PersistenceCollection collection, @NonNull final PersistencePath path) {
    this.checkCollectionRegistered(collection);
    final File file = this.toFullPath(collection, path).toFile();
    return file.exists() ? Optional.ofNullable(this.fileToString(file)) : Optional.empty();
  }

  @Override
  public Map<PersistencePath, String> read(
      @NonNull final PersistenceCollection collection,
      @NonNull final Collection<PersistencePath> paths) {
    return paths.stream()
        .distinct()
        .map(path -> new PersistenceEntity<>(path, this.read(collection, path).orElse(null)))
        .filter(entity -> entity.getValue() != null)
        .collect(Collectors.toMap(PersistenceEntity::getPath, PersistenceEntity::getValue));
  }

  @Override
  public Map<PersistencePath, String> readOrEmpty(
      @NonNull final PersistenceCollection collection,
      @NonNull final Collection<PersistencePath> paths) {
    return paths.stream()
        .distinct()
        .collect(Collectors.toMap(path -> path, path -> this.readOrEmpty(collection, path)));
  }

  @Override
  public Map<PersistencePath, String> readAll(@NonNull final PersistenceCollection collection) {
    return this.streamAll(collection)
        .collect(Collectors.toMap(PersistenceEntity::getPath, PersistenceEntity::getValue));
  }

  @Override
  @SneakyThrows
  public Stream<PersistenceEntity<String>> streamAll(
      @NonNull final PersistenceCollection collection) {

    this.checkCollectionRegistered(collection);
    final Path collectionFile = this.basePath.sub(collection).toPath();

    return this.scanCollection(collectionFile)
        .map(
            path -> {
              final PersistencePath persistencePath =
                  PersistencePath.of(this.fileToKeyMapper.apply(path));
              return new PersistenceEntity<>(persistencePath, this.fileToString(path.toFile()));
            });
  }

  @Override
  @SneakyThrows
  public long count(@NonNull final PersistenceCollection collection) {

    this.checkCollectionRegistered(collection);
    final Path collectionFile = this.basePath.sub(collection).toPath();

    return this.scanCollection(collectionFile).count();
  }

  @Override
  public boolean exists(
      @NonNull final PersistenceCollection collection, @NonNull final PersistencePath path) {
    this.checkCollectionRegistered(collection);
    return this.toFullPath(collection, path).toFile().exists();
  }

  @Override
  @SneakyThrows
  @SuppressWarnings("ResultOfMethodCallIgnored")
  public boolean write(
      @NonNull final PersistenceCollection collection,
      @NonNull final PersistencePath path,
      @NonNull final String raw) {
    this.checkCollectionRegistered(collection);
    final File file = this.toFullPath(collection, path).toFile();
    final File parentFile = file.getParentFile();
    if (parentFile != null) parentFile.mkdirs();
    this.writeToFile(file, raw);
    return true;
  }

  @Override
  public PersistencePath convertPath(@NonNull final PersistencePath path) {
    return path.append(this.fileSuffix);
  }

  @Override
  public boolean delete(
      @NonNull final PersistenceCollection collection, @NonNull final PersistencePath path) {

    this.checkCollectionRegistered(collection);
    final Set<IndexProperty> collectionIndexes = this.getKnownIndexes().get(collection.getValue());

    if (collectionIndexes != null) {
      collectionIndexes.forEach(index -> this.dropIndex(collection, path));
    }

    return this.toFullPath(collection, path).toFile().delete();
  }

  @Override
  public long delete(
      @NonNull final PersistenceCollection collection,
      @NonNull final Collection<PersistencePath> paths) {
    return paths.stream()
        .map(path -> this.delete(collection, path))
        .filter(Predicate.isEqual(true))
        .count();
  }

  @Override
  @SneakyThrows
  public boolean deleteAll(@NonNull final PersistenceCollection collection) {

    this.checkCollectionRegistered(collection);
    final File collectionFile = this.basePath.sub(collection).toFile();
    this.checkCollectionRegistered(collection);
    final Set<IndexProperty> collectionIndexes = this.getKnownIndexes().get(collection.getValue());

    if (collectionIndexes != null) {
      collectionIndexes.forEach(index -> this.dropIndex(collection, index));
    }

    return collectionFile.exists() && (this.delete(collectionFile) > 0);
  }

  @Override
  public long deleteAll() {

    final File[] files = this.basePath.toFile().listFiles();
    if (files == null) {
      return 0;
    }

    return Arrays.stream(files)
        .filter(file -> this.getKnownCollections().containsKey(file.getName()))
        .map(this::delete)
        .filter(deleted -> deleted > 0)
        .count();
  }

  @Override
  public void close() throws IOException {}

  @SneakyThrows
  private long delete(@NonNull final File file) {
    @Cleanup final Stream<Path> walk = Files.walk(file.toPath());
    return walk.sorted(Comparator.reverseOrder())
        .map(Path::toFile)
        .map(File::delete)
        .filter(Predicate.isEqual(true))
        .count();
  }

  @SneakyThrows
  private Stream<Path> scanCollection(@NonNull final Path collectionFile) {
    return Files.list(collectionFile)
        .filter(
            path -> {
              final boolean endsWithSuffix = path.toString().endsWith(this.fileSuffix);
              if (DEBUG && !endsWithSuffix) {
                LOGGER.log(
                    Level.WARNING,
                    "Possibly bogus file found in "
                        + collectionFile
                        + ": "
                        + path
                        + " (not ending with '"
                        + this.fileSuffix
                        + "')");
              }
              return endsWithSuffix;
            });
  }

  private String fileToString(final File file) {
    try {
      return new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8);
    } catch (final IOException exception) {
      if (DEBUG) {
        LOGGER.log(Level.WARNING, "Returning empty data for " + file, exception);
      }
      return null;
    }
  }

  @SneakyThrows
  private void writeToFile(final File file, final String text) {
    @Cleanup final BufferedWriter writer = new BufferedWriter(new FileWriter(file));
    writer.write(text);
  }

  @Data
  @AllArgsConstructor
  private static class Pair<L, R> {
    private L left;
    private R right;
  }
}
