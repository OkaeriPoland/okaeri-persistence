package eu.okaeri.persistence.raw;

import eu.okaeri.persistence.Persistence;
import eu.okaeri.persistence.PersistenceCollection;
import eu.okaeri.persistence.PersistenceEntity;
import eu.okaeri.persistence.PersistencePath;
import eu.okaeri.persistence.document.index.IndexProperty;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@AllArgsConstructor
public abstract class RawPersistence implements Persistence<String> {

  @Getter private final PersistencePath basePath;

  @Getter
  private final Map<String, PersistenceCollection> knownCollections = new ConcurrentHashMap<>();

  @Getter private final Map<String, Set<IndexProperty>> knownIndexes = new ConcurrentHashMap<>();
  @Getter private final boolean canReadByProperty;
  @Getter private final boolean emulatedIndexes;
  @Getter private final boolean nativeIndexes;
  @Getter @Setter private boolean useStringSearch;
  @Getter @Setter private boolean autoFlush;

  @Override
  public void registerCollection(@NonNull final PersistenceCollection collection) {
    this.knownCollections.put(collection.getValue(), collection);
    this.knownIndexes.put(collection.getValue(), collection.getIndexes());
  }

  @Override
  public long fixIndexes(final PersistenceCollection collection) {
    throw new RuntimeException("not implemented yet");
  }

  @Override
  public void flush() {}

  @Override
  public boolean updateIndex(
      final PersistenceCollection collection,
      final PersistencePath path,
      final IndexProperty property,
      final String identity) {
    throw new RuntimeException("not implemented yet");
  }

  @Override
  public boolean updateIndex(
      final PersistenceCollection collection, final PersistencePath path, final String entity) {
    throw new RuntimeException("not implemented yet");
  }

  @Override
  public boolean updateIndex(final PersistenceCollection collection, final PersistencePath path) {
    throw new RuntimeException("not implemented yet");
  }

  @Override
  public boolean dropIndex(
      final PersistenceCollection collection,
      final PersistencePath path,
      final IndexProperty property) {
    throw new RuntimeException("not implemented yet");
  }

  @Override
  public boolean dropIndex(final PersistenceCollection collection, final PersistencePath path) {
    throw new RuntimeException("not implemented yet");
  }

  @Override
  public boolean dropIndex(final PersistenceCollection collection, final IndexProperty property) {
    throw new RuntimeException("not implemented yet");
  }

  @Override
  public Set<PersistencePath> findMissingIndexes(
      final PersistenceCollection collection, final Set<IndexProperty> indexProperties) {
    throw new RuntimeException("not implemented yet");
  }

  @Override
  public Stream<PersistenceEntity<String>> readByProperty(
      final PersistenceCollection collection,
      final PersistencePath property,
      final Object propertyValue) {
    throw new RuntimeException("not implemented yet");
  }

  @Override
  public String readOrEmpty(
      @NonNull final PersistenceCollection collection, @NonNull final PersistencePath path) {
    return this.read(collection, path).orElse("");
  }

  @Override
  public Map<PersistencePath, String> readOrEmpty(
      @NonNull final PersistenceCollection collection,
      @NonNull final Collection<PersistencePath> paths) {

    this.checkCollectionRegistered(collection);
    final Map<PersistencePath, String> map = new LinkedHashMap<>();
    final Map<PersistencePath, String> data = this.read(collection, paths);

    for (final PersistencePath path : paths) {
      map.put(path, data.getOrDefault(path, ""));
    }

    return map;
  }

  @Override
  public long write(
      @NonNull final PersistenceCollection collection,
      @NonNull final Map<PersistencePath, String> entities) {
    return entities.entrySet().stream()
        .map(entry -> this.write(collection, entry.getKey(), entry.getValue()))
        .filter(Predicate.isEqual(true))
        .count();
  }

  public void checkCollectionRegistered(@NonNull final PersistenceCollection collection) {
    if (this.knownCollections.containsKey(collection.getValue())) {
      return;
    }
    throw new IllegalArgumentException("cannot use unregistered collection: " + collection);
  }

  public PersistencePath toFullPath(
      @NonNull final PersistenceCollection collection, @NonNull final PersistencePath path) {
    this.checkCollectionRegistered(collection);
    return this.getBasePath().sub(collection).sub(this.convertPath(path));
  }

  public PersistencePath convertPath(@NonNull final PersistencePath path) {
    return path;
  }

  public boolean isIndexed(
      @NonNull final PersistenceCollection collection, @NonNull final PersistencePath path) {

    final Set<IndexProperty> collectionIndexes = this.knownIndexes.get(collection.getValue());
    if (collectionIndexes == null) {
      return false;
    }

    final IndexProperty indexProperty = IndexProperty.of(path.getValue());
    return collectionIndexes.contains(indexProperty);
  }

  public boolean canUseToString(final Object value) {
    return (value instanceof String) || (value instanceof Integer) || (value instanceof UUID);
  }
}
