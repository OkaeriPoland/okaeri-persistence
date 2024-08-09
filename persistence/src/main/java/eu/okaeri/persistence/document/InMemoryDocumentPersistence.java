package eu.okaeri.persistence.document;

import eu.okaeri.configs.ConfigManager;
import eu.okaeri.configs.configurer.InMemoryConfigurer;
import eu.okaeri.configs.serdes.OkaeriSerdesPack;
import eu.okaeri.persistence.PersistenceCollection;
import eu.okaeri.persistence.PersistenceEntity;
import eu.okaeri.persistence.PersistencePath;
import eu.okaeri.persistence.document.index.InMemoryIndex;
import eu.okaeri.persistence.document.index.IndexProperty;
import eu.okaeri.persistence.raw.RawPersistence;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Getter;
import lombok.NonNull;
import lombok.SneakyThrows;

public class InMemoryDocumentPersistence extends DocumentPersistence {

  @Getter private final PersistencePath basePath = PersistencePath.of("memory");
  private final Map<String, Map<String, InMemoryIndex>> indexMap = new ConcurrentHashMap<>();
  private final Map<String, Map<PersistencePath, Document>> documents = new ConcurrentHashMap<>();

  public InMemoryDocumentPersistence(@NonNull final OkaeriSerdesPack... serdesPacks) {
    super(
        new RawPersistence(PersistencePath.of("memory"), true, true, true, false, true) {
          @Override
          public long count(final PersistenceCollection collection) {
            return 0;
          }

          @Override
          public boolean exists(
              final PersistenceCollection collection, final PersistencePath path) {
            return false;
          }

          @Override
          public Optional<String> read(
              final PersistenceCollection collection, final PersistencePath path) {
            return Optional.empty();
          }

          @Override
          public Map<PersistencePath, String> read(
              final PersistenceCollection collection, final Collection<PersistencePath> paths) {
            return null;
          }

          @Override
          public Map<PersistencePath, String> readAll(final PersistenceCollection collection) {
            return null;
          }

          @Override
          public Stream<PersistenceEntity<String>> streamAll(
              final PersistenceCollection collection) {
            return null;
          }

          @Override
          public boolean write(
              final PersistenceCollection collection,
              final PersistencePath path,
              final String entity) {
            return false;
          }

          @Override
          public boolean delete(
              final PersistenceCollection collection, final PersistencePath path) {
            return false;
          }

          @Override
          public long delete(
              final PersistenceCollection collection, final Collection<PersistencePath> paths) {
            return 0;
          }

          @Override
          public boolean deleteAll(final PersistenceCollection collection) {
            return false;
          }

          @Override
          public long deleteAll() {
            return 0;
          }

          @Override
          public void close() throws IOException {}
        },
        InMemoryConfigurer::new,
        serdesPacks);
  }

  @Override
  public void setAutoFlush(final boolean state) {}

  @Override
  public void flush() {}

  @Override
  public void registerCollection(@NonNull final PersistenceCollection collection) {

    this.getRead().getKnownCollections().put(collection.getValue(), collection);
    this.getRead().getKnownIndexes().put(collection.getValue(), collection.getIndexes());
    this.getWrite().getKnownCollections().put(collection.getValue(), collection);
    this.getWrite().getKnownIndexes().put(collection.getValue(), collection.getIndexes());
    this.documents.put(collection.getValue(), new ConcurrentHashMap<>());

    final Map<String, InMemoryIndex> indexes =
        this.indexMap.computeIfAbsent(collection.getValue(), col -> new ConcurrentHashMap<>());
    collection
        .getIndexes()
        .forEach(index -> indexes.put(index.getValue(), ConfigManager.create(InMemoryIndex.class)));
  }

  @Override
  public boolean updateIndex(
      @NonNull final PersistenceCollection collection,
      @NonNull final PersistencePath path,
      @NonNull final IndexProperty property,
      final String identity) {

    // get index
    this.getWrite().checkCollectionRegistered(collection);
    final InMemoryIndex flatIndex =
        this.indexMap.get(collection.getValue()).get(property.getValue());
    if (flatIndex == null) {
      throw new IllegalArgumentException("non-indexed property used: " + property);
    }

    // get current value by key and remove from mapping
    final String currentValue = flatIndex.getKeyToValue().remove(path.getValue());

    // remove from old set value_to_keys
    if (currentValue != null) {
      flatIndex.getValueToKeys().get(currentValue).remove(path.getValue());
    }

    // add to new value_to_keys
    final Set<String> keys =
        flatIndex.getValueToKeys().computeIfAbsent(identity, s -> new HashSet<>());
    final boolean changed = keys.add(path.getValue());

    // update key to value
    return (flatIndex.getKeyToValue().put(path.getValue(), identity) != null) || changed;
  }

  @Override
  public boolean dropIndex(
      @NonNull final PersistenceCollection collection,
      @NonNull final PersistencePath path,
      @NonNull final IndexProperty property) {

    // get index
    this.getWrite().checkCollectionRegistered(collection);
    final InMemoryIndex flatIndex =
        this.indexMap.get(collection.getValue()).get(property.getValue());
    if (flatIndex == null) {
      throw new IllegalArgumentException("non-indexed property used: " + property);
    }

    // get current value by key and remove from mapping
    final String currentValue = flatIndex.getKeyToValue().remove(path.getValue());

    // delete from value to set
    return (currentValue != null)
        && flatIndex.getValueToKeys().get(currentValue).remove(path.getValue());
  }

  @Override
  public boolean dropIndex(
      @NonNull final PersistenceCollection collection, @NonNull final PersistencePath path) {
    return this.getWrite()
        .getKnownIndexes()
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
    this.getWrite().checkCollectionRegistered(collection);
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

    this.getRead().checkCollectionRegistered(collection);
    final Map<String, InMemoryIndex> collectionIndexes = this.indexMap.get(collection.getValue());
    if (collectionIndexes.isEmpty()) {
      return Collections.emptySet();
    }

    return this.streamAll(collection)
        .map(PersistenceEntity::getValue)
        .map(
            entity ->
                collectionIndexes.values().stream()
                        .allMatch(
                            flatIndex ->
                                flatIndex.getKeyToValue().containsKey(entity.getPath().getValue()))
                    ? null
                    : entity.getPath())
        .filter(Objects::nonNull)
        .collect(Collectors.toSet());
  }

  @Override
  public Document readOrEmpty(
      @NonNull final PersistenceCollection collection, @NonNull final PersistencePath path) {
    return this.read(collection, path).orElse(this.createDocument(collection, path));
  }

  @Override
  public Optional<Document> read(
      @NonNull final PersistenceCollection collection, @NonNull final PersistencePath path) {
    this.getRead().checkCollectionRegistered(collection);
    return Optional.ofNullable(this.documents.get(collection.getValue()).get(path));
  }

  @Override
  public Map<PersistencePath, Document> readOrEmpty(
      @NonNull final PersistenceCollection collection,
      @NonNull final Collection<PersistencePath> paths) {

    this.getRead().checkCollectionRegistered(collection);
    final Map<PersistencePath, Document> map = new LinkedHashMap<>();
    final Map<PersistencePath, Document> data = this.read(collection, paths);

    for (final PersistencePath path : paths) {
      map.put(path, data.getOrDefault(path, this.createDocument(collection, path)));
    }

    return map;
  }

  @Override
  public Map<PersistencePath, Document> read(
      @NonNull final PersistenceCollection collection,
      @NonNull final Collection<PersistencePath> paths) {
    this.getRead().checkCollectionRegistered(collection);
    return paths.stream()
        .map(path -> this.documents.get(collection.getValue()).get(path))
        .filter(Objects::nonNull)
        .collect(Collectors.toMap(Document::getPath, Function.identity()));
  }

  @Override
  public Map<PersistencePath, Document> readAll(@NonNull final PersistenceCollection collection) {
    return new HashMap<>(this.documents.get(collection.getValue()));
  }

  @Override
  public Stream<PersistenceEntity<Document>> readByProperty(
      @NonNull final PersistenceCollection collection,
      @NonNull final PersistencePath property,
      final Object propertyValue) {

    this.getRead().checkCollectionRegistered(collection);
    final InMemoryIndex flatIndex =
        this.indexMap.get(collection.getValue()).get(property.getValue());
    if (flatIndex == null) {
      return this.streamAll(collection);
    }

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
  public Stream<PersistenceEntity<Document>> streamAll(
      @NonNull final PersistenceCollection collection) {
    this.getRead().checkCollectionRegistered(collection);
    final Collection<Document> docList = this.documents.get(collection.getValue()).values();
    return docList.stream().map(document -> new PersistenceEntity<>(document.getPath(), document));
  }

  @Override
  public long count(@NonNull final PersistenceCollection collection) {
    this.getRead().checkCollectionRegistered(collection);
    return this.documents.get(collection.getValue()).size();
  }

  @Override
  public boolean exists(
      @NonNull final PersistenceCollection collection, @NonNull final PersistencePath path) {
    this.getRead().checkCollectionRegistered(collection);
    return this.documents.get(collection.getValue()).containsKey(path);
  }

  @Override
  public boolean write(
      @NonNull final PersistenceCollection collection,
      @NonNull final PersistencePath path,
      @NonNull final Document document) {
    this.getWrite().checkCollectionRegistered(collection);
    this.updateIndex(collection, path, document);
    return this.documents.get(collection.getValue()).put(path, document) != null;
  }

  @Override
  public long write(
      @NonNull final PersistenceCollection collection,
      @NonNull final Map<PersistencePath, Document> entities) {
    return entities.entrySet().stream()
        .map(entity -> this.write(collection, entity.getKey(), entity.getValue()))
        .filter(Predicate.isEqual(true))
        .count();
  }

  @Override
  public boolean delete(
      @NonNull final PersistenceCollection collection, @NonNull final PersistencePath path) {
    this.getWrite().checkCollectionRegistered(collection);
    return this.documents.get(collection.getValue()).remove(path) != null;
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
  public boolean deleteAll(@NonNull final PersistenceCollection collection) {
    this.getWrite().checkCollectionRegistered(collection);
    final Map<PersistencePath, Document> data = this.documents.get(collection.getValue());
    final boolean changed = !data.isEmpty();
    data.clear();
    return changed;
  }

  @Override
  public long deleteAll() {
    return this.documents.values().stream().peek(Map::clear).count();
  }
}
