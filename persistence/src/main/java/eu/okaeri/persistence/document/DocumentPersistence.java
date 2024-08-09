package eu.okaeri.persistence.document;

import eu.okaeri.configs.ConfigManager;
import eu.okaeri.configs.configurer.Configurer;
import eu.okaeri.configs.serdes.OkaeriSerdesPack;
import eu.okaeri.configs.serdes.SerdesRegistry;
import eu.okaeri.configs.serdes.commons.SerdesCommons;
import eu.okaeri.configs.serdes.standard.StandardSerdes;
import eu.okaeri.persistence.Persistence;
import eu.okaeri.persistence.PersistenceCollection;
import eu.okaeri.persistence.PersistenceEntity;
import eu.okaeri.persistence.PersistencePath;
import eu.okaeri.persistence.document.index.IndexProperty;
import eu.okaeri.persistence.document.ref.EagerRefSerializer;
import eu.okaeri.persistence.document.ref.LazyRefSerializer;
import eu.okaeri.persistence.raw.RawPersistence;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Getter;
import lombok.NonNull;

public class DocumentPersistence implements Persistence<Document> {

  private static final Logger LOGGER = Logger.getLogger(DocumentPersistence.class.getSimpleName());

  @Getter protected final ConfigurerProvider configurerProvider;
  @Getter protected final OkaeriSerdesPack[] serdesPacks;

  protected RawPersistence read;
  protected RawPersistence write;

  protected SerdesRegistry serdesRegistry;
  protected Configurer simplifier;

  /**
   * @param configurerProvider Okaeri Config's provider (mostly json)
   * @param serdesPacks Additional serdes packs for the configurerProvider
   */
  public DocumentPersistence(
      @NonNull final ConfigurerProvider configurerProvider,
      @NonNull final OkaeriSerdesPack... serdesPacks) {
    this.configurerProvider = configurerProvider;
    this.serdesPacks = serdesPacks;
    // shared transform registry for faster transformations
    this.serdesRegistry = new SerdesRegistry();
    Stream.concat(Stream.of(new StandardSerdes(), new SerdesCommons()), Stream.of(this.serdesPacks))
        .forEach(pack -> pack.register(this.serdesRegistry));
    this.serdesRegistry.register(new LazyRefSerializer(this));
    this.serdesRegistry.register(new EagerRefSerializer(this));
    // simplifier for document mappings
    this.simplifier = configurerProvider.get();
    this.simplifier.setRegistry(this.serdesRegistry);
  }

  /**
   * @param readPersistence Base persistence provider for read operations
   * @param writePersistence Base persistence provider for write operations
   * @param configurerProvider Okaeri Config's provider (mostly json)
   * @param serdesPacks Additional serdes packs for the configurerProvider
   */
  public DocumentPersistence(
      @NonNull final RawPersistence readPersistence,
      @NonNull final RawPersistence writePersistence,
      @NonNull final ConfigurerProvider configurerProvider,
      @NonNull final OkaeriSerdesPack... serdesPacks) {
    this(configurerProvider, serdesPacks);
    this.read = readPersistence;
    this.write = writePersistence;
  }

  /**
   * @param rawPersistence Base persistence provider
   * @param configurerProvider Okaeri Config's provider (mostly json)
   * @param serdesPacks Additional serdes packs for the configurerProvider
   */
  public DocumentPersistence(
      @NonNull final RawPersistence rawPersistence,
      @NonNull final ConfigurerProvider configurerProvider,
      @NonNull final OkaeriSerdesPack... serdesPacks) {
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
      throw new IllegalArgumentException(
          "Cannot use #getRaw() with DocumentPersistence using separate instances for read and write");
    }
    return this.getRead();
  }

  @Override
  public void setAutoFlush(final boolean state) {
    this.getWrite().setAutoFlush(state);
  }

  @Override
  public void flush() {
    this.getWrite().flush();
  }

  @Override
  public PersistencePath getBasePath() {
    if (!this.getRead().getBasePath().equals(this.getWrite().getBasePath())) {
      throw new IllegalArgumentException(
          "Cannot use #getBasePath() with DocumentPersistence using different paths for read and write");
    }
    return this.getWrite().getBasePath();
  }

  @Override
  public void registerCollection(@NonNull final PersistenceCollection collection) {

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
  public long fixIndexes(@NonNull final PersistenceCollection collection) {

    if (!this.getWrite().isEmulatedIndexes()) {
      return 0;
    }

    final Set<IndexProperty> indexes =
        this.getRead().getKnownIndexes().getOrDefault(collection.getValue(), new HashSet<>());
    final Set<PersistencePath> withMissingIndexes = this.findMissingIndexes(collection, indexes);

    if (withMissingIndexes.isEmpty()) {
      return 0;
    }

    final int total = withMissingIndexes.size();
    final long start = System.currentTimeMillis();
    long lastInfo = System.currentTimeMillis();
    int updated = 0;
    LOGGER.warning(
        "["
            + this.getBasePath().sub(collection).getValue()
            + "] Found "
            + total
            + " entries with missing indexes, updating..");
    this.setAutoFlush(false);

    for (final PersistencePath key : withMissingIndexes) {

      this.updateIndex(collection, key);
      updated++;

      if ((System.currentTimeMillis() - lastInfo) <= 5_000) {
        continue;
      }

      final int percent = (int) (((double) updated / (double) total) * 100);
      LOGGER.warning(
          "["
              + this.getBasePath().sub(collection).getValue()
              + "] "
              + updated
              + " already done ("
              + percent
              + "%)");
      lastInfo = System.currentTimeMillis();
    }

    this.setAutoFlush(true);
    this.flush();
    LOGGER.warning(
        "["
            + this.getBasePath().sub(collection).getValue()
            + "] Finished creating indexes! (took: "
            + (System.currentTimeMillis() - start)
            + " ms)");
    return updated;
  }

  @Override
  public boolean updateIndex(
      @NonNull final PersistenceCollection collection,
      @NonNull final PersistencePath path,
      @NonNull final IndexProperty property,
      final String identity) {
    return this.getWrite().isEmulatedIndexes()
        && this.getWrite().updateIndex(collection, path, property, identity);
  }

  @Override
  public boolean updateIndex(
      @NonNull final PersistenceCollection collection,
      @NonNull final PersistencePath path,
      @NonNull final Document document) {

    if (!this.getWrite().isEmulatedIndexes()) {
      return false;
    }

    final Set<IndexProperty> collectionIndexes =
        this.getRead().getKnownIndexes().get(collection.getValue());
    if (collectionIndexes == null) {
      return false;
    }

    final Map<String, Object> documentMap = document.asMap(this.simplifier, true);
    int changes = 0;

    for (final IndexProperty index : collectionIndexes) {
      final Object value = this.extractValue(documentMap, index.toParts());
      if ((value != null) && !this.getWrite().canUseToString(value)) {
        throw new RuntimeException("cannot transform " + value + " to index as string");
      }
      final boolean changed =
          this.updateIndex(collection, path, index, (value == null) ? null : String.valueOf(value));
      if (changed) {
        changes++;
      }
    }

    return changes > 0;
  }

  @Override
  public boolean updateIndex(
      @NonNull final PersistenceCollection collection, @NonNull final PersistencePath path) {

    if (!this.getWrite().isEmulatedIndexes()) {
      return false;
    }

    final Optional<Document> document = this.read(collection, path);
    return document.map(value -> this.updateIndex(collection, path, value)).isPresent();
  }

  @Override
  public boolean dropIndex(
      @NonNull final PersistenceCollection collection,
      @NonNull final PersistencePath path,
      @NonNull final IndexProperty property) {
    return this.getWrite().isEmulatedIndexes()
        && this.getWrite().dropIndex(collection, path, property);
  }

  @Override
  public boolean dropIndex(
      @NonNull final PersistenceCollection collection, @NonNull final PersistencePath path) {
    return this.getWrite().isEmulatedIndexes() && this.getWrite().dropIndex(collection, path);
  }

  @Override
  public boolean dropIndex(
      @NonNull final PersistenceCollection collection, @NonNull final IndexProperty property) {
    return this.getWrite().isEmulatedIndexes() && this.getWrite().dropIndex(collection, property);
  }

  @Override
  public Set<PersistencePath> findMissingIndexes(
      @NonNull final PersistenceCollection collection,
      @NonNull final Set<IndexProperty> indexProperties) {
    return this.getWrite().findMissingIndexes(collection, indexProperties);
  }

  @Override
  public Document readOrEmpty(
      @NonNull final PersistenceCollection collection, @NonNull final PersistencePath path) {
    return this.read(collection, path).orElse(this.createDocument(collection, path));
  }

  @Override
  public Optional<Document> read(
      @NonNull final PersistenceCollection collection, @NonNull final PersistencePath path) {

    final Optional<String> data = this.getRead().read(collection, path);
    if (!data.isPresent()) {
      return Optional.empty();
    }

    final Document document = this.createDocument(collection, path);
    return Optional.of((Document) document.load(data.get()));
  }

  @Override
  public Map<PersistencePath, Document> readOrEmpty(
      @NonNull final PersistenceCollection collection,
      @NonNull final Collection<PersistencePath> paths) {

    if (paths.isEmpty()) {
      return Collections.emptyMap();
    }

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
    return paths.isEmpty()
        ? Collections.emptyMap()
        : this.getRead().read(collection, paths).entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    entry -> {
                      final Document document = this.createDocument(collection, entry.getKey());
                      return (Document) document.load(entry.getValue());
                    }));
  }

  @Override
  public Map<PersistencePath, Document> readAll(@NonNull final PersistenceCollection collection) {
    return this.getRead().readAll(collection).entrySet().stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                entry -> {
                  final Document document = this.createDocument(collection, entry.getKey());
                  return (Document) document.load(entry.getValue());
                }));
  }

  @Override
  public Stream<PersistenceEntity<Document>> readByProperty(
      @NonNull final PersistenceCollection collection,
      @NonNull final PersistencePath property,
      final Object propertyValue) {

    final List<String> pathParts = property.toParts();
    final Predicate<PersistenceEntity<Document>> documentFilter =
        entity -> {
          if (pathParts.size() == 1) {
            return this.compare(propertyValue, entity.getValue().get(pathParts.get(0)));
          }
          final Map<String, Object> document = entity.getValue().asMap(this.simplifier, true);
          return this.compare(propertyValue, this.extractValue(document, pathParts));
        };

    // native read implementation may or may not filter entries
    // for every query, depending on the backend supported features
    // the goal is to allow extensibility - i trust but i verify
    // with the exception for non-emulated indexes (native)
    if (this.getRead().isCanReadByProperty()) {
      return this.getRead()
          .readByProperty(collection, property, propertyValue)
          .map(this.entityToDocumentMapper(collection))
          .filter(entity -> this.getRead().isNativeIndexes() || documentFilter.test(entity));
    }

    // streaming search optimzied with string search can
    // greatly reduce search time removing parsing overhead
    final boolean stringSearch =
        this.getRead().isUseStringSearch() && this.getWrite().canUseToString(propertyValue);
    return this.getRead()
        .streamAll(collection)
        .filter(
            entity -> !stringSearch || entity.getValue().contains(String.valueOf(propertyValue)))
        .map(this.entityToDocumentMapper(collection))
        .filter(documentFilter);
  }

  @Override
  public Stream<PersistenceEntity<Document>> readByPropertyIgnoreCase(
      @NonNull final PersistenceCollection collection,
      @NonNull final PersistencePath property,
      @NonNull final String propertyValue) {

    final List<String> pathParts = property.toParts();
    final Predicate<PersistenceEntity<Document>> documentFilter =
        entity -> {
          if (pathParts.size() == 1) {
            final Object value = entity.getValue().get(pathParts.get(0));
            if (value instanceof String) {
              return this.containsIgnoreCase((String) value, propertyValue);
            }
            return false;
          }
          final Map<String, Object> document = entity.getValue().asMap(this.simplifier, true);
          final Object value = this.extractValue(document, pathParts);
          if (value instanceof String) {
            return this.containsIgnoreCase((String) value, propertyValue);
          }
          return false;
        };

    if (this.getRead().isCanReadByProperty()) {
      return this.getRead()
          .readByProperty(collection, property, propertyValue)
          .map(this.entityToDocumentMapper(collection))
          .filter(entity -> this.getRead().isNativeIndexes() || documentFilter.test(entity));
    }

    final boolean stringSearch =
        this.getRead().isUseStringSearch() && this.getWrite().canUseToString(propertyValue);
    return this.getRead()
        .streamAll(collection)
        .filter(
            entity ->
                !stringSearch || this.containsIgnoreCase(entity.getValue(), propertyValue))
        .map(this.entityToDocumentMapper(collection))
        .filter(documentFilter);
  }

  private boolean containsIgnoreCase(final String str, final String searchStr) {
    if (str == null || searchStr == null) {
      return false;
    }
    final int length = searchStr.length();
    if (length == 0) {
      return true; // Empty string is contained in any other string
    }
    for (int i = str.length() - length; i >= 0; i--) {
      if (str.regionMatches(true, i, searchStr, 0, length)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public Stream<PersistenceEntity<Document>> streamAll(
      @NonNull final PersistenceCollection collection) {
    return this.getRead().streamAll(collection).map(this.entityToDocumentMapper(collection));
  }

  @Override
  public long count(@NonNull final PersistenceCollection collection) {
    return this.getRead().count(collection);
  }

  @Override
  public boolean exists(
      @NonNull final PersistenceCollection collection, @NonNull final PersistencePath path) {
    return this.getRead().exists(collection, path);
  }

  @Override
  public boolean write(
      @NonNull final PersistenceCollection collection,
      @NonNull final PersistencePath path,
      @NonNull final Document document) {
    this.updateIndex(collection, path, document);
    return this.getWrite()
        .write(collection, path, this.update(document, collection).saveToString());
  }

  @Override
  public long write(
      @NonNull final PersistenceCollection collection,
      @NonNull final Map<PersistencePath, Document> entities) {

    if (entities.isEmpty()) {
      return 0;
    }

    final Map<PersistencePath, String> rawMap = new LinkedHashMap<>();

    for (final Map.Entry<PersistencePath, Document> entry : entities.entrySet()) {
      this.updateIndex(collection, entry.getKey(), entry.getValue());
      rawMap.put(entry.getKey(), this.update(entry.getValue(), collection).saveToString());
    }

    return this.getWrite().write(collection, rawMap);
  }

  @Override
  public boolean delete(
      @NonNull final PersistenceCollection collection, @NonNull final PersistencePath path) {
    return this.getWrite().delete(collection, path);
  }

  @Override
  public long delete(
      @NonNull final PersistenceCollection collection,
      @NonNull final Collection<PersistencePath> paths) {
    return this.getWrite().delete(collection, paths);
  }

  @Override
  public boolean deleteAll(@NonNull final PersistenceCollection collection) {
    return this.getWrite().deleteAll(collection);
  }

  @Override
  public long deleteAll() {
    return this.getWrite().deleteAll();
  }

  public Document createDocument(
      @NonNull final PersistenceCollection collection, @NonNull final PersistencePath path) {
    final Document config = this.update(ConfigManager.create(Document.class), collection);
    config.setPath(path);
    return config;
  }

  protected Function<PersistenceEntity<String>, PersistenceEntity<Document>> entityToDocumentMapper(
      final PersistenceCollection collection) {
    return entity -> {
      final Document document = this.createDocument(collection, entity.getPath());
      document.load(entity.getValue());
      return entity.into(document);
    };
  }

  protected Object extractValue(Map<?, ?> document, final Iterable<String> pathParts) {
    for (final String part : pathParts) {
      final Object element = document.get(part);
      if (element instanceof Map) {
        document = (Map<?, ?>) element;
        continue;
      }
      return element;
    }
    return null;
  }

  protected boolean compare(final Object object1, final Object object2) {

    if ((object1 == null) && (object2 == null)) {
      return true;
    }

    if ((object1 == null) || (object2 == null)) {
      return false;
    }

    if ((object1 instanceof Number) && (object2 instanceof Number)) {
      return ((Number) object1).doubleValue() == ((Number) object2).doubleValue();
    }

    if (object1.getClass() == object2.getClass()) {
      return object1.equals(object2);
    }

    if (((object1 instanceof String) && (object2 instanceof Number))
        || ((object1 instanceof Number) && (object2 instanceof String))) {
      try {
        return new BigDecimal(String.valueOf(object1))
                .compareTo(new BigDecimal(String.valueOf(object2)))
            == 0;
      } catch (final NumberFormatException ignored) {
        return false;
      }
    }

    if (((object1 instanceof String) && (object2 instanceof UUID))
        || ((object1 instanceof UUID) && (object2 instanceof String))) {
      return Objects.equals(String.valueOf(object1), String.valueOf(object2));
    }

    throw new IllegalArgumentException(
        "cannot compare "
            + object1
            + " ["
            + object1.getClass()
            + "] to "
            + object2
            + " ["
            + object2.getClass()
            + "]");
  }

  public Document update(final Document document, final PersistenceCollection collection) {

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

  @Override
  public void close() throws IOException {

    this.getRead().close();

    if (this.getRead().equals(this.getWrite())) {
      return;
    }

    this.getWrite().close();
  }
}
