package eu.okaeri.persistence.mongo;

import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.*;
import eu.okaeri.persistence.PersistenceCollection;
import eu.okaeri.persistence.PersistenceEntity;
import eu.okaeri.persistence.PersistencePath;
import eu.okaeri.persistence.raw.NativeRawPersistence;
import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import lombok.Getter;
import lombok.NonNull;
import lombok.SneakyThrows;
import org.bson.conversions.Bson;

@Getter
public class MongoPersistence extends NativeRawPersistence {

  private static final Logger LOGGER = Logger.getLogger(MongoPersistence.class.getSimpleName());
  private static final ReplaceOptions REPLACE_OPTIONS = new ReplaceOptions().upsert(true);

  private MongoClient client;
  private MongoDatabase database;

  public MongoPersistence(
      @NonNull final PersistencePath basePath,
      @NonNull final MongoClient client,
      @NonNull final String databaseName) {
    super(basePath, true, false, true, false, true);
    this.connect(client, databaseName);
  }

  @SneakyThrows
  private void connect(@NonNull final MongoClient client, @NonNull final String databaseName) {
    do {
      try {
        this.client = client;
        final MongoDatabase database = client.getDatabase(databaseName);
        database.runCommand(new org.bson.Document("ping", 1));
        this.database = database;
      } catch (final Exception exception) {
        if (exception.getCause() != null) {
          LOGGER.severe(
              "["
                  + this.getBasePath().getValue()
                  + "] Cannot connect with database (waiting 30s): "
                  + exception.getMessage()
                  + " caused by "
                  + exception.getCause().getMessage());
        } else {
          LOGGER.severe(
              "["
                  + this.getBasePath().getValue()
                  + "] Cannot connect with database (waiting 30s): "
                  + exception.getMessage());
        }
        Thread.sleep(30_000);
      }
    } while (this.database == null);
  }

  @Override
  public void registerCollection(@NonNull final PersistenceCollection collection) {

    if (!collection.getIndexes().isEmpty()) {
      this.mongo(collection)
          .createIndexes(
              collection.getIndexes().stream()
                  .map(index -> new IndexModel(Indexes.ascending(index.getValue())))
                  .collect(Collectors.toList()));
    }

    super.registerCollection(collection);
  }

  @Override
  public Stream<PersistenceEntity<String>> readByProperty(
      @NonNull final PersistenceCollection collection,
      @NonNull final PersistencePath property,
      final Object propertyValue) {
    return StreamSupport.stream(
        this.mongo(collection)
            .find()
            .filter(Filters.in(property.toMongoPath(), propertyValue))
            .map(object -> this.transformMongoObject(collection, object))
            .spliterator(),
        false);
  }

  @Override
  public Stream<PersistenceEntity<String>> readByPropertyIgnoreCase(
      @NonNull final PersistenceCollection collection,
      @NonNull final PersistencePath property,
      @NonNull final String propertyValue) {
    return StreamSupport.stream(
        this.mongo(collection)
            .aggregate(
                Collections.singletonList(
                    Aggregates.match(
                        Filters.expr(
                            new BasicDBObject(
                                "$eq",
                                Arrays.asList(
                                    new BasicDBObject("$toLower", "$" + property.toMongoPath()),
                                    propertyValue.toLowerCase()))))))
            .map(object -> this.transformMongoObject(collection, object))
            .spliterator(),
        false);
  }

  @Override
  public Optional<String> read(
      @NonNull final PersistenceCollection collection, @NonNull final PersistencePath path) {
    return Optional.ofNullable(
        this.mongo(collection)
            .find()
            .filter(Filters.eq("_id", path.getValue()))
            .map(object -> this.transformMongoObject(collection, object).getValue())
            .first());
  }

  @Override
  public Map<PersistencePath, String> read(
      @NonNull final PersistenceCollection collection,
      @NonNull final Collection<PersistencePath> paths) {

    if (paths.isEmpty()) {
      return Collections.emptyMap();
    }

    final List<String> keys =
        paths.stream().map(PersistencePath::getValue).collect(Collectors.toList());

    return this.mongo(collection)
        .find()
        .filter(Filters.in("_id", keys))
        .map(object -> this.transformMongoObject(collection, object))
        .into(new ArrayList<>())
        .stream()
        .collect(Collectors.toMap(PersistenceEntity::getPath, PersistenceEntity::getValue));
  }

  @Override
  public Map<PersistencePath, String> readAll(@NonNull final PersistenceCollection collection) {
    return this.streamAll(collection)
        .collect(Collectors.toMap(PersistenceEntity::getPath, PersistenceEntity::getValue));
  }

  @Override
  public Stream<PersistenceEntity<String>> streamAll(
      @NonNull final PersistenceCollection collection) {
    return StreamSupport.stream(
        this.mongo(collection)
            .find()
            .map(object -> this.transformMongoObject(collection, object))
            .spliterator(),
        false);
  }

  @Override
  public long count(@NonNull final PersistenceCollection collection) {
    return this.mongo(collection).countDocuments();
  }

  @Override
  public boolean exists(
      @NonNull final PersistenceCollection collection, @NonNull final PersistencePath path) {
    return this.read(collection, path).isPresent();
  }

  @Override
  public boolean write(
      @NonNull final PersistenceCollection collection,
      @NonNull final PersistencePath path,
      @NonNull final String document) {
    final BasicDBObject data = BasicDBObject.parse(document);
    data.put("_id", path.getValue());
    final Bson filters = Filters.in("_id", path.getValue());
    return this.mongo(collection).replaceOne(filters, data, REPLACE_OPTIONS).getModifiedCount() > 0;
  }

  @Override
  public long write(
      @NonNull final PersistenceCollection collection,
      @NonNull final Map<PersistencePath, String> entities) {

    if (entities.isEmpty()) {
      return 0;
    }

    return this.mongo(collection)
        .bulkWrite(
            entities.entrySet().stream()
                .map(
                    entry -> {
                      final BasicDBObject data = BasicDBObject.parse(entry.getValue());
                      data.put("_id", entry.getKey().getValue());
                      return data;
                    })
                .map(
                    document ->
                        new ReplaceOneModel<>(
                            Filters.in("_id", document.get("_id")), document, REPLACE_OPTIONS))
                .collect(Collectors.toList()))
        .getModifiedCount();
  }

  @Override
  public boolean delete(
      @NonNull final PersistenceCollection collection, @NonNull final PersistencePath path) {
    return this.mongo(collection).deleteOne(Filters.eq("_id", path.getValue())).getDeletedCount()
        > 0;
  }

  @Override
  public long delete(
      @NonNull final PersistenceCollection collection,
      @NonNull final Collection<PersistencePath> paths) {

    final List<String> keys =
        paths.stream().map(PersistencePath::getValue).collect(Collectors.toList());

    return this.mongo(collection).deleteMany(Filters.in("_id", keys)).getDeletedCount();
  }

  @Override
  public boolean deleteAll(@NonNull final PersistenceCollection collection) {
    this.mongo(collection).drop();
    return true;
  }

  @Override
  public long deleteAll() {
    throw new RuntimeException("Not implemented yet");
  }

  @Override
  public void close() throws IOException {
    this.client.close();
  }

  protected MongoCollection<BasicDBObject> mongo(final PersistenceCollection collection) {
    final String identifier = this.getBasePath().sub(collection).toSqlIdentifier();
    return this.database.getCollection(identifier, BasicDBObject.class);
  }

  protected PersistenceEntity<String> transformMongoObject(
      final PersistenceCollection collection, final BasicDBObject object) {
    final PersistencePath path = PersistencePath.of(object.getString("_id"));
    object.remove("_id"); // don't need this anymore
    return new PersistenceEntity<>(path, object.toJson());
  }
}
