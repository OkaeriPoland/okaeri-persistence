package eu.okaeri.persistence.mongo;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.WriteModel;
import eu.okaeri.persistence.PersistenceCollection;
import eu.okaeri.persistence.PersistenceEntity;
import eu.okaeri.persistence.PersistencePath;
import eu.okaeri.persistence.document.index.IndexProperty;
import eu.okaeri.persistence.raw.RawPersistence;
import lombok.Getter;
import lombok.NonNull;
import lombok.SneakyThrows;
import org.bson.conversions.Bson;

import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class MongoPersistence extends RawPersistence {

    private static final Logger LOGGER = Logger.getLogger(MongoPersistence.class.getSimpleName());
    private static final ReplaceOptions REPLACE_OPTIONS = new ReplaceOptions().upsert(true);

    @Getter private MongoClient client;
    @Getter private MongoDatabase database;

    public MongoPersistence(@NonNull PersistencePath basePath, @NonNull MongoClient client, @NonNull String databaseName) {
        super(basePath, true, false, true, false, true);
        this.connect(client, databaseName);
    }

    @SneakyThrows
    private void connect(@NonNull MongoClient client, @NonNull String databaseName) {
        do {
            try {
                this.client = client;
                MongoDatabase database = client.getDatabase(databaseName);
                database.runCommand(new org.bson.Document("ping", 1));
                this.database = database;
            } catch (Exception exception) {
                if (exception.getCause() != null) {
                    LOGGER.severe("[" + this.getBasePath().getValue() + "] Cannot connect with database (waiting 30s): " + exception.getMessage() + " caused by " + exception.getCause().getMessage());
                } else {
                    LOGGER.severe("[" + this.getBasePath().getValue() + "] Cannot connect with database (waiting 30s): " + exception.getMessage());
                }
                Thread.sleep(30_000);
            }
        } while (this.database == null);
    }

    @Override
    public boolean updateIndex(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull IndexProperty property, String identity) {
        return false;
    }

    @Override
    public boolean dropIndex(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull IndexProperty property) {
        return false;
    }

    @Override
    public boolean dropIndex(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {
        return false;
    }

    @Override
    public boolean dropIndex(@NonNull PersistenceCollection collection, @NonNull IndexProperty property) {
        return false;
    }

    @Override
    public Set<PersistencePath> findMissingIndexes(@NonNull PersistenceCollection collection, @NonNull Set<IndexProperty> indexProperties) {
        return Collections.emptySet();
    }

    @Override
    public Stream<PersistenceEntity<String>> readByProperty(@NonNull PersistenceCollection collection, @NonNull PersistencePath property, Object propertyValue) {
        return StreamSupport.stream(this.mongo(collection).find()
            .filter(Filters.in(property.toMongoPath(), propertyValue))
            .map(object -> this.transformMongoObject(collection, object))
            .spliterator(), false);
    }

    @Override
    public Optional<String> read(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {
        return Optional.ofNullable(this.mongo(collection).find()
            .filter(Filters.eq("_id", path.getValue()))
            .map(object -> this.transformMongoObject(collection, object).getValue())
            .first());
    }

    @Override
    public Map<PersistencePath, String> read(@NonNull PersistenceCollection collection, @NonNull Collection<PersistencePath> paths) {

        if (paths.isEmpty()) {
            return Collections.emptyMap();
        }

        List<String> keys = paths.stream()
            .map(PersistencePath::getValue)
            .collect(Collectors.toList());

        return this.mongo(collection).find()
            .filter(Filters.in("_id", keys))
            .map(object -> this.transformMongoObject(collection, object))
            .into(new ArrayList<>())
            .stream()
            .collect(Collectors.toMap(PersistenceEntity::getPath, PersistenceEntity::getValue));
    }

    @Override
    public Map<PersistencePath, String> readAll(@NonNull PersistenceCollection collection) {
        return this.streamAll(collection).collect(Collectors.toMap(
            PersistenceEntity::getPath,
            PersistenceEntity::getValue
        ));
    }

    @Override
    public Stream<PersistenceEntity<String>> streamAll(@NonNull PersistenceCollection collection) {
        return StreamSupport.stream(this.mongo(collection).find()
            .map(object -> this.transformMongoObject(collection, object))
            .spliterator(), false);
    }

    @Override
    public long count(@NonNull PersistenceCollection collection) {
        return this.mongo(collection).countDocuments();
    }

    @Override
    public boolean exists(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {
        return this.read(collection, path).isPresent();
    }

    @Override
    public boolean write(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull String document) {
        BasicDBObject data = BasicDBObject.parse(document);
        data.put("_id", path.getValue());
        Bson filters = Filters.in("_id", path.getValue());
        return this.mongo(collection).replaceOne(filters, data, REPLACE_OPTIONS).getModifiedCount() > 0;
    }

    @Override
    public long write(@NonNull PersistenceCollection collection, @NonNull Map<PersistencePath, String> entities) {

        if (entities.isEmpty()) {
            return 0;
        }

        List<BasicDBObject> documents = new ArrayList<>();
        entities.forEach((path, document) -> {
            BasicDBObject data = BasicDBObject.parse(document);
            data.put("_id", path.getValue());
            documents.add(data);
        });

        MongoCollection<BasicDBObject> mongo = this.mongo(collection);
        BulkWriteResult result = mongo.bulkWrite(documents.stream()
            .<WriteModel<BasicDBObject>>map(document -> new ReplaceOneModel<>(
                Filters.in("_id", document.get("_id")),
                document,
                REPLACE_OPTIONS
            ))
            .collect(Collectors.toList()));

        return result.getModifiedCount();
    }

    @Override
    public boolean delete(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {
        return this.mongo(collection)
            .deleteOne(Filters.eq("_id", path.getValue()))
            .getDeletedCount() > 0;
    }

    @Override
    public long delete(@NonNull PersistenceCollection collection, @NonNull Collection<PersistencePath> paths) {

        List<String> keys = paths.stream()
            .map(PersistencePath::getValue)
            .collect(Collectors.toList());

        return this.mongo(collection)
            .deleteMany(Filters.in("_id", keys))
            .getDeletedCount();
    }

    @Override
    public boolean deleteAll(@NonNull PersistenceCollection collection) {
        this.mongo(collection).drop();
        return true;
    }

    @Override
    public long deleteAll() {
        throw new RuntimeException("Not implemented yet");
    }

    @Override
    public void close() throws IOException {
        this.getClient().close();
    }

    protected MongoCollection<BasicDBObject> mongo(PersistenceCollection collection) {
        String identifier = this.getBasePath().sub(collection).toSqlIdentifier();
        return this.getDatabase().getCollection(identifier, BasicDBObject.class);
    }

    protected PersistenceEntity<String> transformMongoObject(PersistenceCollection collection, BasicDBObject object) {
        PersistencePath path = PersistencePath.of(object.getString("_id"));
        object.remove("_id"); // don't need this anymore
        return new PersistenceEntity<>(path, object.toJson());
    }
}
