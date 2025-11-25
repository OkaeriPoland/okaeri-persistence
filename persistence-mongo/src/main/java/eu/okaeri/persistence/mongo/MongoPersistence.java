package eu.okaeri.persistence.mongo;

import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.*;
import eu.okaeri.persistence.PersistenceCollection;
import eu.okaeri.persistence.PersistenceEntity;
import eu.okaeri.persistence.PersistencePath;
import eu.okaeri.persistence.filter.DeleteFilter;
import eu.okaeri.persistence.filter.FindFilter;
import eu.okaeri.persistence.filter.UpdateFilter;
import eu.okaeri.persistence.filter.operation.UpdateOperation;
import eu.okaeri.persistence.filter.renderer.FilterRenderer;
import eu.okaeri.persistence.mongo.filter.MongoFilterRenderer;
import eu.okaeri.persistence.mongo.filter.MongoUpdateRenderer;
import eu.okaeri.persistence.raw.NativeRawPersistence;
import lombok.Getter;
import lombok.NonNull;
import lombok.SneakyThrows;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class MongoPersistence extends NativeRawPersistence {

    private static final Logger LOGGER = Logger.getLogger(MongoPersistence.class.getSimpleName());
    private static final ReplaceOptions REPLACE_OPTIONS = new ReplaceOptions().upsert(true);
    private static final FilterRenderer FILTER_RENDERER = new MongoFilterRenderer(); // TODO: allow customization
    private static final MongoUpdateRenderer UPDATE_RENDERER = new MongoUpdateRenderer();

    @Getter private MongoClient client;
    @Getter private MongoDatabase database;

    public MongoPersistence(@NonNull PersistencePath basePath, @NonNull MongoClient client, @NonNull String databaseName) {
        super(basePath);
        this.connect(client, databaseName);
    }

    public MongoPersistence(@NonNull MongoClient client, @NonNull String databaseName) {
        this(PersistencePath.of(""), client, databaseName);
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
    public void registerCollection(@NonNull PersistenceCollection collection) {

        if (!collection.getIndexes().isEmpty()) {
            List<IndexModel> indexModels = collection.getIndexes().stream()
                .map(index -> new IndexModel(Indexes.ascending(index.getValue())))
                .collect(Collectors.toList());
            this.mongo(collection).createIndexes(indexModels);
        }

        super.registerCollection(collection);
    }

    @Override
    public Stream<PersistenceEntity<String>> readByFilter(@NonNull PersistenceCollection collection, @NonNull FindFilter filter) {

        FindIterable<BasicDBObject> findIterable = this.mongo(collection).find();

        if (filter.getWhere() != null) {
            findIterable = findIterable.filter(Document.parse(this.debugQuery(FILTER_RENDERER.renderCondition(filter.getWhere()))));
        }

        if (filter.hasOrderBy()) {
            findIterable = findIterable.sort(Document.parse(this.debugQuery(FILTER_RENDERER.renderOrderBy(filter.getOrderBy()))));
        }

        if (filter.hasLimit()) {
            findIterable = findIterable.limit(filter.getLimit());
        }

        if (filter.hasSkip()) {
            findIterable = findIterable.skip(filter.getSkip());
        }

        Spliterator<PersistenceEntity<String>> iterator = findIterable
            .map(object -> this.transformMongoObject(collection, object))
            .spliterator();

        return StreamSupport.stream(iterator, false);
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
    public Stream<PersistenceEntity<String>> stream(@NonNull PersistenceCollection collection, int batchSize) {
        return StreamSupport.stream(this.mongo(collection).find()
            .batchSize(batchSize) // MongoDB driver cursor batch size hint
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

        return this.mongo(collection)
            .bulkWrite(entities.entrySet().stream()
                .map(entry -> {
                    BasicDBObject data = BasicDBObject.parse(entry.getValue());
                    data.put("_id", entry.getKey().getValue());
                    return data;
                })
                .map(document -> new ReplaceOneModel<>(
                    Filters.in("_id", document.get("_id")),
                    document,
                    REPLACE_OPTIONS
                ))
                .collect(Collectors.toList()))
            .getModifiedCount();
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
        // Use deleteMany instead of drop() to preserve indexes
        return this.mongo(collection).deleteMany(new Document()).getDeletedCount() >= 0;
    }

    @Override
    public long deleteAll() {
        return this.getKnownCollections().values().stream()
            .map(this::deleteAll)
            .filter(Boolean::booleanValue)
            .count();
    }

    @Override
    public long deleteByFilter(@NonNull PersistenceCollection collection, @NonNull DeleteFilter filter) {
        if (filter.getWhere() == null) {
            throw new IllegalArgumentException("deleteByFilter requires a WHERE condition - use deleteAll() to clear collection");
        }
        return this.mongo(collection)
            .deleteMany(Document.parse(this.debugQuery(FILTER_RENDERER.renderCondition(filter.getWhere()))))
            .getDeletedCount();
    }

    // ===== UPDATE OPERATIONS =====

    @Override
    public boolean updateOne(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull List<UpdateOperation> operations) {

        Document updateDoc = UPDATE_RENDERER.render(operations);
        this.debugQuery(updateDoc.toJson());

        // Use getMatchedCount() - returns true if document exists, even if no fields changed
        // This is correct for operations like $min/$max where the value might not change
        return this.mongo(collection)
            .updateOne(Filters.eq("_id", path.getValue()), updateDoc)
            .getMatchedCount() > 0;
    }

    @Override
    public Optional<String> updateOneAndGet(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull List<UpdateOperation> operations) {

        Document updateDoc = UPDATE_RENDERER.render(operations);
        this.debugQuery(updateDoc.toJson());

        BasicDBObject result = this.mongo(collection)
            .findOneAndUpdate(
                Filters.eq("_id", path.getValue()),
                updateDoc,
                new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER)
            );

        if (result == null) {
            return Optional.empty();
        }

        return Optional.of(this.transformMongoObject(collection, result).getValue());
    }

    @Override
    public Optional<String> getAndUpdateOne(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull List<UpdateOperation> operations) {

        Document updateDoc = UPDATE_RENDERER.render(operations);
        this.debugQuery(updateDoc.toJson());

        BasicDBObject result = this.mongo(collection)
            .findOneAndUpdate(
                Filters.eq("_id", path.getValue()),
                updateDoc,
                new FindOneAndUpdateOptions().returnDocument(ReturnDocument.BEFORE)
            );

        if (result == null) {
            return Optional.empty();
        }

        return Optional.of(this.transformMongoObject(collection, result).getValue());
    }

    @Override
    public long update(@NonNull PersistenceCollection collection, @NonNull UpdateFilter filter) {

        if (filter.getWhere() == null) {
            throw new IllegalArgumentException("update requires a WHERE condition - use updateOne() for single document updates");
        }

        Document updateDoc = UPDATE_RENDERER.render(filter.getOperations());
        this.debugQuery(updateDoc.toJson());
        Document whereDoc = Document.parse(this.debugQuery(FILTER_RENDERER.renderCondition(filter.getWhere())));

        return this.mongo(collection)
            .updateMany(whereDoc, updateDoc)
            .getModifiedCount();
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
