package eu.okaeri.persistence.mongo;

import com.mongodb.BasicDBObject;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.*;
import com.mongodb.client.result.UpdateResult;
import eu.okaeri.configs.configurer.Configurer;
import eu.okaeri.configs.serdes.OkaeriSerdes;
import eu.okaeri.persistence.*;
import eu.okaeri.persistence.document.*;
import eu.okaeri.persistence.filter.DeleteFilter;
import eu.okaeri.persistence.filter.FindFilter;
import eu.okaeri.persistence.filter.UpdateFilter;
import eu.okaeri.persistence.filter.operation.UpdateOperation;
import eu.okaeri.persistence.filter.renderer.FilterRenderer;
import eu.okaeri.persistence.mongo.filter.MongoFilterRenderer;
import eu.okaeri.persistence.mongo.filter.MongoUpdateRenderer;
import eu.okaeri.persistence.util.ConnectionRetry;
import lombok.Getter;
import lombok.NonNull;
import org.bson.conversions.Bson;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * MongoDB persistence backend with full native filtering, indexing, and updates.
 */
public class MongoPersistence implements Persistence, FilterablePersistence, StreamablePersistence, UpdatablePersistence {

    private static final boolean DEBUG = Boolean.parseBoolean(System.getProperty("okaeri.platform.debug", "false"));
    private static final Logger LOGGER = Logger.getLogger(MongoPersistence.class.getSimpleName());
    private static final ReplaceOptions REPLACE_OPTIONS = new ReplaceOptions().upsert(true);
    private static final FilterRenderer FILTER_RENDERER = new MongoFilterRenderer();
    private static final MongoUpdateRenderer UPDATE_RENDERER = new MongoUpdateRenderer();

    private final @Getter PersistencePath basePath;
    private @Getter MongoClient client;
    private @Getter MongoDatabase database;

    private final @Getter DocumentSerializer serializer;
    private final Map<String, PersistenceCollection> knownCollections = new ConcurrentHashMap<>();

    public MongoPersistence(@NonNull PersistencePath basePath, @NonNull MongoClient client, @NonNull String databaseName,
                            @NonNull Configurer configurer, @NonNull OkaeriSerdes... serdes) {
        this.basePath = basePath;
        this.serializer = new DocumentSerializer(configurer, serdes);
        this.connect(client, databaseName);
    }

    public MongoPersistence(@NonNull MongoClient client, @NonNull String databaseName,
                            @NonNull Configurer configurer, @NonNull OkaeriSerdes... serdes) {
        this(PersistencePath.of(""), client, databaseName, configurer, serdes);
    }

    public MongoPersistence(@NonNull PersistencePath basePath, @NonNull MongoClient client, @NonNull String databaseName,
                            @NonNull DocumentSerializerConfig serializerConfig) {
        this.basePath = basePath;
        this.serializer = new DocumentSerializer(serializerConfig);
        this.connect(client, databaseName);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends PersistenceBuilder<Builder, MongoPersistence> {
        private MongoClient client;
        private String databaseName;

        public Builder client(@NonNull MongoClient client) {
            this.client = client;
            return this;
        }

        public Builder databaseName(@NonNull String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        @Override
        public MongoPersistence build() {
            if (this.client == null) {
                throw new IllegalStateException("client is required");
            }
            if (this.databaseName == null) {
                throw new IllegalStateException("databaseName is required");
            }

            DocumentSerializerConfig serializerConfig = this.buildSerializerConfig();
            PersistencePath path = this.resolveBasePath();

            return new MongoPersistence(path, this.client, this.databaseName, serializerConfig);
        }
    }

    private void connect(@NonNull MongoClient client, @NonNull String databaseName) {
        this.client = client;
        this.database = ConnectionRetry.of(this.basePath.getValue())
            .connector(() -> {
                MongoDatabase database = client.getDatabase(databaseName);
                database.runCommand(new org.bson.Document("ping", 1));
                return database;
            })
            .connect();
    }

    // ==================== COLLECTION MANAGEMENT ====================

    @Override
    public void registerCollection(@NonNull PersistenceCollection collection) {
        this.knownCollections.computeIfAbsent(collection.getValue(), key -> {
            // Create native indexes (only once per collection)
            if (!collection.getIndexes().isEmpty()) {
                List<IndexModel> indexModels = collection.getIndexes().stream()
                    .map(index -> new IndexModel(Indexes.ascending(index.getValue())))
                    .collect(Collectors.toList());
                this.mongo(collection).createIndexes(indexModels);
            }
            return collection;
        });
    }

    private void checkCollectionRegistered(@NonNull PersistenceCollection collection) {
        if (!this.knownCollections.containsKey(collection.getValue())) {
            throw new IllegalArgumentException("Collection not registered: " + collection.getValue());
        }
    }

    private MongoCollection<BasicDBObject> mongo(@NonNull PersistenceCollection collection) {
        String identifier = this.basePath.sub(collection).toSqlIdentifier();
        return this.database.getCollection(identifier, BasicDBObject.class);
    }

    private String debugQuery(@NonNull String query) {
        if (DEBUG) {
            System.out.println("[MongoDB] " + query);
        }
        return query;
    }

    // ==================== READ OPERATIONS ====================

    @Override
    public boolean exists(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {
        this.checkCollectionRegistered(collection);
        return this.read(collection, path).isPresent();
    }

    @Override
    public long count(@NonNull PersistenceCollection collection) {
        this.checkCollectionRegistered(collection);
        return this.mongo(collection).countDocuments();
    }

    @Override
    public Optional<Document> read(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {
        this.checkCollectionRegistered(collection);
        BasicDBObject result = this.mongo(collection).find()
            .filter(Filters.eq("_id", path.getValue()))
            .first();

        if (result == null) {
            return Optional.empty();
        }

        return Optional.of(this.transformMongoObject(collection, path, result));
    }

    @Override
    public Map<PersistencePath, Document> read(@NonNull PersistenceCollection collection, @NonNull Collection<PersistencePath> paths) {
        this.checkCollectionRegistered(collection);
        if (paths.isEmpty()) {
            return Collections.emptyMap();
        }

        List<String> keys = paths.stream()
            .map(PersistencePath::getValue)
            .collect(Collectors.toList());

        Map<PersistencePath, Document> result = new LinkedHashMap<>();
        this.mongo(collection).find()
            .filter(Filters.in("_id", keys))
            .forEach(object -> {
                PersistencePath path = PersistencePath.of(object.getString("_id"));
                result.put(path, this.transformMongoObject(collection, path, object));
            });

        return result;
    }

    @Override
    public Map<PersistencePath, Document> readAll(@NonNull PersistenceCollection collection) {
        return this.streamAll(collection).collect(Collectors.toMap(
            PersistenceEntity::getPath,
            PersistenceEntity::getValue
        ));
    }

    // ==================== STREAMING ====================

    @Override
    public Stream<PersistenceEntity<Document>> streamAll(@NonNull PersistenceCollection collection) {
        this.checkCollectionRegistered(collection);
        return StreamSupport.stream(this.mongo(collection).find()
            .map(object -> {
                PersistencePath path = PersistencePath.of(object.getString("_id"));
                return new PersistenceEntity<>(path, this.transformMongoObject(collection, path, object));
            })
            .spliterator(), false);
    }

    @Override
    public Stream<PersistenceEntity<Document>> stream(@NonNull PersistenceCollection collection, int batchSize) {
        this.checkCollectionRegistered(collection);
        return StreamSupport.stream(this.mongo(collection).find()
            .batchSize(batchSize)
            .map(object -> {
                PersistencePath path = PersistencePath.of(object.getString("_id"));
                return new PersistenceEntity<>(path, this.transformMongoObject(collection, path, object));
            })
            .spliterator(), false);
    }

    // ==================== FILTERING ====================

    @Override
    public Stream<PersistenceEntity<Document>> find(@NonNull PersistenceCollection collection, @NonNull FindFilter filter) {
        this.checkCollectionRegistered(collection);

        FindIterable<BasicDBObject> findIterable = this.mongo(collection).find();

        if (filter.getWhere() != null) {
            findIterable = findIterable.filter(org.bson.Document.parse(this.debugQuery(FILTER_RENDERER.renderCondition(filter.getWhere()))));
        }

        if (filter.hasOrderBy()) {
            findIterable = findIterable.sort(org.bson.Document.parse(this.debugQuery(FILTER_RENDERER.renderOrderBy(filter.getOrderBy()))));
        }

        if (filter.hasLimit()) {
            findIterable = findIterable.limit(filter.getLimit());
        }

        if (filter.hasSkip()) {
            findIterable = findIterable.skip(filter.getSkip());
        }

        return StreamSupport.stream(findIterable
            .map(object -> {
                PersistencePath path = PersistencePath.of(object.getString("_id"));
                return new PersistenceEntity<>(path, this.transformMongoObject(collection, path, object));
            })
            .spliterator(), false);
    }

    @Override
    public long delete(@NonNull PersistenceCollection collection, @NonNull DeleteFilter filter) {
        this.checkCollectionRegistered(collection);

        if (filter.getWhere() == null) {
            throw new IllegalArgumentException("DeleteFilter requires WHERE condition - use deleteAll() instead");
        }

        return this.mongo(collection)
            .deleteMany(org.bson.Document.parse(this.debugQuery(FILTER_RENDERER.renderCondition(filter.getWhere()))))
            .getDeletedCount();
    }

    // ==================== UPDATES ====================

    @Override
    public boolean updateOne(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull List<UpdateOperation> operations) {
        this.checkCollectionRegistered(collection);

        org.bson.Document updateDoc = UPDATE_RENDERER.render(operations);
        this.debugQuery(updateDoc.toJson());

        return this.mongo(collection)
            .updateOne(Filters.eq("_id", path.getValue()), updateDoc)
            .getMatchedCount() > 0;
    }

    @Override
    public Optional<Document> updateOneAndGet(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull List<UpdateOperation> operations) {
        this.checkCollectionRegistered(collection);

        org.bson.Document updateDoc = UPDATE_RENDERER.render(operations);
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

        return Optional.of(this.transformMongoObject(collection, path, result));
    }

    @Override
    public Optional<Document> getAndUpdateOne(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull List<UpdateOperation> operations) {
        this.checkCollectionRegistered(collection);

        org.bson.Document updateDoc = UPDATE_RENDERER.render(operations);
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

        return Optional.of(this.transformMongoObject(collection, path, result));
    }

    @Override
    public long update(@NonNull PersistenceCollection collection, @NonNull UpdateFilter filter) {
        this.checkCollectionRegistered(collection);

        if (filter.getWhere() == null) {
            throw new IllegalArgumentException("update requires a WHERE condition - use updateOne() for single document updates");
        }

        org.bson.Document updateDoc = UPDATE_RENDERER.render(filter.getOperations());
        this.debugQuery(updateDoc.toJson());
        org.bson.Document whereDoc = org.bson.Document.parse(this.debugQuery(FILTER_RENDERER.renderCondition(filter.getWhere())));

        return this.mongo(collection)
            .updateMany(whereDoc, updateDoc)
            .getModifiedCount();
    }

    // ==================== WRITE OPERATIONS ====================

    @Override
    public boolean write(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull Document document) {
        this.checkCollectionRegistered(collection);
        this.serializer.setupDocument(document, collection, path);

        String json = this.serializer.serialize(document);
        BasicDBObject data = BasicDBObject.parse(json);
        data.put("_id", path.getValue());
        Bson filters = Filters.eq("_id", path.getValue());

        UpdateResult result = this.mongo(collection).replaceOne(filters, data, REPLACE_OPTIONS);
        return (result.getModifiedCount() > 0) || (result.getUpsertedId() != null);
    }

    @Override
    public long write(@NonNull PersistenceCollection collection, @NonNull Map<PersistencePath, Document> documents) {
        if (documents.isEmpty()) {
            return 0;
        }
        this.checkCollectionRegistered(collection);

        List<ReplaceOneModel<BasicDBObject>> operations = new ArrayList<>();
        for (Map.Entry<PersistencePath, Document> entry : documents.entrySet()) {
            PersistencePath path = entry.getKey();
            Document document = entry.getValue();
            this.serializer.setupDocument(document, collection, path);

            String json = this.serializer.serialize(document);
            BasicDBObject data = BasicDBObject.parse(json);
            data.put("_id", path.getValue());

            operations.add(new ReplaceOneModel<>(
                Filters.eq("_id", data.get("_id")),
                data,
                REPLACE_OPTIONS
            ));
        }

        BulkWriteResult result = this.mongo(collection).bulkWrite(operations);
        return result.getModifiedCount() + result.getUpserts().size();
    }

    // ==================== DELETE OPERATIONS ====================

    @Override
    public boolean delete(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {
        this.checkCollectionRegistered(collection);
        return this.mongo(collection)
            .deleteOne(Filters.eq("_id", path.getValue()))
            .getDeletedCount() > 0;
    }

    @Override
    public long delete(@NonNull PersistenceCollection collection, @NonNull Collection<PersistencePath> paths) {
        this.checkCollectionRegistered(collection);
        if (paths.isEmpty()) {
            return 0;
        }

        List<String> keys = paths.stream()
            .map(PersistencePath::getValue)
            .collect(Collectors.toList());

        return this.mongo(collection)
            .deleteMany(Filters.in("_id", keys))
            .getDeletedCount();
    }

    @Override
    public boolean deleteAll(@NonNull PersistenceCollection collection) {
        this.checkCollectionRegistered(collection);
        // Use deleteMany instead of drop() to preserve indexes
        return this.mongo(collection).deleteMany(new org.bson.Document()).getDeletedCount() >= 0;
    }

    @Override
    public long deleteAll() {
        return this.knownCollections.values().stream()
            .map(this::deleteAll)
            .filter(Predicate.isEqual(true))
            .count();
    }

    @Override
    public void close() throws IOException {
        this.client.close();
    }

    // ==================== HELPERS ====================

    private Document transformMongoObject(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull BasicDBObject object) {
        object.remove("_id"); // Don't need this in the JSON
        String json = object.toJson();
        return this.serializer.deserialize(collection, path, json);
    }
}
