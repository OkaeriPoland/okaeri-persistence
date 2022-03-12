package eu.okaeri.persistence.mongo;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Indexes;
import eu.okaeri.configs.serdes.OkaeriSerdesPack;
import eu.okaeri.persistence.PersistenceCollection;
import eu.okaeri.persistence.PersistenceEntity;
import eu.okaeri.persistence.PersistencePath;
import eu.okaeri.persistence.document.ConfigurerProvider;
import eu.okaeri.persistence.document.Document;
import eu.okaeri.persistence.document.DocumentPersistence;
import eu.okaeri.persistence.document.index.IndexProperty;
import lombok.Getter;
import lombok.NonNull;
import lombok.SneakyThrows;

import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class MongoPersistence extends DocumentPersistence {

    private static final Logger LOGGER = Logger.getLogger(MongoPersistence.class.getSimpleName());

    @Getter private MongoClient client;
    @Getter private MongoDatabase database;

    private PersistencePath basePath;

    public MongoPersistence(@NonNull PersistencePath basePath, @NonNull MongoClient client, @NonNull String databaseName, @NonNull ConfigurerProvider configurerProvider, @NonNull OkaeriSerdesPack... serdesPacks) {
        super(configurerProvider, serdesPacks);
        this.basePath = basePath;
        // init
        this.connect(client, databaseName);
    }

    @SneakyThrows
    private void connect(@NonNull MongoClient client, @NonNull String databaseName) {
        do {
            try {
                this.client = client;
                this.database = client.getDatabase(databaseName);
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
    public void setAutoFlush(boolean state) {
    }

    @Override
    public void flush() {
    }

    @Override
    public PersistencePath getBasePath() {
        return this.basePath;
    }

    @Override
    public void registerCollection(@NonNull PersistenceCollection collection) {
        MongoCollection<BasicDBObject> mongo = this.mongo(collection);
        for (IndexProperty index : collection.getIndexes()) {
            mongo.createIndex(Indexes.hashed(index.toMongoPath()));
        }
    }

    @Override
    public long fixIndexes(@NonNull PersistenceCollection collection) {
        return 0;
    }

    @Override
    public boolean updateIndex(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull IndexProperty property, String identity) {
        return false;
    }

    @Override
    public boolean updateIndex(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull Document document) {
        return false;
    }

    @Override
    public boolean updateIndex(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {
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
    public Optional<Document> read(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {
        return Optional.ofNullable(this.mongo(collection).find()
            .filter(Filters.eq("_id", path.getValue()))
            .map(object -> this.transformDocument(collection, object))
            .first());
    }

    @Override
    public Map<PersistencePath, Document> read(@NonNull PersistenceCollection collection, @NonNull Collection<PersistencePath> paths) {

        if (paths.isEmpty()) {
            return Collections.emptyMap();
        }

        List<String> keys = paths.stream()
            .map(PersistencePath::getValue)
            .collect(Collectors.toList());

        return this.mongo(collection).find()
            .filter(Filters.in("_id", keys))
            .map(object -> this.transformDocument(collection, object))
            .into(new ArrayList<>())
            .stream()
            .collect(Collectors.toMap(Document::getPath, Function.identity()));
    }

    @Override
    public Map<PersistencePath, Document> readAll(@NonNull PersistenceCollection collection) {
        return this.streamAll(collection).collect(Collectors.toMap(
            PersistenceEntity::getPath,
            PersistenceEntity::getValue
        ));
    }

    @Override
    public Stream<PersistenceEntity<Document>> readByProperty(@NonNull PersistenceCollection collection, @NonNull PersistencePath property, Object propertyValue) {
        return StreamSupport.stream(this.mongo(collection).find()
            .filter(Filters.in(property.toMongoPath(), propertyValue))
            .map(object -> this.transformDocument(collection, object))
            .map(document -> new PersistenceEntity<>(document.getPath(), document))
            .spliterator(), false);
    }

    @Override
    public Stream<PersistenceEntity<Document>> streamAll(@NonNull PersistenceCollection collection) {
        return StreamSupport.stream(this.mongo(collection).find()
            .map(object -> this.transformDocument(collection, object))
            .map(document -> new PersistenceEntity<>(document.getPath(), document))
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
    public boolean write(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull Document document) {
        BasicDBObject data = BasicDBObject.parse(document.saveToString());
        data.put("_id", path.getValue());
        this.mongo(collection).insertOne(data);
        return true;
    }

    @Override
    public long write(@NonNull PersistenceCollection collection, @NonNull Map<PersistencePath, Document> entities) {

        if (entities.isEmpty()) {
            return 0;
        }

        List<BasicDBObject> documents = new ArrayList<>();
        entities.forEach((path, document) -> {
            BasicDBObject data = BasicDBObject.parse(document.saveToString());
            data.put("_id", path.getValue());
            documents.add(data);
        });

        this.mongo(collection).insertMany(documents);
        return entities.size();
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

    protected Document transformDocument(PersistenceCollection collection, BasicDBObject object) {
        PersistencePath path = PersistencePath.of(object.getString("_id"));
        object.remove("_id"); // don't need this anymore
        return (Document) this.createDocument(collection, path).load(object.toJson());
    }
}
