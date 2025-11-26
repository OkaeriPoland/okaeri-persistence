package eu.okaeri.persistencetest.containers;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import eu.okaeri.configs.json.simple.JsonSimpleConfigurer;
import eu.okaeri.persistence.document.DocumentPersistence;
import eu.okaeri.persistence.mongo.MongoPersistence;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * MongoDB database backend container using testcontainers.
 */
public class MongoBackendContainer implements BackendContainer {

    private static final MongoDBContainer MONGO;

    static {
        MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7"))
            .withReuse(true);

        MONGO.start();
    }

    @Override
    public String getName() {
        return "MongoDB 7";
    }

    @Override
    public DocumentPersistence createPersistence() {
        MongoClient mongoClient = MongoClients.create(MONGO.getConnectionString());

        return new DocumentPersistence(
            new MongoPersistence(mongoClient, "okaeri_persistence", JsonSimpleConfigurer::new)
        );
    }

    @Override
    public boolean requiresContainer() {
        return true;
    }

    @Override
    public BackendType getType() {
        return BackendType.MONGODB;
    }

    @Override
    public void close() throws Exception {
        // Container is reused across tests, no cleanup needed
    }

    @Override
    public String toString() {
        return getName();
    }
}
