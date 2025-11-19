package persistencefiltertest;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
public abstract class AbstractMongoTest {

    @Container
    protected static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7")).withReuse(true);

    protected static String getDatabaseName() {
        return "okaeri_persistence";
    }

    protected static MongoClient createMongoClient() {
        return MongoClients.create(MONGO.getConnectionString());
    }

    protected static String getConnectionString() {
        return MONGO.getConnectionString();
    }

    @BeforeAll
    static void waitForContainer() {
        MONGO.isRunning();
    }
}
