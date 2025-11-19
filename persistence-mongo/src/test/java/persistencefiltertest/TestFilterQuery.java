package persistencefiltertest;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import eu.okaeri.configs.json.simple.JsonSimpleConfigurer;
import eu.okaeri.persistence.PersistenceCollection;
import eu.okaeri.persistence.PersistencePath;
import eu.okaeri.persistence.document.DocumentPersistence;
import eu.okaeri.persistence.document.index.IndexProperty;
import eu.okaeri.persistence.mongo.MongoPersistence;
import eu.okaeri.persistence.repository.RepositoryDeclaration;
import eu.okaeri.persistencetest.AbstractFilterQueryTest;
import lombok.Getter;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;

import static eu.okaeri.persistence.filter.condition.Condition.on;
import static eu.okaeri.persistence.filter.predicate.SimplePredicate.eq;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestFilterQuery extends AbstractFilterQueryTest {

    @Container
    private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7"))
        .withReuse(true);

    private static final PersistenceCollection USER_COLLECTION = PersistenceCollection.of("users")
        .index(IndexProperty.of("name"))
        .index(IndexProperty.of("exp"));

    private static final PersistenceCollection PROFILE_COLLECTION = PersistenceCollection.of("profiles")
        .index(IndexProperty.of("name"))
        .index(IndexProperty.parse("profile.age"));

    private @Getter UserRepository userRepository;
    private @Getter UserProfileRepository profileRepository;
    private DocumentPersistence persistence;
    private MongoClient mongoClient;

    @BeforeAll
    public void prepare() {
        System.setProperty("okaeri.platform.debug", "true");
        PersistencePath basePath = PersistencePath.of("");

        this.mongoClient = MongoClients.create(MONGO.getConnectionString());
        this.persistence = new DocumentPersistence(
            new MongoPersistence(basePath, this.mongoClient, "okaeri_persistence"),
            JsonSimpleConfigurer::new
        );

        // Create repositories
        this.userRepository = RepositoryDeclaration.of(UserRepository.class)
            .newProxy(this.persistence, USER_COLLECTION, TestFilterQuery.class.getClassLoader());
        this.profileRepository = RepositoryDeclaration.of(UserProfileRepository.class)
            .newProxy(this.persistence, PROFILE_COLLECTION, TestFilterQuery.class.getClassLoader());

        // Setup test data using shared method from base class
        this.setupTestData();
    }

    @AfterAll
    public void shutdown() throws IOException {
        assertEquals(1, this.userRepository.delete(q -> q.where(on("name", eq("tester")))));
        assertEquals(2, this.userRepository.count());

        this.persistence.close();
        if (this.mongoClient != null) {
            this.mongoClient.close();
        }
    }
}
