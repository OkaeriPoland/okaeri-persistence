package persistencefiltertest;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import eu.okaeri.configs.json.simple.JsonSimpleConfigurer;
import eu.okaeri.persistence.PersistenceCollection;
import eu.okaeri.persistence.PersistencePath;
import eu.okaeri.persistence.document.Document;
import eu.okaeri.persistence.document.DocumentPersistence;
import eu.okaeri.persistence.filter.condition.Condition;
import eu.okaeri.persistence.mongo.MongoPersistence;
import eu.okaeri.persistence.repository.DocumentRepository;
import eu.okaeri.persistence.repository.RepositoryDeclaration;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static eu.okaeri.persistence.filter.condition.Condition.and;
import static eu.okaeri.persistence.filter.predicate.SimplePredicate.eq;
import static org.junit.jupiter.api.Assertions.assertEquals;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestFilterQuery {

    private static final PersistenceCollection USER_COLLECTION = PersistenceCollection.of("user");

    private DocumentPersistence persistence;
    private UserRepository repository;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode(callSuper = false)
    class User extends Document {
        private String name;
        private int exp;
    }

    static interface UserRepository extends DocumentRepository<UUID, User> {
    }

    @BeforeAll
    public void prepare() {

        System.setProperty("okaeri.platform.debug", "true");
        PersistencePath basePath = PersistencePath.of("");
        ConnectionString mongoUri = new ConnectionString("mongodb://localhost:27017/okaeri_persistence");
        MongoClient mongoClient = MongoClients.create(mongoUri);

        this.persistence = new DocumentPersistence(
            new MongoPersistence(basePath, mongoClient, mongoUri.getDatabase()),
            JsonSimpleConfigurer::new
        );

        this.persistence.deleteAll(USER_COLLECTION);
        this.persistence.write(USER_COLLECTION, PersistencePath.of(UUID.randomUUID()), new User("tester", 123));
        this.persistence.write(USER_COLLECTION, PersistencePath.of(UUID.randomUUID()), new User("tester2", 456));
        this.persistence.write(USER_COLLECTION, PersistencePath.of(UUID.randomUUID()), new User("tester3", 123));

        this.repository = RepositoryDeclaration.of(UserRepository.class).newProxy(this.persistence, USER_COLLECTION, TestFilterQuery.class.getClassLoader());
    }

    @AfterAll
    public void shutdown() throws IOException {
        this.persistence.close();
    }

    @Test
    public void test_filter_0() {
        Condition filter = and("name", eq("tester2"));
        List<User> users = this.repository.find(filter).collect(Collectors.toList());

        assertEquals(1, users.size());
        assertEquals("tester2", users.get(0).getName());
        assertEquals(456, users.get(0).getExp());
    }

    @Test
    public void test_filter_1() {
        Condition filter = and("exp", eq(123));
        List<User> users = this.repository.find(filter).collect(Collectors.toList());

        assertEquals(2, users.size());
    }
}