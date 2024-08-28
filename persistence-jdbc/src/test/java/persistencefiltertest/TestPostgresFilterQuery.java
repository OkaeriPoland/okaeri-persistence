package persistencefiltertest;

import com.zaxxer.hikari.HikariConfig;
import eu.okaeri.configs.json.simple.JsonSimpleConfigurer;
import eu.okaeri.persistence.PersistenceCollection;
import eu.okaeri.persistence.PersistencePath;
import eu.okaeri.persistence.document.Document;
import eu.okaeri.persistence.document.DocumentPersistence;
import eu.okaeri.persistence.filter.condition.Condition;
import eu.okaeri.persistence.jdbc.PostgresPersistence;
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
public class TestPostgresFilterQuery {

    private static final PersistenceCollection USER_COLLECTION = PersistenceCollection.of("users");

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

        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException ignored) {
        }
        System.setProperty("okaeri.platform.debug", "true");

        PersistencePath basePath = PersistencePath.of("");
        HikariConfig hikari = new HikariConfig();
        hikari.setJdbcUrl("jdbc:postgresql://localhost:5432/okaeri_persistence?currentSchema=public&user=postgres&password=1234");

        this.persistence = new DocumentPersistence(
            new PostgresPersistence(basePath, hikari),
            JsonSimpleConfigurer::new
        );

        this.persistence.registerCollection(USER_COLLECTION);
        this.persistence.deleteAll(USER_COLLECTION);
        this.persistence.write(USER_COLLECTION, PersistencePath.of(UUID.randomUUID()), new User("tester", 123));
        this.persistence.write(USER_COLLECTION, PersistencePath.of(UUID.randomUUID()), new User("tester2", 456));
        this.persistence.write(USER_COLLECTION, PersistencePath.of(UUID.randomUUID()), new User("tester3", 123));

        this.repository = RepositoryDeclaration.of(UserRepository.class).newProxy(this.persistence, USER_COLLECTION, TestPostgresFilterQuery.class.getClassLoader());
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

    @Test
    public void test_filter_1_limit_1() {
        List<User> users = this.repository
            .find(q -> q
                .where(and("exp", eq(123)))
                .limit(1))
            .collect(Collectors.toList());
        assertEquals(1, users.size());
    }

    @Test
    public void test_filter_1_skip_1_limit_1() {
        List<User> users = this.repository
            .find(q -> q
                .where(and("exp", eq(123)))
                .skip(1)
                .limit(1))
            .collect(Collectors.toList());
        assertEquals(1, users.size());
    }

    @Test
    public void test_filter_1_skip_2() {
        List<User> users = this.repository
            .find(q -> q
                .where(and("exp", eq(123)))
                .skip(2))
            .collect(Collectors.toList());
        assertEquals(0, users.size());
    }
}
