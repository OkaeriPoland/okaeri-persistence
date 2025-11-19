package persistencefiltertest;

import com.zaxxer.hikari.HikariConfig;
import eu.okaeri.configs.json.simple.JsonSimpleConfigurer;
import eu.okaeri.persistence.PersistenceCollection;
import eu.okaeri.persistence.PersistencePath;
import eu.okaeri.persistence.document.DocumentPersistence;
import eu.okaeri.persistence.document.index.IndexProperty;
import eu.okaeri.persistence.jdbc.PostgresPersistence;
import eu.okaeri.persistence.repository.RepositoryDeclaration;
import eu.okaeri.persistencetest.AbstractFilterQueryTest;
import lombok.Getter;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;

import static eu.okaeri.persistence.filter.condition.Condition.on;
import static eu.okaeri.persistence.filter.predicate.SimplePredicate.eq;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestPostgresFilterQuery extends AbstractFilterQueryTest {

    @Container
    private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"))
        .withDatabaseName("okaeri_persistence")
        .withUsername("postgres")
        .withPassword("test")
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

    @BeforeAll
    public void prepare() {
        System.setProperty("okaeri.platform.debug", "true");

        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(POSTGRES.getJdbcUrl());
        hikariConfig.setUsername(POSTGRES.getUsername());
        hikariConfig.setPassword(POSTGRES.getPassword());
        hikariConfig.setDriverClassName("org.postgresql.Driver");
        hikariConfig.setMaximumPoolSize(5);
        hikariConfig.setMinimumIdle(1);
        hikariConfig.setConnectionTimeout(30000);

        PersistencePath basePath = PersistencePath.of("");
        this.persistence = new DocumentPersistence(
            new PostgresPersistence(basePath, hikariConfig),
            JsonSimpleConfigurer::new
        );

        this.persistence.registerCollection(USER_COLLECTION);
        this.persistence.registerCollection(PROFILE_COLLECTION);

        this.userRepository = RepositoryDeclaration.of(UserRepository.class)
            .newProxy(this.persistence, USER_COLLECTION, TestPostgresFilterQuery.class.getClassLoader());
        this.profileRepository = RepositoryDeclaration.of(UserProfileRepository.class)
            .newProxy(this.persistence, PROFILE_COLLECTION, TestPostgresFilterQuery.class.getClassLoader());

        this.setupTestData();
    }

    @AfterAll
    public void shutdown() throws IOException {
        assertEquals(1, this.userRepository.delete(q -> q.where(on("name", eq("tester")))));
        assertEquals(2, this.userRepository.count());

        this.persistence.close();
    }
}
