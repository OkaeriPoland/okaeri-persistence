package eu.okaeri.persistencetest.containers;

import com.zaxxer.hikari.HikariConfig;
import eu.okaeri.configs.json.simple.JsonSimpleConfigurer;
import eu.okaeri.persistence.PersistencePath;
import eu.okaeri.persistence.document.DocumentPersistence;
import eu.okaeri.persistence.jdbc.PostgresPersistence;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * PostgreSQL database backend container using testcontainers.
 */
public class PostgresBackendContainer implements BackendContainer {

    private static final PostgreSQLContainer<?> POSTGRES;

    static {
        POSTGRES = new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"))
            .withDatabaseName("okaeri_persistence")
            .withUsername("postgres")
            .withPassword("test")
            .withReuse(true);

        POSTGRES.start();
    }

    @Override
    public String getName() {
        return "PostgreSQL 16";
    }

    @Override
    public DocumentPersistence createPersistence() {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(POSTGRES.getJdbcUrl());
        hikariConfig.setUsername(POSTGRES.getUsername());
        hikariConfig.setPassword(POSTGRES.getPassword());
        hikariConfig.setDriverClassName("org.postgresql.Driver");
        hikariConfig.setMaximumPoolSize(5);
        hikariConfig.setMinimumIdle(1);
        hikariConfig.setConnectionTimeout(30000);

        return new DocumentPersistence(
            new PostgresPersistence(PersistencePath.of(""), hikariConfig),
            JsonSimpleConfigurer::new
        );
    }

    @Override
    public boolean requiresContainer() {
        return true;
    }

    @Override
    public BackendType getType() {
        return BackendType.POSTGRESQL;
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
