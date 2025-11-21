package eu.okaeri.persistencetest.containers;

import com.zaxxer.hikari.HikariConfig;
import eu.okaeri.configs.json.simple.JsonSimpleConfigurer;
import eu.okaeri.persistence.PersistencePath;
import eu.okaeri.persistence.document.DocumentPersistence;
import eu.okaeri.persistence.jdbc.MariaDbPersistence;
import org.testcontainers.containers.MariaDBContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * MariaDB database backend container using testcontainers.
 */
public class MariaDbBackendContainer implements BackendContainer {

    private static final MariaDBContainer<?> MARIADB;

    static {
        MARIADB = new MariaDBContainer<>(DockerImageName.parse("mariadb:11"))
            .withDatabaseName("okaeri_persistence")
            .withUsername("test")
            .withPassword("test")
            .withCommand("--max-connections=500")
            .withReuse(true);

        MARIADB.start();
    }

    @Override
    public String getName() {
        return "MariaDB 11";
    }

    @Override
    public DocumentPersistence createPersistence() {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(MARIADB.getJdbcUrl());
        hikariConfig.setUsername(MARIADB.getUsername());
        hikariConfig.setPassword(MARIADB.getPassword());
        hikariConfig.setDriverClassName("org.mariadb.jdbc.Driver");
        hikariConfig.setMaximumPoolSize(5);
        hikariConfig.setMinimumIdle(1);
        hikariConfig.setConnectionTimeout(30000);

        return new DocumentPersistence(
            new MariaDbPersistence(PersistencePath.of(""), hikariConfig),
            JsonSimpleConfigurer::new
        );
    }

    @Override
    public boolean requiresContainer() {
        return true;
    }

    @Override
    public BackendType getType() {
        return BackendType.MARIADB;
    }

    @Override
    public void close() throws Exception {
        // Container is reused across tests, no cleanup needed
    }

    @Override
    public String toString() {
        return this.getName();
    }
}
