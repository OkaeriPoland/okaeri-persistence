package eu.okaeri.persistencetest.containers;

import com.zaxxer.hikari.HikariConfig;
import eu.okaeri.configs.json.simple.JsonSimpleConfigurer;
import eu.okaeri.persistence.PersistencePath;
import eu.okaeri.persistence.document.DocumentPersistence;
import eu.okaeri.persistence.jdbc.H2Persistence;
import eu.okaeri.persistence.jdbc.commons.JdbcHelper;

/**
 * H2 in-memory database backend container.
 * No Docker container required - uses in-memory database.
 */
public class H2BackendContainer implements BackendContainer {

    @Override
    public String getName() {
        return "H2";
    }

    @Override
    public DocumentPersistence createPersistence() {
        HikariConfig hikariConfig = JdbcHelper.configureHikari(
            "jdbc:h2:mem:test_" + System.nanoTime() + ";mode=mysql",
            "org.h2.Driver"
        );

        return new DocumentPersistence(
            new H2Persistence(PersistencePath.of(""), hikariConfig),
            JsonSimpleConfigurer::new
        );
    }

    @Override
    public boolean requiresContainer() {
        return false;
    }

    @Override
    public BackendType getType() {
        return BackendType.H2;
    }

    @Override
    public void close() throws Exception {
        // No cleanup needed for in-memory database
    }

    @Override
    public String toString() {
        return getName();
    }
}
