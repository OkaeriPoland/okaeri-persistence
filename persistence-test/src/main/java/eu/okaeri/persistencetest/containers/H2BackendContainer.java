package eu.okaeri.persistencetest.containers;

import com.zaxxer.hikari.HikariConfig;
import eu.okaeri.configs.json.simple.JsonSimpleConfigurer;
import eu.okaeri.persistence.document.DocumentPersistence;
import eu.okaeri.persistence.jdbc.H2Persistence;
import eu.okaeri.persistence.jdbc.commons.JdbcHelper;

import java.io.File;

/**
 * H2 file-based database backend container.
 * Uses temp file for more realistic performance testing.
 */
public class H2BackendContainer implements BackendContainer {

    private final String dbPath;

    public H2BackendContainer() {
        this.dbPath = System.getProperty("java.io.tmpdir") + File.separator + "h2_test_" + System.nanoTime();
    }

    @Override
    public String getName() {
        return "H2";
    }

    @Override
    public DocumentPersistence createPersistence() {
        HikariConfig hikariConfig = JdbcHelper.configureHikari(
            "jdbc:h2:" + this.dbPath + ";mode=mysql",
            "org.h2.Driver"
        );

        return new DocumentPersistence(new H2Persistence(hikariConfig, new JsonSimpleConfigurer()));
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
        // Clean up temp database files
        File dbFile = new File(this.dbPath + ".mv.db");
        if (dbFile.exists()) {
            dbFile.delete();
        }
        File traceFile = new File(this.dbPath + ".trace.db");
        if (traceFile.exists()) {
            traceFile.delete();
        }
    }

    @Override
    public String toString() {
        return getName();
    }
}
