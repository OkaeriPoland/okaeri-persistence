package eu.okaeri.persistencetest.containers;

import eu.okaeri.configs.json.simple.JsonSimpleConfigurer;
import eu.okaeri.persistence.PersistencePath;
import eu.okaeri.persistence.document.DocumentPersistence;
import eu.okaeri.persistence.flat.FlatPersistence;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

/**
 * Flat file backend container using temporary directory.
 */
public class FlatFileBackendContainer implements BackendContainer {

    private Path tempDir;

    @Override
    public String getName() {
        return "Flat Files";
    }

    @Override
    public DocumentPersistence createPersistence() {
        try {
            this.tempDir = Files.createTempDirectory("okaeri-persistence-test-");
        } catch (IOException e) {
            throw new RuntimeException("Failed to create temp directory for flat file persistence", e);
        }

        return new DocumentPersistence(
            new FlatPersistence(this.tempDir.toFile(), ".json", JsonSimpleConfigurer::new),
            JsonSimpleConfigurer::new
        );
    }

    @Override
    public boolean requiresContainer() {
        return false;
    }

    @Override
    public BackendType getType() {
        return BackendType.FLAT_FILE;
    }

    @Override
    public void close() throws Exception {
        if (this.tempDir != null && Files.exists(this.tempDir)) {
            Files.walk(this.tempDir)
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
        }
    }

    @Override
    public String toString() {
        return getName();
    }
}
