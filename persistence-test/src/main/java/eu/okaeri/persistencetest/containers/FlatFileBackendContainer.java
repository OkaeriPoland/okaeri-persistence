package eu.okaeri.persistencetest.containers;

import eu.okaeri.configs.json.simple.JsonSimpleConfigurer;
import eu.okaeri.persistence.document.DocumentPersistence;
import eu.okaeri.persistence.flat.FlatPersistence;
import lombok.Cleanup;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Stream;

/**
 * Flat file backend container using temporary directory.
 */
public class FlatFileBackendContainer implements BackendContainer {

    private Path tempDir;

    public Path getTempDir() {
        return this.tempDir;
    }

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

        return new DocumentPersistence(FlatPersistence.builder()
            .storageDir(this.tempDir.toFile())
            .configurer(new JsonSimpleConfigurer())
            .build());
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
        if ((this.tempDir != null) && Files.exists(this.tempDir)) {
            @Cleanup Stream<Path> walk = Files.walk(this.tempDir);
            walk
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
        }
    }

    @Override
    public String toString() {
        return this.getName();
    }
}
