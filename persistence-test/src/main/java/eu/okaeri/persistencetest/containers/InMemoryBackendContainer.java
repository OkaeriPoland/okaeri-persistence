package eu.okaeri.persistencetest.containers;

import eu.okaeri.configs.json.simple.JsonSimpleConfigurer;
import eu.okaeri.persistence.document.DocumentPersistence;
import eu.okaeri.persistence.document.InMemoryPersistence;

/**
 * InMemory backend container for E2E tests.
 * Pure in-memory storage - fastest backend for testing.
 */
public class InMemoryBackendContainer implements BackendContainer {

    @Override
    public String getName() {
        return "InMemory";
    }

    @Override
    public InMemoryPersistence.Builder createPersistenceBuilder() {
        return InMemoryPersistence.builder();
    }

    @Override
    public DocumentPersistence createPersistence() {
        return new DocumentPersistence(this.createPersistenceBuilder()
            .configurer(new JsonSimpleConfigurer())
            .build());
    }

    @Override
    public boolean requiresContainer() {
        return false;
    }

    @Override
    public BackendType getType() {
        return BackendType.IN_MEMORY;
    }

    @Override
    public void close() {
        // No cleanup needed for in-memory
    }

    @Override
    public String toString() {
        return this.getName();
    }
}
