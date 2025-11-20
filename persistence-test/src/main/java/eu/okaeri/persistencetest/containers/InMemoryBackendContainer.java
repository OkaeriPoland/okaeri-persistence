package eu.okaeri.persistencetest.containers;

import eu.okaeri.persistence.document.DocumentPersistence;
import eu.okaeri.persistence.document.InMemoryDocumentPersistence;

/**
 * InMemory backend container for E2E tests.
 * Pure in-memory storage - fastest backend for testing.
 */
public class InMemoryBackendContainer implements BackendContainer {

    private final InMemoryDocumentPersistence persistence;

    public InMemoryBackendContainer() {
        this.persistence = new InMemoryDocumentPersistence();
    }

    @Override
    public String getName() {
        return "InMemory";
    }

    @Override
    public DocumentPersistence createPersistence() {
        return persistence;
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
        return getName();
    }
}
