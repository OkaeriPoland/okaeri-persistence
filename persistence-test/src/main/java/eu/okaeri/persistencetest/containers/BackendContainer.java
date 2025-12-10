package eu.okaeri.persistencetest.containers;

import eu.okaeri.persistence.document.DocumentPersistence;

/**
 * Abstraction for backend-specific test container setup.
 * Each backend implementation provides its own container configuration.
 * ALL backends must support ALL repository features - this is the core goal
 * of the persistence abstraction layer.
 */
public interface BackendContainer extends AutoCloseable {

    /**
     * Display name for test output (e.g., "PostgreSQL 16", "H2", "MongoDB 7")
     * Used by JUnit for parameterized test display names.
     */
    String getName();

    /**
     * Create and configure a DocumentPersistence instance for this backend.
     * Called for each test to ensure clean state.
     */
    DocumentPersistence createPersistence();

    /**
     * Create a builder for this backend's persistence implementation.
     * Allows tests to customize configuration (e.g., add validators).
     * Returns null for backends that don't support builders.
     */
    default Object createPersistenceBuilder() {
        return null;
    }

    /**
     * Whether this backend requires Docker testcontainers.
     * Informational only - does not affect test execution.
     */
    boolean requiresContainer();

    /**
     * Get backend type for identification.
     */
    BackendType getType();

    /**
     * Cleanup resources (close containers, delete temp files, etc.)
     * Called after test execution.
     */
    @Override
    void close() throws Exception;

    /**
     * Backend types supported by the persistence library.
     */
    enum BackendType {
        IN_MEMORY,
        H2,
        POSTGRESQL,
        MARIADB,
        MONGODB,
        REDIS,
        FLAT_FILE
    }
}
