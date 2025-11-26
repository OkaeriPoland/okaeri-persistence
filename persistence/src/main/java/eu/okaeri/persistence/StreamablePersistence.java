package eu.okaeri.persistence;

import eu.okaeri.persistence.document.Document;

import java.util.stream.Stream;

/**
 * Capability interface for backends that support memory-efficient batch streaming.
 * <p>
 * Backends that don't implement this interface will have streaming
 * handled by loading all documents via streamAll().
 */
public interface StreamablePersistence extends Persistence {

    /**
     * Stream entities with configurable batch size.
     * More memory efficient than streamAll() for large collections.
     * <p>
     * <b>IMPORTANT: This stream must be closed after use.</b>
     * Use try-with-resources or explicitly call close().
     *
     * @param collection Target collection
     * @param batchSize  Number of records per batch (hint for backend)
     * @return Stream of entities (must be closed after use)
     */
    Stream<PersistenceEntity<Document>> stream(PersistenceCollection collection, int batchSize);
}
