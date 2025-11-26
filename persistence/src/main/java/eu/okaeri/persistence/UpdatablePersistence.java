package eu.okaeri.persistence;

import eu.okaeri.persistence.document.Document;
import eu.okaeri.persistence.filter.UpdateFilter;
import eu.okaeri.persistence.filter.operation.UpdateOperation;

import java.util.List;
import java.util.Optional;

/**
 * Capability interface for backends that support atomic update operations.
 * <p>
 * Provides atomic field-level updates (SET, INCREMENT, PUSH, etc.) that
 * are executed by the database without read-modify-write cycles.
 * <p>
 * Backends that don't implement this interface will have updates
 * handled by reading the document, applying changes in memory, and writing back.
 */
public interface UpdatablePersistence extends Persistence {

    /**
     * Update a single entity atomically.
     *
     * @param collection Target collection
     * @param path       Entity path (key)
     * @param operations Update operations to apply
     * @return True if entity was found and updated
     */
    boolean updateOne(PersistenceCollection collection, PersistencePath path, List<UpdateOperation> operations);

    /**
     * Update a single entity and return the new (updated) version.
     *
     * @param collection Target collection
     * @param path       Entity path (key)
     * @param operations Update operations to apply
     * @return Updated entity, or empty if not found
     */
    Optional<Document> updateOneAndGet(PersistenceCollection collection, PersistencePath path, List<UpdateOperation> operations);

    /**
     * Get the current entity and then update it, returning the old version.
     *
     * @param collection Target collection
     * @param path       Entity path (key)
     * @param operations Update operations to apply
     * @return Old entity (before update), or empty if not found
     */
    Optional<Document> getAndUpdateOne(PersistenceCollection collection, PersistencePath path, List<UpdateOperation> operations);

    /**
     * Update multiple entities matching a filter.
     *
     * @param collection Target collection
     * @param filter     Update filter with WHERE clause and operations
     * @return Number of entities updated
     */
    long update(PersistenceCollection collection, UpdateFilter filter);
}
