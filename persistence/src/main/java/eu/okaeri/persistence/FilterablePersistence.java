package eu.okaeri.persistence;

import eu.okaeri.persistence.document.Document;
import eu.okaeri.persistence.filter.DeleteFilter;
import eu.okaeri.persistence.filter.FindFilter;

import java.util.stream.Stream;

/**
 * Capability interface for backends that support native filtering.
 * <p>
 * Provides WHERE, ORDER BY, LIMIT, SKIP operations that are executed
 * by the database/storage backend rather than in application memory.
 * <p>
 * Backends that don't implement this interface will have filtering
 * handled by loading all documents and filtering in memory.
 */
public interface FilterablePersistence extends Persistence {

    /**
     * Find entities matching a filter.
     * Supports WHERE conditions, ORDER BY, LIMIT, and SKIP.
     *
     * @param collection Target collection
     * @param filter     Find filter with conditions and ordering
     * @return Stream of matching entities
     */
    Stream<PersistenceEntity<Document>> find(PersistenceCollection collection, FindFilter filter);

    /**
     * Delete entities matching a filter.
     *
     * @param collection Target collection
     * @param filter     Delete filter with WHERE condition
     * @return Number of entities deleted
     */
    long delete(PersistenceCollection collection, DeleteFilter filter);
}
