package eu.okaeri.persistence.repository.query;

/**
 * Type of repository operation extracted from method name prefix.
 */
public enum MethodOperation {
    /**
     * Read operation: find, read, get, query, stream, findAll, streamAll, getAll
     * Returns entities matching conditions.
     */
    FIND,

    /**
     * Count operation: count
     * Returns number of entities matching conditions.
     */
    COUNT,

    /**
     * Exists check operation: exists
     * Returns boolean indicating if any entity matches conditions.
     */
    EXISTS,

    /**
     * Delete operation: delete, remove
     * Deletes entities matching conditions, returns count.
     */
    DELETE
}
