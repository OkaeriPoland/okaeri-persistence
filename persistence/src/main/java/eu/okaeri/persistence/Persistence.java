package eu.okaeri.persistence;

import eu.okaeri.persistence.document.Document;
import eu.okaeri.persistence.document.DocumentSerializer;

import java.io.Closeable;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Core persistence interface for document storage.
 * <p>
 * Backends implement this interface directly. For additional capabilities
 * (filtering, atomic updates), backends implement capability interfaces:
 * <ul>
 *   <li>{@link FilterablePersistence} - WHERE queries, ORDER BY, LIMIT/SKIP</li>
 *   <li>{@link UpdatablePersistence} - Atomic field updates (SET, INCREMENT, PUSH, etc.)</li>
 * </ul>
 * <p>
 * Index management is internal to each backend - no index methods exposed.
 * Backends handle indexing transparently during write/delete operations.
 */
public interface Persistence extends Closeable {

    // ==================== COLLECTION MANAGEMENT ====================

    /**
     * Register a collection to be tracked by persistence.
     * Backends may create tables, indexes, or other structures as needed.
     *
     * @param collection Collection to be registered
     */
    void registerCollection(PersistenceCollection collection);

    /**
     * Get the base storage path prefix.
     * <p>
     * Examples:
     * <ul>
     *   <li>Redis: key prefix "{basePath}:{collection}:{id}"</li>
     *   <li>MariaDB: table name "{basePath}_{collection}"</li>
     *   <li>Flat: directory "{basePath}/{collection}/"</li>
     * </ul>
     *
     * @return Base storage path
     */
    PersistencePath getBasePath();

    /**
     * Get the document serializer used by this backend.
     *
     * @return The document serializer
     */
    DocumentSerializer getSerializer();

    // ==================== READ OPERATIONS ====================

    /**
     * Check if an entity exists at the given path.
     *
     * @param collection Target collection
     * @param path       Entity path (key)
     * @return True if entity exists
     */
    boolean exists(PersistenceCollection collection, PersistencePath path);

    /**
     * Count all entities in a collection.
     *
     * @param collection Target collection
     * @return Number of entities
     */
    long count(PersistenceCollection collection);

    /**
     * Read a single entity by path.
     *
     * @param collection Target collection
     * @param path       Entity path (key)
     * @return Entity if found, empty otherwise
     */
    Optional<Document> read(PersistenceCollection collection, PersistencePath path);

    /**
     * Read multiple entities by paths (batch operation).
     *
     * @param collection Target collection
     * @param paths      Entity paths (keys)
     * @return Map of path to entity (only includes found entities)
     */
    Map<PersistencePath, Document> read(PersistenceCollection collection, Collection<PersistencePath> paths);

    /**
     * Read all entities in a collection.
     * <p>
     * Warning: May load large amounts of data into memory.
     * For large collections, use {@link #streamAll(PersistenceCollection)}.
     *
     * @param collection Target collection
     * @return Map of all entities
     */
    Map<PersistencePath, Document> readAll(PersistenceCollection collection);

    // ==================== STREAMING ====================

    /**
     * Stream all entities in a collection.
     * <p>
     * This method loads all data and does not require explicit resource management.
     * Safe for general use but may consume significant memory for large collections.
     *
     * @param collection Target collection
     * @return Stream of entities (safe to use without try-with-resources)
     */
    Stream<PersistenceEntity<Document>> streamAll(PersistenceCollection collection);

    // ==================== WRITE OPERATIONS ====================

    /**
     * Write a single entity to the collection.
     * If the entity exists, it will be replaced.
     *
     * @param collection Target collection
     * @param path       Entity path (key)
     * @param document   Document to save
     * @return True if the write succeeded
     */
    boolean write(PersistenceCollection collection, PersistencePath path, Document document);

    /**
     * Write multiple entities to the collection (batch operation).
     *
     * @param collection Target collection
     * @param documents  Map of path to document
     * @return Number of entities written
     */
    long write(PersistenceCollection collection, Map<PersistencePath, Document> documents);

    // ==================== DELETE OPERATIONS ====================

    /**
     * Delete a single entity by path.
     *
     * @param collection Target collection
     * @param path       Entity path (key)
     * @return True if entity was deleted
     */
    boolean delete(PersistenceCollection collection, PersistencePath path);

    /**
     * Delete multiple entities by paths (batch operation).
     *
     * @param collection Target collection
     * @param paths      Entity paths (keys)
     * @return Number of entities deleted
     */
    long delete(PersistenceCollection collection, Collection<PersistencePath> paths);

    /**
     * Delete all entities in a collection (truncate).
     *
     * @param collection Target collection
     * @return True if collection was cleared
     */
    boolean deleteAll(PersistenceCollection collection);

    /**
     * Delete all entities in all registered collections.
     *
     * @return Number of collections cleared
     */
    long deleteAll();
}
