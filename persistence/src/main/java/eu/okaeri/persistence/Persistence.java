package eu.okaeri.persistence;

import eu.okaeri.persistence.document.index.IndexProperty;
import eu.okaeri.persistence.filter.DeleteFilter;
import eu.okaeri.persistence.filter.FindFilter;
import eu.okaeri.persistence.filter.UpdateFilter;
import eu.okaeri.persistence.filter.operation.UpdateOperation;

import java.io.Closeable;
import java.util.*;
import java.util.stream.Stream;

public interface Persistence<T> extends Closeable {

    /**
     * Register a collection to be tracked by persistence.
     *
     * @param collection Collection to be registered
     */
    void registerCollection(PersistenceCollection collection);

    /**
     * Automatically scan and fix indexes.
     *
     * @param collection Collection to be scanned
     * @return Count of updated entities
     */
    long fixIndexes(PersistenceCollection collection);

    /**
     * Enable/disable flushing to the database.
     * <p>
     * Mainly for the filesystem or in-memory persistence
     * backends. Not expected to be a guarantee, just
     * something to use when performing mass changes
     * and hoping for the implementation to take care of it.
     *
     * @param state New autoFlush state
     */
    void setFlushOnWrite(boolean state);

    /**
     * Flush data to the database manually, ignoring {@link #setFlushOnWrite(boolean)}.
     */
    void flush();

    /**
     * Update entry of entity's index.
     *
     * @param collection Target collection (eg. player)
     * @param path       Target index path (eg. uuid)
     * @param property   Target property (eg. name)
     * @param identity   New value for property
     * @return True when index was changed false otherwise
     */
    boolean updateIndex(PersistenceCollection collection, PersistencePath path, IndexProperty property, String identity);

    /**
     * Update whole entity's index using entity.
     *
     * @param collection Target collection (eg. player)
     * @param path       Target index path (eg. uuid)
     * @param entity     New value for path
     * @return True when index was changed false otherwise
     */
    boolean updateIndex(PersistenceCollection collection, PersistencePath path, T entity);

    /**
     * Update whole entity's index by entity's key.
     *
     * @param collection Target collection (eg. player)
     * @param path       Target index path (eg. uuid)
     * @return True when index was changed false otherwise
     */
    boolean updateIndex(PersistenceCollection collection, PersistencePath path);

    /**
     * Delete entity's index.
     *
     * @param collection Target collection (eg. player)
     * @param path       Target index path (eg. uuid)
     * @param property   Target property (eg. name)
     * @return True when index was changed false otherwise
     */
    boolean dropIndex(PersistenceCollection collection, PersistencePath path, IndexProperty property);

    /**
     * Delete all entity's indexes.
     *
     * @param collection Target collection (eg. player)
     * @param path       Target index path (eg. uuid)
     * @return True when index was changed false otherwise
     */
    boolean dropIndex(PersistenceCollection collection, PersistencePath path);

    /**
     * Delete whole index.
     *
     * @param collection Target collection (eg. player)
     * @param property   Target property (eg. name)
     * @return True when index was changed false otherwise
     */
    boolean dropIndex(PersistenceCollection collection, IndexProperty property);

    /**
     * Search for missing indexes.
     *
     * @param collection      Target collection (eg. player)
     * @param indexProperties Index properties to be accounted
     * @return Persistence paths with missing indexes
     */
    Set<PersistencePath> findMissingIndexes(PersistenceCollection collection, Set<IndexProperty> indexProperties);

    /**
     * Prefix for the storage operations.
     * <p>
     * For example:
     * - Redis: as key prefix "{basePath}:{collection}:{id}"
     * - MariaDB: in table name "{basePath}_{collection}"
     *
     * @return Base storage path
     */
    PersistencePath getBasePath();

    /**
     * @param collection Target collection (eg. player)
     * @return Count of all the entities in the collection
     */
    long count(PersistenceCollection collection);

    /**
     * Check existence of an object in specific path.
     * Important as it is advised that read returns
     * empty objects instead of throwing an exception.
     *
     * @param collection Target collection (eg. player)
     * @param path       Target index path (eg. uuid)
     * @return True if element exists false otherwise
     */
    boolean exists(PersistenceCollection collection, PersistencePath path);

    /**
     * Fetches entity by path from specific collection.
     *
     * @param collection Target collection (eg. player)
     * @param path       Target entity path (eg. uuid)
     * @return Saved entity
     */
    Optional<T> read(PersistenceCollection collection, PersistencePath path);

    /**
     * Fetches entity by path from specific collection.
     * Creates empty entity if no saved entity was found.
     *
     * @param collection Target collection (eg. player)
     * @param path       Target entity path (eg. uuid)
     * @return Saved entity
     */
    T readOrEmpty(PersistenceCollection collection, PersistencePath path);

    /**
     * Optimized {@link #read(PersistenceCollection, PersistencePath)} for multiple entities.
     *
     * @param collection Target collection (eg. player)
     * @param paths      Target entity paths (eg. uuids)
     * @return Saved entities
     */
    Map<PersistencePath, T> read(PersistenceCollection collection, Collection<PersistencePath> paths);

    /**
     * Optimized {@link #readOrEmpty(PersistenceCollection, PersistencePath)} for multiple entities.
     *
     * @param collection Target collection (eg. player)
     * @param paths      Target entity paths (eg. uuids)
     * @return Saved entities
     */
    Map<PersistencePath, T> readOrEmpty(PersistenceCollection collection, Collection<PersistencePath> paths);

    /**
     * Fetches all entities from the specific collection.
     *
     * @param collection Target collection (eg. player)
     * @return Collection entities
     */
    Map<PersistencePath, T> readAll(PersistenceCollection collection);

    /**
     * Filter document based persistence entities.
     *
     * @param collection    Target collection (eg. player)
     * @param property      Property to filter on (eg. name)
     * @param propertyValue Searched property value (eg. SomePlayer)
     * @return Stream of entities matching the query
     */
    Stream<PersistenceEntity<T>> readByProperty(PersistenceCollection collection, PersistencePath property, Object propertyValue);

    /**
     * @param collection Target collection (eg. player)
     * @param filter     Search filter
     * @return Stream of entities matching the query
     */
    Stream<PersistenceEntity<T>> readByFilter(PersistenceCollection collection, FindFilter filter);

    /**
     * Visit all entities from the specific collection.
     * This method loads all data and does not require explicit resource management.
     * Safe for general use but may consume significant memory for large collections.
     * For memory-efficient streaming of large collections, use {@link #stream(PersistenceCollection, int)}.
     *
     * @param collection Target collection (eg. player)
     * @return Stream of collection entities (safe to use without try-with-resources)
     */
    Stream<PersistenceEntity<T>> streamAll(PersistenceCollection collection);

    /**
     * Stream all entities from the specific collection with configurable batch fetching.
     * More memory efficient than streamAll() for large collections by fetching data in batches.
     * <p>
     * <b>IMPORTANT: This stream must be closed after use to prevent resource leaks.</b>
     * Use try-with-resources or explicitly call {@code close()} on the stream.
     * <p>
     * Backend-specific behavior:
     * <ul>
     * <li>PostgreSQL: Uses JDBC cursor (requires open transaction/connection until stream closes)</li>
     * <li>H2/MariaDB: Uses LIMIT/OFFSET pagination (fetches batches on demand)</li>
     * <li>MongoDB: Uses driver cursor with batchSize hint</li>
     * <li>Redis: Uses HSCAN with custom step size</li>
     * </ul>
     * <p>
     * Example usage:
     * <pre>{@code
     * // Recommended: try-with-resources
     * try (Stream<PersistenceEntity<String>> stream = persistence.stream(collection, 100)) {
     *     return stream
     *         .filter(entity -> entity.getValue().contains("active"))
     *         .map(PersistenceEntity::getPath)
     *         .collect(Collectors.toList());
     * }
     *
     * // Process large collection without loading all into memory
     * try (Stream<PersistenceEntity<String>> stream = persistence.stream(collection, 50)) {
     *     stream.forEach(entity -> {
     *         // Process each entity as it's fetched (e.g., export, transform, etc.)
     *         processEntity(entity);
     *     });
     * }
     * }</pre>
     *
     * @param collection Target collection (eg. player)
     * @param batchSize  Number of records to fetch per batch (hint, actual behavior depends on backend)
     * @return Stream of collection entities fetched in batches (must be closed after use)
     */
    Stream<PersistenceEntity<T>> stream(PersistenceCollection collection, int batchSize);

    /**
     * Write entity to specific collection and path.
     *
     * @param collection Target collection (eg. player)
     * @param path       Target entity path (eg. uuid)
     * @param entity     Entity to be saved
     * @return True when changed else otherwise
     */
    boolean write(PersistenceCollection collection, PersistencePath path, T entity);

    /**
     * Optimized {@link #write(PersistenceCollection, PersistencePath, Object)} for multiple entities.
     *
     * @param collection Target collection (eg. player)
     * @param entities   Entities to be saved
     * @return Count of changes
     */
    long write(PersistenceCollection collection, Map<PersistencePath, T> entities);

    /**
     * Delete single entity from the collection.
     *
     * @param collection Target collection (eg. player)
     * @param path       Target entity path (eg. uuid)
     * @return True when changed else otherwise
     */
    boolean delete(PersistenceCollection collection, PersistencePath path);

    /**
     * Delete multiple entities from the collection.
     *
     * @param collection Target collection (eg. player)
     * @param paths      Target entities paths (eg. uuids)
     * @return Count of changes
     */
    long delete(PersistenceCollection collection, Collection<PersistencePath> paths);

    /**
     * Truncate collection.
     *
     * @param collection Target collection (eg. player)
     * @return True when changed else otherwise
     */
    boolean deleteAll(PersistenceCollection collection);

    /**
     * @param collection Target collection (eg. player)
     * @param filter     Search filter
     * @return Count of changes
     */
    long deleteByFilter(PersistenceCollection collection, DeleteFilter filter);

    /**
     * Truncate all registered collections.
     *
     * @return Count of changes
     */
    long deleteAll();

    /**
     * Update a single entity using atomic update operations.
     *
     * @param collection Target collection (eg. player)
     * @param path       Target entity path (eg. uuid)
     * @param operations List of update operations to apply
     * @return True if entity was updated, false if not found
     */
    boolean updateOne(PersistenceCollection collection, PersistencePath path, List<UpdateOperation> operations);

    /**
     * Update a single entity and return the updated version.
     *
     * @param collection Target collection (eg. player)
     * @param path       Target entity path (eg. uuid)
     * @param operations List of update operations to apply
     * @return Updated entity, or empty if not found
     */
    Optional<T> updateOneAndGet(PersistenceCollection collection, PersistencePath path, List<UpdateOperation> operations);

    /**
     * Get the current entity and then update it, returning the old version.
     *
     * @param collection Target collection (eg. player)
     * @param path       Target entity path (eg. uuid)
     * @param operations List of update operations to apply
     * @return Old entity before update, or empty if not found
     */
    Optional<T> getAndUpdateOne(PersistenceCollection collection, PersistencePath path, List<UpdateOperation> operations);

    /**
     * Update multiple entities matching a filter.
     *
     * @param collection Target collection (eg. player)
     * @param filter     Update filter with WHERE clause and operations
     * @return Count of entities updated
     */
    long update(PersistenceCollection collection, UpdateFilter filter);
}
