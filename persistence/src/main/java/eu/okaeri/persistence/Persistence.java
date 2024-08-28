package eu.okaeri.persistence;

import eu.okaeri.persistence.document.index.IndexProperty;
import eu.okaeri.persistence.filter.condition.Condition;

import java.io.Closeable;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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
     * @param condition  Search condition
     * @return Stream of entities matching the query
     */
    Stream<PersistenceEntity<T>> readByFilter(PersistenceCollection collection, Condition condition);

    /**
     * Visit all entities from the specific collection.
     * Makes use of the partial fetching when possible.
     *
     * @param collection Target collection (eg. player)
     * @return Stream of collection entities
     */
    Stream<PersistenceEntity<T>> streamAll(PersistenceCollection collection);

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
     * Truncate all registered collections.
     *
     * @return Count of changes
     */
    long deleteAll();
}
