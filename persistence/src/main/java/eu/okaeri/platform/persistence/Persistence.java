package eu.okaeri.platform.persistence;

import eu.okaeri.platform.persistence.index.IndexProperty;

import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

public interface Persistence<T> {

    // allows to track tables etc.
    void registerCollection(PersistenceCollection collection);

    // allows to disable flushing to the database
    // mainly for the filesystem/inmemory persistence backends
    // not expected to be a guarantee, just something
    // to use when performing mass changes and hoping
    // for the implementation to take care of it
    void setAutoFlush(boolean state);

    // allows to flush/save when autoflush is disabled
    void flush();

    // allows to update entry of entity's index
    boolean updateIndex(PersistenceCollection collection, IndexProperty property, PersistencePath path, String identity);

    // allows to update whole entity's index using entity
    boolean updateIndex(PersistenceCollection collection, PersistencePath path, T entity);

    // allows to update whole entity's index by entity's key
    boolean updateIndex(PersistenceCollection collection, PersistencePath path);

    // allows to delete entity's index
    boolean dropIndex(PersistenceCollection collection, IndexProperty property, PersistencePath path);

    // allows to delete all entity's indexes
    boolean dropIndex(PersistenceCollection collection, PersistencePath path);

    // allows to delete whole index
    boolean dropIndex(PersistenceCollection collection, IndexProperty property);

    // allows to search for missing indexes
    Set<PersistencePath> findMissingIndexes(PersistenceCollection collection, Set<IndexProperty> indexProperties);

    // basic group "ExamplePlugin:" -> "example_plugin:player:USER_IDENTIFIER"
    PersistencePath getBasePath();

    // check if element exists
    // important as it is advised that read returns empty objects
    // instead of throwing an exception
    boolean exists(PersistenceCollection collection, PersistencePath path);

    // read saved object at key
    T read(PersistenceCollection collection, PersistencePath path);

    // read all saved objects in the collection
    Map<PersistencePath, T> readAll(PersistenceCollection collection);

    // read based on property (mostly for document based implementations)
    Stream<PersistenceEntity<T>> readByProperty(PersistenceCollection collection, PersistencePath property, Object propertyValue);

    // visit all saved objects in the collection
    Stream<PersistenceEntity<T>> streamAll(PersistenceCollection collection);

    // write object to exact key
    boolean write(PersistenceCollection collection, PersistencePath path, T entity);

    // delete single
    boolean delete(PersistenceCollection collection, PersistencePath path);

    // delete all from collection
    boolean deleteAll(PersistenceCollection collection);

    // delete all - purge all collections
    long deleteAll();
}
