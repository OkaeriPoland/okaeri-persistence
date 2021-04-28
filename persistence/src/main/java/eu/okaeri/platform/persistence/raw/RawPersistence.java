package eu.okaeri.platform.persistence.raw;

import eu.okaeri.platform.persistence.Persistence;
import eu.okaeri.platform.persistence.PersistenceCollection;
import eu.okaeri.platform.persistence.PersistenceEntity;
import eu.okaeri.platform.persistence.PersistencePath;
import eu.okaeri.platform.persistence.index.IndexProperty;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.util.*;
import java.util.stream.Stream;

@AllArgsConstructor
public abstract class RawPersistence implements Persistence<String> {

    @Getter private final PersistencePath basePath;
    @Getter private final Map<String, PersistenceCollection> knownCollections = new HashMap<>();
    @Getter private final Map<PersistenceCollection, Set<IndexProperty>> knownIndexes = new HashMap<>();
    @Getter private final boolean nativeReadByProperty;
    @Getter private final boolean nativeIndexes;
    @Getter @Setter private boolean useStringSearch;
    @Getter @Setter private boolean autoFlush;

    @Override
    public void registerCollection(PersistenceCollection collection) {
        this.knownCollections.put(collection.getValue(), collection);
        this.knownIndexes.put(collection, collection.getIndexes());
    }

    @Override
    public void flush() {
    }

    @Override
    public boolean updateIndex(PersistenceCollection collection, IndexProperty property, PersistencePath path, String identity) {
        throw new RuntimeException("not implemented yet");
    }

    @Override
    public boolean updateIndex(PersistenceCollection collection, PersistencePath path, String entity) {
        throw new RuntimeException("not implemented yet");
    }

    @Override
    public boolean updateIndex(PersistenceCollection collection, PersistencePath path) {
        throw new RuntimeException("not implemented yet");
    }

    @Override
    public boolean dropIndex(PersistenceCollection collection, IndexProperty property, PersistencePath path) {
        throw new RuntimeException("not implemented yet");
    }

    @Override
    public boolean dropIndex(PersistenceCollection collection, PersistencePath path) {
        throw new RuntimeException("not implemented yet");
    }

    @Override
    public boolean dropIndex(PersistenceCollection collection, IndexProperty property) {
        throw new RuntimeException("not implemented yet");
    }

    @Override
    public Set<PersistencePath> findMissingIndexes(PersistenceCollection collection, Set<IndexProperty> indexProperties) {
        throw new RuntimeException("not implemented yet");
    }

    @Override
    public Stream<PersistenceEntity<String>> readByProperty(PersistenceCollection collection, PersistencePath property, Object propertyValue) {
        throw new RuntimeException("not implemented yet");
    }

    public void checkCollectionRegistered(PersistenceCollection collection) {
        if (this.knownCollections.containsKey(collection.getValue())) {
            return;
        }
        throw new IllegalArgumentException("cannot use unregistered collection: " + collection);
    }

    public PersistencePath toFullPath(PersistenceCollection collection, PersistencePath path) {
        this.checkCollectionRegistered(collection);
        return this.getBasePath().sub(collection).sub(this.convertPath(path));
    }

    public PersistencePath convertPath(PersistencePath path) {
        return path;
    }

    public boolean isIndexed(PersistenceCollection collection, PersistencePath path) {

        Set<IndexProperty> collectionIndexes = this.getKnownIndexes().get(collection);
        if (collectionIndexes == null) {
            return false;
        }

        IndexProperty indexProperty = IndexProperty.of(path.getValue());
        return collectionIndexes.contains(indexProperty);
    }
}

