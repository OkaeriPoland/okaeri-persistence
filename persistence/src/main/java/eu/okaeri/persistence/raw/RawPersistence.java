package eu.okaeri.persistence.raw;

import eu.okaeri.persistence.index.IndexProperty;
import eu.okaeri.persistence.Persistence;
import eu.okaeri.persistence.PersistenceCollection;
import eu.okaeri.persistence.PersistenceEntity;
import eu.okaeri.persistence.PersistencePath;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.util.*;
import java.util.stream.Stream;

@AllArgsConstructor
public abstract class RawPersistence implements Persistence<String> {

    @Getter private final PersistencePath basePath;
    @Getter private final Map<String, PersistenceCollection> knownCollections = new HashMap<>();
    @Getter private final Map<String, Set<IndexProperty>> knownIndexes = new HashMap<>();
    @Getter private final boolean nativeReadByProperty;
    @Getter private final boolean nativeIndexes;
    @Getter @Setter private boolean useStringSearch;
    @Getter @Setter private boolean autoFlush;

    @Override
    public void registerCollection(PersistenceCollection collection) {
        this.knownCollections.put(collection.getValue(), collection);
        this.knownIndexes.put(collection.getValue(), collection.getIndexes());
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
    public String readOrEmpty(PersistenceCollection collection, PersistencePath path) {
        return this.read(collection, path).orElse("");
    }

    @Override
    public Map<PersistencePath, String> readOrEmpty(PersistenceCollection collection, Collection<PersistencePath> paths) {

        this.checkCollectionRegistered(collection);
        Map<PersistencePath, String> map = new LinkedHashMap<>();
        Map<PersistencePath, String> data = this.read(collection, paths);

        for (PersistencePath path : paths) {
            map.put(path, data.getOrDefault(path, ""));
        }

        return map;
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

        Set<IndexProperty> collectionIndexes = this.getKnownIndexes().get(collection.getValue());
        if (collectionIndexes == null) {
            return false;
        }

        IndexProperty indexProperty = IndexProperty.of(path.getValue());
        return collectionIndexes.contains(indexProperty);
    }

    public boolean canUseToString(Object value) {
        return (value instanceof String) || (value instanceof Integer) || (value instanceof UUID);
    }
}

