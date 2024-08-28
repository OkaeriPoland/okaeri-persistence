package eu.okaeri.persistence.raw;

import eu.okaeri.persistence.Persistence;
import eu.okaeri.persistence.PersistenceCollection;
import eu.okaeri.persistence.PersistenceEntity;
import eu.okaeri.persistence.PersistencePath;
import eu.okaeri.persistence.document.index.IndexProperty;
import eu.okaeri.persistence.filter.condition.Condition;
import lombok.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.stream.Stream;

@RequiredArgsConstructor
public abstract class RawPersistence implements Persistence<String> {

    @Getter private final Map<String, PersistenceCollection> knownCollections = new ConcurrentHashMap<>();
    @Getter private final Map<String, Set<IndexProperty>> knownIndexes = new ConcurrentHashMap<>();

    @Getter private final PersistencePath basePath;
    @Getter private final PersistencePropertyMode propertyMode;
    @Getter private final PersistenceIndexMode indexMode;

    // currently only used in FlatPersistence for indexes
    @Getter @Setter private boolean flushOnWrite = true;

    @Override
    public void registerCollection(@NonNull PersistenceCollection collection) {
        this.knownCollections.put(collection.getValue(), collection);
        this.knownIndexes.put(collection.getValue(), collection.getIndexes());
    }

    @Override
    public long fixIndexes(PersistenceCollection collection) {
        throw new RuntimeException("not implemented yet");
    }

    @Override
    public void flush() {
    }

    @Override
    public boolean updateIndex(PersistenceCollection collection, PersistencePath path, IndexProperty property, String identity) {
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
    public boolean dropIndex(PersistenceCollection collection, PersistencePath path, IndexProperty property) {
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

    @Override
    public Stream<PersistenceEntity<String>> readByFilter(@NonNull PersistenceCollection collection, @NonNull Condition condition) {
        throw new RuntimeException("not implemented yet");
    }

    @Override
    public String readOrEmpty(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {
        return this.read(collection, path).orElse("");
    }

    @Override
    public Map<PersistencePath, String> readOrEmpty(@NonNull PersistenceCollection collection, @NonNull Collection<PersistencePath> paths) {

        this.checkCollectionRegistered(collection);
        Map<PersistencePath, String> map = new LinkedHashMap<>();
        Map<PersistencePath, String> data = this.read(collection, paths);

        for (PersistencePath path : paths) {
            map.put(path, data.getOrDefault(path, ""));
        }

        return map;
    }

    @Override
    public long write(@NonNull PersistenceCollection collection, @NonNull Map<PersistencePath, String> entities) {
        return entities.entrySet().stream()
            .map(entry -> this.write(collection, entry.getKey(), entry.getValue()))
            .filter(Predicate.isEqual(true))
            .count();
    }

    public void checkCollectionRegistered(@NonNull PersistenceCollection collection) {
        if (this.getKnownCollections().containsKey(collection.getValue())) {
            return;
        }
        throw new IllegalArgumentException("cannot use unregistered collection: " + collection);
    }

    public PersistencePath toFullPath(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {
        this.checkCollectionRegistered(collection);
        return this.getBasePath().sub(collection).sub(this.convertPath(path));
    }

    public PersistencePath convertPath(@NonNull PersistencePath path) {
        return path;
    }

    public boolean isIndexed(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {

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

