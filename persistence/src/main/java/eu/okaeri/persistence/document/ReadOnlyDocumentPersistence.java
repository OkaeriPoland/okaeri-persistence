package eu.okaeri.persistence.document;

import eu.okaeri.persistence.PersistenceCollection;
import eu.okaeri.persistence.PersistenceEntity;
import eu.okaeri.persistence.PersistencePath;
import eu.okaeri.persistence.document.index.IndexProperty;
import lombok.NonNull;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

public class ReadOnlyDocumentPersistence extends DocumentPersistence {

    private final DocumentPersistence parentPersistence;

    public ReadOnlyDocumentPersistence(@NonNull DocumentPersistence parentPersistence) {
        super(parentPersistence.getRead(), parentPersistence.getWrite(), parentPersistence.getConfigurerProvider(), parentPersistence.getSerdesPacks());
        this.parentPersistence = parentPersistence;
    }

    // WRITE
    @Override
    public void flush() {
    }

    @Override
    public long fixIndexes(@NonNull PersistenceCollection collection) {
        return 0;
    }

    @Override
    public boolean updateIndex(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull IndexProperty property, String identity) {
        return false;
    }

    @Override
    public boolean updateIndex(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull Document document) {
        return false;
    }

    @Override
    public boolean updateIndex(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {
        return false;
    }

    @Override
    public boolean dropIndex(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull IndexProperty property) {
        return false;
    }

    @Override
    public boolean dropIndex(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {
        return false;
    }

    @Override
    public boolean dropIndex(@NonNull PersistenceCollection collection, @NonNull IndexProperty property) {
        return false;
    }

    @Override
    public boolean write(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull Document document) {
        return false;
    }

    @Override
    public long write(@NonNull PersistenceCollection collection, @NonNull Map<PersistencePath, Document> entities) {
        return 0;
    }

    @Override
    public boolean delete(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {
        return false;
    }

    @Override
    public long delete(@NonNull PersistenceCollection collection, @NonNull Collection<PersistencePath> paths) {
        return 0;
    }

    @Override
    public boolean deleteAll(@NonNull PersistenceCollection collection) {
        return false;
    }

    @Override
    public long deleteAll() {
        return 0;
    }

    // READ
    @Override
    public Set<PersistencePath> findMissingIndexes(@NonNull PersistenceCollection collection, @NonNull Set<IndexProperty> indexProperties) {
        return this.parentPersistence.findMissingIndexes(collection, indexProperties);
    }

    @Override
    public Document readOrEmpty(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {
        return this.parentPersistence.readOrEmpty(collection, path);
    }

    @Override
    public Optional<Document> read(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {
        return this.parentPersistence.read(collection, path);
    }

    @Override
    public Map<PersistencePath, Document> readOrEmpty(@NonNull PersistenceCollection collection, @NonNull Collection<PersistencePath> paths) {
        return this.parentPersistence.readOrEmpty(collection, paths);
    }

    @Override
    public Map<PersistencePath, Document> read(@NonNull PersistenceCollection collection, @NonNull Collection<PersistencePath> paths) {
        return this.parentPersistence.read(collection, paths);
    }

    @Override
    public Map<PersistencePath, Document> readAll(@NonNull PersistenceCollection collection) {
        return this.parentPersistence.readAll(collection);
    }

    @Override
    public Stream<PersistenceEntity<Document>> readByProperty(@NonNull PersistenceCollection collection, @NonNull PersistencePath property, Object propertyValue) {
        return this.parentPersistence.readByProperty(collection, property, propertyValue);
    }

    @Override
    public Stream<PersistenceEntity<Document>> streamAll(@NonNull PersistenceCollection collection) {
        return this.parentPersistence.streamAll(collection);
    }

    @Override
    public long count(@NonNull PersistenceCollection collection) {
        return this.parentPersistence.count(collection);
    }

    @Override
    public boolean exists(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {
        return this.parentPersistence.exists(collection, path);
    }
}
