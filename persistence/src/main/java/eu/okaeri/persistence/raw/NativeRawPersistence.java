package eu.okaeri.persistence.raw;

import eu.okaeri.persistence.PersistenceCollection;
import eu.okaeri.persistence.PersistencePath;
import eu.okaeri.persistence.document.index.IndexProperty;
import lombok.NonNull;

import java.util.Collections;
import java.util.Set;

public abstract class NativeRawPersistence extends RawPersistence {

    public NativeRawPersistence(PersistencePath basePath, boolean canReadByProperty, boolean emulatedIndexes, boolean nativeIndexes, boolean useStringSearch, boolean autoFlush) {
        super(basePath, canReadByProperty, emulatedIndexes, nativeIndexes, useStringSearch, autoFlush);
    }

    @Override
    public boolean updateIndex(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull IndexProperty property, String identity) {
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
    public Set<PersistencePath> findMissingIndexes(@NonNull PersistenceCollection collection, @NonNull Set<IndexProperty> indexProperties) {
        return Collections.emptySet();
    }
}
