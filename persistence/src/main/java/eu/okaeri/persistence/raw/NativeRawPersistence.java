package eu.okaeri.persistence.raw;

import eu.okaeri.persistence.PersistenceCollection;
import eu.okaeri.persistence.PersistencePath;
import eu.okaeri.persistence.document.index.IndexProperty;
import java.util.Collections;
import java.util.Set;
import lombok.NonNull;

public abstract class NativeRawPersistence extends RawPersistence {

  public NativeRawPersistence(
      final PersistencePath basePath,
      final boolean canReadByProperty,
      final boolean emulatedIndexes,
      final boolean nativeIndexes,
      final boolean useStringSearch,
      final boolean autoFlush) {
    super(basePath, canReadByProperty, emulatedIndexes, nativeIndexes, useStringSearch, autoFlush);
  }

  @Override
  public boolean updateIndex(
      @NonNull final PersistenceCollection collection,
      @NonNull final PersistencePath path,
      @NonNull final IndexProperty property,
      final String identity) {
    return false;
  }

  @Override
  public boolean dropIndex(
      @NonNull final PersistenceCollection collection,
      @NonNull final PersistencePath path,
      @NonNull final IndexProperty property) {
    return false;
  }

  @Override
  public boolean dropIndex(
      @NonNull final PersistenceCollection collection, @NonNull final PersistencePath path) {
    return false;
  }

  @Override
  public boolean dropIndex(
      @NonNull final PersistenceCollection collection, @NonNull final IndexProperty property) {
    return false;
  }

  @Override
  public Set<PersistencePath> findMissingIndexes(
      @NonNull final PersistenceCollection collection,
      @NonNull final Set<IndexProperty> indexProperties) {
    return Collections.emptySet();
  }
}
