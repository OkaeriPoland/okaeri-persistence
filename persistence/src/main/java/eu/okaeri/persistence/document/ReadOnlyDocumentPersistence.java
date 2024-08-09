package eu.okaeri.persistence.document;

import eu.okaeri.persistence.PersistenceCollection;
import eu.okaeri.persistence.PersistenceEntity;
import eu.okaeri.persistence.PersistencePath;
import eu.okaeri.persistence.document.index.IndexProperty;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import lombok.NonNull;

public class ReadOnlyDocumentPersistence extends DocumentPersistence {

  private final DocumentPersistence parentPersistence;

  public ReadOnlyDocumentPersistence(@NonNull final DocumentPersistence parentPersistence) {
    super(
        parentPersistence.getRead(),
        parentPersistence.getWrite(),
        parentPersistence.getConfigurerProvider(),
        parentPersistence.getSerdesPacks());
    this.parentPersistence = parentPersistence;
  }

  // WRITE
  @Override
  public void flush() {}

  @Override
  public long fixIndexes(@NonNull final PersistenceCollection collection) {
    return 0;
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
  public boolean updateIndex(
      @NonNull final PersistenceCollection collection,
      @NonNull final PersistencePath path,
      @NonNull final Document document) {
    return false;
  }

  @Override
  public boolean updateIndex(
      @NonNull final PersistenceCollection collection, @NonNull final PersistencePath path) {
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
  public boolean write(
      @NonNull final PersistenceCollection collection,
      @NonNull final PersistencePath path,
      @NonNull final Document document) {
    return false;
  }

  @Override
  public long write(
      @NonNull final PersistenceCollection collection,
      @NonNull final Map<PersistencePath, Document> entities) {
    return 0;
  }

  @Override
  public boolean delete(
      @NonNull final PersistenceCollection collection, @NonNull final PersistencePath path) {
    return false;
  }

  @Override
  public long delete(
      @NonNull final PersistenceCollection collection,
      @NonNull final Collection<PersistencePath> paths) {
    return 0;
  }

  @Override
  public boolean deleteAll(@NonNull final PersistenceCollection collection) {
    return false;
  }

  @Override
  public long deleteAll() {
    return 0;
  }

  // READ
  @Override
  public Set<PersistencePath> findMissingIndexes(
      @NonNull final PersistenceCollection collection,
      @NonNull final Set<IndexProperty> indexProperties) {
    return this.parentPersistence.findMissingIndexes(collection, indexProperties);
  }

  @Override
  public Document readOrEmpty(
      @NonNull final PersistenceCollection collection, @NonNull final PersistencePath path) {
    return this.parentPersistence.readOrEmpty(collection, path);
  }

  @Override
  public Optional<Document> read(
      @NonNull final PersistenceCollection collection, @NonNull final PersistencePath path) {
    return this.parentPersistence.read(collection, path);
  }

  @Override
  public Map<PersistencePath, Document> readOrEmpty(
      @NonNull final PersistenceCollection collection,
      @NonNull final Collection<PersistencePath> paths) {
    return this.parentPersistence.readOrEmpty(collection, paths);
  }

  @Override
  public Map<PersistencePath, Document> read(
      @NonNull final PersistenceCollection collection,
      @NonNull final Collection<PersistencePath> paths) {
    return this.parentPersistence.read(collection, paths);
  }

  @Override
  public Map<PersistencePath, Document> readAll(@NonNull final PersistenceCollection collection) {
    return this.parentPersistence.readAll(collection);
  }

  @Override
  public Stream<PersistenceEntity<Document>> readByProperty(
      @NonNull final PersistenceCollection collection,
      @NonNull final PersistencePath property,
      final Object propertyValue) {
    return this.parentPersistence.readByProperty(collection, property, propertyValue);
  }

  @Override
  public Stream<PersistenceEntity<Document>> streamAll(
      @NonNull final PersistenceCollection collection) {
    return this.parentPersistence.streamAll(collection);
  }

  @Override
  public long count(@NonNull final PersistenceCollection collection) {
    return this.parentPersistence.count(collection);
  }

  @Override
  public boolean exists(
      @NonNull final PersistenceCollection collection, @NonNull final PersistencePath path) {
    return this.parentPersistence.exists(collection, path);
  }
}
