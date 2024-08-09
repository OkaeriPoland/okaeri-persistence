package eu.okaeri.persistence.repository;

import eu.okaeri.persistence.PersistenceCollection;
import eu.okaeri.persistence.PersistenceEntity;
import eu.okaeri.persistence.PersistencePath;
import eu.okaeri.persistence.document.Document;
import eu.okaeri.persistence.document.DocumentPersistence;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public class DefaultDocumentRepository<T extends Document>
    implements DocumentRepository<Object, T> {

  private final DocumentPersistence persistence;
  private final PersistenceCollection collection;
  private final Class<T> documentType;

  private static PersistencePath toPath(final Object object) {
    if (object instanceof PersistencePath) {
      return (PersistencePath) object;
    }
    return PersistencePath.of(String.valueOf(object));
  }

  @Override
  public long count() {
    return this.persistence.count(this.collection);
  }

  @Override
  public boolean deleteAll() {
    return this.persistence.deleteAll(this.collection);
  }

  @Override
  public long deleteAllByPath(@NonNull final Iterable<?> paths) {
    return this.persistence.delete(
        this.collection,
        StreamSupport.stream(paths.spliterator(), false)
            .map(DefaultDocumentRepository::toPath)
            .collect(Collectors.toSet()));
  }

  @Override
  public boolean deleteByPath(@NonNull final Object path) {
    return this.persistence.delete(this.collection, toPath(path));
  }

  @Override
  public boolean existsByPath(@NonNull final Object path) {
    return this.persistence.exists(this.collection, toPath(path));
  }

  @Override
  public Stream<T> streamAll() {
    return this.persistence
        .streamAll(this.collection)
        .map(document -> document.into(this.documentType))
        .map(PersistenceEntity::getValue);
  }

  @Override
  public Collection<T> findAll() {
    return this.persistence.readAll(this.collection).values().stream()
        .map(entity -> entity.into(this.documentType))
        .collect(Collectors.toList());
  }

  @Override
  public Collection<T> findAllByPath(@NonNull final Iterable<?> paths) {

    final Set<PersistencePath> pathSet =
        StreamSupport.stream(paths.spliterator(), false)
            .map(DefaultDocumentRepository::toPath)
            .collect(Collectors.toSet());

    return this.persistence.read(this.collection, pathSet).values().stream()
        .map(document -> document.into(this.documentType))
        .collect(Collectors.toList());
  }

  @Override
  public Collection<T> findOrCreateAllByPath(@NonNull final Iterable<?> paths) {

    final Set<PersistencePath> pathSet =
        StreamSupport.stream(paths.spliterator(), false)
            .map(DefaultDocumentRepository::toPath)
            .collect(Collectors.toSet());

    return this.persistence.readOrEmpty(this.collection, pathSet).values().stream()
        .map(document -> document.into(this.documentType))
        .collect(Collectors.toList());
  }

  @Override
  public Optional<T> findByPath(@NonNull final Object path) {
    return this.persistence
        .read(this.collection, toPath(path))
        .map(document -> document.into(this.documentType));
  }

  @Override
  public T findOrCreateByPath(@NonNull final Object path) {
    final Document document = this.persistence.readOrEmpty(this.collection, toPath(path));
    return document.into(this.documentType);
  }

  @Override
  public T save(@NonNull final T document) {
    this.persistence.write(this.collection, document.getPath(), document);
    return document;
  }

  @Override
  public Iterable<T> saveAll(@NonNull final Iterable<T> documents) {
    final Map<PersistencePath, Document> documentMap =
        StreamSupport.stream(documents.spliterator(), false)
            .collect(Collectors.toMap(Document::getPath, Function.identity()));
    this.persistence.write(this.collection, documentMap);
    return documents;
  }
}
