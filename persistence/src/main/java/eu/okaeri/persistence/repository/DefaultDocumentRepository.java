package eu.okaeri.persistence.repository;

import eu.okaeri.persistence.PersistenceCollection;
import eu.okaeri.persistence.PersistenceEntity;
import eu.okaeri.persistence.PersistencePath;
import eu.okaeri.persistence.document.Document;
import eu.okaeri.persistence.document.DocumentPersistence;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@Getter
@RequiredArgsConstructor
public class DefaultDocumentRepository<T extends Document> implements DocumentRepository<Object, T> {

    private final DocumentPersistence persistence;
    private final PersistenceCollection collection;
    private final Class<T> documentType;

    @Override
    public long count() {
        return this.persistence.count(this.collection);
    }

    @Override
    public boolean deleteAll() {
        return this.persistence.deleteAll(this.collection);
    }

    @Override
    public long deleteAllByPath(@NonNull Iterable<?> paths) {
        return this.persistence.delete(this.collection, StreamSupport.stream(paths.spliterator(), false)
                .map(DefaultDocumentRepository::toPath)
                .collect(Collectors.toSet()));
    }

    @Override
    public boolean deleteByPath(@NonNull Object path) {
        return this.persistence.delete(this.collection, toPath(path));
    }

    @Override
    public boolean existsByPath(@NonNull Object path) {
        return this.persistence.exists(this.collection, toPath(path));
    }

    @Override
    public Stream<T> streamAll() {
        return this.persistence.streamAll(this.collection)
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
    public Collection<T> findAllByPath(@NonNull Iterable<?> paths) {

        Set<PersistencePath> pathSet = StreamSupport.stream(paths.spliterator(), false)
                .map(DefaultDocumentRepository::toPath)
                .collect(Collectors.toSet());

        return this.persistence.read(this.collection, pathSet).values().stream()
                .map(document -> document.into(this.documentType))
                .collect(Collectors.toList());
    }

    @Override
    public Collection<T> findOrCreateAllByPath(@NonNull Iterable<?> paths) {

        Set<PersistencePath> pathSet = StreamSupport.stream(paths.spliterator(), false)
                .map(DefaultDocumentRepository::toPath)
                .collect(Collectors.toSet());

        return this.persistence.readOrEmpty(this.collection, pathSet).values().stream()
                .map(document -> document.into(this.documentType))
                .collect(Collectors.toList());
    }

    @Override
    public Optional<T> findByPath(@NonNull Object path) {
        return this.persistence.read(this.collection, toPath(path))
                .map(document -> document.into(this.documentType));
    }

    @Override
    public T findOrCreateByPath(@NonNull Object path) {
        Document document = this.persistence.readOrEmpty(this.collection, toPath(path));
        return document.into(this.documentType);
    }

    @Override
    public T save(@NonNull T document) {
        this.persistence.write(this.collection, document.getPath(), document);
        return document;
    }

    @Override
    public Iterable<T> saveAll(@NonNull Iterable<T> documents) {
        Map<PersistencePath, Document> documentMap = StreamSupport.stream(documents.spliterator(), false)
                .collect(Collectors.toMap(Document::getPath, Function.identity()));
        this.persistence.write(this.collection, documentMap);
        return documents;
    }

    private static PersistencePath toPath(Object object) {
        if (object instanceof PersistencePath) {
            return (PersistencePath) object;
        }
        return PersistencePath.of(String.valueOf(object));
    }
}
