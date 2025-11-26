package eu.okaeri.persistence.repository;

import eu.okaeri.persistence.PersistenceCollection;
import eu.okaeri.persistence.PersistenceEntity;
import eu.okaeri.persistence.PersistencePath;
import eu.okaeri.persistence.document.Document;
import eu.okaeri.persistence.document.DocumentPersistence;
import eu.okaeri.persistence.filter.*;
import eu.okaeri.persistence.filter.condition.Condition;
import eu.okaeri.persistence.filter.operation.UpdateOperation;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.*;
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

    private static PersistencePath toPath(Object object) {
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
    public long delete(DeleteFilter filter) {
        return this.persistence.delete(this.collection, filter);
    }

    @Override
    public long delete(Function<DeleteFilterBuilder, DeleteFilterBuilder> function) {
        return this.delete(function.apply(DeleteFilter.builder()).build());
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
    public Stream<T> stream(int batchSize) {
        return this.persistence.stream(this.collection, batchSize)
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
    public Stream<T> find(@NonNull FindFilter filter) {
        return this.persistence.find(this.collection, filter)
            .map(document -> document.into(this.documentType))
            .map(PersistenceEntity::getValue);
    }

    @Override
    public Stream<T> find(@NonNull Function<FindFilterBuilder, FindFilterBuilder> function) {
        return this.find(function.apply(FindFilter.builder()).build());
    }

    @Override
    public Stream<T> find(@NonNull Condition condition) {
        return this.find(q -> q.where(condition));
    }

    @Override
    public Optional<T> findOne(@NonNull Condition condition) {
        return this.find(q -> q.where(condition).limit(1)).findAny();
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

        // Get existing documents
        Map<PersistencePath, Document> existing = this.persistence.read(this.collection, pathSet);

        // Create empty documents for missing paths
        List<T> result = new ArrayList<>();
        for (PersistencePath path : pathSet) {
            Document doc = existing.get(path);
            if (doc == null) {
                // Create new empty document
                doc = this.persistence.getSerializer().createDocument(this.collection, path);
            }
            result.add(doc.into(this.documentType));
        }
        return result;
    }

    @Override
    public Optional<T> findByPath(@NonNull Object path) {
        return this.persistence.read(this.collection, toPath(path))
            .map(document -> document.into(this.documentType));
    }

    @Override
    public T findOrCreateByPath(@NonNull Object path) {
        PersistencePath persistencePath = toPath(path);
        Optional<Document> existing = this.persistence.read(this.collection, persistencePath);
        Document document = existing.orElseGet(() ->
            this.persistence.getSerializer().createDocument(this.collection, persistencePath));
        return document.into(this.documentType);
    }

    @Override
    public T save(@NonNull T document) {
        if (document.getPath() == null) {
            document.setPath(PersistencePath.randomUUID());
        }
        this.persistence.write(this.collection, document.getPath(), document);
        return document;
    }

    @Override
    public Iterable<T> saveAll(@NonNull Iterable<T> documents) {
        Map<PersistencePath, Document> documentMap = StreamSupport.stream(documents.spliterator(), false)
            .peek(doc -> {
                if (doc.getPath() == null) {
                    doc.setPath(PersistencePath.randomUUID());
                }
            })
            .collect(Collectors.toMap(Document::getPath, Function.identity()));
        this.persistence.write(this.collection, documentMap);
        return documents;
    }

    // ===== UPDATE OPERATIONS =====

    @Override
    public long update(@NonNull Function<UpdateFilterBuilder, UpdateFilterBuilder> updater) {
        UpdateFilter filter = updater.apply(UpdateFilter.builder()).build();
        return this.persistence.update(this.collection, filter);
    }

    @Override
    public boolean updateOne(@NonNull Object path, @NonNull Function<UpdateBuilder, UpdateBuilder> operations) {
        UpdateBuilder builder = new UpdateBuilder();
        List<UpdateOperation> ops = operations.apply(builder).getOperations();
        return this.persistence.updateOne(this.collection, toPath(path), ops);
    }

    @Override
    public boolean updateOne(@NonNull T entity, @NonNull Function<UpdateBuilder, UpdateBuilder> operations) {
        if (entity.getPath() == null) {
            throw new IllegalArgumentException("Entity must have a path set");
        }
        return this.updateOne(entity.getPath(), operations);
    }

    @Override
    public Optional<T> updateOneAndGet(@NonNull Object path, @NonNull Function<UpdateBuilder, UpdateBuilder> operations) {
        UpdateBuilder builder = new UpdateBuilder();
        List<UpdateOperation> ops = operations.apply(builder).getOperations();
        return this.persistence.updateOneAndGet(this.collection, toPath(path), ops)
            .map(document -> document.into(this.documentType));
    }

    @Override
    public Optional<T> getAndUpdateOne(@NonNull Object path, @NonNull Function<UpdateBuilder, UpdateBuilder> operations) {
        UpdateBuilder builder = new UpdateBuilder();
        List<UpdateOperation> ops = operations.apply(builder).getOperations();
        return this.persistence.getAndUpdateOne(this.collection, toPath(path), ops)
            .map(document -> document.into(this.documentType));
    }
}
