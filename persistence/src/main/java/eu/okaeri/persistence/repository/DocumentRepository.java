package eu.okaeri.persistence.repository;

import eu.okaeri.persistence.PersistenceCollection;
import eu.okaeri.persistence.document.Document;
import eu.okaeri.persistence.document.DocumentPersistence;
import eu.okaeri.persistence.filter.condition.Condition;

import java.util.Collection;
import java.util.Optional;
import java.util.stream.Stream;

public interface DocumentRepository<PATH, T extends Document> {

    DocumentPersistence getPersistence();

    PersistenceCollection getCollection();

    Class<? extends Document> getDocumentType();

    long count();

    boolean deleteAll();

    long deleteAllByPath(Iterable<? extends PATH> paths);

    boolean deleteByPath(PATH path);

    boolean existsByPath(PATH path);

    Collection<T> findAll();

    Stream<T> streamAll();

    Stream<T> find(Condition condition);

    Collection<T> findAllByPath(Iterable<? extends PATH> paths);

    Collection<T> findOrCreateAllByPath(Iterable<? extends PATH> paths);

    Optional<T> findByPath(PATH path);

    T findOrCreateByPath(PATH path);

    T save(T document);

    Iterable<T> saveAll(Iterable<T> documents);
}
