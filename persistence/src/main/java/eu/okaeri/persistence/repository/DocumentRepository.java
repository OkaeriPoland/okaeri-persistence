package eu.okaeri.persistence.repository;

import eu.okaeri.persistence.PersistenceEntity;
import eu.okaeri.persistence.document.Document;

import java.util.Collection;
import java.util.Optional;
import java.util.stream.Stream;

public interface DocumentRepository<PATH, T extends Document> {
    long count();
    boolean deleteAll();
    long deleteAllByPath(Iterable<? extends PATH> paths);
    boolean deleteByPath(PATH path);
    boolean existsByPath(PATH path);
    Stream<PersistenceEntity<T>> findAll();
    Collection<PersistenceEntity<T>> findAllByPath(Iterable<? extends PATH> paths);
    Optional<T> findByPath(PATH path);
    T save(T document);
    Iterable<T> saveAll(Iterable<T> documents);
}
