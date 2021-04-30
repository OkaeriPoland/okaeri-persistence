package eu.okaeri.persistence.repository;

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
    Stream<T> findAll();
    Collection<T> findAllByPath(Iterable<? extends PATH> paths);
    Collection<T> findOrCreateAllByPath(Iterable<? extends PATH> paths);
    Optional<T> findByPath(PATH path);
    T findOrCreateByPath(PATH path);
    T save(T document);
    Iterable<T> saveAll(Iterable<T> documents);
}
