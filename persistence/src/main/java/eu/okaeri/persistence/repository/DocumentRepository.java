package eu.okaeri.persistence.repository;

import eu.okaeri.persistence.PersistenceCollection;
import eu.okaeri.persistence.document.Document;
import eu.okaeri.persistence.document.DocumentPersistence;
import eu.okaeri.persistence.filter.DeleteFilter;
import eu.okaeri.persistence.filter.DeleteFilterBuilder;
import eu.okaeri.persistence.filter.FindFilter;
import eu.okaeri.persistence.filter.FindFilterBuilder;
import eu.okaeri.persistence.filter.UpdateBuilder;
import eu.okaeri.persistence.filter.UpdateFilter;
import eu.okaeri.persistence.filter.UpdateFilterBuilder;
import eu.okaeri.persistence.filter.condition.Condition;

import java.util.Collection;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

public interface DocumentRepository<PATH, T extends Document> {

    DocumentPersistence getPersistence();

    PersistenceCollection getCollection();

    Class<? extends Document> getDocumentType();

    long count();

    boolean deleteAll();

    long delete(DeleteFilter filter);

    long delete(Function<DeleteFilterBuilder, DeleteFilterBuilder> function);

    long deleteAllByPath(Iterable<? extends PATH> paths);

    boolean deleteByPath(PATH path);

    boolean existsByPath(PATH path);

    Collection<T> findAll();

    /**
     * Stream all documents from the collection.
     * This method loads all data and does not require explicit resource management.
     * Safe for general use but may consume significant memory for large collections.
     * For memory-efficient streaming of large collections, use {@link #stream(int)}.
     *
     * @return Stream of documents (safe to use without try-with-resources)
     */
    Stream<T> streamAll();

    /**
     * Stream all documents with batched fetching for better memory efficiency.
     * More memory efficient than {@link #streamAll()} for large collections by fetching data in batches.
     * <p>
     * <b>IMPORTANT: This stream must be closed after use to prevent resource leaks.</b>
     * Use try-with-resources or explicitly call {@code close()} on the stream.
     * <p>
     * Example usage:
     * <pre>{@code
     * // Recommended: try-with-resources
     * try (Stream<User> stream = userRepository.stream(100)) {
     *     return stream
     *         .filter(user -> user.isActive())
     *         .map(User::getName)
     *         .collect(Collectors.toList());
     * }
     *
     * // Process large collection without loading all into memory
     * try (Stream<User> stream = userRepository.stream(50)) {
     *     stream.forEach(user -> {
     *         // Process each user as it's fetched (e.g., send email, export data, etc.)
     *         processUser(user);
     *     });
     * }
     * }</pre>
     *
     * @param batchSize Preferred batch size (hint, actual behavior depends on backend)
     * @return Stream of documents (must be closed after use)
     */
    Stream<T> stream(int batchSize);

    /**
     * Stream all documents with default batch size (100).
     * Convenience method for {@link #stream(int)} with default batch size.
     * <p>
     * <b>IMPORTANT: This stream must be closed after use.</b> See {@link #stream(int)} for details.
     * <p>
     * Example usage:
     * <pre>{@code
     * try (Stream<User> stream = userRepository.stream()) {
     *     return stream.map(User::getName).collect(Collectors.toList());
     * }
     * }</pre>
     *
     * @return Stream of documents (must be closed after use)
     */
    default Stream<T> stream() {
        return this.stream(100);
    }

    Stream<T> find(FindFilter filter);

    Stream<T> find(Function<FindFilterBuilder, FindFilterBuilder> function);

    Stream<T> find(Condition condition);

    Optional<T> findOne(Condition condition);

    Collection<T> findAllByPath(Iterable<? extends PATH> paths);

    Collection<T> findOrCreateAllByPath(Iterable<? extends PATH> paths);

    Optional<T> findByPath(PATH path);

    T findOrCreateByPath(PATH path);

    T save(T document);

    Iterable<T> saveAll(Iterable<T> documents);

    // ===== UPDATE OPERATIONS =====

    /**
     * Update multiple documents matching the WHERE clause.
     * Applies atomic update operations to all documents that match the filter condition.
     *
     * @param updater Function that builds the update filter with WHERE clause and operations
     * @return Number of documents modified
     */
    long update(Function<UpdateFilterBuilder, UpdateFilterBuilder> updater);

    /**
     * Update a single document by its path.
     * Applies atomic update operations to the document identified by the path.
     *
     * @param path The document path (ID)
     * @param operations Function that builds the update operations
     * @return true if the document was modified, false if not found
     */
    boolean updateOne(PATH path, Function<UpdateBuilder, UpdateBuilder> operations);

    /**
     * Update a single document using the entity's path.
     * Convenience method that extracts the path from the entity.
     *
     * @param entity The document entity (must have a path set)
     * @param operations Function that builds the update operations
     * @return true if the document was modified, false if not found
     */
    boolean updateOne(T entity, Function<UpdateBuilder, UpdateBuilder> operations);

    /**
     * Update a single document and return the updated version.
     * Performs an atomic update and returns the document after modifications.
     *
     * @param path The document path (ID)
     * @param operations Function that builds the update operations
     * @return Optional containing the updated document, or empty if not found
     */
    Optional<T> updateOneAndGet(PATH path, Function<UpdateBuilder, UpdateBuilder> operations);

    /**
     * Update a single document and return the original version.
     * Performs an atomic update and returns the document before modifications.
     *
     * @param path The document path (ID)
     * @param operations Function that builds the update operations
     * @return Optional containing the original document, or empty if not found
     */
    Optional<T> getAndUpdateOne(PATH path, Function<UpdateBuilder, UpdateBuilder> operations);
}
