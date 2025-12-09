package eu.okaeri.persistence.document;

import eu.okaeri.configs.serdes.OkaeriSerdes;
import eu.okaeri.persistence.*;
import eu.okaeri.persistence.filter.*;
import eu.okaeri.persistence.filter.operation.UpdateOperation;
import eu.okaeri.persistence.repository.DocumentRepository;
import eu.okaeri.persistence.repository.RepositoryDeclaration;
import lombok.Getter;
import lombok.NonNull;

import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Wrapper that provides repository creation and fallback capabilities
 * for persistence backends.
 * <p>
 * This class wraps any {@link Persistence} backend and provides:
 * <ul>
 *   <li>Repository creation via {@link #createRepository(Class)}</li>
 *   <li>Fallback filtering for backends that don't implement {@link FilterablePersistence}</li>
 *   <li>Fallback updates for backends that don't implement {@link UpdatablePersistence}</li>
 * </ul>
 */
public class DocumentPersistence implements Persistence, FilterablePersistence, StreamablePersistence, UpdatablePersistence {

    private static final Logger LOGGER = Logger.getLogger(DocumentPersistence.class.getSimpleName());

    @Getter
    private final Persistence backend;
    @Getter
    private final DocumentSerializer serializer;
    private final InMemoryFilterEvaluator filterEvaluator;
    private final InMemoryUpdateEvaluator updateEvaluator;

    /**
     * Wrap a persistence backend with document handling capabilities.
     *
     * @param backend The underlying persistence backend
     */
    public DocumentPersistence(@NonNull Persistence backend) {
        this.backend = backend;
        this.serializer = backend.getSerializer();
        this.serializer.setPersistence(this);
        this.filterEvaluator = new InMemoryFilterEvaluator(this.serializer.getSimplifier());
        this.updateEvaluator = new InMemoryUpdateEvaluator(
            this.serializer.getConfigurerProvider(), this.serializer.getSerdesRegistry());
    }

    /**
     * Wrap a persistence backend with document handling capabilities.
     *
     * @param backend            The underlying persistence backend
     * @param configurerProvider Configurer for document serialization
     * @param serdes             Additional serialization packs
     */
    public DocumentPersistence(@NonNull Persistence backend,
                               @NonNull ConfigurerProvider configurerProvider,
                               @NonNull OkaeriSerdes... serdes) {
        this.backend = backend;
        this.serializer = new DocumentSerializer(configurerProvider, serdes);
        this.serializer.setPersistence(this);
        this.filterEvaluator = new InMemoryFilterEvaluator(this.serializer.getSimplifier());
        this.updateEvaluator = new InMemoryUpdateEvaluator(configurerProvider, this.serializer.getSerdesRegistry());
    }

    /**
     * Create and register a repository for the given interface.
     */
    public <T extends DocumentRepository<?, ?>> T createRepository(@NonNull Class<T> repositoryClass) {
        PersistenceCollection collection = PersistenceCollection.of(repositoryClass);
        this.registerCollection(collection);
        return RepositoryDeclaration.of(repositoryClass)
            .newProxy(this, collection, repositoryClass.getClassLoader());
    }

    // ==================== DELEGATION TO BACKEND ====================

    @Override
    public void registerCollection(@NonNull PersistenceCollection collection) {
        this.backend.registerCollection(collection);
    }

    @Override
    public PersistencePath getBasePath() {
        return this.backend.getBasePath();
    }

    @Override
    public boolean exists(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {
        return this.backend.exists(collection, path);
    }

    @Override
    public long count(@NonNull PersistenceCollection collection) {
        return this.backend.count(collection);
    }

    @Override
    public Optional<Document> read(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {
        return this.backend.read(collection, path);
    }

    @Override
    public Map<PersistencePath, Document> read(@NonNull PersistenceCollection collection, @NonNull Collection<PersistencePath> paths) {
        return this.backend.read(collection, paths);
    }

    @Override
    public Map<PersistencePath, Document> readAll(@NonNull PersistenceCollection collection) {
        return this.backend.readAll(collection);
    }

    @Override
    public Stream<PersistenceEntity<Document>> streamAll(@NonNull PersistenceCollection collection) {
        return this.backend.streamAll(collection);
    }

    @Override
    public boolean write(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull Document document) {
        return this.backend.write(collection, path, document);
    }

    @Override
    public long write(@NonNull PersistenceCollection collection, @NonNull Map<PersistencePath, Document> documents) {
        return this.backend.write(collection, documents);
    }

    @Override
    public boolean delete(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {
        return this.backend.delete(collection, path);
    }

    @Override
    public long delete(@NonNull PersistenceCollection collection, @NonNull Collection<PersistencePath> paths) {
        return this.backend.delete(collection, paths);
    }

    @Override
    public boolean deleteAll(@NonNull PersistenceCollection collection) {
        return this.backend.deleteAll(collection);
    }

    @Override
    public long deleteAll() {
        return this.backend.deleteAll();
    }

    @Override
    public void close() throws IOException {
        this.backend.close();
    }

    // ==================== FILTERING (WITH FALLBACK) ====================

    @Override
    public Stream<PersistenceEntity<Document>> find(@NonNull PersistenceCollection collection, @NonNull FindFilter filter) {
        // Try native filtering first
        if (this.backend instanceof FilterablePersistence) {
            return ((FilterablePersistence) this.backend).find(collection, filter);
        }

        // Fallback: load all and filter in memory
        LOGGER.fine("Backend doesn't support native find(), using in-memory filtering");
        return this.filterEvaluator.applyFilter(this.streamAll(collection), filter);
    }

    @Override
    public long delete(@NonNull PersistenceCollection collection, @NonNull DeleteFilter filter) {
        // Try native delete first
        if (this.backend instanceof FilterablePersistence) {
            return ((FilterablePersistence) this.backend).delete(collection, filter);
        }

        // Fallback: find matching and delete
        LOGGER.fine("Backend doesn't support native deleteByFilter(), using in-memory filtering");
        if (filter.getWhere() == null) {
            throw new IllegalArgumentException("DeleteFilter requires WHERE condition - use deleteAll() instead");
        }

        List<PersistencePath> toDelete = this.streamAll(collection)
            .filter(entity -> this.filterEvaluator.evaluateCondition(filter.getWhere(), entity.getValue()))
            .map(PersistenceEntity::getPath)
            .collect(Collectors.toList());

        return this.delete(collection, toDelete);
    }

    // ==================== STREAMING (WITH FALLBACK) ====================

    @Override
    public Stream<PersistenceEntity<Document>> stream(@NonNull PersistenceCollection collection, int batchSize) {
        // Try native streaming first
        if (this.backend instanceof StreamablePersistence) {
            return ((StreamablePersistence) this.backend).stream(collection, batchSize);
        }

        // Fallback: streamAll (no batching)
        return this.streamAll(collection);
    }

    // ==================== UPDATES (WITH FALLBACK) ====================

    @Override
    public boolean updateOne(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull List<UpdateOperation> operations) {
        this.validateNoFieldConflicts(operations);

        if (this.backend instanceof UpdatablePersistence) {
            try {
                return ((UpdatablePersistence) this.backend).updateOne(collection, path, operations);
            } catch (UnsupportedOperationException ignored) {
                // Fall through to in-memory implementation
            }
        }

        // Fallback: read-modify-write
        return this.updateOneInMemory(collection, path, operations);
    }

    @Override
    public Optional<Document> updateOneAndGet(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull List<UpdateOperation> operations) {
        this.validateNoFieldConflicts(operations);

        if (this.backend instanceof UpdatablePersistence) {
            try {
                return ((UpdatablePersistence) this.backend).updateOneAndGet(collection, path, operations);
            } catch (UnsupportedOperationException ignored) {
                // Fall through to in-memory implementation
            }
        }

        // Fallback: read-modify-write
        return this.updateOneAndGetInMemory(collection, path, operations);
    }

    @Override
    public Optional<Document> getAndUpdateOne(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull List<UpdateOperation> operations) {
        this.validateNoFieldConflicts(operations);

        if (this.backend instanceof UpdatablePersistence) {
            try {
                return ((UpdatablePersistence) this.backend).getAndUpdateOne(collection, path, operations);
            } catch (UnsupportedOperationException ignored) {
                // Fall through to in-memory implementation
            }
        }

        // Fallback: read-modify-write
        return this.getAndUpdateOneInMemory(collection, path, operations);
    }

    @Override
    public long update(@NonNull PersistenceCollection collection, @NonNull UpdateFilter filter) {
        this.validateNoFieldConflicts(filter.getOperations());

        if (this.backend instanceof UpdatablePersistence) {
            try {
                return ((UpdatablePersistence) this.backend).update(collection, filter);
            } catch (UnsupportedOperationException ignored) {
                // Fall through to in-memory implementation
            }
        }

        // Fallback: iterate and update each
        return this.updateInMemory(collection, filter);
    }

    // ==================== HELPER METHODS ====================

    private void validateNoFieldConflicts(List<UpdateOperation> operations) {
        Map<String, Integer> fieldCounts = new HashMap<>();
        for (UpdateOperation op : operations) {
            fieldCounts.merge(op.getField(), 1, Integer::sum);
        }

        List<String> conflicts = fieldCounts.entrySet().stream()
            .filter(e -> e.getValue() > 1)
            .map(e -> String.format("'%s' appears %d times", e.getKey(), e.getValue()))
            .collect(Collectors.toList());

        if (!conflicts.isEmpty()) {
            throw new IllegalArgumentException(
                "Multiple operations on same field(s): " + String.join(", ", conflicts));
        }
    }

    private boolean updateOneInMemory(PersistenceCollection collection, PersistencePath path, List<UpdateOperation> operations) {
        Optional<Document> docOpt = this.read(collection, path);
        if (!docOpt.isPresent()) {
            return false;
        }

        Document document = docOpt.get();
        boolean modified = this.updateEvaluator.applyUpdate(document, operations);

        if (modified) {
            this.write(collection, path, document);
        }
        return true;
    }

    private Optional<Document> updateOneAndGetInMemory(PersistenceCollection collection, PersistencePath path, List<UpdateOperation> operations) {
        Optional<Document> docOpt = this.read(collection, path);
        if (!docOpt.isPresent()) {
            return Optional.empty();
        }

        Document document = docOpt.get();
        boolean modified = this.updateEvaluator.applyUpdate(document, operations);

        if (modified) {
            this.write(collection, path, document);
        }
        return Optional.of(document);
    }

    private Optional<Document> getAndUpdateOneInMemory(PersistenceCollection collection, PersistencePath path, List<UpdateOperation> operations) {
        Optional<Document> docOpt = this.read(collection, path);
        if (!docOpt.isPresent()) {
            return Optional.empty();
        }

        Document document = docOpt.get();
        Document oldVersion = this.serializer.deepCopy(document);

        boolean modified = this.updateEvaluator.applyUpdate(document, operations);
        if (modified) {
            this.write(collection, path, document);
        }
        return Optional.of(oldVersion);
    }

    private long updateInMemory(PersistenceCollection collection, UpdateFilter filter) {
        Stream<PersistenceEntity<Document>> stream = this.streamAll(collection);

        if (filter.getWhere() != null) {
            stream = stream.filter(entity ->
                this.filterEvaluator.evaluateCondition(filter.getWhere(), entity.getValue()));
        }

        List<PersistencePath> pathsToUpdate = stream
            .map(PersistenceEntity::getPath)
            .collect(Collectors.toList());

        long count = 0;
        for (PersistencePath path : pathsToUpdate) {
            if (this.updateOneInMemory(collection, path, filter.getOperations())) {
                count++;
            }
        }
        return count;
    }
}
