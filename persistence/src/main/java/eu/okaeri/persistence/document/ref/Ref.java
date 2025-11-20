package eu.okaeri.persistence.document.ref;

import eu.okaeri.persistence.PersistenceCollection;
import eu.okaeri.persistence.PersistencePath;
import eu.okaeri.persistence.document.Document;
import eu.okaeri.persistence.document.DocumentPersistence;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.logging.Logger;

/**
 * Base class for document references. References store a document ID and collection path,
 * allowing lazy or eager loading of referenced documents from persistence.
 * <p>
 * Subclasses:
 * <ul>
 *   <li>{@link EagerRef} - fetches referenced document immediately during deserialization</li>
 *   <li>{@link LazyRef} - defers fetch until first access via {@link #get()}</li>
 * </ul>
 *
 * @param <T> the type of the referenced document
 */
@Data
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public abstract class Ref<T extends Document> {

    private static final boolean DEBUG = Boolean.parseBoolean(System.getProperty("okaeri.platform.debug", "false"));
    private static final Logger LOGGER = Logger.getLogger(Ref.class.getSimpleName());

    private final PersistencePath id;
    private final PersistencePath collection;
    private Class<? extends Document> valueType;
    private T value;

    private boolean fetched;
    private DocumentPersistence persistence;

    /**
     * Returns an {@code Optional} containing the referenced document if present, otherwise an empty {@code Optional}.
     * If the document has not been fetched yet, triggers a fetch from persistence.
     *
     * @return an {@code Optional} containing the referenced document, or empty if not found
     */
    public Optional<T> get() {
        return Optional.ofNullable(this.fetched ? this.value : this.fetch());
    }

    /**
     * Returns the referenced document if present, otherwise returns {@code null}.
     * Convenience method equivalent to {@code get().orElse(null)}.
     *
     * @return the referenced document, or {@code null} if not found
     */
    public T orNull() {
        return this.get().orElse(null);
    }

    /**
     * If a referenced document is present, returns the document, otherwise throws
     * {@code NoSuchElementException}.
     *
     * @return the non-{@code null} referenced document
     * @throws NoSuchElementException if the reference is not found
     */
    public T orThrow() {
        return this.get().orElseThrow(() -> new NoSuchElementException("Reference not found: " + this.collection.getValue() + "/" + this.id.getValue()));
    }

    /**
     * If a referenced document is present, returns the document, otherwise throws an exception
     * produced by the exception supplying function.
     *
     * @param <X> Type of the exception to be thrown
     * @param exceptionSupplier the supplying function that produces an exception to be thrown
     * @return the non-{@code null} referenced document
     * @throws X if the reference is not found
     * @throws NullPointerException if the reference is not found and the exception supplying function is null
     */
    public <X extends Throwable> T orThrow(Supplier<? extends X> exceptionSupplier) throws X {
        return this.get().orElseThrow(exceptionSupplier);
    }

    /**
     * Fetches the referenced document from persistence and caches it.
     * If the document has already been fetched, returns the cached value without fetching again.
     * <p>
     * This method is typically called automatically by {@link #get()} when needed.
     * Direct use is rarely necessary.
     *
     * @return the referenced document, or {@code null} if not found in persistence
     */
    @SuppressWarnings("unchecked")
    public T fetch() {

        long start = System.currentTimeMillis();
        PersistenceCollection collection = PersistenceCollection.of(this.collection.getValue());
        this.value = (T) this.persistence.read(collection, this.id).orElse(null);

        if (this.value != null) {
            this.value = (T) this.value.into(this.valueType);
        }

        if (DEBUG) {
            long took = System.currentTimeMillis() - start;
            LOGGER.info("Fetched document reference for " + this.collection.getValue() + " [" + this.id.getValue() + "]: " + took + " ms");
        }

        this.fetched = true;
        return this.value;
    }
}
