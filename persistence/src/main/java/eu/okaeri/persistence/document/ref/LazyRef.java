package eu.okaeri.persistence.document.ref;

import eu.okaeri.persistence.PersistenceCollection;
import eu.okaeri.persistence.PersistencePath;
import eu.okaeri.persistence.document.Document;
import eu.okaeri.persistence.document.DocumentPersistence;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public class LazyRef<T extends Document> {

    public static <A extends Document> LazyRef<A> of(A document) {
        PersistencePath path = document.getPath();
        if (path == null) throw new IllegalArgumentException("document.path cannot be null");
        PersistenceCollection collection = document.getCollection();
        if (collection == null) throw new IllegalArgumentException("document.collection cannot be null");
        DocumentPersistence persistence = document.getPersistence();
        if (persistence == null) throw new IllegalArgumentException("document.persistence cannot be null");
        return new LazyRef<A>(path, collection, document.getClass(), document, true, persistence);
    }

    private final PersistencePath id;
    private final PersistencePath collection;
    private Class<? extends Document> valueType;
    private T value;

    private boolean fetched;
    private DocumentPersistence persistence;

    public T get() {
        return this.fetched ? this.value : this.fetch();
    }

    @SuppressWarnings("unchecked")
    public T fetch() {

        PersistenceCollection collection = PersistenceCollection.of(this.collection.getValue());
        this.value = (T) this.persistence.read(collection, this.id).orElse(null);

        if (this.value != null) {
            this.value = (T) this.value.into(this.valueType);
        }

        this.fetched = true;
        return this.value;
    }
}
