package eu.okaeri.persistencetestjdbc.relations;

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

    public static <A extends Document> LazyRef<A> of(A value) {
        return new LazyRef<A>(value.getPath(), value.getCollection(), value.getClass(), value, true, value.getPersistence());
    }

    private final PersistencePath id;
    private final PersistencePath collection;
    private Class<? extends Document> valueType;
    private T value;

    private boolean fetched;
    private DocumentPersistence persistence;

    @SuppressWarnings("unchecked")
    public T get() {
        if (!this.fetched) {
            PersistenceCollection collection = PersistenceCollection.of(this.collection.getValue());
            this.value = (T) this.persistence.read(collection, this.id).orElse(null);
            if (this.value != null) {
                this.value = (T) this.value.into(this.valueType);
            }
            this.fetched = true;
        }
        return this.value;
    }
}
