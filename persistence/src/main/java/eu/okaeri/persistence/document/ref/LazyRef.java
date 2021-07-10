package eu.okaeri.persistence.document.ref;

import eu.okaeri.persistence.PersistenceCollection;
import eu.okaeri.persistence.PersistencePath;
import eu.okaeri.persistence.document.Document;
import eu.okaeri.persistence.document.DocumentPersistence;
import lombok.NonNull;

public class LazyRef<T extends Document> extends Ref<T> {

    public static <A extends Document> LazyRef<A> of(@NonNull A document) {
        PersistencePath path = document.getPath();
        PersistenceCollection collection = document.getCollection();
        DocumentPersistence persistence = document.getPersistence();
        return new LazyRef<A>(path, collection, document.getClass(), document, true, persistence);
    }

    protected LazyRef(@NonNull PersistencePath id, @NonNull PersistencePath collection, @NonNull Class<? extends Document> valueType, T value, boolean fetched, @NonNull DocumentPersistence persistence) {
        super(id, collection, valueType, value, fetched, persistence);
    }
}
