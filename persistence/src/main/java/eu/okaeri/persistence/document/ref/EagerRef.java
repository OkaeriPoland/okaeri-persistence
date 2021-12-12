package eu.okaeri.persistence.document.ref;

import eu.okaeri.persistence.PersistenceCollection;
import eu.okaeri.persistence.PersistencePath;
import eu.okaeri.persistence.document.Document;
import eu.okaeri.persistence.document.DocumentPersistence;
import lombok.NonNull;

public class EagerRef<T extends Document> extends Ref<T> {

    protected EagerRef(@NonNull PersistencePath id, @NonNull PersistencePath collection, @NonNull Class<? extends Document> valueType, T value, boolean fetched, @NonNull DocumentPersistence persistence) {
        super(id, collection, valueType, value, fetched, persistence);
    }

    public static <A extends Document> EagerRef<A> of(@NonNull A document) {
        PersistencePath path = document.getPath();
        PersistenceCollection collection = document.getCollection();
        DocumentPersistence persistence = document.getPersistence();
        return new EagerRef<A>(path, collection, document.getClass(), document, true, persistence);
    }
}
