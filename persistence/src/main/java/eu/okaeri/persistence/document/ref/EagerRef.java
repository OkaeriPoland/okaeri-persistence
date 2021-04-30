package eu.okaeri.persistence.document.ref;

import eu.okaeri.persistence.PersistenceCollection;
import eu.okaeri.persistence.PersistencePath;
import eu.okaeri.persistence.document.Document;
import eu.okaeri.persistence.document.DocumentPersistence;

public class EagerRef<T extends Document> extends LazyRef<T> {

    public static <A extends Document> EagerRef<A> of(A document) {
        PersistencePath path = document.getPath();
        if (path == null) throw new IllegalArgumentException("document.path cannot be null");
        PersistenceCollection collection = document.getCollection();
        if (collection == null) throw new IllegalArgumentException("document.collection cannot be null");
        DocumentPersistence persistence = document.getPersistence();
        if (persistence == null) throw new IllegalArgumentException("document.persistence cannot be null");
        return new EagerRef<A>(path, collection, document.getClass(), document, true, persistence);
    }

    protected EagerRef(PersistencePath id, PersistencePath collection, Class<? extends Document> valueType, T value, boolean fetched, DocumentPersistence persistence) {
        super(id, collection, valueType, value, fetched, persistence);
    }
}
