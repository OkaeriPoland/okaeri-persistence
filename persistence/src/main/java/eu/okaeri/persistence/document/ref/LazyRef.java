package eu.okaeri.persistence.document.ref;

import eu.okaeri.persistence.PersistenceCollection;
import eu.okaeri.persistence.PersistencePath;
import eu.okaeri.persistence.document.Document;
import eu.okaeri.persistence.document.DocumentPersistence;
import lombok.NonNull;

public class LazyRef<T extends Document> extends Ref<T> {

  protected LazyRef(
      @NonNull final PersistencePath id,
      @NonNull final PersistencePath collection,
      @NonNull final Class<? extends Document> valueType,
      final T value,
      final boolean fetched,
      @NonNull final DocumentPersistence persistence) {
    super(id, collection, valueType, value, fetched, persistence);
  }

  public static <A extends Document> LazyRef<A> of(@NonNull final A document) {
    final PersistencePath path = document.getPath();
    final PersistenceCollection collection = document.getCollection();
    final DocumentPersistence persistence = document.getPersistence();
    return new LazyRef<A>(path, collection, document.getClass(), document, true, persistence);
  }
}
