package eu.okaeri.persistence.document.ref;

import eu.okaeri.persistence.PersistenceCollection;
import eu.okaeri.persistence.PersistencePath;
import eu.okaeri.persistence.document.Document;
import eu.okaeri.persistence.document.DocumentPersistence;
import java.util.Optional;
import java.util.logging.Logger;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public abstract class Ref<T extends Document> {

  private static final boolean DEBUG =
      Boolean.parseBoolean(System.getProperty("okaeri.platform.debug", "false"));
  private static final Logger LOGGER = Logger.getLogger(Ref.class.getSimpleName());

  private final PersistencePath id;
  private final PersistencePath collection;
  private Class<? extends Document> valueType;
  private T value;

  private boolean fetched;
  private DocumentPersistence persistence;

  public Optional<T> get() {
    return Optional.ofNullable(this.fetched ? this.value : this.fetch());
  }

  @SuppressWarnings("unchecked")
  public T fetch() {

    final long start = System.currentTimeMillis();
    final PersistenceCollection collection = PersistenceCollection.of(this.collection.getValue());
    this.value = (T) this.persistence.read(collection, this.id).orElse(null);

    if (this.value != null) {
      this.value = (T) this.value.into(this.valueType);
    }

    if (DEBUG) {
      final long took = System.currentTimeMillis() - start;
      LOGGER.info(
          "Fetched document reference for "
              + this.collection.getValue()
              + " ["
              + this.id.getValue()
              + "]: "
              + took
              + " ms");
    }

    this.fetched = true;
    return this.value;
  }
}
