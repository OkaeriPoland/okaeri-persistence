package eu.okaeri.persistence;

import eu.okaeri.persistence.document.Document;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NonNull;

@Data
@AllArgsConstructor
public class PersistenceEntity<V> {

  private PersistencePath path;
  private V value;

  // limit new allocations when shuffling types
  @SuppressWarnings("unchecked")
  public <T> PersistenceEntity<T> into(@NonNull final T value) {
    this.value = (V) value;
    return (PersistenceEntity<T>) this;
  }

  // unsafe convert for cleaner code :O
  public <T extends Document> PersistenceEntity<T> into(@NonNull final Class<T> configClazz) {
    return this.into(((Document) this.value).into(configClazz));
  }
}
