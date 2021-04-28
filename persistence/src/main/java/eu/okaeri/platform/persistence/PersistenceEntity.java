package eu.okaeri.platform.persistence;

import eu.okaeri.platform.persistence.document.Document;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class PersistenceEntity<V> {

    private PersistencePath path;
    private V value;

    // limit new allocations when shuffling types
    @SuppressWarnings("unchecked")
    public <T> PersistenceEntity<T> into(T value) {
        this.value = (V) value;
        return (PersistenceEntity<T>) this;
    }

    // unsafe convert for cleaner code :O
    public <T extends Document> PersistenceEntity<T> into(Class<T> configClazz) {
        return this.into(((Document) this.value).into(configClazz));
    }
}
