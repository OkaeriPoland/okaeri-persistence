package eu.okaeri.persistence;

import eu.okaeri.persistence.document.index.IndexProperty;
import eu.okaeri.persistence.repository.annotation.DocumentCollection;
import eu.okaeri.persistence.repository.annotation.DocumentIndex;
import java.util.HashSet;
import java.util.Set;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

@Getter
@ToString(callSuper = true)
public class PersistenceCollection extends PersistencePath {

  private final Set<IndexProperty> indexes = new HashSet<>();
  private int keyLength;
  private boolean autofixIndexes = true;

  private PersistenceCollection(@NonNull final String value, final int keyLength) {
    super(value);
    this.keyLength = keyLength;
  }

  public static PersistenceCollection of(@NonNull final Class<?> clazz) {

    final DocumentCollection collection = clazz.getAnnotation(DocumentCollection.class);
    if (collection == null) {
      throw new IllegalArgumentException(clazz + " is not annotated with @DocumentCollection");
    }

    final PersistenceCollection out =
        PersistenceCollection.of(collection.path(), collection.keyLength());
    for (final DocumentIndex index : collection.indexes()) {
      out.index(IndexProperty.parse(index.path()).maxLength(index.maxLength()));
    }

    return out.autofixIndexes(collection.autofixIndexes());
  }

  public static PersistenceCollection of(@NonNull final String path) {
    return new PersistenceCollection(path, 255);
  }

  public static PersistenceCollection of(@NonNull final String path, final int keyLength) {
    return of(path).keyLength(keyLength);
  }

  public PersistenceCollection keyLength(final int keyLength) {
    if ((keyLength < 1) || (keyLength > 255)) {
      throw new IllegalArgumentException("key length should be between 1 and 255");
    }
    this.keyLength = keyLength;
    return this;
  }

  public PersistenceCollection index(@NonNull final IndexProperty indexProperty) {
    this.indexes.add(indexProperty);
    return this;
  }

  public PersistenceCollection autofixIndexes(final boolean autofixIndexes) {
    this.autofixIndexes = autofixIndexes;
    return this;
  }

  public int getMaxIndexIdentityLength() {
    return this.indexes.stream().mapToInt(IndexProperty::getMaxLength).max().orElse(255);
  }

  public int getMaxIndexPropertyLength() {
    return this.indexes.stream().mapToInt(index -> index.getValue().length()).max().orElse(255);
  }
}
