package eu.okaeri.persistence;

import eu.okaeri.persistence.document.index.IndexProperty;
import eu.okaeri.persistence.repository.annotation.DocumentCollection;
import eu.okaeri.persistence.repository.annotation.DocumentIndex;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

@Getter
@ToString(callSuper = true)
public class PersistenceCollection extends PersistencePath {

    private int keyLength;
    private boolean autofixIndexes = true;
    private Set<IndexProperty> indexes = new HashSet<>();

    private PersistenceCollection(@NonNull String value, int keyLength) {
        super(value);
        this.keyLength = keyLength;
    }

    public static PersistenceCollection of(@NonNull Class<?> clazz) {

        DocumentCollection collection = clazz.getAnnotation(DocumentCollection.class);
        if (collection == null) {
            throw new IllegalArgumentException(clazz + " is not annotated with @DocumentCollection");
        }

        // Auto-detect keyLength for common path types
        int keyLength = collection.keyLength();
        if (keyLength == 255) { // User didn't specify, use smart default
            try {
                Type[] types = ((ParameterizedType) clazz.getGenericInterfaces()[0]).getActualTypeArguments();
                Class<?> pathType = (Class<?>) types[0];
                if (pathType == UUID.class) {
                    keyLength = 36; // UUID strings are exactly 36 characters
                } else if (pathType == Integer.class) {
                    keyLength = 11; // Integer.MIN_VALUE is -2147483648 (11 characters)
                } else if (pathType == Long.class) {
                    keyLength = 20; // Long.MIN_VALUE is -9223372036854775808 (20 characters)
                }
            } catch (Exception ignored) {
                // Not a DocumentRepository or couldn't resolve generic type - use default
            }
        }

        PersistenceCollection out = PersistenceCollection.of(collection.path(), keyLength);
        for (DocumentIndex index : collection.indexes()) {
            out.index(IndexProperty.parse(index.path()).maxLength(index.maxLength()));
        }

        return out.autofixIndexes(collection.autofixIndexes());
    }

    public static PersistenceCollection of(@NonNull String path) {
        return new PersistenceCollection(path, 255);
    }

    public static PersistenceCollection of(@NonNull String path, int keyLength) {
        return of(path).keyLength(keyLength);
    }

    public PersistenceCollection keyLength(int keyLength) {
        if ((keyLength < 1) || (keyLength > 255))
            throw new IllegalArgumentException("key length should be between 1 and 255");
        this.keyLength = keyLength;
        return this;
    }

    public PersistenceCollection index(@NonNull IndexProperty indexProperty) {
        this.indexes.add(indexProperty);
        return this;
    }

    public PersistenceCollection autofixIndexes(boolean autofixIndexes) {
        this.autofixIndexes = autofixIndexes;
        return this;
    }

    public int getMaxIndexIdentityLength() {
        return this.indexes.stream()
            .mapToInt(IndexProperty::getMaxLength)
            .max()
            .orElse(255);
    }

    public int getMaxIndexPropertyLength() {
        return this.indexes.stream()
            .mapToInt(index -> index.getValue().length())
            .max()
            .orElse(255);
    }
}
