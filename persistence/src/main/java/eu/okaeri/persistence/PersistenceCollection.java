package eu.okaeri.persistence;

import eu.okaeri.configs.OkaeriConfig;
import eu.okaeri.configs.schema.ConfigDeclaration;
import eu.okaeri.configs.schema.FieldDeclaration;
import eu.okaeri.persistence.document.index.IndexProperty;
import eu.okaeri.persistence.repository.annotation.DocumentCollection;
import eu.okaeri.persistence.repository.annotation.DocumentIndex;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.HashSet;
import java.util.Optional;
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

        // Auto-detect keyLength and entityType from generic parameters
        int keyLength = collection.keyLength();
        Class<?> entityType = null;
        try {
            Type[] types = ((ParameterizedType) clazz.getGenericInterfaces()[0]).getActualTypeArguments();
            // Auto-detect keyLength if user didn't specify
            if (keyLength == 255) {
                Class<?> pathType = (Class<?>) types[0];
                if (pathType == UUID.class) {
                    keyLength = 36; // UUID strings are exactly 36 characters
                } else if (pathType == Integer.class) {
                    keyLength = 11; // Integer.MIN_VALUE is -2147483648 (11 characters)
                } else if (pathType == Long.class) {
                    keyLength = 20; // Long.MIN_VALUE is -9223372036854775808 (20 characters)
                }
            }
            // Get entity type (second type argument) for index field type detection
            if (types.length > 1 && types[1] instanceof Class) {
                entityType = (Class<?>) types[1];
            }
        } catch (Exception ignored) {
            // Not a DocumentRepository or couldn't resolve generic type - use default
        }

        PersistenceCollection out = PersistenceCollection.of(collection.path(), keyLength);
        for (DocumentIndex index : collection.indexes()) {
            IndexProperty indexProperty = IndexProperty.parse(index.path()).maxLength(index.maxLength());
            // Auto-detect field type from entity class
            if (entityType != null) {
                Class<?> fieldType = resolveFieldType(index.path(), entityType);
                if (fieldType != null) {
                    indexProperty.fieldType(fieldType);
                }
            }
            out.index(indexProperty);
        }

        return out.autofixIndexes(collection.autofixIndexes());
    }

    /**
     * Resolve field type from entity class using ConfigDeclaration.
     * Supports nested paths like "profile.age".
     */
    private static Class<?> resolveFieldType(String path, Class<?> entityType) {
        try {
            ConfigDeclaration declaration = ConfigDeclaration.of(entityType);
            String[] parts = path.split("\\.");

            for (int i = 0; i < parts.length; i++) {
                Optional<FieldDeclaration> field = declaration.getField(parts[i]);
                if (!field.isPresent()) {
                    return null;
                }

                Class<?> fieldType = field.get().getType().getType();

                // If this is the last part, return the type
                if (i == parts.length - 1) {
                    return fieldType;
                }

                // For nested paths, descend into the field type
                if (OkaeriConfig.class.isAssignableFrom(fieldType)) {
                    declaration = ConfigDeclaration.of(fieldType);
                } else {
                    // Non-config type can't have subfields
                    return null;
                }
            }
        } catch (Exception ignored) {
            // Failed to resolve field type
        }
        return null;
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
}
