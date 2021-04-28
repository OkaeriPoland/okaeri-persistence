package eu.okaeri.platform.persistence;

import eu.okaeri.platform.persistence.index.IndexProperty;
import lombok.Getter;
import lombok.ToString;

import java.util.HashSet;
import java.util.Set;

@Getter
@ToString(callSuper = true)
public class PersistenceCollection extends PersistencePath {

    public static PersistenceCollection of(String path) {
        return new PersistenceCollection(path, 255);
    }

    public static PersistenceCollection of(String path, int keyLength) {
        return of(path).keyLength(keyLength);
    }

    private PersistenceCollection(String value, int keyLength) {
        super(value);
        this.keyLength = keyLength;
    }

    private int keyLength;
    private Set<IndexProperty> indexes = new HashSet<>();

    public PersistenceCollection keyLength(int keyLength) {
        if ((keyLength < 1) || (keyLength > 255)) throw new IllegalArgumentException("key length should be between 1 and 255");
        this.keyLength = keyLength;
        return this;
    }

    public PersistenceCollection index(IndexProperty indexProperty) {
        this.indexes.add(indexProperty);
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
