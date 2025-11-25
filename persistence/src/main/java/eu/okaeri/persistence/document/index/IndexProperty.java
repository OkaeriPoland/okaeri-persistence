package eu.okaeri.persistence.document.index;

import eu.okaeri.persistence.PersistencePath;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

import java.math.BigDecimal;
import java.math.BigInteger;

@Getter
@ToString(callSuper = true)
public class IndexProperty extends PersistencePath {

    private int maxLength;
    private Class<?> fieldType;

    private IndexProperty(@NonNull String value, int maxLength, Class<?> fieldType) {
        super(value);
        this.maxLength = maxLength;
        this.fieldType = fieldType;
    }

    public static IndexProperty of(@NonNull String path) {
        return new IndexProperty(path, 255, null);
    }

    public static IndexProperty of(@NonNull String path, int maxLength) {
        return of(path).maxLength(maxLength);
    }

    public static IndexProperty parse(@NonNull String source) {
        return of(source.replace(".", SEPARATOR));
    }

    @Override
    public IndexProperty sub(@NonNull String sub) {
        return new IndexProperty(super.sub(sub).getValue(), this.maxLength, this.fieldType);
    }

    public IndexProperty maxLength(int maxLength) {
        if ((maxLength < 1) || (maxLength > 255)) throw new IllegalArgumentException("max length should be between 1 and 255");
        this.maxLength = maxLength;
        return this;
    }

    public IndexProperty fieldType(Class<?> fieldType) {
        this.fieldType = fieldType;
        return this;
    }

    /**
     * Check if this index is for a numeric field type.
     */
    public boolean isNumeric() {
        if (this.fieldType == null) return false;
        return Number.class.isAssignableFrom(this.fieldType)
            || this.fieldType == int.class
            || this.fieldType == long.class
            || this.fieldType == double.class
            || this.fieldType == float.class
            || this.fieldType == short.class
            || this.fieldType == byte.class
            || this.fieldType == BigDecimal.class
            || this.fieldType == BigInteger.class;
    }

    /**
     * Check if this index is for a boolean field type.
     */
    public boolean isBoolean() {
        if (this.fieldType == null) return false;
        return this.fieldType == Boolean.class || this.fieldType == boolean.class;
    }

    /**
     * Check if this index is for a text/string field type.
     */
    public boolean isText() {
        if (this.fieldType == null) return false;
        return this.fieldType == String.class || CharSequence.class.isAssignableFrom(this.fieldType);
    }
}
