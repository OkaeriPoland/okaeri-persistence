package eu.okaeri.platform.persistence.index;

import eu.okaeri.platform.persistence.PersistencePath;
import lombok.Getter;
import lombok.ToString;

@Getter
@ToString(callSuper = true)
public class IndexProperty extends PersistencePath {

    public static IndexProperty of(String path) {
        return new IndexProperty(path, 255);
    }

    public static IndexProperty of(String path, int maxLength) {
        return of(path).maxLength(maxLength);
    }

    public static IndexProperty parse(String source) {
        return of(source.replace(".", SEPARATOR));
    }

    @Override
    public IndexProperty sub(String sub) {
        return of(super.sub(sub).getValue(), this.maxLength);
    }

    private IndexProperty(String value, int maxLength) {
        super(value);
        this.maxLength = maxLength;
    }

    private int maxLength;

    public IndexProperty maxLength(int maxLength) {
        if ((maxLength < 1) || (maxLength > 255)) throw new IllegalArgumentException("max length should be between 1 and 255");
        this.maxLength = maxLength;
        return this;
    }
}
