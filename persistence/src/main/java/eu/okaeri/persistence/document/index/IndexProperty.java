package eu.okaeri.persistence.document.index;

import eu.okaeri.persistence.PersistencePath;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

@Getter
@ToString(callSuper = true)
public class IndexProperty extends PersistencePath {

    private int maxLength;

    private IndexProperty(@NonNull String value, int maxLength) {
        super(value);
        this.maxLength = maxLength;
    }

    public static IndexProperty of(@NonNull String path) {
        return new IndexProperty(path, 255);
    }

    public static IndexProperty of(@NonNull String path, int maxLength) {
        return of(path).maxLength(maxLength);
    }

    public static IndexProperty parse(@NonNull String source) {
        return of(source.replace(".", SEPARATOR));
    }

    @Override
    public IndexProperty sub(@NonNull String sub) {
        return of(super.sub(sub).getValue(), this.maxLength);
    }

    public IndexProperty maxLength(int maxLength) {
        if ((maxLength < 1) || (maxLength > 255)) throw new IllegalArgumentException("max length should be between 1 and 255");
        this.maxLength = maxLength;
        return this;
    }
}
