package eu.okaeri.persistence.filter;

import eu.okaeri.persistence.PersistencePath;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NonNull;

@Data
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class OrderBy {

    private final PersistencePath path;
    private final OrderDirection direction;

    public static OrderBy asc(@NonNull String path) {
        return new OrderBy(PersistencePath.parse(path, "."), OrderDirection.ASC);
    }

    public static OrderBy asc(@NonNull PersistencePath path) {
        return new OrderBy(path, OrderDirection.ASC);
    }

    public static OrderBy desc(@NonNull String path) {
        return new OrderBy(PersistencePath.parse(path, "."), OrderDirection.DESC);
    }

    public static OrderBy desc(@NonNull PersistencePath path) {
        return new OrderBy(path, OrderDirection.DESC);
    }
}
