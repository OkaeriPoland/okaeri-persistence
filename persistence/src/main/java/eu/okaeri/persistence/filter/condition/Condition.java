package eu.okaeri.persistence.filter.condition;

import eu.okaeri.persistence.PersistencePath;
import eu.okaeri.persistence.filter.predicate.Predicate;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class Condition {

    private final PersistencePath path;
    private final Predicate<?>[] predicates;

    public static Condition cond(String path, Predicate<?>... predicates) {
        return cond(PersistencePath.of(path), predicates);
    }

    public static Condition cond(PersistencePath path, Predicate<?>... predicates) {
        if (predicates.length <= 0) throw new IllegalArgumentException("one or more predicate is required");
        return new Condition(path, predicates);
    }
}
