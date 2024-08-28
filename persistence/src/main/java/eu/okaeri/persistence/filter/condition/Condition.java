package eu.okaeri.persistence.filter.condition;

import eu.okaeri.persistence.PersistencePath;
import eu.okaeri.persistence.filter.predicate.Predicate;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NonNull;

import java.util.Arrays;

@Data
@SuppressWarnings("unchecked")
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class Condition implements Predicate {

    private final LogicalOperator operator;
    private final PersistencePath path;
    private final Predicate[] predicates;

    public static Condition and(@NonNull Predicate... predicates) {
        return new Condition(LogicalOperator.AND, null, predicates);
    }

    public static Condition and(@NonNull String path, @NonNull Predicate... predicates) {
        return and(PersistencePath.of(path), predicates);
    }

    public static Condition and(@NonNull PersistencePath path, @NonNull Predicate... predicates) {
        if (predicates.length <= 0) throw new IllegalArgumentException("one or more predicate is required");
        return new Condition(LogicalOperator.AND, path, predicates);
    }

    public static Condition or(@NonNull Predicate... predicates) {
        return new Condition(LogicalOperator.OR, null, predicates);
    }

    public static Condition or(@NonNull String path, @NonNull Predicate... predicates) {
        return or(PersistencePath.of(path), predicates);
    }

    public static Condition or(@NonNull PersistencePath path, @NonNull Predicate... predicates) {
        if (predicates.length <= 0) throw new IllegalArgumentException("one or more predicate is required");
        return new Condition(LogicalOperator.OR, path, predicates);
    }

    @Override
    public boolean check(Object leftOperand) {
        if (this.operator == LogicalOperator.AND) {
            return Arrays.stream(this.predicates).allMatch(p -> p.check(leftOperand));
        }
        if (this.operator == LogicalOperator.OR) {
            return Arrays.stream(this.predicates).anyMatch(p -> p.check(leftOperand));
        }
        throw new IllegalArgumentException("Unsupported operator: " + this.operator);
    }
}
