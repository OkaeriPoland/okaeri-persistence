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
public class Condition<T> implements Predicate<T> {

    private final ConditionOperator operator;
    private final PersistencePath path;
    private final Predicate<T>[] predicates;

    public static <T> Condition<T> and(@NonNull Predicate<T>... predicates) {
        return new Condition<>(ConditionOperator.AND, null, predicates);
    }

    public static <T> Condition<T> and(@NonNull String path, @NonNull Predicate<T>... predicates) {
        return and(PersistencePath.of(path), predicates);
    }

    public static <T> Condition<T> and(@NonNull PersistencePath path, @NonNull Predicate<T>... predicates) {
        if (predicates.length <= 0) throw new IllegalArgumentException("one or more predicate is required");
        return new Condition<>(ConditionOperator.AND, path, predicates);
    }

    public static <T> Condition<T> or(@NonNull Predicate<T>... predicates) {
        return new Condition<>(ConditionOperator.OR, null, predicates);
    }

    public static <T> Condition<T> or(@NonNull String path, @NonNull Predicate<T>... predicates) {
        return or(PersistencePath.of(path), predicates);
    }

    public static <T> Condition<T> or(@NonNull PersistencePath path, @NonNull Predicate<T>... predicates) {
        if (predicates.length <= 0) throw new IllegalArgumentException("one or more predicate is required");
        return new Condition<>(ConditionOperator.OR, path, predicates);
    }

    @Override
    public boolean check(Object leftOperand) {
        if (this.operator == ConditionOperator.AND) {
            return Arrays.stream(this.predicates).allMatch(p -> p.check(leftOperand));
        }
        if (this.operator == ConditionOperator.OR) {
            return Arrays.stream(this.predicates).anyMatch(p -> p.check(leftOperand));
        }
        throw new IllegalArgumentException("Unsupported operator: " + this.operator);
    }
}
