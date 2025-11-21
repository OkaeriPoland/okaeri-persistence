package eu.okaeri.persistence.filter.predicate;

import eu.okaeri.persistence.filter.predicate.collection.InPredicate;
import eu.okaeri.persistence.filter.predicate.collection.NotInPredicate;
import eu.okaeri.persistence.filter.predicate.equality.EqPredicate;
import eu.okaeri.persistence.filter.predicate.equality.NePredicate;
import eu.okaeri.persistence.filter.predicate.nullity.IsNullPredicate;
import eu.okaeri.persistence.filter.predicate.nullity.NotNullPredicate;
import eu.okaeri.persistence.filter.predicate.numeric.GtPredicate;
import eu.okaeri.persistence.filter.predicate.numeric.GtePredicate;
import eu.okaeri.persistence.filter.predicate.numeric.LtPredicate;
import eu.okaeri.persistence.filter.predicate.numeric.LtePredicate;
import eu.okaeri.persistence.filter.predicate.string.ContainsPredicate;
import eu.okaeri.persistence.filter.predicate.string.EndsWithPredicate;
import eu.okaeri.persistence.filter.predicate.string.StartsWithPredicate;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NonNull;

import java.util.Arrays;
import java.util.Collection;

@Data
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public abstract class SimplePredicate implements Predicate {

    private final Object rightOperand;

    /**
     * Validates that a collection contains homogeneous types (all numbers or all non-numbers).
     * Returns true if the collection contains numbers, false otherwise.
     *
     * @param values        the collection to validate
     * @param predicateName the predicate name for error messages (e.g., "IN", "NOT IN")
     * @return true if the collection contains numbers, false otherwise
     * @throws IllegalArgumentException if the collection contains mixed types
     */
    protected static boolean validateAndCheckNumeric(@NonNull Collection<?> values, @NonNull String predicateName) {
        if (values.isEmpty()) {
            return false;
        }

        // Check first element to determine expected type
        Object first = values.iterator().next();
        boolean expectNumeric = first instanceof Number;

        // Validate all elements are consistent (all numbers or all non-numbers)
        for (Object value : values) {
            boolean isNumeric = value instanceof Number;
            if (isNumeric != expectNumeric) {
                throw new IllegalArgumentException(
                    predicateName + " predicate values must be homogeneous (all numbers or all non-numbers). " +
                        "Got mixed types in: " + values
                );
            }
        }

        return expectNumeric;
    }

    public static SimplePredicate eq(double rightOperand) {
        return new EqPredicate(rightOperand);
    }

    public static SimplePredicate eq(@NonNull CharSequence rightOperand) {
        return new EqPredicate(rightOperand);
    }

    public static SimplePredicate gte(double rightOperand) {
        return new GtePredicate(rightOperand);
    }

    public static SimplePredicate gt(double rightOperand) {
        return new GtPredicate(rightOperand);
    }

    public static SimplePredicate lte(double rightOperand) {
        return new LtePredicate(rightOperand);
    }

    public static SimplePredicate lt(double rightOperand) {
        return new LtPredicate(rightOperand);
    }

    public static SimplePredicate ne(double rightOperand) {
        return new NePredicate(rightOperand);
    }

    public static SimplePredicate ne(@NonNull CharSequence rightOperand) {
        return new NePredicate(rightOperand);
    }

    public static Predicate isNull() {
        return new IsNullPredicate();
    }

    public static Predicate notNull() {
        return new NotNullPredicate();
    }

    public static SimplePredicate in(@NonNull Object... values) {
        return new InPredicate(Arrays.asList(values));
    }

    public static SimplePredicate notIn(@NonNull Object... values) {
        return new NotInPredicate(Arrays.asList(values));
    }

    public static StartsWithPredicate startsWith(@NonNull String prefix) {
        return new StartsWithPredicate(prefix);
    }

    public static EndsWithPredicate endsWith(@NonNull String suffix) {
        return new EndsWithPredicate(suffix);
    }

    public static ContainsPredicate contains(@NonNull String substring) {
        return new ContainsPredicate(substring);
    }
}
