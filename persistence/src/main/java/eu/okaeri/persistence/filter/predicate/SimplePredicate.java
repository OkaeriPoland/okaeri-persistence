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

    /**
     * Creates an equals predicate for numeric values.
     * {@code field == value}
     *
     * @param rightOperand the value to compare against
     * @return equals predicate
     */
    public static SimplePredicate eq(double rightOperand) {
        return new EqPredicate(rightOperand);
    }

    /**
     * Creates an equals predicate for string values.
     * {@code field == value}
     * <p>
     * Use {@link #eqi(CharSequence)} or {@code eq(value).ignoreCase()} for case-insensitive comparison.
     *
     * @param rightOperand the value to compare against
     * @return equals predicate
     */
    public static EqPredicate eq(@NonNull CharSequence rightOperand) {
        return new EqPredicate(rightOperand);
    }

    /**
     * Creates a case-insensitive equals predicate for string values.
     * Shorthand for {@code eq(value).ignoreCase()}.
     * {@code lower(field) == lower(value)}
     *
     * @param rightOperand the value to compare against (case-insensitive)
     * @return case-insensitive equals predicate
     */
    public static SimplePredicate eqi(@NonNull CharSequence rightOperand) {
        return new EqPredicate(rightOperand, true);
    }

    /**
     * Creates a greater-than-or-equal predicate.
     * {@code field >= value}
     *
     * @param rightOperand the value to compare against
     * @return greater-than-or-equal predicate
     */
    public static SimplePredicate gte(double rightOperand) {
        return new GtePredicate(rightOperand);
    }

    /**
     * Creates a greater-than predicate.
     * {@code field > value}
     *
     * @param rightOperand the value to compare against
     * @return greater-than predicate
     */
    public static SimplePredicate gt(double rightOperand) {
        return new GtPredicate(rightOperand);
    }

    /**
     * Creates a less-than-or-equal predicate.
     * {@code field <= value}
     *
     * @param rightOperand the value to compare against
     * @return less-than-or-equal predicate
     */
    public static SimplePredicate lte(double rightOperand) {
        return new LtePredicate(rightOperand);
    }

    /**
     * Creates a less-than predicate.
     * {@code field < value}
     *
     * @param rightOperand the value to compare against
     * @return less-than predicate
     */
    public static SimplePredicate lt(double rightOperand) {
        return new LtPredicate(rightOperand);
    }

    /**
     * Creates a not-equals predicate for numeric values.
     * {@code field != value}
     *
     * @param rightOperand the value to compare against
     * @return not-equals predicate
     */
    public static SimplePredicate ne(double rightOperand) {
        return new NePredicate(rightOperand);
    }

    /**
     * Creates a not-equals predicate for string values.
     * {@code field != value}
     *
     * @param rightOperand the value to compare against
     * @return not-equals predicate
     */
    public static SimplePredicate ne(@NonNull CharSequence rightOperand) {
        return new NePredicate(rightOperand);
    }

    /**
     * Creates a null-check predicate.
     * {@code field == null}
     *
     * @return is-null predicate
     */
    public static Predicate isNull() {
        return new IsNullPredicate();
    }

    /**
     * Creates a not-null-check predicate.
     * {@code field != null}
     *
     * @return not-null predicate
     */
    public static Predicate notNull() {
        return new NotNullPredicate();
    }

    /**
     * Creates an IN predicate for collection membership.
     * {@code field IN (value1, value2, ...)}
     * <p>
     * Values must be homogeneous (all numbers or all non-numbers).
     *
     * @param values the values to check membership against
     * @return IN predicate
     * @throws IllegalArgumentException if values contain mixed types
     */
    public static SimplePredicate in(@NonNull Object... values) {
        return new InPredicate(Arrays.asList(values));
    }

    /**
     * Creates a NOT IN predicate for collection exclusion.
     * {@code field NOT IN (value1, value2, ...)}
     * <p>
     * Values must be homogeneous (all numbers or all non-numbers).
     *
     * @param values the values to exclude
     * @return NOT IN predicate
     * @throws IllegalArgumentException if values contain mixed types
     */
    public static SimplePredicate notIn(@NonNull Object... values) {
        return new NotInPredicate(Arrays.asList(values));
    }

    /**
     * Creates a starts-with predicate for string prefix matching.
     * {@code field LIKE 'prefix%'}
     * <p>
     * Use {@code startsWith(prefix).ignoreCase()} for case-insensitive matching.
     *
     * @param prefix the prefix to match
     * @return starts-with predicate
     */
    public static StartsWithPredicate startsWith(@NonNull String prefix) {
        return new StartsWithPredicate(prefix);
    }

    /**
     * Creates an ends-with predicate for string suffix matching.
     * {@code field LIKE '%suffix'}
     * <p>
     * Use {@code endsWith(suffix).ignoreCase()} for case-insensitive matching.
     *
     * @param suffix the suffix to match
     * @return ends-with predicate
     */
    public static EndsWithPredicate endsWith(@NonNull String suffix) {
        return new EndsWithPredicate(suffix);
    }

    /**
     * Creates a contains predicate for substring matching.
     * {@code field LIKE '%substring%'}
     * <p>
     * Use {@code contains(substring).ignoreCase()} for case-insensitive matching.
     *
     * @param substring the substring to match
     * @return contains predicate
     */
    public static ContainsPredicate contains(@NonNull String substring) {
        return new ContainsPredicate(substring);
    }
}
