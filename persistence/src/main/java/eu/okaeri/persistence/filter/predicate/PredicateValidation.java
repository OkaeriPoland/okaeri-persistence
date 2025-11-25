package eu.okaeri.persistence.filter.predicate;

import java.util.Collection;

/**
 * Utility class for predicate input validation.
 */
public final class PredicateValidation {

    private PredicateValidation() {
    }

    /**
     * Validates that an operand does not contain null bytes if it's a string.
     * Null bytes are not supported across all backends (e.g., PostgreSQL JSON).
     *
     * @param operand the operand to validate
     * @return the operand if valid
     * @throws IllegalArgumentException if the operand is a CharSequence containing null bytes
     */
    public static <T> T validated(T operand) {
        if (operand instanceof CharSequence) {
            CharSequence cs = (CharSequence) operand;
            for (int i = 0; i < cs.length(); i++) {
                if (cs.charAt(i) == '\0') {
                    throw new IllegalArgumentException("Null bytes are not supported in string values");
                }
            }
        }
        return operand;
    }

    /**
     * Validates that a collection does not contain strings with null bytes.
     *
     * @param values the collection to validate
     * @return the collection if valid
     * @throws IllegalArgumentException if any element is a CharSequence containing null bytes
     */
    public static <T extends Collection<?>> T validatedAll(T values) {
        for (Object value : values) {
            validated(value);
        }
        return values;
    }
}
