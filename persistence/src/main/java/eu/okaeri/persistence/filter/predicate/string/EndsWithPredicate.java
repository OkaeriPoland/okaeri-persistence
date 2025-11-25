package eu.okaeri.persistence.filter.predicate.string;

import eu.okaeri.persistence.filter.predicate.SimplePredicate;
import lombok.Getter;
import lombok.NonNull;

import static eu.okaeri.persistence.filter.predicate.PredicateValidation.validated;

/**
 * String ends with predicate.
 * {@code field endsWith "suffix"}
 */
public class EndsWithPredicate extends SimplePredicate {

    @Getter
    private final boolean ignoreCase;

    public EndsWithPredicate(@NonNull String suffix) {
        this(suffix, false);
    }

    public EndsWithPredicate(@NonNull String suffix, boolean ignoreCase) {
        super(validated(suffix));
        this.ignoreCase = ignoreCase;
    }

    /**
     * Returns a case-insensitive version of this predicate.
     */
    public SimplePredicate ignoreCase() {
        return new EndsWithPredicate((String) this.getRightOperand(), true);
    }

    @Override
    public boolean check(Object leftOperand) {
        if (leftOperand == null) {
            return false;
        }
        String left = String.valueOf(leftOperand);
        String right = (String) this.getRightOperand();
        if (this.ignoreCase) {
            return left.toLowerCase().endsWith(right.toLowerCase());
        }
        return left.endsWith(right);
    }
}
