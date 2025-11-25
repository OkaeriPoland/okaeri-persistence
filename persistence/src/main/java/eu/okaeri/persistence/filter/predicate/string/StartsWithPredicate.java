package eu.okaeri.persistence.filter.predicate.string;

import eu.okaeri.persistence.filter.predicate.SimplePredicate;
import lombok.Getter;
import lombok.NonNull;

import static eu.okaeri.persistence.filter.predicate.PredicateValidation.validated;

/**
 * String starts with predicate.
 * {@code field startsWith "prefix"}
 */
public class StartsWithPredicate extends SimplePredicate {

    @Getter
    private final boolean ignoreCase;

    public StartsWithPredicate(@NonNull String prefix) {
        this(prefix, false);
    }

    public StartsWithPredicate(@NonNull String prefix, boolean ignoreCase) {
        super(validated(prefix));
        this.ignoreCase = ignoreCase;
    }

    /**
     * Returns a case-insensitive version of this predicate.
     */
    public SimplePredicate ignoreCase() {
        return new StartsWithPredicate((String) this.getRightOperand(), true);
    }

    @Override
    public boolean check(Object leftOperand) {
        if (leftOperand == null) {
            return false;
        }
        String left = String.valueOf(leftOperand);
        String right = (String) this.getRightOperand();
        if (this.ignoreCase) {
            return left.toLowerCase().startsWith(right.toLowerCase());
        }
        return left.startsWith(right);
    }
}
