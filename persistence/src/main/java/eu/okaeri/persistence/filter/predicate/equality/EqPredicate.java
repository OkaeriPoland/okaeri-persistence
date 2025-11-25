package eu.okaeri.persistence.filter.predicate.equality;

import eu.okaeri.persistence.filter.predicate.SimplePredicate;
import lombok.Getter;
import lombok.NonNull;

import static eu.okaeri.persistence.document.DocumentValueUtils.compareEquals;
import static eu.okaeri.persistence.filter.predicate.PredicateValidation.validated;

/**
 * VALUE equals X
 * {@code val == x}
 */
public class EqPredicate extends SimplePredicate {

    @Getter
    private final boolean ignoreCase;

    public EqPredicate(@NonNull Object rightOperand) {
        this(rightOperand, false);
    }

    public EqPredicate(@NonNull Object rightOperand, boolean ignoreCase) {
        super(validated(rightOperand));
        this.ignoreCase = ignoreCase;
    }

    /**
     * Returns a case-insensitive version of this predicate.
     * Only applicable for string comparisons.
     *
     * @throws IllegalStateException if the right operand is not a string
     */
    public SimplePredicate ignoreCase() {
        if (!(this.getRightOperand() instanceof CharSequence)) {
            throw new IllegalStateException("ignoreCase() can only be used with string equality predicates, got: " + this.getRightOperand().getClass().getName());
        }
        return new EqPredicate(this.getRightOperand(), true);
    }

    @Override
    public boolean check(Object leftOperand) {
        if (this.ignoreCase && (leftOperand instanceof String) && (this.getRightOperand() instanceof String)) {
            return ((String) leftOperand).equalsIgnoreCase((String) this.getRightOperand());
        }
        return compareEquals(leftOperand, this.getRightOperand());
    }
}
