package eu.okaeri.persistence.filter.predicate;

import lombok.NonNull;

/**
 * VALUE greater than or equal to X
 * {@code val >= x}
 */
public class GtePredicate extends PredicateNumeric {

    public GtePredicate(@NonNull Object rightOperand) {
        super(rightOperand);
    }

    @Override
    public boolean results(int compareResult) {
        return compareResult >= 0;
    }
}
