package eu.okaeri.persistence.filter.predicate;

import lombok.NonNull;

/**
 * VALUE greater than X
 * {@code val > x}
 */
public class GtPredicate extends PredicateNumeric {

    public GtPredicate(@NonNull Object rightOperand) {
        super(rightOperand);
    }

    @Override
    public boolean results(int compareResult) {
        return compareResult > 0;
    }
}
