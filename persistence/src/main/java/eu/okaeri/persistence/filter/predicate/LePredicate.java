package eu.okaeri.persistence.filter.predicate;

import lombok.NonNull;

/**
 * VALUE lower than or equal to X
 * {@code val <= x}
 */
public class LePredicate extends PredicateNumeric {

    public LePredicate(@NonNull Object rightOperand) {
        super(rightOperand);
    }

    @Override
    public boolean results(int compareResult) {
        return compareResult <= 0;
    }
}
