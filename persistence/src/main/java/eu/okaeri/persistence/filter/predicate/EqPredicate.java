package eu.okaeri.persistence.filter.predicate;

import lombok.NonNull;

/**
 * VALUE equals X
 * {@code val == x}
 */
public class EqPredicate extends PredicateNumeric {

    public EqPredicate(@NonNull Object rightOperand) {
        super(rightOperand);
    }

    @Override
    public boolean results(int compareResult) {
        return compareResult == 0;
    }
}
