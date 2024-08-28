package eu.okaeri.persistence.filter.predicate;

import lombok.NonNull;

/**
 * VALUE greater than or equal to X
 * {@code val >= x}
 */
public class GePredicate extends PredicateNumeric {

    public GePredicate(@NonNull Object rightOperand) {
        super(rightOperand);
    }

    @Override
    public boolean results(int compareResult) {
        return compareResult >= 0;
    }
}
