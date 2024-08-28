package eu.okaeri.persistence.filter.predicate;

import lombok.NonNull;

/**
 * VALUE not equals X
 * {@code val != x}
 */
public class NePredicate extends PredicateNumeric {

    public NePredicate(@NonNull Object rightOperand) {
        super(rightOperand);
    }

    @Override
    public boolean results(int compareResult) {
        return compareResult != 0;
    }
}
