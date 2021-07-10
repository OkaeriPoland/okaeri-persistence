package eu.okaeri.persistence.filter.predicate;

import lombok.NonNull;

/**
 * VALUE greater than or equal to X
 * val >= x
 */
public class GePredicate<T> extends PredicateNumeric<T> {

    public GePredicate(@NonNull T rightOperand) {
        super(rightOperand);
    }

    @Override
    public boolean results(int compareResult) {
        return compareResult >= 0;
    }
}
