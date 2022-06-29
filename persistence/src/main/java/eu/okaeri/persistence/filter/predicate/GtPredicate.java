package eu.okaeri.persistence.filter.predicate;

import lombok.NonNull;

/**
 * VALUE greater than X
 * {@code val > x}
 */
public class GtPredicate<T> extends PredicateNumeric<T> {

    public GtPredicate(@NonNull T rightOperand) {
        super(rightOperand);
    }

    @Override
    public boolean results(int compareResult) {
        return compareResult > 0;
    }
}
