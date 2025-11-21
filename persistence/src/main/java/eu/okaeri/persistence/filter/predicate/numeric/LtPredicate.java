package eu.okaeri.persistence.filter.predicate.numeric;

import eu.okaeri.persistence.filter.predicate.PredicateNumeric;
import lombok.NonNull;

/**
 * VALUE lower than X
 * {@code val < x}
 */
public class LtPredicate extends PredicateNumeric {

    public LtPredicate(@NonNull Object rightOperand) {
        super(rightOperand);
    }

    @Override
    public boolean results(int compareResult) {
        return compareResult < 0;
    }
}
