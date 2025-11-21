package eu.okaeri.persistence.filter.predicate.numeric;

import eu.okaeri.persistence.filter.predicate.PredicateNumeric;
import lombok.NonNull;

/**
 * VALUE lower than or equal to X
 * {@code val <= x}
 */
public class LtePredicate extends PredicateNumeric {

    public LtePredicate(@NonNull Object rightOperand) {
        super(rightOperand);
    }

    @Override
    public boolean results(int compareResult) {
        return compareResult <= 0;
    }
}
