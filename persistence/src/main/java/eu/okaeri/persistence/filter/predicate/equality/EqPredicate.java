package eu.okaeri.persistence.filter.predicate.equality;

import eu.okaeri.persistence.filter.predicate.SimplePredicate;
import lombok.NonNull;

import static eu.okaeri.persistence.document.DocumentValueUtils.compareEquals;

/**
 * VALUE equals X
 * {@code val == x}
 */
public class EqPredicate extends SimplePredicate {

    public EqPredicate(@NonNull Object rightOperand) {
        super(rightOperand);
    }

    @Override
    public boolean check(@NonNull Object leftOperand) {
        return compareEquals(leftOperand, this.getRightOperand());
    }
}
