package eu.okaeri.persistence.filter.predicate.equality;

import eu.okaeri.persistence.filter.predicate.SimplePredicate;
import lombok.NonNull;

import static eu.okaeri.persistence.document.DocumentValueUtils.compareEquals;
import static eu.okaeri.persistence.filter.predicate.PredicateValidation.validated;

/**
 * VALUE not equals X
 * {@code val != x}
 */
public class NePredicate extends SimplePredicate {

    public NePredicate(@NonNull Object rightOperand) {
        super(validated(rightOperand));
    }

    @Override
    public boolean check(Object leftOperand) {
        // null values satisfy ne (document-first: null != X is true)
        if (leftOperand == null) {
            return true;
        }
        return !compareEquals(leftOperand, this.getRightOperand());
    }
}
