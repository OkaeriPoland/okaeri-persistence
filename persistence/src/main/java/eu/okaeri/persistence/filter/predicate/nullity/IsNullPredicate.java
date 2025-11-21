package eu.okaeri.persistence.filter.predicate.nullity;

import eu.okaeri.persistence.filter.predicate.Predicate;

/**
 * VALUE is null
 * {@code val == null}
 */
public class IsNullPredicate implements Predicate {

    @Override
    public boolean check(Object leftOperand) {
        return leftOperand == null;
    }
}
