package eu.okaeri.persistence.filter.predicate;

/**
 * VALUE equals X
 * val == x
 */
public class EqPredicate<T> extends PredicateNumeric<T> {

    public EqPredicate(T rightOperand) {
        super(rightOperand);
    }

    @Override
    public boolean results(int compareResult) {
        return compareResult == 0;
    }
}
