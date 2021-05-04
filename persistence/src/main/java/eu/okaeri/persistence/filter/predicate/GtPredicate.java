package eu.okaeri.persistence.filter.predicate;

/**
 * VALUE greater than X
 * val > x
 */
public class GtPredicate<T> extends PredicateNumeric<T> {

    public GtPredicate(T rightOperand) {
        super(rightOperand);
    }

    @Override
    public boolean results(int compareResult) {
        return compareResult > 0;
    }
}
