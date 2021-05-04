package eu.okaeri.persistence.filter.predicate;

/**
 * VALUE lower than X
 * val < x
 */
public class LtPredicate<T> extends PredicateNumeric<T> {

    public LtPredicate(T rightOperand) {
        super(rightOperand);
    }

    @Override
    public boolean results(int compareResult) {
        return compareResult < 0;
    }
}
