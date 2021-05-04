package eu.okaeri.persistence.filter.predicate;

/**
 * VALUE lower than or equal to X
 * val <= x
 */
public class LePredicate<T> extends PredicateNumeric<T> {

    public LePredicate(T rightOperand) {
        super(rightOperand);
    }

    @Override
    public boolean results(int compareResult) {
        return compareResult <= 0;
    }
}
