package eu.okaeri.persistence.filter.predicate;

/**
 * VALUE greater than or equal to X
 * val >= x
 */
public class GePredicate<T> extends PredicateNumeric<T> {

    public GePredicate(T rightOperand) {
        super(rightOperand);
    }

    @Override
    public boolean results(int compareResult) {
        return compareResult >= 0;
    }
}
