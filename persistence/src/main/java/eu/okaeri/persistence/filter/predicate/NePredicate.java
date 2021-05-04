package eu.okaeri.persistence.filter.predicate;

/**
 * VALUE not equals X
 * val != x
 */
public class NePredicate<T> extends PredicateNumeric<T> {

    public NePredicate(T rightOperand) {
        super(rightOperand);
    }

    @Override
    public boolean results(int compareResult) {
        return compareResult != 0;
    }
}
