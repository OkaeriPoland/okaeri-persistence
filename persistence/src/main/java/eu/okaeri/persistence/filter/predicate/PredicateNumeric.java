package eu.okaeri.persistence.filter.predicate;

import lombok.NonNull;

import java.math.BigDecimal;

public abstract class PredicateNumeric<T> extends SimplePredicate<T> {

    protected PredicateNumeric(@NonNull T rightOperand) {
        super(rightOperand);
    }

    @Override
    public boolean check(@NonNull Object leftOperand) {

        if ((leftOperand instanceof Number) && (this.getRightOperand() instanceof Number)) {
            BigDecimal left = new BigDecimal(String.valueOf(leftOperand));
            BigDecimal right = new BigDecimal(String.valueOf(this.getRightOperand()));
            return this.results(left.compareTo(right));
        }

        if ((leftOperand instanceof CharSequence) && (this.getRightOperand() instanceof Number)) {
            BigDecimal left = new BigDecimal(String.valueOf(leftOperand).length());
            BigDecimal right = new BigDecimal(String.valueOf(this.getRightOperand()));
            return this.results(left.compareTo(right));
        }

        if ((leftOperand instanceof Number) && (this.getRightOperand() instanceof CharSequence)) {
            BigDecimal left = new BigDecimal(String.valueOf(leftOperand));
            BigDecimal right = new BigDecimal(String.valueOf(this.getRightOperand()).length());
            return this.results(left.compareTo(right));
        }

        throw new IllegalArgumentException("cannot check " + this.getRightOperand() + " against " + leftOperand);
    }

    public abstract boolean results(int compareResult);
}
