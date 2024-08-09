package eu.okaeri.persistence.filter.predicate;

import java.math.BigDecimal;
import lombok.NonNull;

public abstract class PredicateNumeric<T> extends Predicate<T> {

  protected PredicateNumeric(@NonNull final T rightOperand) {
    super(rightOperand);
  }

  @Override
  public boolean check(@NonNull final Object leftOperand) {

    if ((leftOperand instanceof Number) && (this.getRightOperand() instanceof Number)) {
      final BigDecimal left = new BigDecimal(String.valueOf(leftOperand));
      final BigDecimal right = new BigDecimal(String.valueOf(this.getRightOperand()));
      return this.results(left.compareTo(right));
    }

    if ((leftOperand instanceof CharSequence) && (this.getRightOperand() instanceof Number)) {
      final BigDecimal left = new BigDecimal(String.valueOf(leftOperand).length());
      final BigDecimal right = new BigDecimal(String.valueOf(this.getRightOperand()));
      return this.results(left.compareTo(right));
    }

    if ((leftOperand instanceof Number) && (this.getRightOperand() instanceof CharSequence)) {
      final BigDecimal left = new BigDecimal(String.valueOf(leftOperand));
      final BigDecimal right = new BigDecimal(String.valueOf(this.getRightOperand()).length());
      return this.results(left.compareTo(right));
    }

    throw new IllegalArgumentException(
        "cannot check " + this.getRightOperand() + " against " + leftOperand);
  }

  public abstract boolean results(int compareResult);
}
