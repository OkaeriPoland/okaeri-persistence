package eu.okaeri.persistence.filter.predicate;

import lombok.NonNull;

/** VALUE lower than or equal to X {@code val <= x} */
public class LePredicate<T> extends PredicateNumeric<T> {

  public LePredicate(@NonNull final T rightOperand) {
    super(rightOperand);
  }

  @Override
  public boolean results(final int compareResult) {
    return compareResult <= 0;
  }
}
