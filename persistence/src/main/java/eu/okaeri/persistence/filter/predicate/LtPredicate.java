package eu.okaeri.persistence.filter.predicate;

import lombok.NonNull;

/** VALUE lower than X {@code val < x} */
public class LtPredicate<T> extends PredicateNumeric<T> {

  public LtPredicate(@NonNull final T rightOperand) {
    super(rightOperand);
  }

  @Override
  public boolean results(final int compareResult) {
    return compareResult < 0;
  }
}
