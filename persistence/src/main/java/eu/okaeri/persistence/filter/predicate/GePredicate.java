package eu.okaeri.persistence.filter.predicate;

import lombok.NonNull;

/** VALUE greater than or equal to X {@code val >= x} */
public class GePredicate<T> extends PredicateNumeric<T> {

  public GePredicate(@NonNull final T rightOperand) {
    super(rightOperand);
  }

  @Override
  public boolean results(final int compareResult) {
    return compareResult >= 0;
  }
}
