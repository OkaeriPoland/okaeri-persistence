package eu.okaeri.persistence.filter.predicate;

import lombok.NonNull;

/** VALUE equals X {@code val == x} */
public class EqPredicate<T> extends PredicateNumeric<T> {

  public EqPredicate(@NonNull final T rightOperand) {
    super(rightOperand);
  }

  @Override
  public boolean results(final int compareResult) {
    return compareResult == 0;
  }
}
