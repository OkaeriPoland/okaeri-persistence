package eu.okaeri.persistence.filter.predicate;

import lombok.NonNull;

/** VALUE not equals X {@code val != x} */
public class NePredicate<T> extends PredicateNumeric<T> {

  public NePredicate(@NonNull final T rightOperand) {
    super(rightOperand);
  }

  @Override
  public boolean results(final int compareResult) {
    return compareResult != 0;
  }
}
