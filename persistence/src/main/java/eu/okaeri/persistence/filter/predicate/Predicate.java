package eu.okaeri.persistence.filter.predicate;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NonNull;

@Data
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public abstract class Predicate<T> {

  private final T rightOperand;

  public static Predicate<Double> eq(final double rightOperand) {
    return new EqPredicate<>(rightOperand);
  }

  public static Predicate<CharSequence> eq(@NonNull final CharSequence rightOperand) {
    return new EqPredicate<>(rightOperand);
  }

  public static Predicate<Double> ge(final double rightOperand) {
    return new GePredicate<>(rightOperand);
  }

  public static Predicate<CharSequence> ge(@NonNull final CharSequence rightOperand) {
    return new GePredicate<>(rightOperand);
  }

  public static Predicate<Double> gt(final double rightOperand) {
    return new GtPredicate<>(rightOperand);
  }

  public static Predicate<CharSequence> gt(@NonNull final CharSequence rightOperand) {
    return new GtPredicate<>(rightOperand);
  }

  public static Predicate<Double> le(final double rightOperand) {
    return new LePredicate<>(rightOperand);
  }

  public static Predicate<CharSequence> le(@NonNull final CharSequence rightOperand) {
    return new LePredicate<>(rightOperand);
  }

  public static Predicate<Double> lt(final double rightOperand) {
    return new LtPredicate<>(rightOperand);
  }

  public static Predicate<CharSequence> lt(@NonNull final CharSequence rightOperand) {
    return new LtPredicate<>(rightOperand);
  }

  public static Predicate<Double> ne(final double rightOperand) {
    return new NePredicate<>(rightOperand);
  }

  public static Predicate<CharSequence> ne(@NonNull final CharSequence rightOperand) {
    return new NePredicate<>(rightOperand);
  }

  public abstract boolean check(Object leftOperand);
}
