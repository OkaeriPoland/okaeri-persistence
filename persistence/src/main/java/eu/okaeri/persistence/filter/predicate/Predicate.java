package eu.okaeri.persistence.filter.predicate;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NonNull;

@Data
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public abstract class Predicate<T> {

    public static Predicate<Double> eq(double rightOperand) {
        return new EqPredicate<>(rightOperand);
    }

    public static Predicate<CharSequence> eq(@NonNull CharSequence rightOperand) {
        return new EqPredicate<>(rightOperand);
    }

    public static Predicate<Double> ge(double rightOperand) {
        return new GePredicate<>(rightOperand);
    }

    public static Predicate<CharSequence> ge(@NonNull CharSequence rightOperand) {
        return new GePredicate<>(rightOperand);
    }

    public static Predicate<Double> gt(double rightOperand) {
        return new GtPredicate<>(rightOperand);
    }

    public static Predicate<CharSequence> gt(@NonNull CharSequence rightOperand) {
        return new GtPredicate<>(rightOperand);
    }

    public static Predicate<Double> le(double rightOperand) {
        return new LePredicate<>(rightOperand);
    }

    public static Predicate<CharSequence> le(@NonNull CharSequence rightOperand) {
        return new LePredicate<>(rightOperand);
    }

    public static Predicate<Double> lt(double rightOperand) {
        return new LtPredicate<>(rightOperand);
    }

    public static Predicate<CharSequence> lt(@NonNull CharSequence rightOperand) {
        return new LtPredicate<>(rightOperand);
    }

    public static Predicate<Double> ne(double rightOperand) {
        return new NePredicate<>(rightOperand);
    }

    public static Predicate<CharSequence> ne(@NonNull CharSequence rightOperand) {
        return new NePredicate<>(rightOperand);
    }

    private final T rightOperand;

    public abstract boolean check(Object leftOperand);
}
