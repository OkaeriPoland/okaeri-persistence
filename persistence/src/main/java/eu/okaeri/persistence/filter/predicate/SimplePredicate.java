package eu.okaeri.persistence.filter.predicate;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NonNull;

@Data
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public abstract class SimplePredicate<T> implements Predicate<T> {

    private final T rightOperand;

    public static SimplePredicate<Double> eq(double rightOperand) {
        return new EqPredicate<>(rightOperand);
    }

    public static SimplePredicate<CharSequence> eq(@NonNull CharSequence rightOperand) {
        return new EqPredicate<>(rightOperand);
    }

    public static SimplePredicate<Double> ge(double rightOperand) {
        return new GePredicate<>(rightOperand);
    }

    public static SimplePredicate<CharSequence> ge(@NonNull CharSequence rightOperand) {
        return new GePredicate<>(rightOperand);
    }

    public static SimplePredicate<Double> gt(double rightOperand) {
        return new GtPredicate<>(rightOperand);
    }

    public static SimplePredicate<CharSequence> gt(@NonNull CharSequence rightOperand) {
        return new GtPredicate<>(rightOperand);
    }

    public static SimplePredicate<Double> le(double rightOperand) {
        return new LePredicate<>(rightOperand);
    }

    public static SimplePredicate<CharSequence> le(@NonNull CharSequence rightOperand) {
        return new LePredicate<>(rightOperand);
    }

    public static SimplePredicate<Double> lt(double rightOperand) {
        return new LtPredicate<>(rightOperand);
    }

    public static SimplePredicate<CharSequence> lt(@NonNull CharSequence rightOperand) {
        return new LtPredicate<>(rightOperand);
    }

    public static SimplePredicate<Double> ne(double rightOperand) {
        return new NePredicate<>(rightOperand);
    }

    public static SimplePredicate<CharSequence> ne(@NonNull CharSequence rightOperand) {
        return new NePredicate<>(rightOperand);
    }
}
