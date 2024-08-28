package eu.okaeri.persistence.filter.predicate;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NonNull;

@Data
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public abstract class SimplePredicate implements Predicate {

    private final Object rightOperand;

    public static SimplePredicate eq(double rightOperand) {
        return new EqPredicate(rightOperand);
    }

    public static SimplePredicate eq(@NonNull CharSequence rightOperand) {
        return new EqPredicate(rightOperand);
    }

    public static SimplePredicate ge(double rightOperand) {
        return new GePredicate(rightOperand);
    }

    public static SimplePredicate gt(double rightOperand) {
        return new GtPredicate(rightOperand);
    }

    public static SimplePredicate le(double rightOperand) {
        return new LePredicate(rightOperand);
    }

    public static SimplePredicate lt(double rightOperand) {
        return new LtPredicate(rightOperand);
    }

    public static SimplePredicate ne(double rightOperand) {
        return new NePredicate(rightOperand);
    }

    public static SimplePredicate ne(@NonNull CharSequence rightOperand) {
        return new NePredicate(rightOperand);
    }
}
