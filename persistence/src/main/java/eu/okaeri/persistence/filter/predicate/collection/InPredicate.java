package eu.okaeri.persistence.filter.predicate.collection;

import eu.okaeri.persistence.filter.predicate.SimplePredicate;
import lombok.Getter;
import lombok.NonNull;

import java.util.Collection;

import static eu.okaeri.persistence.document.DocumentValueUtils.compareEquals;

/**
 * VALUE in collection
 * {@code val in [x, y, z]}
 */
public class InPredicate extends SimplePredicate {

    @Getter
    private final boolean numeric;

    public InPredicate(@NonNull Collection<?> values) {
        super(values);
        this.numeric = validateAndCheckNumeric(values, "IN");
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean check(@NonNull Object leftOperand) {
        Collection<?> collection = (Collection<?>) this.getRightOperand();
        return collection.stream().anyMatch(value -> compareEquals(leftOperand, value));
    }
}
