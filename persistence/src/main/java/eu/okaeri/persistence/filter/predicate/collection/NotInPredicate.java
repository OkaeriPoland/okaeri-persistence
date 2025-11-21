package eu.okaeri.persistence.filter.predicate.collection;

import eu.okaeri.persistence.filter.predicate.SimplePredicate;
import lombok.Getter;
import lombok.NonNull;

import java.util.Collection;

import static eu.okaeri.persistence.document.DocumentValueUtils.compareEquals;

/**
 * VALUE not in collection
 * {@code val not in [x, y, z]}
 */
public class NotInPredicate extends SimplePredicate {

    @Getter
    private final boolean numeric;

    public NotInPredicate(@NonNull Collection<?> values) {
        super(values);
        this.numeric = validateAndCheckNumeric(values, "NOT IN");
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean check(@NonNull Object leftOperand) {
        Collection<?> collection = (Collection<?>) this.getRightOperand();
        return collection.stream().noneMatch(value -> compareEquals(leftOperand, value));
    }
}
