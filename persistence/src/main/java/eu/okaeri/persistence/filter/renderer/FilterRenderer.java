package eu.okaeri.persistence.filter.renderer;

import eu.okaeri.persistence.filter.condition.Condition;
import eu.okaeri.persistence.filter.condition.ConditionOperator;
import eu.okaeri.persistence.filter.predicate.Predicate;
import lombok.NonNull;

public interface FilterRenderer {

    String renderOperator(@NonNull ConditionOperator operator);

    String renderCondition(@NonNull Condition condition);

    String renderPredicate(@NonNull Predicate<?> predicate);

    String renderPredicate(@NonNull Object leftOperand, @NonNull Predicate<?> predicate);

    String renderOperand(@NonNull Object operand);
}
