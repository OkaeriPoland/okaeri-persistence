package eu.okaeri.persistence.filter.renderer;

import eu.okaeri.persistence.filter.condition.Condition;
import eu.okaeri.persistence.filter.condition.LogicalOperator;
import eu.okaeri.persistence.filter.predicate.Predicate;
import lombok.NonNull;

public interface FilterRenderer {

    String renderOperator(@NonNull LogicalOperator operator);

    String renderOperator(@NonNull Predicate predicate);

    String renderCondition(@NonNull Condition condition);

    String renderPredicate(@NonNull Predicate predicate);

    String renderPredicate(@NonNull Object leftOperand, @NonNull Predicate predicate);

    String renderOperand(@NonNull Object operand);
}
