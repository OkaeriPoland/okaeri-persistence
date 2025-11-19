package eu.okaeri.persistence.filter.renderer;

import eu.okaeri.persistence.PersistencePath;
import eu.okaeri.persistence.filter.OrderBy;
import eu.okaeri.persistence.filter.condition.Condition;
import eu.okaeri.persistence.filter.condition.LogicalOperator;
import eu.okaeri.persistence.filter.predicate.Predicate;
import lombok.NonNull;

import java.util.List;

public interface FilterRenderer {

    String renderOperator(@NonNull LogicalOperator operator);

    String renderOperator(@NonNull Predicate predicate);

    String renderCondition(@NonNull Condition condition);

    String renderPredicate(@NonNull PersistencePath path, @NonNull Predicate predicate);

    String renderOperand(@NonNull Object operand);

    String renderOrderBy(@NonNull List<OrderBy> orderBy);
}
