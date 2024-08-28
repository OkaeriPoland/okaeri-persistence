package eu.okaeri.persistence.jdbc.filter;

import eu.okaeri.persistence.filter.condition.LogicalOperator;
import eu.okaeri.persistence.filter.predicate.EqPredicate;
import eu.okaeri.persistence.filter.predicate.Predicate;
import eu.okaeri.persistence.filter.renderer.DefaultFilterRenderer;
import eu.okaeri.persistence.filter.renderer.StringRenderer;
import lombok.NonNull;

public class SqlFilterRenderer extends DefaultFilterRenderer {

    public SqlFilterRenderer(@NonNull StringRenderer stringRenderer) {
        super(stringRenderer);
    }

    @Override
    public String renderOperator(@NonNull LogicalOperator operator) {
        if (operator == LogicalOperator.AND) {
            return " and ";
        }
        if (operator == LogicalOperator.OR) {
            return " or ";
        }
        throw new IllegalArgumentException("Unsupported operator: " + operator);
    }

    @Override
    public String renderOperator(@NonNull Predicate predicate) {
        if (predicate instanceof EqPredicate) {
            return "=";
        }
        return super.renderOperator(predicate);
    }
}
