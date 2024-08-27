package eu.okaeri.persistence.mongo.filter;

import eu.okaeri.persistence.filter.condition.Condition;
import eu.okaeri.persistence.filter.condition.LogicalOperator;
import eu.okaeri.persistence.filter.predicate.*;
import eu.okaeri.persistence.filter.renderer.DefaultFilterRenderer;
import eu.okaeri.persistence.filter.renderer.FilterRendererLiteral;
import eu.okaeri.persistence.filter.renderer.VariableRenderer;
import lombok.NonNull;

import java.util.Arrays;
import java.util.stream.Collectors;

public class MongoFilterRenderer extends DefaultFilterRenderer {

    private static final FilterRendererLiteral LITERAL_X = new FilterRendererLiteral("x");

    public MongoFilterRenderer(VariableRenderer variableRenderer) {
        super(variableRenderer);
    }

    @Override
    public String renderOperator(@NonNull LogicalOperator operator) {
        if (operator == LogicalOperator.AND) {
            return "$and";
        }
        if (operator == LogicalOperator.OR) {
            return "$or";
        }
        throw new IllegalArgumentException("Unsupported operator: " + operator);
    }

    @Override
    public String renderOperator(@NonNull Predicate<?> predicate) {

        if (predicate instanceof EqPredicate) {
            return "$eq";
        } else if (predicate instanceof GePredicate) {
            return "$ge";
        } else if (predicate instanceof GtPredicate) {
            return "$gt";
        } else if (predicate instanceof LePredicate) {
            return "$le";
        } else if (predicate instanceof LtPredicate) {
            return "$lt";
        } else if (predicate instanceof NePredicate) {
            return "$ne";
        }

        throw new IllegalArgumentException("cannot render operator " + predicate + " [" + predicate.getClass() + "]");
    }

    @Override
    public String renderCondition(@NonNull Condition condition) {

        String operator = this.renderOperator(condition.getOperator());
        String conditions = Arrays.stream(condition.getPredicates())
            .map(predicate -> {
                if (predicate instanceof Condition) {
                    return this.renderPredicate(predicate);
                } else {
                    String variable = this.variableRenderer.render(condition.getPath());
                    FilterRendererLiteral variableLiteral = new FilterRendererLiteral(variable);
                    return this.renderPredicate(variableLiteral, predicate);
                }
            })
            .collect(Collectors.joining(", "));

        return "\"" + operator + "\": [" + conditions + "]";
    }

    @Override
    public String renderPredicate(@NonNull Object leftOperand, @NonNull Predicate<?> predicate) {
        return "{ \"" + this.renderOperand(leftOperand) + "\": { \"" + this.renderOperator(predicate) + "\": " + this.renderOperand(predicate) + " }";
    }
}
