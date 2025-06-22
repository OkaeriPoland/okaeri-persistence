package eu.okaeri.persistence.mongo.filter;

import eu.okaeri.persistence.PersistencePath;
import eu.okaeri.persistence.filter.condition.Condition;
import eu.okaeri.persistence.filter.condition.LogicalOperator;
import eu.okaeri.persistence.filter.predicate.*;
import eu.okaeri.persistence.filter.renderer.DefaultFilterRenderer;
import lombok.NonNull;

import java.util.Arrays;
import java.util.stream.Collectors;

public class MongoFilterRenderer extends DefaultFilterRenderer {

    public MongoFilterRenderer() {
        super(new MongoStringRenderer());
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
    public String renderOperator(@NonNull Predicate predicate) {

        if (predicate instanceof EqPredicate) {
            return "$eq";
        } else if (predicate instanceof GtePredicate) {
            return "$gte";
        } else if (predicate instanceof GtPredicate) {
            return "$gt";
        } else if (predicate instanceof LtePredicate) {
            return "$lte";
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
                    return this.renderCondition((Condition) predicate);
                } else {
                    return this.renderPredicate(condition.getPath(), predicate);
                }
            })
            .collect(Collectors.joining(", "));

        return "{\"" + operator + "\": [" + conditions + "]}";
    }

    @Override
    public String renderPredicate(@NonNull PersistencePath path, @NonNull Predicate predicate) {
        return "{ \"" + path.toMongoPath() + "\": { \"" + this.renderOperator(predicate) + "\": " + this.renderOperand(predicate) + " }}";
    }
}
