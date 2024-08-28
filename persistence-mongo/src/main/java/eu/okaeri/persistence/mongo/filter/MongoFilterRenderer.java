package eu.okaeri.persistence.mongo.filter;

import eu.okaeri.persistence.PersistencePath;
import eu.okaeri.persistence.filter.condition.LogicalOperator;
import eu.okaeri.persistence.filter.predicate.*;
import eu.okaeri.persistence.filter.renderer.DefaultFilterRenderer;
import lombok.NonNull;

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
    public String renderPredicate(@NonNull PersistencePath path, @NonNull Predicate predicate) {
        return "{ \"" + path.toMongoPath() + "\": { \"" + this.renderOperator(predicate) + "\": " + this.renderOperand(predicate) + " }}";
    }
}
