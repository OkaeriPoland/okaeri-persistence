package eu.okaeri.persistence.mongo.filter;

import eu.okaeri.persistence.PersistencePath;
import eu.okaeri.persistence.filter.OrderBy;
import eu.okaeri.persistence.filter.OrderDirection;
import eu.okaeri.persistence.filter.condition.Condition;
import eu.okaeri.persistence.filter.condition.LogicalOperator;
import eu.okaeri.persistence.filter.predicate.Predicate;
import eu.okaeri.persistence.filter.predicate.collection.InPredicate;
import eu.okaeri.persistence.filter.predicate.collection.NotInPredicate;
import eu.okaeri.persistence.filter.predicate.equality.EqPredicate;
import eu.okaeri.persistence.filter.predicate.equality.NePredicate;
import eu.okaeri.persistence.filter.predicate.nullity.IsNullPredicate;
import eu.okaeri.persistence.filter.predicate.nullity.NotNullPredicate;
import eu.okaeri.persistence.filter.predicate.numeric.GtPredicate;
import eu.okaeri.persistence.filter.predicate.numeric.GtePredicate;
import eu.okaeri.persistence.filter.predicate.numeric.LtPredicate;
import eu.okaeri.persistence.filter.predicate.numeric.LtePredicate;
import eu.okaeri.persistence.filter.predicate.string.ContainsPredicate;
import eu.okaeri.persistence.filter.predicate.string.EndsWithPredicate;
import eu.okaeri.persistence.filter.predicate.string.StartsWithPredicate;
import eu.okaeri.persistence.filter.renderer.DefaultFilterRenderer;
import lombok.NonNull;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class MongoFilterRenderer extends DefaultFilterRenderer {

    public MongoFilterRenderer() {
        super(new MongoStringRenderer());
    }

    /**
     * Escapes special regex characters for literal string matching in MongoDB $regex.
     * Regex special characters: . * + ? [ ] ( ) { } ^ $ | \
     * Also escapes for JSON context (backslashes and quotes).
     */
    protected String escapeRegex(@NonNull String value) {
        // First escape regex metacharacters with backslash
        String regexEscaped = value.replaceAll("([.\\\\+*?\\[\\](){}^$|])", "\\\\$1");
        // Then escape for JSON context (backslash and quotes must be doubled)
        return regexEscaped.replace("\\", "\\\\").replace("\"", "\\\"");
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
        } else if (predicate instanceof InPredicate) {
            return "$in";
        } else if (predicate instanceof NotInPredicate) {
            return "$nin";
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

        // Special handling for null predicates
        if (predicate instanceof IsNullPredicate) {
            return "{ \"" + path.toMongoPath() + "\": null }";
        }
        if (predicate instanceof NotNullPredicate) {
            return "{ \"" + path.toMongoPath() + "\": { \"$ne\": null } }";
        }

        // Handle case-insensitive equals
        if ((predicate instanceof EqPredicate) && ((EqPredicate) predicate).isIgnoreCase()) {
            String value = this.escapeRegex((String) ((EqPredicate) predicate).getRightOperand());
            return "{ \"" + path.toMongoPath() + "\": { \"$regex\": \"^" + value + "$\", \"$options\": \"i\" }}";
        }

        // Handle string predicates with $regex
        if (predicate instanceof StartsWithPredicate) {
            String value = this.escapeRegex((String) ((StartsWithPredicate) predicate).getRightOperand());
            String options = ((StartsWithPredicate) predicate).isIgnoreCase() ? ", \"$options\": \"i\"" : "";
            return "{ \"" + path.toMongoPath() + "\": { \"$regex\": \"^" + value + "\"" + options + " }}";
        }
        if (predicate instanceof EndsWithPredicate) {
            String value = this.escapeRegex((String) ((EndsWithPredicate) predicate).getRightOperand());
            String options = ((EndsWithPredicate) predicate).isIgnoreCase() ? ", \"$options\": \"i\"" : "";
            return "{ \"" + path.toMongoPath() + "\": { \"$regex\": \"" + value + "$\"" + options + " }}";
        }
        if (predicate instanceof ContainsPredicate) {
            String value = this.escapeRegex((String) ((ContainsPredicate) predicate).getRightOperand());
            String options = ((ContainsPredicate) predicate).isIgnoreCase() ? ", \"$options\": \"i\"" : "";
            return "{ \"" + path.toMongoPath() + "\": { \"$regex\": \"" + value + "\"" + options + " }}";
        }

        return "{ \"" + path.toMongoPath() + "\": { \"" + this.renderOperator(predicate) + "\": " + this.renderOperand(predicate) + " }}";
    }

    @Override
    public String renderOrderBy(@NonNull List<OrderBy> orderBy) {
        String fields = orderBy.stream()
            .map(order -> {
                int direction = (order.getDirection() == OrderDirection.ASC) ? 1 : -1;
                return "\"" + order.getPath().toMongoPath() + "\": " + direction;
            })
            .collect(Collectors.joining(", "));
        return "{" + fields + "}";
    }
}
