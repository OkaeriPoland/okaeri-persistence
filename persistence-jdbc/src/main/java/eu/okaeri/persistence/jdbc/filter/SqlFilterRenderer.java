package eu.okaeri.persistence.jdbc.filter;

import eu.okaeri.persistence.filter.condition.LogicalOperator;
import eu.okaeri.persistence.filter.predicate.Predicate;
import eu.okaeri.persistence.filter.predicate.SimplePredicate;
import eu.okaeri.persistence.filter.predicate.collection.InPredicate;
import eu.okaeri.persistence.filter.predicate.collection.NotInPredicate;
import eu.okaeri.persistence.filter.predicate.equality.EqPredicate;
import eu.okaeri.persistence.filter.renderer.DefaultFilterRenderer;
import eu.okaeri.persistence.filter.renderer.StringRenderer;
import lombok.NonNull;

import java.util.Collection;
import java.util.stream.Collectors;

public class SqlFilterRenderer extends DefaultFilterRenderer {

    public SqlFilterRenderer(@NonNull StringRenderer stringRenderer) {
        super(stringRenderer);
    }

    /**
     * Escapes special characters in LIKE patterns.
     * Uses | as the escape character to avoid backslash escaping complexity.
     * SQL LIKE special characters: % (any chars), _ (single char), and our escape char |
     */
    protected String escapeLikePattern(@NonNull String value) {
        return value
            .replace("|", "||")  // Escape character must be escaped first!
            .replace("%", "|%")
            .replace("_", "|_");
    }

    /**
     * Renders a LIKE pattern with optional prefix/suffix wildcards.
     * Properly escapes the value and wraps in SQL quotes via StringRenderer.
     */
    protected String renderLikePattern(@NonNull String value, String prefix, String suffix) {
        String escaped = this.escapeLikePattern(value);
        String pattern = (prefix != null ? prefix : "") + escaped + (suffix != null ? suffix : "");
        return this.stringRenderer.render(pattern);
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
        if (predicate instanceof InPredicate) {
            return "in";
        }
        if (predicate instanceof NotInPredicate) {
            return "not in";
        }
        return super.renderOperator(predicate);
    }

    @Override
    public String renderOperand(@NonNull Object operand) {
        // Extract right operand from SimplePredicate first
        if (operand instanceof SimplePredicate) {
            operand = ((SimplePredicate) operand).getRightOperand();
        }
        // For SQL, booleans in JSON are stored as string literals 'true'/'false'
        // so we need to render them as strings for comparison
        if (operand instanceof Boolean) {
            return this.stringRenderer.render(String.valueOf(operand));
        }
        // For SQL, collections should use parentheses instead of brackets
        if (operand instanceof Collection) {
            Collection<?> collection = (Collection<?>) operand;
            return "(" + collection.stream()
                .map(this::renderOperand)
                .collect(Collectors.joining(", ")) + ")";
        }
        return super.renderOperand(operand);
    }
}
