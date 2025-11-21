package eu.okaeri.persistence.jdbc.filter;

import eu.okaeri.persistence.PersistencePath;
import eu.okaeri.persistence.filter.OrderBy;
import eu.okaeri.persistence.filter.predicate.Predicate;
import eu.okaeri.persistence.filter.predicate.SimplePredicate;
import eu.okaeri.persistence.filter.predicate.collection.InPredicate;
import eu.okaeri.persistence.filter.predicate.collection.NotInPredicate;
import eu.okaeri.persistence.filter.predicate.nullity.IsNullPredicate;
import eu.okaeri.persistence.filter.predicate.nullity.NotNullPredicate;
import eu.okaeri.persistence.filter.predicate.string.ContainsPredicate;
import eu.okaeri.persistence.filter.predicate.string.EndsWithPredicate;
import eu.okaeri.persistence.filter.predicate.string.StartsWithPredicate;
import eu.okaeri.persistence.filter.renderer.StringRenderer;
import lombok.NonNull;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class MariaDbFilterRenderer extends SqlFilterRenderer {

    public MariaDbFilterRenderer(@NonNull StringRenderer stringRenderer) {
        super(stringRenderer);
    }

    @Override
    public String renderPredicate(@NonNull PersistencePath path, @NonNull Predicate predicate) {

        String jsonPath = path.toMariaDbJsonPath();

        // Special handling for null predicates
        if (predicate instanceof IsNullPredicate) {
            return "(json_extract(`value`, " + this.renderOperand(jsonPath) + ") is null)";
        }
        if (predicate instanceof NotNullPredicate) {
            return "(json_extract(`value`, " + this.renderOperand(jsonPath) + ") is not null)";
        }

        // Handle IN/NOT IN predicates with numeric collections
        if ((predicate instanceof InPredicate) && ((InPredicate) predicate).isNumeric()) {
            Collection<?> collection = (Collection<?>) ((InPredicate) predicate).getRightOperand();
            Number firstValue = (Number) collection.iterator().next();
            String castType = ((firstValue instanceof Double) || (firstValue instanceof Float))
                ? "decimal(20,10)"
                : "signed";

            return "(cast(json_extract(`value`, " + this.renderOperand(jsonPath) + ") as " + castType + ") "
                + this.renderOperator(predicate) + " " + this.renderOperand(predicate) + ")";
        }
        if ((predicate instanceof NotInPredicate) && ((NotInPredicate) predicate).isNumeric()) {
            Collection<?> collection = (Collection<?>) ((NotInPredicate) predicate).getRightOperand();
            Number firstValue = (Number) collection.iterator().next();
            String castType = ((firstValue instanceof Double) || (firstValue instanceof Float))
                ? "decimal(20,10)"
                : "signed";

            return "(cast(json_extract(`value`, " + this.renderOperand(jsonPath) + ") as " + castType + ") "
                + this.renderOperator(predicate) + " " + this.renderOperand(predicate) + ")";
        }

        // Handle numeric comparisons with proper type casting
        if ((predicate instanceof SimplePredicate) && (((SimplePredicate) predicate).getRightOperand() instanceof Number)) {
            Number value = (Number) ((SimplePredicate) predicate).getRightOperand();

            // Use decimal for floating point, signed for integers
            String castType = ((value instanceof Double) || (value instanceof Float))
                ? "decimal(20,10)"
                : "signed";

            return "(cast(json_extract(`value`, " + this.renderOperand(jsonPath) + ") as " + castType + ") "
                + this.renderOperator(predicate) + " " + this.renderOperand(predicate) + ")";
        }

        // Handle string predicates with LIKE
        String unquotedField = "json_unquote(json_extract(`value`, " + this.renderOperand(jsonPath) + "))";
        if (predicate instanceof StartsWithPredicate) {
            String value = (String) ((StartsWithPredicate) predicate).getRightOperand();
            String pattern = this.renderLikePattern(value, null, "%");
            String comparison = ((StartsWithPredicate) predicate).isIgnoreCase()
                ? ("lower(" + unquotedField + ") like lower(" + pattern + ")")
                : (unquotedField + " like " + pattern);
            return "(" + comparison + " escape '|')";
        }
        if (predicate instanceof EndsWithPredicate) {
            String value = (String) ((EndsWithPredicate) predicate).getRightOperand();
            String pattern = this.renderLikePattern(value, "%", null);
            String comparison = ((EndsWithPredicate) predicate).isIgnoreCase()
                ? ("lower(" + unquotedField + ") like lower(" + pattern + ")")
                : (unquotedField + " like " + pattern);
            return "(" + comparison + " escape '|')";
        }
        if (predicate instanceof ContainsPredicate) {
            String value = (String) ((ContainsPredicate) predicate).getRightOperand();
            String pattern = this.renderLikePattern(value, "%", "%");
            String comparison = ((ContainsPredicate) predicate).isIgnoreCase()
                ? ("lower(" + unquotedField + ") like lower(" + pattern + ")")
                : (unquotedField + " like " + pattern);
            return "(" + comparison + " escape '|')";
        }

        // String comparisons - use json_unquote for proper string matching
        return "(" + unquotedField + " "
            + this.renderOperator(predicate) + " " + this.renderOperand(predicate) + ")";
    }

    @Override
    public String renderOrderBy(@NonNull List<OrderBy> orderBy) {
        return orderBy.stream()
            .map(order -> {
                String jsonPath = order.getPath().toMariaDbJsonPath();
                // Use json_unquote for string sorting
                return "json_unquote(json_extract(`value`, " + this.renderOperand(jsonPath) + ")) "
                    + order.getDirection().name().toLowerCase();
            })
            .collect(Collectors.joining(", "));
    }
}
