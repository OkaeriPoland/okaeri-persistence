package eu.okaeri.persistence.jdbc.filter;

import eu.okaeri.persistence.PersistencePath;
import eu.okaeri.persistence.filter.OrderBy;
import eu.okaeri.persistence.filter.predicate.Predicate;
import eu.okaeri.persistence.filter.predicate.SimplePredicate;
import eu.okaeri.persistence.filter.predicate.collection.InPredicate;
import eu.okaeri.persistence.filter.predicate.collection.NotInPredicate;
import eu.okaeri.persistence.filter.predicate.equality.EqPredicate;
import eu.okaeri.persistence.filter.predicate.equality.NePredicate;
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

public class H2FilterRenderer extends SqlFilterRenderer {

    public H2FilterRenderer(@NonNull StringRenderer stringRenderer) {
        super(stringRenderer);
    }

    @Override
    public String renderPredicate(@NonNull PersistencePath path, @NonNull Predicate predicate) {

        // Build H2 field reference syntax: (`value`)."field1"."field2"
        String fieldReference = "(`value`)." + path.toH2FieldReference();

        // Special handling for null predicates
        if (predicate instanceof IsNullPredicate) {
            return "(" + fieldReference + " is null)";
        }
        if (predicate instanceof NotNullPredicate) {
            return "(" + fieldReference + " is not null)";
        }

        // Handle ne/notIn with null inclusion (document-first: null != X is true)
        if (predicate instanceof NePredicate) {
            Object rightOperand = ((NePredicate) predicate).getRightOperand();
            String baseCondition;
            if (rightOperand instanceof Number) {
                Number value = (Number) rightOperand;
                String castType = ((value instanceof Double) || (value instanceof Float))
                    ? "decimal(20,10)"
                    : "int";
                baseCondition = "cast(cast(" + fieldReference + " as varchar) as " + castType + ") "
                    + this.renderOperator(predicate) + " " + this.renderOperand(predicate);
            } else if (rightOperand instanceof Boolean) {
                // H2 JSON booleans don't have quotes, just cast to varchar
                baseCondition = "cast(" + fieldReference + " as varchar) "
                    + this.renderOperator(predicate) + " " + this.renderOperand(predicate);
            } else {
                String castField = "cast(" + fieldReference + " as varchar)";
                String unquotedField = "replace(replace(substring(" + castField + ", 2, length(" + castField + ") - 2), '\\\\', '\\'), '\\\"', '\"')";
                baseCondition = unquotedField + " " + this.renderOperator(predicate) + " " + this.renderOperand(predicate);
            }
            return "((" + baseCondition + ") or (" + fieldReference + " is null))";
        }

        // Handle IN/NOT IN predicates with numeric collections
        if ((predicate instanceof InPredicate) && ((InPredicate) predicate).isNumeric()) {
            Collection<?> collection = (Collection<?>) ((InPredicate) predicate).getRightOperand();
            Number firstValue = (Number) collection.iterator().next();
            String castType = ((firstValue instanceof Double) || (firstValue instanceof Float))
                ? "decimal(20,10)"
                : "int";

            return "(cast(cast(" + fieldReference + " as varchar) as " + castType + ") "
                + this.renderOperator(predicate) + " " + this.renderOperand(predicate) + ")";
        }
        if ((predicate instanceof NotInPredicate) && ((NotInPredicate) predicate).isNumeric()) {
            Collection<?> collection = (Collection<?>) ((NotInPredicate) predicate).getRightOperand();
            Number firstValue = (Number) collection.iterator().next();
            String castType = ((firstValue instanceof Double) || (firstValue instanceof Float))
                ? "decimal(20,10)"
                : "int";

            String baseCondition = "cast(cast(" + fieldReference + " as varchar) as " + castType + ") "
                + this.renderOperator(predicate) + " " + this.renderOperand(predicate);
            return "((" + baseCondition + ") or (" + fieldReference + " is null))";
        }
        // Handle non-numeric NotInPredicate
        if (predicate instanceof NotInPredicate) {
            String castField = "cast(" + fieldReference + " as varchar)";
            String unquotedField = "replace(replace(substring(" + castField + ", 2, length(" + castField + ") - 2), '\\\\', '\\'), '\\\"', '\"')";
            String baseCondition = unquotedField + " " + this.renderOperator(predicate) + " " + this.renderOperand(predicate);
            return "((" + baseCondition + ") or (" + fieldReference + " is null))";
        }

        // Handle numeric comparisons with proper type casting
        if ((predicate instanceof SimplePredicate) && (((SimplePredicate) predicate).getRightOperand() instanceof Number)) {
            Number value = (Number) ((SimplePredicate) predicate).getRightOperand();

            // Use decimal for floating point, int for integers
            String castType = ((value instanceof Double) || (value instanceof Float))
                ? "decimal(20,10)"
                : "int";

            // H2 JSON field reference returns JSON type, must cast to VARCHAR first before numeric cast
            return "(cast(cast(" + fieldReference + " as varchar) as " + castType + ") "
                + this.renderOperator(predicate) + " " + this.renderOperand(predicate) + ")";
        }

        // Handle boolean comparisons - JSON booleans don't have quotes, just cast to varchar
        if ((predicate instanceof SimplePredicate) && (((SimplePredicate) predicate).getRightOperand() instanceof Boolean)) {
            // H2 JSON boolean values (true/false) are stored without quotes in JSON
            // Cast to varchar gives us "true" or "false" as strings
            return "(cast(" + fieldReference + " as varchar) "
                + this.renderOperator(predicate) + " " + this.renderOperand(predicate) + ")";
        }

        // Unescape JSON: remove outer quotes, unescape \\ to \, and unescape \" to "
        // Use SUBSTRING instead of TRIM to remove exactly first and last character (the outer quotes)
        // This prevents accidentally removing quotes that are part of escaped sequences like \"
        // Order matters: unescape \\ first, then \" (so \\\" becomes \" not \")
        String castField = "cast(" + fieldReference + " as varchar)";
        String unquotedField = "replace(replace(substring(" + castField + ", 2, length(" + castField + ") - 2), '\\\\', '\\'), '\\\"', '\"')";

        // Handle case-insensitive equals
        if ((predicate instanceof EqPredicate) && ((EqPredicate) predicate).isIgnoreCase()) {
            return "(lower(" + unquotedField + ") = lower(" + this.renderOperand(predicate) + "))";
        }

        // Handle string predicates with LIKE
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

        // String comparisons - cast to varchar, trim quotes, and unescape JSON
        // H2 JSON field reference returns strings with quotes and escape sequences (e.g., \"book\")
        // We need to: 1) trim outer quotes, 2) unescape \" to "
        return "(" + unquotedField + " "
            + this.renderOperator(predicate) + " " + this.renderOperand(predicate) + ")";
    }

    @Override
    public String renderOrderBy(@NonNull List<OrderBy> orderBy) {
        return orderBy.stream()
            .map(order -> {
                String fieldReference = "(`value`)." + order.getPath().toH2FieldReference();
                return fieldReference + " " + order.getDirection().name().toLowerCase();
            })
            .collect(Collectors.joining(", "));
    }
}
