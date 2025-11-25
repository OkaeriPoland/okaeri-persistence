package eu.okaeri.persistence.jdbc.filter;

import eu.okaeri.persistence.PersistencePath;
import eu.okaeri.persistence.document.index.IndexProperty;
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
import eu.okaeri.persistence.jdbc.MariaDbPersistence;
import lombok.NonNull;
import lombok.Setter;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class MariaDbFilterRenderer extends SqlFilterRenderer {

    /**
     * Set of indexed properties for the current query context.
     * When set, the renderer will use generated column names instead of JSON expressions
     * for indexed fields, allowing the database to use indexes.
     */
    @Setter
    private Set<IndexProperty> indexedProperties;

    public MariaDbFilterRenderer(@NonNull StringRenderer stringRenderer) {
        super(stringRenderer);
    }

    /**
     * Check if a field path is indexed and return the IndexProperty if so.
     */
    private IndexProperty getIndexProperty(@NonNull String fieldPath) {
        if (this.indexedProperties == null) return null;
        for (IndexProperty index : this.indexedProperties) {
            if (index.getValue().equals(fieldPath)) {
                return index;
            }
        }
        return null;
    }

    @Override
    public String renderPredicate(@NonNull PersistencePath path, @NonNull Predicate predicate) {

        // Check if this field is indexed - if so, use the generated column
        IndexProperty indexProperty = this.getIndexProperty(path.getValue());
        if (indexProperty != null) {
            return this.renderIndexedPredicate(path, predicate, indexProperty);
        }

        String jsonPath = path.toMariaDbJsonPath();

        // Special handling for null predicates
        if (predicate instanceof IsNullPredicate) {
            return "(json_extract(`value`, " + this.renderOperand(jsonPath) + ") is null)";
        }
        if (predicate instanceof NotNullPredicate) {
            return "(json_extract(`value`, " + this.renderOperand(jsonPath) + ") is not null)";
        }

        // Handle ne/notIn with null inclusion (document-first: null != X is true)
        if (predicate instanceof NePredicate) {
            Object rightOperand = ((NePredicate) predicate).getRightOperand();
            String baseCondition;
            if (rightOperand instanceof Number) {
                Number value = (Number) rightOperand;
                String castType = ((value instanceof Double) || (value instanceof Float))
                    ? "decimal(20,10)"
                    : "signed";
                baseCondition = "cast(json_extract(`value`, " + this.renderOperand(jsonPath) + ") as " + castType + ") "
                    + this.renderOperator(predicate) + " " + this.renderOperand(predicate);
            } else {
                String unquotedField = "json_unquote(json_extract(`value`, " + this.renderOperand(jsonPath) + "))";
                baseCondition = unquotedField + " " + this.renderOperator(predicate) + " " + this.renderOperand(predicate);
            }
            return "((" + baseCondition + ") or (json_extract(`value`, " + this.renderOperand(jsonPath) + ") is null))";
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

            String baseCondition = "cast(json_extract(`value`, " + this.renderOperand(jsonPath) + ") as " + castType + ") "
                + this.renderOperator(predicate) + " " + this.renderOperand(predicate);
            return "((" + baseCondition + ") or (json_extract(`value`, " + this.renderOperand(jsonPath) + ") is null))";
        }
        // Handle non-numeric NotInPredicate
        if (predicate instanceof NotInPredicate) {
            String unquotedField = "json_unquote(json_extract(`value`, " + this.renderOperand(jsonPath) + "))";
            String baseCondition = unquotedField + " " + this.renderOperator(predicate) + " " + this.renderOperand(predicate);
            return "((" + baseCondition + ") or (json_extract(`value`, " + this.renderOperand(jsonPath) + ") is null))";
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

        // Handle case-insensitive equals
        String unquotedField = "json_unquote(json_extract(`value`, " + this.renderOperand(jsonPath) + "))";
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

        // String comparisons - use json_unquote for proper string matching
        return "(" + unquotedField + " "
            + this.renderOperator(predicate) + " " + this.renderOperand(predicate) + ")";
    }

    /**
     * Render a predicate for an indexed field using the generated column name directly.
     * This allows the database to use the index instead of scanning JSON.
     */
    private String renderIndexedPredicate(@NonNull PersistencePath path, @NonNull Predicate predicate,
                                          @NonNull IndexProperty indexProperty) {
        String columnName = "`" + MariaDbPersistence.getIndexColumnName(indexProperty) + "`";

        // Null predicates
        if (predicate instanceof IsNullPredicate) {
            return "(" + columnName + " is null)";
        }
        if (predicate instanceof NotNullPredicate) {
            return "(" + columnName + " is not null)";
        }

        // Ne predicate with null inclusion
        if (predicate instanceof NePredicate) {
            String baseCondition = columnName + " " + this.renderOperator(predicate) + " " + this.renderOperand(predicate);
            return "((" + baseCondition + ") or (" + columnName + " is null))";
        }

        // NotIn predicate with null inclusion
        if (predicate instanceof NotInPredicate) {
            String baseCondition = columnName + " " + this.renderOperator(predicate) + " " + this.renderOperand(predicate);
            return "((" + baseCondition + ") or (" + columnName + " is null))";
        }

        // Case-insensitive equals for strings
        if ((predicate instanceof EqPredicate) && ((EqPredicate) predicate).isIgnoreCase()) {
            return "(lower(" + columnName + ") = lower(" + this.renderOperand(predicate) + "))";
        }

        // String LIKE predicates
        if (predicate instanceof StartsWithPredicate) {
            String value = (String) ((StartsWithPredicate) predicate).getRightOperand();
            String pattern = this.renderLikePattern(value, null, "%");
            String comparison = ((StartsWithPredicate) predicate).isIgnoreCase()
                ? ("lower(" + columnName + ") like lower(" + pattern + ")")
                : (columnName + " like " + pattern);
            return "(" + comparison + " escape '|')";
        }
        if (predicate instanceof EndsWithPredicate) {
            String value = (String) ((EndsWithPredicate) predicate).getRightOperand();
            String pattern = this.renderLikePattern(value, "%", null);
            String comparison = ((EndsWithPredicate) predicate).isIgnoreCase()
                ? ("lower(" + columnName + ") like lower(" + pattern + ")")
                : (columnName + " like " + pattern);
            return "(" + comparison + " escape '|')";
        }
        if (predicate instanceof ContainsPredicate) {
            String value = (String) ((ContainsPredicate) predicate).getRightOperand();
            String pattern = this.renderLikePattern(value, "%", "%");
            String comparison = ((ContainsPredicate) predicate).isIgnoreCase()
                ? ("lower(" + columnName + ") like lower(" + pattern + ")")
                : (columnName + " like " + pattern);
            return "(" + comparison + " escape '|')";
        }

        // Standard comparison: column op value
        return "(" + columnName + " " + this.renderOperator(predicate) + " " + this.renderOperand(predicate) + ")";
    }

    @Override
    public String renderOrderBy(@NonNull List<OrderBy> orderBy) {
        return orderBy.stream()
            .map(order -> {
                // Check if this field is indexed - if so, use the generated column for ORDER BY
                IndexProperty indexProperty = this.getIndexProperty(order.getPath().getValue());
                if (indexProperty != null) {
                    String columnName = "`" + MariaDbPersistence.getIndexColumnName(indexProperty) + "`";
                    return columnName + " " + order.getDirection().name().toLowerCase();
                }
                String jsonPath = order.getPath().toMariaDbJsonPath();
                // Use json_unquote for string sorting
                return "json_unquote(json_extract(`value`, " + this.renderOperand(jsonPath) + ")) "
                    + order.getDirection().name().toLowerCase();
            })
            .collect(Collectors.joining(", "));
    }
}
