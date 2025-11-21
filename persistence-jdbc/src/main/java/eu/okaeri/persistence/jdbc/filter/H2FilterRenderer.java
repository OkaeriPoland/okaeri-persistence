package eu.okaeri.persistence.jdbc.filter;

import eu.okaeri.persistence.PersistencePath;
import eu.okaeri.persistence.filter.OrderBy;
import eu.okaeri.persistence.filter.predicate.Predicate;
import eu.okaeri.persistence.filter.predicate.SimplePredicate;
import eu.okaeri.persistence.filter.renderer.StringRenderer;
import lombok.NonNull;

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

        // String comparisons - cast to varchar and trim quotes for proper string matching
        // H2 JSON field reference returns strings with quotes, so we need to remove them
        return "(trim('\"' from cast(" + fieldReference + " as varchar)) "
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
