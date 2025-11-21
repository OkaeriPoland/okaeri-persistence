package eu.okaeri.persistence.jdbc.filter;

import eu.okaeri.persistence.PersistencePath;
import eu.okaeri.persistence.filter.OrderBy;
import eu.okaeri.persistence.filter.predicate.Predicate;
import eu.okaeri.persistence.filter.predicate.SimplePredicate;
import eu.okaeri.persistence.filter.renderer.StringRenderer;
import lombok.NonNull;

import java.util.List;
import java.util.stream.Collectors;

public class MariaDbFilterRenderer extends SqlFilterRenderer {

    public MariaDbFilterRenderer(@NonNull StringRenderer stringRenderer) {
        super(stringRenderer);
    }

    @Override
    public String renderPredicate(@NonNull PersistencePath path, @NonNull Predicate predicate) {

        String jsonPath = path.toMariaDbJsonPath();

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

        // String comparisons - use json_unquote for proper string matching
        return "(json_unquote(json_extract(`value`, " + this.renderOperand(jsonPath) + ")) "
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
