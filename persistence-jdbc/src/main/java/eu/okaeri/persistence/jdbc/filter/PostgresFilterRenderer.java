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

import java.util.List;
import java.util.stream.Collectors;

public class PostgresFilterRenderer extends SqlFilterRenderer {

    public PostgresFilterRenderer(@NonNull StringRenderer stringRenderer) {
        super(stringRenderer);
    }

    @Override
    public String renderPredicate(@NonNull PersistencePath path, @NonNull Predicate predicate) {

        path = PersistencePath.of("value").sub(path);

        // Special handling for null predicates
        if (predicate instanceof IsNullPredicate) {
            return "(" + path.toPostgresJsonPath(true) + " is null)";
        }
        if (predicate instanceof NotNullPredicate) {
            return "(" + path.toPostgresJsonPath(true) + " is not null)";
        }

        // Handle ne/notIn with null inclusion (document-first: null != X is true)
        if (predicate instanceof NePredicate) {
            Object rightOperand = ((NePredicate) predicate).getRightOperand();
            String baseCondition;
            if (rightOperand instanceof Number) {
                baseCondition = "(" + path.toPostgresJsonPath() + ")::numeric "
                    + this.renderOperator(predicate) + " " + this.renderOperand(predicate);
            } else {
                baseCondition = path.toPostgresJsonPath(true) + " "
                    + this.renderOperator(predicate) + " " + this.renderOperand(predicate);
            }
            return "((" + baseCondition + ") or (" + path.toPostgresJsonPath(true) + " is null))";
        }

        // Handle IN/NOT IN predicates with numeric collections
        if ((predicate instanceof InPredicate) && ((InPredicate) predicate).isNumeric()) {
            return "((" + path.toPostgresJsonPath() + ")::numeric " + this.renderOperator(predicate) + " " + this.renderOperand(predicate) + ")";
        }
        if ((predicate instanceof NotInPredicate) && ((NotInPredicate) predicate).isNumeric()) {
            String baseCondition = "(" + path.toPostgresJsonPath() + ")::numeric "
                + this.renderOperator(predicate) + " " + this.renderOperand(predicate);
            return "((" + baseCondition + ") or (" + path.toPostgresJsonPath(true) + " is null))";
        }
        // Handle non-numeric NotInPredicate
        if (predicate instanceof NotInPredicate) {
            String baseCondition = path.toPostgresJsonPath(true) + " "
                + this.renderOperator(predicate) + " " + this.renderOperand(predicate);
            return "((" + baseCondition + ") or (" + path.toPostgresJsonPath(true) + " is null))";
        }

        // Handle other numeric comparisons
        if ((predicate instanceof SimplePredicate) && (((SimplePredicate) predicate).getRightOperand() instanceof Number)) {
            return "((" + path.toPostgresJsonPath() + ")::numeric " + this.renderOperator(predicate) + " " + this.renderOperand(predicate) + ")";
        }

        // Handle case-insensitive equals
        if ((predicate instanceof EqPredicate) && ((EqPredicate) predicate).isIgnoreCase()) {
            return "(lower(" + path.toPostgresJsonPath(true) + ") = lower(" + this.renderOperand(predicate) + "))";
        }

        // Handle string predicates with LIKE
        if (predicate instanceof StartsWithPredicate) {
            String value = (String) ((StartsWithPredicate) predicate).getRightOperand();
            String pattern = this.renderLikePattern(value, null, "%");
            String comparison = ((StartsWithPredicate) predicate).isIgnoreCase()
                ? ("lower(" + path.toPostgresJsonPath(true) + ") like lower(" + pattern + ")")
                : (path.toPostgresJsonPath(true) + " like " + pattern);
            return "(" + comparison + " escape '|')";
        }
        if (predicate instanceof EndsWithPredicate) {
            String value = (String) ((EndsWithPredicate) predicate).getRightOperand();
            String pattern = this.renderLikePattern(value, "%", null);
            String comparison = ((EndsWithPredicate) predicate).isIgnoreCase()
                ? ("lower(" + path.toPostgresJsonPath(true) + ") like lower(" + pattern + ")")
                : (path.toPostgresJsonPath(true) + " like " + pattern);
            return "(" + comparison + " escape '|')";
        }
        if (predicate instanceof ContainsPredicate) {
            String value = (String) ((ContainsPredicate) predicate).getRightOperand();
            String pattern = this.renderLikePattern(value, "%", "%");
            String comparison = ((ContainsPredicate) predicate).isIgnoreCase()
                ? ("lower(" + path.toPostgresJsonPath(true) + ") like lower(" + pattern + ")")
                : (path.toPostgresJsonPath(true) + " like " + pattern);
            return "(" + comparison + " escape '|')";
        }

        return "(" + path.toPostgresJsonPath(true) + " " + this.renderOperator(predicate) + " " + this.renderOperand(predicate) + ")";
    }

    @Override
    public String renderOrderBy(@NonNull List<OrderBy> orderBy) {
        return orderBy.stream()
            .map(order -> {
                PersistencePath path = PersistencePath.of("value").sub(order.getPath());
                return path.toPostgresJsonPath(true) + " " + order.getDirection().name().toLowerCase();
            })
            .collect(Collectors.joining(", "));
    }
}
