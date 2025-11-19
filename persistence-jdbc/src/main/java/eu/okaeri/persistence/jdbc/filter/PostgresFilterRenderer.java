package eu.okaeri.persistence.jdbc.filter;

import eu.okaeri.persistence.PersistencePath;
import eu.okaeri.persistence.filter.OrderBy;
import eu.okaeri.persistence.filter.predicate.Predicate;
import eu.okaeri.persistence.filter.predicate.SimplePredicate;
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

        if ((predicate instanceof SimplePredicate) && (((SimplePredicate) predicate).getRightOperand() instanceof Number)) {
            return "((" + path.toPostgresJsonPath() + ")::numeric " + this.renderOperator(predicate) + " " + this.renderOperand(predicate) + ")";
        }

        return "(" + path.toPostgresJsonPath(true) + this.renderOperator(predicate) + " " + this.renderOperand(predicate) + ")";
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
