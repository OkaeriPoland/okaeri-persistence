package eu.okaeri.persistence.jdbc.filter;

import eu.okaeri.persistence.PersistencePath;
import eu.okaeri.persistence.filter.predicate.Predicate;
import eu.okaeri.persistence.filter.predicate.SimplePredicate;
import eu.okaeri.persistence.filter.renderer.StringRenderer;
import lombok.NonNull;

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
}
