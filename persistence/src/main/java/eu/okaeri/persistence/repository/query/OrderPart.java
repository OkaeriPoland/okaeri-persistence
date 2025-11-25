package eu.okaeri.persistence.repository.query;

import lombok.Data;
import lombok.NonNull;

/**
 * Represents an ordering directive in a query.
 * Example: "level" field, descending direction.
 */
@Data
public class OrderPart {
    /**
     * Field path to order by (e.g., "level", "meta.score").
     * Nested fields use dot notation.
     */
    @NonNull
    private final String field;

    /**
     * Ordering direction: true for ascending, false for descending.
     */
    private final boolean ascending;
}
