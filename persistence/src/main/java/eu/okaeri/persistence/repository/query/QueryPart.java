package eu.okaeri.persistence.repository.query;

import eu.okaeri.persistence.filter.condition.LogicalOperator;
import lombok.Data;
import lombok.NonNull;

/**
 * Represents a single field condition in a parsed query.
 * Used during method name parsing to track field names and parameter positions.
 * The parser converts QueryParts into Condition objects for execution.
 * <p>
 * Example: "name" field with equality (implicit), consuming parameter at index 0.
 */
@Data
public class QueryPart {
    /**
     * Field path to query (e.g., "name", "meta.email").
     * Nested fields use dot notation after $ separator is converted.
     */
    @NonNull
    private final String field;

    /**
     * Index of the method parameter that provides the value for this condition.
     * For example, in findByNameAndLevel(String name, int level):
     * - "name" field uses parameterIndex = 0
     * - "level" field uses parameterIndex = 1
     */
    private final int parameterIndex;

    /**
     * Logical operator combining this condition with the previous one.
     * Null for the first condition in a query.
     * Uses existing eu.okaeri.persistence.filter.condition.LogicalOperator.
     */
    private final LogicalOperator logicalOperator;
}
