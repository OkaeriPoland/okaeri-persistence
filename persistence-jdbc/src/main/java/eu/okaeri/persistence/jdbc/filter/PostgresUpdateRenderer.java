package eu.okaeri.persistence.jdbc.filter;

import eu.okaeri.persistence.PersistencePath;
import eu.okaeri.persistence.filter.operation.*;
import eu.okaeri.persistence.filter.renderer.JsonStringRenderer;
import eu.okaeri.persistence.filter.renderer.StringRenderer;
import lombok.NonNull;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Renders update operations to PostgreSQL JSONB update expressions.
 * Uses PostgreSQL's JSONB functions and operators to perform atomic updates.
 */
public class PostgresUpdateRenderer {

    private final StringRenderer sqlStringRenderer;
    private final JsonStringRenderer jsonStringRenderer;

    public PostgresUpdateRenderer(@NonNull StringRenderer sqlStringRenderer) {
        this.sqlStringRenderer = sqlStringRenderer;
        this.jsonStringRenderer = new JsonStringRenderer();
    }

    /**
     * Renders a list of update operations to a PostgreSQL UPDATE expression.
     * Returns the SQL fragment for setting the value column.
     *
     * @param operations List of update operations
     * @return SQL expression for UPDATE value = ...
     */
    public String render(@NonNull List<UpdateOperation> operations) {
        if (operations.isEmpty()) {
            return "value"; // No changes
        }

        String expression = "value";

        // Group operations by type for efficient rendering
        Map<UpdateOperationType, List<UpdateOperation>> grouped = new HashMap<>();
        for (UpdateOperation op : operations) {
            grouped.computeIfAbsent(op.getType(), k -> new ArrayList<>()).add(op);
        }

        // Apply operations in a specific order to ensure correctness
        expression = this.applySet(expression, grouped.get(UpdateOperationType.SET));
        expression = this.applyUnset(expression, grouped.get(UpdateOperationType.UNSET));
        expression = this.applyIncrement(expression, grouped.get(UpdateOperationType.INCREMENT));
        expression = this.applyMultiply(expression, grouped.get(UpdateOperationType.MULTIPLY));
        expression = this.applyMin(expression, grouped.get(UpdateOperationType.MIN));
        expression = this.applyMax(expression, grouped.get(UpdateOperationType.MAX));
        expression = this.applyCurrentDate(expression, grouped.get(UpdateOperationType.CURRENT_DATE));
        expression = this.applyPush(expression, grouped.get(UpdateOperationType.PUSH));
        expression = this.applyPopFirst(expression, grouped.get(UpdateOperationType.POP_FIRST));
        expression = this.applyPopLast(expression, grouped.get(UpdateOperationType.POP_LAST));
        expression = this.applyPull(expression, grouped.get(UpdateOperationType.PULL));
        expression = this.applyPullAll(expression, grouped.get(UpdateOperationType.PULL_ALL));
        expression = this.applyAddToSet(expression, grouped.get(UpdateOperationType.ADD_TO_SET));

        return expression;
    }

    private String applySet(String expr, List<UpdateOperation> operations) {
        if ((operations == null) || operations.isEmpty()) {
            return expr;
        }

        for (UpdateOperation op : operations) {
            SetOperation setOp = (SetOperation) op;
            String path = this.toPostgresArrayPath(setOp.getField());
            String value = this.toJsonbValue(setOp.getValue());
            expr = String.format("jsonb_set(%s, '%s', %s)", expr, path, value);
        }

        return expr;
    }

    private String applyUnset(String expr, List<UpdateOperation> operations) {
        if ((operations == null) || operations.isEmpty()) {
            return expr;
        }

        for (UpdateOperation op : operations) {
            UnsetOperation unsetOp = (UnsetOperation) op;
            String path = this.toPostgresArrayPath(unsetOp.getField());
            expr = String.format("%s #- '%s'", expr, path);
        }

        return expr;
    }

    private String applyIncrement(String expr, List<UpdateOperation> operations) {
        if ((operations == null) || operations.isEmpty()) {
            return expr;
        }

        for (UpdateOperation op : operations) {
            IncrementOperation incOp = (IncrementOperation) op;
            String path = this.toPostgresArrayPath(incOp.getField());
            String accessor = this.toPostgresAccessor(incOp.getField());

            expr = String.format(
                "jsonb_set(%s, '%s', to_jsonb(coalesce((%s)::numeric, 0) + %s))",
                expr, path, accessor, incOp.getDelta()
            );
        }

        return expr;
    }

    private String applyMultiply(String expr, List<UpdateOperation> operations) {
        if ((operations == null) || operations.isEmpty()) {
            return expr;
        }

        for (UpdateOperation op : operations) {
            MultiplyOperation mulOp = (MultiplyOperation) op;
            String path = this.toPostgresArrayPath(mulOp.getField());
            String accessor = this.toPostgresAccessor(mulOp.getField());

            expr = String.format(
                "jsonb_set(%s, '%s', to_jsonb(coalesce((%s)::numeric, 1) * %s))",
                expr, path, accessor, mulOp.getFactor()
            );
        }

        return expr;
    }

    private String applyMin(String expr, List<UpdateOperation> operations) {
        if ((operations == null) || operations.isEmpty()) {
            return expr;
        }

        for (UpdateOperation op : operations) {
            MinOperation minOp = (MinOperation) op;
            String path = this.toPostgresArrayPath(minOp.getField());
            String accessor = this.toPostgresAccessor(minOp.getField());
            String value = this.toComparableValue(minOp.getValue());

            expr = String.format(
                "jsonb_set(%s, '%s', to_jsonb(least((%s)::numeric, %s)))",
                expr, path, accessor, value
            );
        }

        return expr;
    }

    private String applyMax(String expr, List<UpdateOperation> operations) {
        if ((operations == null) || operations.isEmpty()) {
            return expr;
        }

        for (UpdateOperation op : operations) {
            MaxOperation maxOp = (MaxOperation) op;
            String path = this.toPostgresArrayPath(maxOp.getField());
            String accessor = this.toPostgresAccessor(maxOp.getField());
            String value = this.toComparableValue(maxOp.getValue());

            expr = String.format(
                "jsonb_set(%s, '%s', to_jsonb(greatest((%s)::numeric, %s)))",
                expr, path, accessor, value
            );
        }

        return expr;
    }

    private String applyCurrentDate(String expr, List<UpdateOperation> operations) {
        if ((operations == null) || operations.isEmpty()) {
            return expr;
        }

        for (UpdateOperation op : operations) {
            CurrentDateOperation dateOp = (CurrentDateOperation) op;
            String path = this.toPostgresArrayPath(dateOp.getField());
            // Use toJsonbValue to get proper JSON string: '"2025-11-24T..."'::jsonb
            String timestamp = this.toJsonbValue(Instant.now().toString());

            expr = String.format("jsonb_set(%s, '%s', %s)", expr, path, timestamp);
        }

        return expr;
    }

    private String applyPush(String expr, List<UpdateOperation> operations) {
        if ((operations == null) || operations.isEmpty()) {
            return expr;
        }

        for (UpdateOperation op : operations) {
            PushOperation pushOp = (PushOperation) op;
            String path = this.toPostgresArrayPath(pushOp.getField());
            String accessor = this.toPostgresAccessor(pushOp.getField());

            String arrayValues;
            if (pushOp.isSingleValue()) {
                arrayValues = String.format("'[%s]'::jsonb", this.toJsonValue(pushOp.getSingleValue()));
            } else {
                List<String> elements = pushOp.getValues().stream()
                    .map(this::toJsonValue)
                    .collect(Collectors.toList());
                arrayValues = String.format("'[%s]'::jsonb", String.join(", ", elements));
            }

            expr = String.format(
                "jsonb_set(%s, '%s', coalesce(%s, '[]'::jsonb) || %s)",
                expr, path, accessor, arrayValues
            );
        }

        return expr;
    }

    private String applyPopFirst(String expr, List<UpdateOperation> operations) {
        if ((operations == null) || operations.isEmpty()) {
            return expr;
        }

        for (UpdateOperation op : operations) {
            PopFirstOperation popOp = (PopFirstOperation) op;
            String path = this.toPostgresArrayPath(popOp.getField() + ".0");
            expr = String.format("%s #- '%s'", expr, path);
        }

        return expr;
    }

    private String applyPopLast(String expr, List<UpdateOperation> operations) {
        if ((operations == null) || operations.isEmpty()) {
            return expr;
        }

        for (UpdateOperation op : operations) {
            PopLastOperation popOp = (PopLastOperation) op;
            String path = this.toPostgresArrayPath(popOp.getField());
            String accessor = this.toPostgresAccessor(popOp.getField());

            // Use jsonb_set with array slicing to remove last element
            // This is more reliable than dynamic path construction with #-
            expr = String.format(
                "jsonb_set(%s, '%s', case when jsonb_array_length(%s) > 0 then (select jsonb_agg(elem) from (select elem from jsonb_array_elements(%s) with ordinality as t(elem, idx) where idx < jsonb_array_length(%s)) as sub) else '[]'::jsonb end)",
                expr, path, accessor, accessor, accessor
            );
        }

        return expr;
    }

    private String applyPull(String expr, List<UpdateOperation> operations) {
        if ((operations == null) || operations.isEmpty()) {
            return expr;
        }

        for (UpdateOperation op : operations) {
            PullOperation pullOp = (PullOperation) op;
            String path = this.toPostgresArrayPath(pullOp.getField());
            String accessor = this.toPostgresAccessor(pullOp.getField());
            String valueToRemove = this.toJsonbValue(pullOp.getValue());

            // Use IS DISTINCT FROM to handle NULL correctly (treats NULL as a value, not unknown)
            expr = String.format(
                "jsonb_set(%s, '%s', coalesce((select jsonb_agg(elem) from jsonb_array_elements(%s) as elem where elem is distinct from %s), '[]'::jsonb))",
                expr, path, accessor, valueToRemove
            );
        }

        return expr;
    }

    private String applyPullAll(String expr, List<UpdateOperation> operations) {
        if ((operations == null) || operations.isEmpty()) {
            return expr;
        }

        for (UpdateOperation op : operations) {
            PullAllOperation pullAllOp = (PullAllOperation) op;
            String path = this.toPostgresArrayPath(pullAllOp.getField());
            String accessor = this.toPostgresAccessor(pullAllOp.getField());

            List<String> valuesToRemoveList = pullAllOp.getValues().stream()
                .map(this::toJsonValue)
                .collect(Collectors.toList());
            String valuesToRemoveArray = String.format("'[%s]'::jsonb", String.join(", ", valuesToRemoveList));

            // Use NOT EXISTS with IS NOT DISTINCT FROM to handle NULL correctly
            // IS NOT DISTINCT FROM treats NULL = NULL as TRUE (unlike regular =)
            expr = String.format(
                "jsonb_set(%s, '%s', coalesce((select jsonb_agg(elem) from jsonb_array_elements(%s) as elem where not exists (select 1 from jsonb_array_elements(%s) as v where elem is not distinct from v)), '[]'::jsonb))",
                expr, path, accessor, valuesToRemoveArray
            );
        }

        return expr;
    }

    private String applyAddToSet(String expr, List<UpdateOperation> operations) {
        if ((operations == null) || operations.isEmpty()) {
            return expr;
        }

        for (UpdateOperation op : operations) {
            AddToSetOperation addOp = (AddToSetOperation) op;
            String path = this.toPostgresArrayPath(addOp.getField());
            String accessor = this.toPostgresAccessor(addOp.getField());

            String newValues;
            if (addOp.isSingleValue()) {
                newValues = String.format("'[%s]'::jsonb", this.toJsonValue(addOp.getSingleValue()));
            } else {
                List<String> elements = addOp.getValues().stream()
                    .map(this::toJsonValue)
                    .collect(Collectors.toList());
                newValues = String.format("'[%s]'::jsonb", String.join(", ", elements));
            }

            // Concatenate then deduplicate (ORDER BY not supported with DISTINCT in PostgreSQL)
            expr = String.format(
                "jsonb_set(%s, '%s', (select jsonb_agg(distinct elem) from jsonb_array_elements(coalesce(%s, '[]'::jsonb) || %s) as elem))",
                expr, path, accessor, newValues
            );
        }

        return expr;
    }

    /**
     * Converts a field path like "user.name" to PostgreSQL path format: {user,name}
     * Validates field components to prevent SQL injection.
     */
    private String toPostgresArrayPath(String field) {
        // Validate each path component for dangerous characters
        String[] parts = field.split("\\.");
        for (String part : parts) {
            if (part.contains("'") || part.contains("}") || part.contains("{") ||
                part.contains("--") || part.contains(";")) {
                throw new IllegalArgumentException("Invalid field name: " + field);
            }
        }
        return "{" + field.replace(".", ",") + "}";
    }

    /**
     * Converts a field path to PostgreSQL JSON accessor path like: value->'user'->'name'
     * Uses PersistencePath.parse() to properly handle dot notation.
     */
    private String toPostgresAccessor(String field) {
        PersistencePath parsed = PersistencePath.parse(field, ".");
        return PersistencePath.of("value").sub(parsed).toPostgresJsonPath();
    }

    /**
     * Converts a value to JSONB format for use in PostgreSQL.
     * Returns a properly escaped JSONB literal.
     */
    private String toJsonbValue(Object value) {
        if (value == null) {
            return "'null'::jsonb";
        }
        if (value instanceof String) {
            return "'" + this.escapeJsonString((String) value) + "'::jsonb";
        }
        if ((value instanceof Number) || (value instanceof Boolean)) {
            return "'" + value + "'::jsonb";
        }
        // For complex objects, treat as string
        return "'" + this.escapeJsonString(value.toString()) + "'::jsonb";
    }

    /**
     * Converts a value to JSON string (without ::jsonb cast) for use in JSON arrays.
     */
    private String toJsonValue(Object value) {
        if (value == null) {
            return "null";
        }
        if (value instanceof String) {
            return this.escapeJsonString((String) value);
        }
        return value.toString();
    }

    /**
     * Escapes a string value as a JSON string literal embedded in SQL.
     * Uses JsonStringRenderer for JSON escaping, then adds SQL single-quote escaping.
     *
     * @param value The string to escape
     * @return A properly escaped JSON string literal (e.g., "\"hello\"")
     */
    private String escapeJsonString(String value) {
        // Use JsonStringRenderer to get proper JSON escaping: "value"
        String jsonString = this.jsonStringRenderer.render(value);

        // Now escape single quotes for SQL (since we're embedding in SQL string literal)
        return jsonString.replace("'", "''");
    }

    /**
     * Converts a comparable value to numeric for LEAST/GREATEST
     */
    private String toComparableValue(Comparable<?> value) {
        if (value instanceof Number) {
            return value.toString();
        }
        // For other types, wrap in SQL quotes
        return this.sqlStringRenderer.render(value.toString());
    }
}
