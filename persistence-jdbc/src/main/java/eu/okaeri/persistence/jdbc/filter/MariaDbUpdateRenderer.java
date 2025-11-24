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

/**
 * Renders update operations to MariaDB/MySQL JSON update expressions.
 * Uses MariaDB/MySQL JSON functions to perform atomic updates.
 */
public class MariaDbUpdateRenderer {

    private final StringRenderer sqlStringRenderer;
    private final JsonStringRenderer jsonStringRenderer;

    public MariaDbUpdateRenderer(@NonNull StringRenderer sqlStringRenderer) {
        this.sqlStringRenderer = sqlStringRenderer;
        this.jsonStringRenderer = new JsonStringRenderer();
    }

    /**
     * Renders a list of update operations to a MariaDB/MySQL UPDATE expression.
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
            String path = PersistencePath.parse(setOp.getField(), ".").toMariaDbJsonPath();
            String value = this.toJsonValue(setOp.getValue());
            expr = String.format("json_set(%s, '%s', %s)", expr, path, value);
        }

        return expr;
    }

    private String applyUnset(String expr, List<UpdateOperation> operations) {
        if ((operations == null) || operations.isEmpty()) {
            return expr;
        }

        for (UpdateOperation op : operations) {
            UnsetOperation unsetOp = (UnsetOperation) op;
            String path = PersistencePath.parse(unsetOp.getField(), ".").toMariaDbJsonPath();
            expr = String.format("json_remove(%s, '%s')", expr, path);
        }

        return expr;
    }

    private String applyIncrement(String expr, List<UpdateOperation> operations) {
        if ((operations == null) || operations.isEmpty()) {
            return expr;
        }

        for (UpdateOperation op : operations) {
            IncrementOperation incOp = (IncrementOperation) op;
            String path = PersistencePath.parse(incOp.getField(), ".").toMariaDbJsonPath();

            expr = String.format(
                "json_set(%s, '%s', coalesce(json_extract(%s, '%s'), 0) + %s)",
                expr, path, expr, path, incOp.getDelta()
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
            String path = PersistencePath.parse(mulOp.getField(), ".").toMariaDbJsonPath();

            expr = String.format(
                "json_set(%s, '%s', coalesce(json_extract(%s, '%s'), 1) * %s)",
                expr, path, expr, path, mulOp.getFactor()
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
            String path = PersistencePath.parse(minOp.getField(), ".").toMariaDbJsonPath();
            String value = this.toComparableValue(minOp.getValue());

            expr = String.format(
                "json_set(%s, '%s', least(cast(json_extract(%s, '%s') as decimal), %s))",
                expr, path, expr, path, value
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
            String path = PersistencePath.parse(maxOp.getField(), ".").toMariaDbJsonPath();
            String value = this.toComparableValue(maxOp.getValue());

            expr = String.format(
                "json_set(%s, '%s', greatest(cast(json_extract(%s, '%s') as decimal), %s))",
                expr, path, expr, path, value
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
            String path = PersistencePath.parse(dateOp.getField(), ".").toMariaDbJsonPath();
            // Use Instant.now() for ISO 8601 format timestamp
            String timestamp = this.toJsonValue(Instant.now().toString());

            expr = String.format("json_set(%s, '%s', %s)", expr, path, timestamp);
        }

        return expr;
    }

    private String applyPush(String expr, List<UpdateOperation> operations) {
        if ((operations == null) || operations.isEmpty()) {
            return expr;
        }

        for (UpdateOperation op : operations) {
            PushOperation pushOp = (PushOperation) op;
            String path = PersistencePath.parse(pushOp.getField(), ".").toMariaDbJsonPath();

            // First ensure the field exists as an array
            expr = String.format("json_set(%s, '%s', coalesce(json_extract(%s, '%s'), json_array()))", expr, path, expr, path);

            if (pushOp.isSingleValue()) {
                String value = this.toJsonValue(pushOp.getSingleValue());
                expr = String.format("json_array_append(%s, '%s', %s)", expr, path, value);
            } else {
                // Multiple values: append each one sequentially
                for (Object value : pushOp.getValues()) {
                    String jsonValue = this.toJsonValue(value);
                    expr = String.format("json_array_append(%s, '%s', %s)", expr, path, jsonValue);
                }
            }
        }

        return expr;
    }

    private String applyPopFirst(String expr, List<UpdateOperation> operations) {
        if ((operations == null) || operations.isEmpty()) {
            return expr;
        }

        for (UpdateOperation op : operations) {
            PopFirstOperation popOp = (PopFirstOperation) op;
            String path = PersistencePath.parse(popOp.getField(), ".").toMariaDbJsonPath() + "[0]";
            expr = String.format("json_remove(%s, '%s')", expr, path);
        }

        return expr;
    }

    private String applyPopLast(String expr, List<UpdateOperation> operations) {
        if ((operations == null) || operations.isEmpty()) {
            return expr;
        }

        for (UpdateOperation op : operations) {
            PopLastOperation popOp = (PopLastOperation) op;
            String path = PersistencePath.parse(popOp.getField(), ".").toMariaDbJsonPath() + "[last]";
            expr = String.format("json_remove(%s, '%s')", expr, path);
        }

        return expr;
    }

    private String applyPull(String expr, List<UpdateOperation> operations) {
        if ((operations == null) || operations.isEmpty()) {
            return expr;
        }

        // PULL operation not supported in MariaDB - triggers in-memory fallback
        throw new UnsupportedOperationException("PULL operation not supported natively in MariaDB");
    }

    private String applyPullAll(String expr, List<UpdateOperation> operations) {
        if ((operations == null) || operations.isEmpty()) {
            return expr;
        }

        // PULL_ALL operation not supported in MariaDB - triggers in-memory fallback
        throw new UnsupportedOperationException("PULL_ALL operation not supported natively in MariaDB");
    }

    private String applyAddToSet(String expr, List<UpdateOperation> operations) {
        if ((operations == null) || operations.isEmpty()) {
            return expr;
        }

        // ADD_TO_SET operation not supported in MariaDB - triggers in-memory fallback
        throw new UnsupportedOperationException("ADD_TO_SET operation not supported natively in MariaDB");
    }

    /**
     * Converts a value to SQL format for use in MySQL/MariaDB JSON functions.
     * MariaDB's JSON functions automatically handle JSON encoding of values.
     */
    private String toJsonValue(Object value) {
        if (value == null) {
            return "null";
        }
        if (value instanceof String) {
            return this.sqlStringRenderer.render((String) value);
        }
        if ((value instanceof Number) || (value instanceof Boolean)) {
            return value.toString();
        }
        // For complex objects, treat as string
        return this.sqlStringRenderer.render(value.toString());
    }

    /**
     * Converts a value to a JSON-encoded SQL string for functions that expect JSON documents (like json_contains).
     * Returns a SQL string literal containing a JSON value.
     */
    private String toJsonDocument(Object value) {
        if (value == null) {
            return "'null'";
        }
        if (value instanceof String) {
            // Use JsonStringRenderer to get proper JSON escaping: "value"
            String jsonString = this.jsonStringRenderer.render((String) value);
            // Escape single quotes for SQL and wrap in SQL string literal
            return "'" + jsonString.replace("'", "''") + "'";
        }
        if ((value instanceof Number) || (value instanceof Boolean)) {
            return "'" + value + "'";
        }
        // For complex objects, treat as string
        String jsonString = this.jsonStringRenderer.render(value.toString());
        return "'" + jsonString.replace("'", "''") + "'";
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
