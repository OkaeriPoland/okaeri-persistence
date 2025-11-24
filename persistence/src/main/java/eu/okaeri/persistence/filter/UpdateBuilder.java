package eu.okaeri.persistence.filter;

import eu.okaeri.persistence.filter.operation.*;
import lombok.NoArgsConstructor;
import lombok.NonNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Builder for atomic update operations on documents.
 * Used for single-document updates without WHERE clause.
 * <p>
 * Example usage:
 * <pre>{@code
 * userRepo.updateOne(userId, u -> u
 *     .increment("exp", 100)
 *     .push("achievements", "SPEEDRUN")
 *     .set("lastActive", Instant.now()));
 * }</pre>
 */
@NoArgsConstructor
public class UpdateBuilder {

    protected List<UpdateOperation> operations = new ArrayList<>();

    // ===== FIELD OPERATIONS =====

    /**
     * Set a field to a specific value.
     *
     * @param field Field path (e.g., "name", "profile.age")
     * @param value Value to set
     * @return This builder for chaining
     */
    public UpdateBuilder set(@NonNull String field, Object value) {
        this.operations.add(new SetOperation(field, value));
        return this;
    }

    /**
     * Remove a field from the document.
     *
     * @param field Field path to remove
     * @return This builder for chaining
     */
    public UpdateBuilder unset(@NonNull String field) {
        this.operations.add(new UnsetOperation(field));
        return this;
    }

    /**
     * Atomically increment a numeric field.
     *
     * @param field Field path
     * @param delta Amount to increment (can be negative to decrement)
     * @return This builder for chaining
     */
    public UpdateBuilder increment(@NonNull String field, long delta) {
        this.operations.add(new IncrementOperation(field, delta));
        return this;
    }

    /**
     * Atomically increment a numeric field.
     *
     * @param field Field path
     * @param delta Amount to increment (can be negative to decrement)
     * @return This builder for chaining
     */
    public UpdateBuilder increment(@NonNull String field, int delta) {
        this.operations.add(new IncrementOperation(field, delta));
        return this;
    }

    /**
     * Atomically increment a numeric field.
     *
     * @param field Field path
     * @param delta Amount to increment (can be negative to decrement)
     * @return This builder for chaining
     */
    public UpdateBuilder increment(@NonNull String field, double delta) {
        this.operations.add(new IncrementOperation(field, delta));
        return this;
    }

    /**
     * Multiply a numeric field by a factor.
     *
     * @param field Field path
     * @param factor Multiplication factor
     * @return This builder for chaining
     */
    public UpdateBuilder multiply(@NonNull String field, double factor) {
        this.operations.add(new MultiplyOperation(field, factor));
        return this;
    }

    /**
     * Update field only if the new value is less than the current value.
     *
     * @param field Field path
     * @param value Value to compare and potentially set
     * @return This builder for chaining
     */
    public UpdateBuilder min(@NonNull String field, @NonNull Comparable<?> value) {
        this.operations.add(new MinOperation(field, value));
        return this;
    }

    /**
     * Update field only if the new value is greater than the current value.
     *
     * @param field Field path
     * @param value Value to compare and potentially set
     * @return This builder for chaining
     */
    public UpdateBuilder max(@NonNull String field, @NonNull Comparable<?> value) {
        this.operations.add(new MaxOperation(field, value));
        return this;
    }

    /**
     * Set a field to the current date/time.
     *
     * @param field Field path
     * @return This builder for chaining
     */
    public UpdateBuilder currentDate(@NonNull String field) {
        this.operations.add(new CurrentDateOperation(field));
        return this;
    }

    // ===== ARRAY OPERATIONS =====

    /**
     * Append a value to an array field.
     *
     * @param field Field path to array
     * @param value Value to append
     * @return This builder for chaining
     */
    public UpdateBuilder push(@NonNull String field, Object value) {
        this.operations.add(new PushOperation(field, value));
        return this;
    }

    /**
     * Append multiple values to an array field.
     *
     * @param field Field path to array
     * @param values Values to append
     * @return This builder for chaining
     */
    public UpdateBuilder push(@NonNull String field, Object... values) {
        this.operations.add(new PushOperation(field, values));
        return this;
    }

    /**
     * Append multiple values to an array field.
     *
     * @param field Field path to array
     * @param values Collection of values to append
     * @return This builder for chaining
     */
    public UpdateBuilder push(@NonNull String field, @NonNull Collection<?> values) {
        this.operations.add(new PushOperation(field, values));
        return this;
    }

    /**
     * Remove the first element from an array field.
     *
     * @param field Field path to array
     * @return This builder for chaining
     */
    public UpdateBuilder popFirst(@NonNull String field) {
        this.operations.add(new PopFirstOperation(field));
        return this;
    }

    /**
     * Remove the last element from an array field.
     *
     * @param field Field path to array
     * @return This builder for chaining
     */
    public UpdateBuilder popLast(@NonNull String field) {
        this.operations.add(new PopLastOperation(field));
        return this;
    }

    /**
     * Remove all occurrences of a value from an array field.
     *
     * @param field Field path to array
     * @param value Value to remove
     * @return This builder for chaining
     */
    public UpdateBuilder pull(@NonNull String field, Object value) {
        this.operations.add(new PullOperation(field, value));
        return this;
    }

    /**
     * Remove all occurrences of multiple values from an array field.
     *
     * @param field Field path to array
     * @param values Values to remove
     * @return This builder for chaining
     */
    public UpdateBuilder pullAll(@NonNull String field, Object... values) {
        this.operations.add(new PullAllOperation(field, values));
        return this;
    }

    /**
     * Add a value to an array field only if it doesn't already exist.
     *
     * @param field Field path to array
     * @param value Value to add (if not present)
     * @return This builder for chaining
     */
    public UpdateBuilder addToSet(@NonNull String field, Object value) {
        this.operations.add(new AddToSetOperation(field, value));
        return this;
    }

    /**
     * Add multiple values to an array field, only adding values that don't already exist.
     *
     * @param field Field path to array
     * @param values Values to add (if not present)
     * @return This builder for chaining
     */
    public UpdateBuilder addToSet(@NonNull String field, @NonNull Object... values) {
        this.operations.add(new AddToSetOperation(field, values));
        return this;
    }

    /**
     * Add multiple values to an array field, only adding values that don't already exist.
     *
     * @param field Field path to array
     * @param values Collection of values to add (if not present)
     * @return This builder for chaining
     */
    public UpdateBuilder addToSet(@NonNull String field, @NonNull Collection<?> values) {
        this.operations.add(new AddToSetOperation(field, values));
        return this;
    }

    // ===== INTERNAL =====

    /**
     * Get the list of operations. Used internally by repository implementations.
     * @return List of update operations
     */
    public List<UpdateOperation> getOperations() {
        if (this.operations.isEmpty()) {
            throw new IllegalStateException("No update operations specified");
        }
        return new ArrayList<>(this.operations);
    }
}
