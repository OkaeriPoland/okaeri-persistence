package eu.okaeri.persistence.filter;

import eu.okaeri.persistence.filter.condition.Condition;
import lombok.NoArgsConstructor;
import lombok.NonNull;

/**
 * Builder for multi-document updates with WHERE clause.
 * Extends UpdateBuilder to inherit all update operations.
 */
@NoArgsConstructor
public class UpdateFilterBuilder extends UpdateBuilder {

    private Condition where;

    public UpdateFilterBuilder where(@NonNull Condition where) {
        this.where = where;
        return this;
    }

    public UpdateFilter build() {
        if (this.operations.isEmpty()) {
            throw new IllegalStateException("No update operations specified");
        }
        if (this.where == null) {
            throw new IllegalStateException("WHERE clause is required for multi-document updates - use updateOne() for single document updates");
        }
        return new UpdateFilter(this.getOperations(), this.where);
    }

    // Override all methods to return UpdateFilterBuilder instead of UpdateBuilder
    // This maintains fluent API type safety

    @Override
    public UpdateFilterBuilder set(@NonNull String field, Object value) {
        super.set(field, value);
        return this;
    }

    @Override
    public UpdateFilterBuilder unset(@NonNull String field) {
        super.unset(field);
        return this;
    }

    @Override
    public UpdateFilterBuilder increment(@NonNull String field, long delta) {
        super.increment(field, delta);
        return this;
    }

    @Override
    public UpdateFilterBuilder increment(@NonNull String field, int delta) {
        super.increment(field, delta);
        return this;
    }

    @Override
    public UpdateFilterBuilder increment(@NonNull String field, double delta) {
        super.increment(field, delta);
        return this;
    }

    @Override
    public UpdateFilterBuilder multiply(@NonNull String field, double factor) {
        super.multiply(field, factor);
        return this;
    }

    @Override
    public UpdateFilterBuilder min(@NonNull String field, @NonNull Comparable<?> value) {
        super.min(field, value);
        return this;
    }

    @Override
    public UpdateFilterBuilder max(@NonNull String field, @NonNull Comparable<?> value) {
        super.max(field, value);
        return this;
    }

    @Override
    public UpdateFilterBuilder currentDate(@NonNull String field) {
        super.currentDate(field);
        return this;
    }

    @Override
    public UpdateFilterBuilder push(@NonNull String field, Object value) {
        super.push(field, value);
        return this;
    }

    @Override
    public UpdateFilterBuilder push(@NonNull String field, @NonNull Object... values) {
        super.push(field, values);
        return this;
    }

    @Override
    public UpdateFilterBuilder push(@NonNull String field, @NonNull java.util.Collection<?> values) {
        super.push(field, values);
        return this;
    }

    @Override
    public UpdateFilterBuilder popFirst(@NonNull String field) {
        super.popFirst(field);
        return this;
    }

    @Override
    public UpdateFilterBuilder popLast(@NonNull String field) {
        super.popLast(field);
        return this;
    }

    @Override
    public UpdateFilterBuilder pull(@NonNull String field, Object value) {
        super.pull(field, value);
        return this;
    }

    @Override
    public UpdateFilterBuilder pullAll(@NonNull String field, @NonNull Object... values) {
        super.pullAll(field, values);
        return this;
    }

    @Override
    public UpdateFilterBuilder addToSet(@NonNull String field, Object value) {
        super.addToSet(field, value);
        return this;
    }

    @Override
    public UpdateFilterBuilder addToSet(@NonNull String field, @NonNull Object... values) {
        super.addToSet(field, values);
        return this;
    }

    @Override
    public UpdateFilterBuilder addToSet(@NonNull String field, @NonNull java.util.Collection<?> values) {
        super.addToSet(field, values);
        return this;
    }
}
