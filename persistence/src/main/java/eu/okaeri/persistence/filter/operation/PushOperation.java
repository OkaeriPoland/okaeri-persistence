package eu.okaeri.persistence.filter.operation;

import lombok.Data;
import lombok.NonNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

@Data
public class PushOperation implements UpdateOperation {

    private final String field;
    private final List<Object> values;

    public PushOperation(@NonNull String field, Object value) {
        this.field = field;
        this.values = new ArrayList<>();
        this.values.add(value);
    }

    public PushOperation(@NonNull String field, @NonNull Object... values) {
        this.field = field;
        this.values = new ArrayList<>(Arrays.asList(values));
    }

    public PushOperation(@NonNull String field, @NonNull Collection<?> values) {
        this.field = field;
        this.values = new ArrayList<>(values);
    }

    @Override
    public UpdateOperationType getType() {
        return UpdateOperationType.PUSH;
    }

    public boolean isSingleValue() {
        return this.values.size() == 1;
    }

    public Object getSingleValue() {
        if (!this.isSingleValue()) {
            throw new IllegalStateException("PushOperation contains multiple values");
        }
        return this.values.get(0);
    }
}
