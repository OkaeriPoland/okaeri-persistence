package eu.okaeri.persistence.filter.operation;

import lombok.Data;
import lombok.NonNull;

import java.util.Arrays;
import java.util.List;

@Data
public class PullAllOperation implements UpdateOperation {

    private final String field;
    private final List<Object> values;

    public PullAllOperation(@NonNull String field, @NonNull Object... values) {
        this.field = field;
        this.values = Arrays.asList(values);
    }

    @Override
    public UpdateOperationType getType() {
        return UpdateOperationType.PULL_ALL;
    }
}
