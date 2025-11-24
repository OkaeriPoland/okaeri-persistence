package eu.okaeri.persistence.filter.operation;

import lombok.Data;
import lombok.NonNull;

@Data
public class MaxOperation implements UpdateOperation {

    private final String field;
    private final Comparable<?> value;

    public MaxOperation(@NonNull String field, @NonNull Comparable<?> value) {
        this.field = field;
        this.value = value;
    }

    @Override
    public UpdateOperationType getType() {
        return UpdateOperationType.MAX;
    }
}
