package eu.okaeri.persistence.filter.operation;

import lombok.Data;
import lombok.NonNull;

@Data
public class MinOperation implements UpdateOperation {

    private final String field;
    private final Comparable<?> value;

    public MinOperation(@NonNull String field, @NonNull Comparable<?> value) {
        this.field = field;
        this.value = value;
    }

    @Override
    public UpdateOperationType getType() {
        return UpdateOperationType.MIN;
    }
}
