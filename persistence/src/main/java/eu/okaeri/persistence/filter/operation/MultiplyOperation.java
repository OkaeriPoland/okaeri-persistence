package eu.okaeri.persistence.filter.operation;

import lombok.Data;
import lombok.NonNull;

@Data
public class MultiplyOperation implements UpdateOperation {

    private final String field;
    private final double factor;

    public MultiplyOperation(@NonNull String field, double factor) {
        this.field = field;
        this.factor = factor;
    }

    @Override
    public UpdateOperationType getType() {
        return UpdateOperationType.MULTIPLY;
    }
}
