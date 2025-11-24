package eu.okaeri.persistence.filter.operation;

import lombok.Data;
import lombok.NonNull;

@Data
public class IncrementOperation implements UpdateOperation {

    private final String field;
    private final Number delta;

    public IncrementOperation(@NonNull String field, @NonNull Number delta) {
        this.field = field;
        this.delta = delta;
    }

    @Override
    public UpdateOperationType getType() {
        return UpdateOperationType.INCREMENT;
    }
}
