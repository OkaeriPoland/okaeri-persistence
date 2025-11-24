package eu.okaeri.persistence.filter.operation;

import lombok.Data;
import lombok.NonNull;

@Data
public class PopFirstOperation implements UpdateOperation {

    private final String field;

    public PopFirstOperation(@NonNull String field) {
        this.field = field;
    }

    @Override
    public UpdateOperationType getType() {
        return UpdateOperationType.POP_FIRST;
    }
}
