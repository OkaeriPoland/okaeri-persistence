package eu.okaeri.persistence.filter.operation;

import lombok.Data;
import lombok.NonNull;

@Data
public class SetOperation implements UpdateOperation {

    private final String field;
    private final Object value;

    public SetOperation(@NonNull String field, Object value) {
        this.field = field;
        this.value = value;
    }

    @Override
    public UpdateOperationType getType() {
        return UpdateOperationType.SET;
    }
}
