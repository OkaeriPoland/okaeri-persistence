package eu.okaeri.persistence.filter.operation;

import lombok.Data;
import lombok.NonNull;

@Data
public class CurrentDateOperation implements UpdateOperation {

    private final String field;

    public CurrentDateOperation(@NonNull String field) {
        this.field = field;
    }

    @Override
    public UpdateOperationType getType() {
        return UpdateOperationType.CURRENT_DATE;
    }
}
