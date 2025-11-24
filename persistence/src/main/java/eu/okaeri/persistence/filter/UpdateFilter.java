package eu.okaeri.persistence.filter;

import eu.okaeri.persistence.filter.condition.Condition;
import eu.okaeri.persistence.filter.operation.UpdateOperation;
import lombok.Data;

import java.util.List;

@Data
public class UpdateFilter {

    private final List<UpdateOperation> operations;
    private final Condition where;

    public static UpdateFilterBuilder builder() {
        return new UpdateFilterBuilder();
    }

    public boolean hasWhere() {
        return this.where != null;
    }
}
