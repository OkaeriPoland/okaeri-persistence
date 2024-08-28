package eu.okaeri.persistence.filter;

import eu.okaeri.persistence.filter.condition.Condition;
import lombok.Data;

@Data
public class DeleteFilter {

    private final Condition where;

    public static DeleteFilterBuilder builder() {
        return new DeleteFilterBuilder();
    }
}
