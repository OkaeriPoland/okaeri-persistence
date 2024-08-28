package eu.okaeri.persistence.filter;

import eu.okaeri.persistence.filter.condition.Condition;
import lombok.Data;

@Data
public class FindFilter {

    private final Condition where;
    private final int limit;
    private final int skip;

    public static FindFilterBuilder builder() {
        return new FindFilterBuilder();
    }

    public boolean hasSkip() {
        return this.skip > 0;
    }

    public boolean hasLimit() {
        return this.limit > 0;
    }
}
