package eu.okaeri.persistence.filter;

import eu.okaeri.persistence.filter.condition.Condition;
import lombok.Data;

import java.util.List;

@Data
public class FindFilter {

    private final Condition where;
    private final int limit;
    private final int skip;
    private final List<OrderBy> orderBy;

    public static FindFilterBuilder builder() {
        return new FindFilterBuilder();
    }

    public boolean hasSkip() {
        return this.skip > 0;
    }

    public boolean hasLimit() {
        return this.limit > 0;
    }

    public boolean hasOrderBy() {
        return (this.orderBy != null) && !this.orderBy.isEmpty();
    }
}
