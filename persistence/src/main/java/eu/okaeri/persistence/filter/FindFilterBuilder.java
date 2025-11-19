package eu.okaeri.persistence.filter;

import eu.okaeri.persistence.filter.condition.Condition;
import lombok.NoArgsConstructor;
import lombok.NonNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@NoArgsConstructor
public class FindFilterBuilder {

    private Condition where;
    private int limit;
    private int skip;
    private List<OrderBy> orderBy;

    public FindFilterBuilder where(@NonNull Condition where) {
        this.where = where;
        return this;
    }

    public FindFilterBuilder limit(int limit) {
        this.limit = limit;
        return this;
    }

    public FindFilterBuilder skip(int skip) {
        this.skip = skip;
        return this;
    }

    public FindFilterBuilder orderBy(@NonNull OrderBy... order) {
        if (this.orderBy == null) {
            this.orderBy = new ArrayList<>();
        }
        this.orderBy.addAll(Arrays.asList(order));
        return this;
    }

    public FindFilter build() {
        return new FindFilter(this.where, this.limit, this.skip, this.orderBy);
    }
}
