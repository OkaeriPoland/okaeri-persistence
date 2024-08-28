package eu.okaeri.persistence.filter;

import eu.okaeri.persistence.filter.condition.Condition;
import lombok.NoArgsConstructor;
import lombok.NonNull;

@NoArgsConstructor
public class FindFilterBuilder {

    private Condition where;
    private int limit;
    private int skip;

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

    public FindFilter build() {
        return new FindFilter(this.where, this.limit, this.skip);
    }
}
