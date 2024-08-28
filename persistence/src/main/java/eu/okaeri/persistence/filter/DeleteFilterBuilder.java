package eu.okaeri.persistence.filter;

import eu.okaeri.persistence.filter.condition.Condition;
import lombok.NoArgsConstructor;
import lombok.NonNull;

@NoArgsConstructor
public class DeleteFilterBuilder {

    private Condition where;

    public DeleteFilterBuilder where(@NonNull Condition where) {
        this.where = where;
        return this;
    }

    public DeleteFilter build() {
        return new DeleteFilter(this.where);
    }
}
