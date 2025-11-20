package eu.okaeri.persistence.filter;

import eu.okaeri.configs.configurer.Configurer;
import eu.okaeri.persistence.PersistenceEntity;
import eu.okaeri.persistence.document.Document;
import eu.okaeri.persistence.filter.condition.Condition;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static eu.okaeri.persistence.document.DocumentValueUtils.compareForSort;
import static eu.okaeri.persistence.document.DocumentValueUtils.extractValue;

/**
 * Evaluates filters in-memory for backends that don't support native query translation.
 * Used as a fallback when backend throws UnsupportedOperationException.
 */
@RequiredArgsConstructor
public class InMemoryFilterEvaluator {

    private final Configurer configurer;

    /**
     * Apply WHERE, ORDER BY, SKIP, LIMIT to a stream of documents in memory.
     */
    public Stream<PersistenceEntity<Document>> applyFilter(@NonNull Stream<PersistenceEntity<Document>> stream, @NonNull FindFilter filter) {
        // Apply WHERE clause
        if (filter.getWhere() != null) {
            stream = stream.filter(entity -> this.evaluateCondition(filter.getWhere(), entity.getValue()));
        }

        // Apply ORDER BY
        if (filter.hasOrderBy()) {
            Comparator<PersistenceEntity<Document>> comparator = this.buildComparator(filter.getOrderBy());
            if (comparator != null) {
                stream = stream.sorted(comparator);
            }
        }

        // Apply SKIP
        if (filter.hasSkip()) {
            stream = stream.skip(filter.getSkip());
        }

        // Apply LIMIT
        if (filter.hasLimit()) {
            stream = stream.limit(filter.getLimit());
        }

        return stream;
    }

    /**
     * Evaluate a condition against a document.
     */
    public boolean evaluateCondition(@NonNull Condition condition, @NonNull Document document) {
        Map<String, Object> docMap = document.asMap(this.configurer, true);

        if (condition.getPath() != null) {
            Object value = extractValue(docMap, condition.getPath().toParts());
            return condition.check(value);
        }

        // Top-level AND/OR without specific path
        return condition.check(docMap);
    }

    /**
     * Build a comparator for ORDER BY clauses.
     */
    protected Comparator<PersistenceEntity<Document>> buildComparator(@NonNull List<OrderBy> orderBys) {
        if (orderBys.isEmpty()) {
            return null;
        }

        Comparator<PersistenceEntity<Document>> comparator = null;

        for (OrderBy orderBy : orderBys) {
            Comparator<PersistenceEntity<Document>> fieldComparator = (e1, e2) -> {
                Map<String, Object> map1 = e1.getValue().asMap(this.configurer, true);
                Map<String, Object> map2 = e2.getValue().asMap(this.configurer, true);

                Object val1 = extractValue(map1, orderBy.getPath().toParts());
                Object val2 = extractValue(map2, orderBy.getPath().toParts());

                int cmp = compareForSort(val1, val2);
                return (orderBy.getDirection() == OrderDirection.DESC) ? -cmp : cmp;
            };

            if (comparator == null) {
                comparator = fieldComparator;
            } else {
                comparator = comparator.thenComparing(fieldComparator);
            }
        }

        return comparator;
    }
}
