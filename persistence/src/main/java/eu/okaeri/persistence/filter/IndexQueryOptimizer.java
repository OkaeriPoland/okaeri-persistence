package eu.okaeri.persistence.filter;

import eu.okaeri.persistence.document.index.PropertyIndex;
import eu.okaeri.persistence.filter.condition.Condition;
import eu.okaeri.persistence.filter.condition.LogicalOperator;
import eu.okaeri.persistence.filter.predicate.Predicate;
import lombok.Data;
import lombok.NonNull;

import java.util.*;

/**
 * Optimizes queries by analyzing conditions and determining which indexes can be used.
 * Supports complex AND/OR conditions across multiple indexed fields.
 * <p>
 * Usage:
 * <pre>
 * IndexQueryOptimizer optimizer = new IndexQueryOptimizer();
 * IndexResult result = optimizer.optimize(condition, indexes);
 * if (!result.requiresFullScan()) {
 *     // Load documents by docIds from result.getDocIds()
 *     // If result.getRemainingCondition() != null, filter loaded docs with it
 * } else {
 *     // Fall back to full scan
 * }
 * </pre>
 */
public class IndexQueryOptimizer {

    /**
     * Result of query optimization.
     */
    @Data
    public static class IndexResult {
        private final Set<String> docIds;
        private final Condition remainingCondition;

        /**
         * Indicates a full table scan is required (no indexes can help).
         */
        public static IndexResult fullScan() {
            return new IndexResult(null, null);
        }

        /**
         * Indicates indexes were used successfully.
         *
         * @param docIds    document IDs matching the indexed conditions
         * @param remaining condition that still needs in-memory evaluation (null if fully indexed)
         */
        public static IndexResult indexed(Set<String> docIds, Condition remaining) {
            return new IndexResult(docIds, remaining);
        }

        /**
         * @return true if indexes couldn't help and full scan is needed
         */
        public boolean requiresFullScan() {
            return this.docIds == null;
        }

        /**
         * @return true if there are remaining conditions that need in-memory filtering
         */
        public boolean hasRemainingCondition() {
            return this.remainingCondition != null;
        }
    }

    /**
     * Optimize a query using available indexes.
     *
     * @param condition the WHERE condition to optimize
     * @param indexes   map of field path to PropertyIndex
     * @return optimization result with docIds and/or remaining conditions
     */
    public IndexResult optimize(Condition condition, Map<String, PropertyIndex> indexes) {
        if ((condition == null) || (indexes == null) || indexes.isEmpty()) {
            return IndexResult.fullScan();
        }

        return this.analyzeCondition(condition, indexes);
    }

    private IndexResult analyzeCondition(@NonNull Condition condition, @NonNull Map<String, PropertyIndex> indexes) {
        // Simple single-field condition (has a path)
        if (condition.getPath() != null) {
            PropertyIndex index = indexes.get(condition.getPath().getValue());
            if (index != null) {
                Optional<Set<String>> result = index.tryQuery(condition);
                if (result.isPresent()) {
                    return IndexResult.indexed(result.get(), null);
                }
            }
            // No index for this field or index can't handle this predicate
            return IndexResult.fullScan();
        }

        // Complex condition without direct path - check for AND/OR of sub-conditions
        Predicate[] predicates = condition.getPredicates();
        if ((predicates == null) || (predicates.length == 0)) {
            return IndexResult.fullScan();
        }

        if (condition.getOperator() == LogicalOperator.AND) {
            return this.optimizeAnd(predicates, indexes);
        }

        if (condition.getOperator() == LogicalOperator.OR) {
            return this.optimizeOr(predicates, indexes);
        }

        return IndexResult.fullScan();
    }

    /**
     * Optimize AND condition by intersecting index results.
     * Partial optimization is supported - indexed parts narrow down candidates,
     * remaining conditions filter in-memory.
     */
    private IndexResult optimizeAnd(@NonNull Predicate[] predicates, @NonNull Map<String, PropertyIndex> indexes) {
        Set<String> result = null;
        List<Predicate> unindexedPredicates = new ArrayList<>();

        for (Predicate predicate : predicates) {
            if (predicate instanceof Condition) {
                Condition subCondition = (Condition) predicate;
                IndexResult subResult = this.analyzeCondition(subCondition, indexes);

                if (!subResult.requiresFullScan()) {
                    // Got indexed results - intersect with previous
                    if (result == null) {
                        result = new HashSet<>(subResult.getDocIds());
                    } else {
                        result.retainAll(subResult.getDocIds());
                    }

                    // Early exit if intersection is empty
                    if (result.isEmpty()) {
                        return IndexResult.indexed(Collections.emptySet(), null);
                    }

                    // Track any remaining conditions from sub-result
                    if (subResult.hasRemainingCondition()) {
                        unindexedPredicates.add(subResult.getRemainingCondition());
                    }
                } else {
                    // This sub-condition couldn't use index - needs in-memory eval
                    unindexedPredicates.add(subCondition);
                }
            } else {
                // Non-Condition predicate (shouldn't happen at top level, but handle it)
                unindexedPredicates.add(predicate);
            }
        }

        if (result != null) {
            // Build remaining condition if any predicates couldn't use index
            Condition remaining = this.buildRemainingCondition(unindexedPredicates, LogicalOperator.AND);
            return IndexResult.indexed(result, remaining);
        }

        return IndexResult.fullScan();
    }

    /**
     * Optimize OR condition by unioning index results.
     * Only works if ALL parts can be fully indexed (no partial optimization for OR).
     */
    private IndexResult optimizeOr(@NonNull Predicate[] predicates, @NonNull Map<String, PropertyIndex> indexes) {
        Set<String> result = new HashSet<>();

        for (Predicate predicate : predicates) {
            if (predicate instanceof Condition) {
                Condition subCondition = (Condition) predicate;
                IndexResult subResult = this.analyzeCondition(subCondition, indexes);

                if (subResult.requiresFullScan()) {
                    // One part of OR can't use index - must fall back to full scan
                    return IndexResult.fullScan();
                }

                if (subResult.hasRemainingCondition()) {
                    // OR with partial index coverage is complex - fall back to full scan
                    // (Would need to union indexed docs AND filter, OR full scan the rest)
                    return IndexResult.fullScan();
                }

                result.addAll(subResult.getDocIds());
            } else {
                // Non-Condition predicate in OR - can't optimize
                return IndexResult.fullScan();
            }
        }

        return IndexResult.indexed(result, null);
    }

    /**
     * Build a remaining condition from unindexed predicates.
     */
    private Condition buildRemainingCondition(@NonNull List<Predicate> predicates, @NonNull LogicalOperator operator) {
        if (predicates.isEmpty()) {
            return null;
        }
        if ((predicates.size() == 1) && (predicates.get(0) instanceof Condition)) {
            return (Condition) predicates.get(0);
        }
        if (operator == LogicalOperator.AND) {
            return Condition.and(predicates.toArray(new Predicate[0]));
        }
        return Condition.or(predicates.toArray(new Predicate[0]));
    }
}
