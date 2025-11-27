package eu.okaeri.persistence.document.index;

import eu.okaeri.persistence.filter.condition.Condition;
import eu.okaeri.persistence.filter.predicate.Predicate;
import eu.okaeri.persistence.filter.predicate.collection.InPredicate;
import eu.okaeri.persistence.filter.predicate.equality.EqPredicate;
import eu.okaeri.persistence.filter.predicate.nullity.IsNullPredicate;
import eu.okaeri.persistence.filter.predicate.nullity.NotNullPredicate;
import eu.okaeri.persistence.filter.predicate.numeric.GtPredicate;
import eu.okaeri.persistence.filter.predicate.numeric.GtePredicate;
import eu.okaeri.persistence.filter.predicate.numeric.LtPredicate;
import eu.okaeri.persistence.filter.predicate.numeric.LtePredicate;
import lombok.NonNull;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Unified index supporting equality and range queries.
 * Thread-safe for concurrent reads and writes.
 * <p>
 * Supports:
 * - Exact match (EqPredicate)
 * - Range queries (GtPredicate, GtePredicate, LtPredicate, LtePredicate)
 * - IN queries (InPredicate)
 * <p>
 * Uses TreeMap for numeric values to enable O(log n) range queries.
 */
public class PropertyIndex {

    // docId -> stored value (for reverse lookup and removal)
    private final ConcurrentHashMap<String, Object> docIdToValue = new ConcurrentHashMap<>();

    // value -> set of docIds (for equality queries)
    private final ConcurrentHashMap<Object, Set<String>> valueToDocIds = new ConcurrentHashMap<>();

    // lowercase string -> set of docIds (for case-insensitive string queries)
    private final ConcurrentHashMap<String, Set<String>> lowercaseToDocIds = new ConcurrentHashMap<>();

    // docIds with null values (for isNull queries)
    private final Set<String> nullDocIds = ConcurrentHashMap.newKeySet();

    // BigDecimal -> set of docIds (for numeric range queries, sorted)
    // Note: Not using synchronizedNavigableMap - we use synchronized(this) blocks instead
    private final NavigableMap<BigDecimal, Set<String>> numericIndex = new TreeMap<>();

    // Track which docIds have numeric values (for cleanup)
    private final ConcurrentHashMap<String, BigDecimal> docIdToNumeric = new ConcurrentHashMap<>();

    // ===== WRITE OPERATIONS =====

    /**
     * Add or update a value for a document in the index.
     *
     * @param docId document identifier
     * @param value the value to index (can be null)
     * @return true if this changed the index
     */
    public boolean put(@NonNull String docId, Object value) {
        synchronized (this) {
            // Remove old entry if exists
            Object oldValue = this.docIdToValue.get(docId);
            if (oldValue != null) {
                this.removeFromMaps(docId, oldValue);
            }

            if (value == null) {
                this.docIdToValue.remove(docId);
                this.nullDocIds.add(docId);
                return true;
            }
            this.nullDocIds.remove(docId);

            // Store the value
            this.docIdToValue.put(docId, value);

            // Add to equality index
            this.valueToDocIds.computeIfAbsent(value, k -> ConcurrentHashMap.newKeySet()).add(docId);

            // Add to lowercase index if string
            if (value instanceof String) {
                String lowercase = ((String) value).toLowerCase();
                this.lowercaseToDocIds.computeIfAbsent(lowercase, k -> ConcurrentHashMap.newKeySet()).add(docId);
            }

            // Add to numeric index if applicable
            BigDecimal numeric = this.toNumeric(value);
            if (numeric != null) {
                this.numericIndex.computeIfAbsent(numeric, k -> ConcurrentHashMap.newKeySet()).add(docId);
                this.docIdToNumeric.put(docId, numeric);
            }

            return true;
        }
    }

    /**
     * Remove a document from the index.
     *
     * @param docId document identifier
     * @return true if the document was in the index
     */
    public boolean remove(@NonNull String docId) {
        synchronized (this) {
            Object value = this.docIdToValue.remove(docId);
            if (value != null) {
                this.removeFromMaps(docId, value);
                return true;
            }
            return this.nullDocIds.remove(docId);
        }
    }

    /**
     * Clear all entries from the index.
     */
    public void clear() {
        synchronized (this) {
            this.docIdToValue.clear();
            this.valueToDocIds.clear();
            this.lowercaseToDocIds.clear();
            this.nullDocIds.clear();
            this.numericIndex.clear();
            this.docIdToNumeric.clear();
        }
    }

    // ===== READ OPERATIONS - EQUALITY =====

    /**
     * Find all documents with an exact value match.
     *
     * @param value the value to find
     * @return set of document IDs (never null)
     */
    public Set<String> findEquals(Object value) {
        if (value == null) {
            return Collections.emptySet();
        }
        Set<String> result = this.valueToDocIds.get(value);
        return (result != null) ? new HashSet<>(result) : Collections.emptySet();
    }

    /**
     * Find all documents with a case-insensitive string match.
     *
     * @param value the string value to find (case-insensitive)
     * @return set of document IDs (never null)
     */
    public Set<String> findEqualsIgnoreCase(@NonNull String value) {
        Set<String> result = this.lowercaseToDocIds.get(value.toLowerCase());
        return (result != null) ? new HashSet<>(result) : Collections.emptySet();
    }

    /**
     * Find all documents with null values.
     *
     * @return set of document IDs (never null)
     */
    public Set<String> findNull() {
        return new HashSet<>(this.nullDocIds);
    }

    /**
     * Find all documents with non-null values.
     *
     * @return set of document IDs (never null)
     */
    public Set<String> findNotNull() {
        return new HashSet<>(this.docIdToValue.keySet());
    }

    /**
     * Find all documents where value is in the given collection.
     *
     * @param values the values to match
     * @return set of document IDs matching any value
     */
    public Set<String> findIn(@NonNull Collection<?> values) {
        Set<String> result = new HashSet<>();
        for (Object value : values) {
            Set<String> docIds = this.valueToDocIds.get(value);
            if (docIds != null) {
                result.addAll(docIds);
            }
        }
        return result;
    }

    /**
     * Get the indexed value for a document.
     *
     * @param docId document identifier
     * @return the indexed value, or null if not found
     */
    public Object getValue(@NonNull String docId) {
        return this.docIdToValue.get(docId);
    }

    /**
     * Check if a document is in the index.
     */
    public boolean containsDoc(@NonNull String docId) {
        return this.docIdToValue.containsKey(docId);
    }

    // ===== READ OPERATIONS - RANGE QUERIES =====

    /**
     * Find all documents with values greater than the threshold.
     *
     * @param value the threshold (exclusive)
     * @return set of document IDs
     */
    public Set<String> findGreaterThan(@NonNull Number value) {
        BigDecimal threshold = this.toNumeric(value);
        if (threshold == null) {
            return Collections.emptySet();
        }

        synchronized (this) {
            Set<String> result = new HashSet<>();
            for (Set<String> docIds : this.numericIndex.tailMap(threshold, false).values()) {
                result.addAll(docIds);
            }
            return result;
        }
    }

    /**
     * Find all documents with values greater than or equal to the threshold.
     *
     * @param value the threshold (inclusive)
     * @return set of document IDs
     */
    public Set<String> findGreaterThanOrEqual(@NonNull Number value) {
        BigDecimal threshold = this.toNumeric(value);
        if (threshold == null) {
            return Collections.emptySet();
        }

        synchronized (this) {
            Set<String> result = new HashSet<>();
            for (Set<String> docIds : this.numericIndex.tailMap(threshold, true).values()) {
                result.addAll(docIds);
            }
            return result;
        }
    }

    /**
     * Find all documents with values less than the threshold.
     *
     * @param value the threshold (exclusive)
     * @return set of document IDs
     */
    public Set<String> findLessThan(@NonNull Number value) {
        BigDecimal threshold = this.toNumeric(value);
        if (threshold == null) {
            return Collections.emptySet();
        }

        synchronized (this) {
            Set<String> result = new HashSet<>();
            for (Set<String> docIds : this.numericIndex.headMap(threshold, false).values()) {
                result.addAll(docIds);
            }
            return result;
        }
    }

    /**
     * Find all documents with values less than or equal to the threshold.
     *
     * @param value the threshold (inclusive)
     * @return set of document IDs
     */
    public Set<String> findLessThanOrEqual(@NonNull Number value) {
        BigDecimal threshold = this.toNumeric(value);
        if (threshold == null) {
            return Collections.emptySet();
        }

        synchronized (this) {
            Set<String> result = new HashSet<>();
            for (Set<String> docIds : this.numericIndex.headMap(threshold, true).values()) {
                result.addAll(docIds);
            }
            return result;
        }
    }

    /**
     * Find all documents with values in a range (inclusive).
     *
     * @param min minimum value (inclusive)
     * @param max maximum value (inclusive)
     * @return set of document IDs
     */
    public Set<String> findBetween(@NonNull Number min, @NonNull Number max) {
        BigDecimal minThreshold = this.toNumeric(min);
        BigDecimal maxThreshold = this.toNumeric(max);
        if ((minThreshold == null) || (maxThreshold == null)) {
            return Collections.emptySet();
        }

        synchronized (this) {
            Set<String> result = new HashSet<>();
            for (Set<String> docIds : this.numericIndex.subMap(minThreshold, true, maxThreshold, true).values()) {
                result.addAll(docIds);
            }
            return result;
        }
    }

    // ===== PREDICATE-BASED QUERY =====

    /**
     * Try to use the index to satisfy a condition.
     * Returns empty Optional if the condition cannot be optimized with this index.
     *
     * @param condition the condition to evaluate
     * @return Optional containing matching docIds, or empty if index cannot help
     */
    public Optional<Set<String>> tryQuery(@NonNull Condition condition) {
        Predicate[] predicates = condition.getPredicates();
        if ((predicates == null) || (predicates.length == 0)) {
            return Optional.empty();
        }

        // Single predicate - direct index lookup
        if (predicates.length == 1) {
            return this.tryQueryPredicate(predicates[0]);
        }

        // Multiple predicates - intersect results from each
        // ALL predicates must be indexable, otherwise fall back to full scan
        Set<String> result = null;
        for (Predicate p : predicates) {
            Optional<Set<String>> indexed = this.tryQueryPredicate(p);
            if (!indexed.isPresent()) {
                // Can't index this predicate - must fall back to full scan
                return Optional.empty();
            }
            if (result == null) {
                result = new HashSet<>(indexed.get());
            } else {
                result.retainAll(indexed.get());
            }
            // Early exit if intersection is empty
            if (result.isEmpty()) {
                return Optional.of(Collections.emptySet());
            }
        }

        return Optional.of(result);
    }

    /**
     * Try to use the index for a single predicate.
     */
    private Optional<Set<String>> tryQueryPredicate(@NonNull Predicate predicate) {
        if (predicate instanceof EqPredicate) {
            EqPredicate eq = (EqPredicate) predicate;
            if (eq.isIgnoreCase()) {
                Object val = eq.getRightOperand();
                if (val instanceof String) {
                    return Optional.of(this.findEqualsIgnoreCase((String) val));
                }
                return Optional.empty();
            }
            return Optional.of(this.findEquals(eq.getRightOperand()));
        }

        if (predicate instanceof InPredicate) {
            InPredicate in = (InPredicate) predicate;
            @SuppressWarnings("unchecked")
            Collection<?> values = (Collection<?>) in.getRightOperand();
            return Optional.of(this.findIn(values));
        }

        if (predicate instanceof GtPredicate) {
            GtPredicate gt = (GtPredicate) predicate;
            Object val = gt.getRightOperand();
            if (val instanceof Number) {
                return Optional.of(this.findGreaterThan((Number) val));
            }
        }

        if (predicate instanceof GtePredicate) {
            GtePredicate gte = (GtePredicate) predicate;
            Object val = gte.getRightOperand();
            if (val instanceof Number) {
                return Optional.of(this.findGreaterThanOrEqual((Number) val));
            }
        }

        if (predicate instanceof LtPredicate) {
            LtPredicate lt = (LtPredicate) predicate;
            Object val = lt.getRightOperand();
            if (val instanceof Number) {
                return Optional.of(this.findLessThan((Number) val));
            }
        }

        if (predicate instanceof LtePredicate) {
            LtePredicate lte = (LtePredicate) predicate;
            Object val = lte.getRightOperand();
            if (val instanceof Number) {
                return Optional.of(this.findLessThanOrEqual((Number) val));
            }
        }

        if (predicate instanceof IsNullPredicate) {
            return Optional.of(this.findNull());
        }

        if (predicate instanceof NotNullPredicate) {
            return Optional.of(this.findNotNull());
        }

        // Cannot use index for this predicate
        return Optional.empty();
    }

    // ===== UTILITY =====

    /**
     * Get the number of indexed documents.
     */
    public int size() {
        return this.docIdToValue.size();
    }

    /**
     * Check if the index is empty.
     */
    public boolean isEmpty() {
        return this.docIdToValue.isEmpty();
    }

    /**
     * Get all document IDs in this index.
     */
    public Set<String> getAllDocIds() {
        return new HashSet<>(this.docIdToValue.keySet());
    }

    /**
     * Check if the index has numeric values (supports range queries).
     */
    public boolean hasNumericIndex() {
        return !this.numericIndex.isEmpty();
    }

    // ===== INTERNAL HELPERS =====

    private void removeFromMaps(@NonNull String docId, @NonNull Object value) {
        // Remove from equality index
        Set<String> docIds = this.valueToDocIds.get(value);
        if (docIds != null) {
            docIds.remove(docId);
            if (docIds.isEmpty()) {
                this.valueToDocIds.remove(value);
            }
        }

        // Remove from lowercase index
        if (value instanceof String) {
            String lowercase = ((String) value).toLowerCase();
            Set<String> lcDocIds = this.lowercaseToDocIds.get(lowercase);
            if (lcDocIds != null) {
                lcDocIds.remove(docId);
                if (lcDocIds.isEmpty()) {
                    this.lowercaseToDocIds.remove(lowercase);
                }
            }
        }

        // Remove from numeric index
        BigDecimal numeric = this.docIdToNumeric.remove(docId);
        if (numeric != null) {
            Set<String> numericDocIds = this.numericIndex.get(numeric);
            if (numericDocIds != null) {
                numericDocIds.remove(docId);
                if (numericDocIds.isEmpty()) {
                    this.numericIndex.remove(numeric);
                }
            }
        }
    }

    /**
     * Convert a value to BigDecimal for numeric indexing.
     * Returns null if the value is not numeric.
     */
    private BigDecimal toNumeric(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof BigDecimal) {
            return (BigDecimal) value;
        }
        if (value instanceof Number) {
            // Use string conversion to preserve precision
            return new BigDecimal(value.toString());
        }
        // Try parsing string as number
        if (value instanceof String) {
            try {
                return new BigDecimal((String) value);
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return null;
    }

    // ===== SERIALIZATION SUPPORT =====

    /**
     * Export index state for persistence.
     * Returns a map that can be serialized to JSON/YAML.
     */
    public Map<String, Object> toMap() {
        Map<String, Object> data = new HashMap<>();
        // Store docId -> value mappings
        Map<String, Object> entries = new HashMap<>();
        entries.putAll(this.docIdToValue);
        data.put("entries", entries);
        return data;
    }

    /**
     * Import index state from persisted data.
     * Clears existing data and rebuilds from the map.
     */
    @SuppressWarnings("unchecked")
    public void fromMap(@NonNull Map<String, Object> data) {
        this.clear();
        Map<String, Object> entries = (Map<String, Object>) data.get("entries");
        if (entries != null) {
            for (Map.Entry<String, Object> entry : entries.entrySet()) {
                this.put(entry.getKey(), entry.getValue());
            }
        }
    }
}
