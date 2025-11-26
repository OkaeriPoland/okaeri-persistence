package eu.okaeri.persistence.document.index;

import eu.okaeri.configs.configurer.Configurer;
import eu.okaeri.persistence.PersistenceCollection;
import eu.okaeri.persistence.document.Document;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.LinkedHashMap;
import java.util.Map;

import static eu.okaeri.persistence.document.DocumentValueUtils.extractValue;

/**
 * Utility for extracting index values from documents.
 * Used by backends with emulated (in-memory) indexing.
 * <p>
 * Extracts values for all indexed properties defined in a collection,
 * preserving types for range queries (numbers stay as numbers).
 */
@RequiredArgsConstructor
public class IndexExtractor {

    private final Configurer simplifier;

    /**
     * Extract all indexed property values from a document.
     *
     * @param collection Collection with index definitions
     * @param document   Document to extract values from
     * @return Map of IndexProperty to value (null values excluded)
     */
    public Map<IndexProperty, Object> extract(@NonNull PersistenceCollection collection, @NonNull Document document) {
        Map<String, Object> documentMap = document.asMap(this.simplifier, true);
        return this.extract(collection, documentMap);
    }

    /**
     * Extract all indexed property values from a document map.
     *
     * @param collection  Collection with index definitions
     * @param documentMap Document as a map (from document.asMap() or JSON parsing)
     * @return Map of IndexProperty to value (null values excluded)
     */
    public Map<IndexProperty, Object> extract(@NonNull PersistenceCollection collection, @NonNull Map<String, Object> documentMap) {
        Map<IndexProperty, Object> result = new LinkedHashMap<>();

        for (IndexProperty index : collection.getIndexes()) {
            Object value = extractValue(documentMap, index.toParts());
            if (value != null) {
                result.put(index, value);
            }
        }

        return result;
    }

    /**
     * Extract a single indexed property value from a document.
     *
     * @param document Document to extract from
     * @param property Index property to extract
     * @return Value or null if not present
     */
    public Object extractSingle(@NonNull Document document, @NonNull IndexProperty property) {
        Map<String, Object> documentMap = document.asMap(this.simplifier, true);
        return extractValue(documentMap, property.toParts());
    }
}
