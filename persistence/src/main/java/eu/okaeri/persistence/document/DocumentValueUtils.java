package eu.okaeri.persistence.document;

import lombok.NonNull;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Utility methods for extracting and comparing values from document maps.
 */
public final class DocumentValueUtils {

    private DocumentValueUtils() {
    }

    /**
     * Extract a value from a nested map using a path.
     *
     * @param map   the map to extract from
     * @param parts the path parts (e.g., ["user", "profile", "name"])
     * @return the value at the path, or null if not found
     */
    @SuppressWarnings("unchecked")
    public static Object extractValue(Map<?, ?> map, @NonNull List<String> parts) {
        Object current = map;

        for (String part : parts) {
            if (!(current instanceof Map)) {
                return null;
            }
            current = ((Map<?, ?>) current).get(part);
            if (current == null) {
                return null;
            }
        }

        return current;
    }

    /**
     * Compare two values for equality with type coercion.
     * Handles nulls, numbers, strings, UUIDs with intelligent type conversion.
     *
     * @param value1 first value
     * @param value2 second value
     * @return true if values are equal
     * @throws IllegalArgumentException if values cannot be compared
     */
    public static boolean compareEquals(Object value1, Object value2) {
        // Both null
        if ((value1 == null) && (value2 == null)) {
            return true;
        }

        // One null
        if ((value1 == null) || (value2 == null)) {
            return false;
        }

        // Both numbers - compare as double
        if ((value1 instanceof Number) && (value2 instanceof Number)) {
            return ((Number) value1).doubleValue() == ((Number) value2).doubleValue();
        }

        // Same type - use equals
        if (value1.getClass() == value2.getClass()) {
            return value1.equals(value2);
        }

        // String and number - compare numerically
        if (((value1 instanceof String) || (value1 instanceof Number)) && ((value2 instanceof String) || (value2 instanceof Number))) {
            try {
                return new BigDecimal(String.valueOf(value1)).compareTo(new BigDecimal(String.valueOf(value2))) == 0;
            } catch (NumberFormatException ignored) {
                return false;
            }
        }

        // String and UUID - compare as strings
        if (((value1 instanceof String) || (value1 instanceof UUID)) && ((value2 instanceof String) || (value2 instanceof UUID))) {
            return Objects.equals(String.valueOf(value1), String.valueOf(value2));
        }

        // String and Enum - compare as strings (enum uses name()), with case-insensitive fallback
        if ((value1 instanceof String) && (value2 instanceof Enum)) {
            String enumName = ((Enum<?>) value2).name();
            return value1.equals(enumName) || ((String) value1).equalsIgnoreCase(enumName);
        }
        if ((value1 instanceof Enum) && (value2 instanceof String)) {
            String enumName = ((Enum<?>) value1).name();
            return enumName.equals(value2) || enumName.equalsIgnoreCase((String) value2);
        }

        throw new IllegalArgumentException("cannot compare " + value1 + " [" + value1.getClass() + "] to " + value2 + " [" + value2.getClass() + "]");
    }

    /**
     * Compare two values for sorting with type coercion.
     * Handles nulls (sort last), numbers, strings, UUIDs with intelligent type conversion.
     *
     * @param value1 first value
     * @param value2 second value
     * @return negative if value1 < value2, 0 if equal, positive if value1 > value2
     */
    @SuppressWarnings("unchecked")
    public static int compareForSort(Object value1, Object value2) {
        // Nulls sort last
        if ((value1 == null) && (value2 == null)) return 0;
        if (value1 == null) return 1;
        if (value2 == null) return -1;

        // Both numbers - compare as double
        if ((value1 instanceof Number) && (value2 instanceof Number)) {
            return Double.compare(((Number) value1).doubleValue(), ((Number) value2).doubleValue());
        }

        // String and number mixed - try numeric comparison
        if (((value1 instanceof String) || (value1 instanceof Number)) && ((value2 instanceof String) || (value2 instanceof Number))) {
            try {
                BigDecimal bd1 = new BigDecimal(String.valueOf(value1));
                BigDecimal bd2 = new BigDecimal(String.valueOf(value2));
                return bd1.compareTo(bd2);
            } catch (NumberFormatException ignored) {
                // Fall through to string comparison
            }
        }

        // Same comparable type - use natural ordering
        if ((value1 instanceof Comparable) && (value2 instanceof Comparable) && (value1.getClass() == value2.getClass())) {
            return ((Comparable) value1).compareTo(value2);
        }

        // Fallback - string comparison
        return String.valueOf(value1).compareTo(String.valueOf(value2));
    }

    /**
     * Set a value in a nested map using a path, creating intermediate maps as needed.
     *
     * @param map   the map to set the value in
     * @param parts the path parts (e.g., ["user", "profile", "name"])
     * @param value the value to set
     */
    @SuppressWarnings("unchecked")
    public static void setValue(@NonNull Map<String, Object> map, @NonNull List<String> parts, Object value) {
        if (parts.isEmpty()) {
            throw new IllegalArgumentException("Path cannot be empty");
        }

        Map<String, Object> current = map;

        // Navigate to parent, creating intermediate maps
        for (int i = 0; i < parts.size() - 1; i++) {
            String part = parts.get(i);
            Object next = current.get(part);

            if (next == null) {
                Map<String, Object> newMap = new java.util.LinkedHashMap<>();
                current.put(part, newMap);
                current = newMap;
            } else if (next instanceof Map) {
                current = (Map<String, Object>) next;
            } else {
                throw new IllegalArgumentException(
                    "Cannot navigate through non-map value at path: " + String.join(".", parts.subList(0, i + 1))
                );
            }
        }

        // Set the final value
        current.put(parts.get(parts.size() - 1), value);
    }

    /**
     * Remove a value from a nested map using a path.
     *
     * @param map   the map to remove the value from
     * @param parts the path parts (e.g., ["user", "profile", "name"])
     * @return true if the field was removed, false if it didn't exist
     */
    @SuppressWarnings("unchecked")
    public static boolean removeValue(@NonNull Map<String, Object> map, @NonNull List<String> parts) {
        if (parts.isEmpty()) {
            throw new IllegalArgumentException("Path cannot be empty");
        }

        // Navigate to parent
        Map<String, Object> current = map;
        for (int i = 0; i < parts.size() - 1; i++) {
            String part = parts.get(i);
            Object next = current.get(part);

            if (next == null) {
                return false; // Path doesn't exist
            }

            if (!(next instanceof Map)) {
                throw new IllegalArgumentException(
                    "Cannot navigate through non-map value at path: " + String.join(".", parts.subList(0, i + 1))
                );
            }

            current = (Map<String, Object>) next;
        }

        // Remove the final key
        String finalKey = parts.get(parts.size() - 1);
        return current.remove(finalKey) != null;
    }
}
