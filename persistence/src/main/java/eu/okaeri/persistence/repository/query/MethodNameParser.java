package eu.okaeri.persistence.repository.query;

import eu.okaeri.configs.OkaeriConfig;
import eu.okaeri.configs.schema.ConfigDeclaration;
import eu.okaeri.configs.schema.FieldDeclaration;
import eu.okaeri.configs.schema.GenericsDeclaration;
import eu.okaeri.persistence.document.Document;
import eu.okaeri.persistence.filter.condition.LogicalOperator;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.NonNull;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Parses repository method names into structured query representation.
 * Supports hybrid approach: only unambiguous patterns are parsed.
 * <p>
 * Supported patterns:
 * - Simple equality: findByEmail(String email)
 * - Multiple AND: findByNameAndLevel(String name, int level)
 * - Multiple OR: findByNameOrEmail(String name, String email)
 * - Ordering: findByActiveOrderByLevelDesc(boolean active)
 * - Limiting: findFirstByOrderByLevelDesc(), findTop10ByActive(boolean active)
 * - Count/Exists/Delete: countByActive(boolean), existsByEmail(String), deleteByLevel(int)
 * - Query all: findAll(), streamAllOrderByLevel()
 * <p>
 * Nested field syntax: Use $ separator (e.g., findByMeta$Name maps to "meta.name")
 * Underscores are ignored for readability (e.g., findBy_name_or_email$domain)
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class MethodNameParser {

    // Operation prefixes
    private static final Set<String> FIND_PREFIXES = new HashSet<>(Arrays.asList(
        "find", "read", "get", "query", "stream"
    ));
    private static final String COUNT_PREFIX = "count";
    private static final String EXISTS_PREFIX = "exists";
    private static final Set<String> DELETE_PREFIXES = new HashSet<>(Arrays.asList(
        "delete", "remove"
    ));

    // Keywords
    private static final String BY_KEYWORD = "By";
    private static final String ALL_KEYWORD = "All";
    private static final String AND_KEYWORD = "And";
    private static final String OR_KEYWORD = "Or";
    private static final String ORDER_BY_KEYWORD = "OrderBy";
    private static final String ASC_KEYWORD = "Asc";
    private static final String DESC_KEYWORD = "Desc";
    private static final String FIRST_KEYWORD = "First";
    private static final String TOP_PREFIX = "Top";

    /**
     * Parse a repository method into structured representation.
     *
     * @param method     the method to parse
     * @param entityType the entity class for field validation
     * @return parsed method structure
     * @throws MethodParseException if method cannot be parsed or is invalid
     */
    public static ParsedMethod parse(@NonNull Method method, @NonNull Class<? extends Document> entityType) {
        String methodName = method.getName();

        // Step 1: Extract operation type and prefix
        MethodOperation operation = extractOperation(methodName);
        String remaining = stripOperationPrefix(methodName, operation);

        // Step 2: Check for stream prefix (enforces Stream<T> return type)
        boolean requiresStreamReturn = methodName.startsWith("stream");

        // Step 3: Extract result limiting (First/Top10)
        Integer resultLimit = null;
        if (remaining.startsWith(FIRST_KEYWORD)) {
            resultLimit = 1;
            remaining = remaining.substring(FIRST_KEYWORD.length());
        } else if (remaining.startsWith(TOP_PREFIX)) {
            int limitEnd = TOP_PREFIX.length();
            while ((limitEnd < remaining.length()) && Character.isDigit(remaining.charAt(limitEnd))) {
                limitEnd++;
            }
            if (limitEnd > TOP_PREFIX.length()) {
                String limitStr = remaining.substring(TOP_PREFIX.length(), limitEnd);
                resultLimit = Integer.parseInt(limitStr);
                remaining = remaining.substring(limitEnd);
            }
        }

        // Step 4: Check for "All" (query all documents, no WHERE clause)
        boolean queryAll = false;
        if (remaining.equals(ALL_KEYWORD) || remaining.startsWith(ALL_KEYWORD + ORDER_BY_KEYWORD)) {
            queryAll = true;
            remaining = remaining.substring(ALL_KEYWORD.length());
        }

        // Step 5: Extract query parts (fields and logical operators)
        List<QueryPart> queryParts = new ArrayList<>();
        List<OrderPart> orderParts = new ArrayList<>();

        // Check if we have OrderBy directly (e.g., findFirstByOrderByLevel)
        boolean hasByClause = remaining.startsWith(BY_KEYWORD);
        boolean hasOrderByDirectly = remaining.startsWith(ORDER_BY_KEYWORD);

        if (!queryAll && !hasByClause && !hasOrderByDirectly) {
            throw new MethodParseException("Method must start with operation prefix followed by 'By', 'All', or 'OrderBy': " + methodName);
        }

        if (hasByClause) {
            remaining = remaining.substring(BY_KEYWORD.length());

            // Split by OrderBy to separate conditions from ordering
            int orderByIndex = remaining.indexOf(ORDER_BY_KEYWORD);
            String conditionsPart = (orderByIndex >= 0) ? remaining.substring(0, orderByIndex) : remaining;
            String orderByPart = (orderByIndex >= 0) ? remaining.substring(orderByIndex + ORDER_BY_KEYWORD.length()) : "";

            // Parse conditions if not empty
            if (!conditionsPart.isEmpty()) {
                queryParts = parseConditions(conditionsPart, method.getParameterCount(), entityType);
            }

            // Parse ordering
            if (!orderByPart.isEmpty()) {
                orderParts = parseOrderBy(orderByPart, entityType);
            }
        } else if (hasOrderByDirectly || queryAll) {
            // Query all or direct OrderBy without conditions
            if (remaining.startsWith(ORDER_BY_KEYWORD)) {
                String orderByPart = remaining.substring(ORDER_BY_KEYWORD.length());
                orderParts = parseOrderBy(orderByPart, entityType);
            } else if (!remaining.isEmpty() && queryAll) {
                throw new MethodParseException("Invalid method name after 'All': " + methodName);
            }
        }

        // Build ParsedMethod
        ParsedMethod.ParsedMethodBuilder builder = ParsedMethod.builder()
            .method(method)
            .operation(operation)
            .requiresStreamReturn(requiresStreamReturn)
            .queryAll(queryAll)
            .resultLimit(resultLimit);

        queryParts.forEach(builder::queryPart);
        orderParts.forEach(builder::orderPart);

        ParsedMethod parsed = builder.build();

        // Step 6: Validate against method signature
        validate(parsed, entityType);

        return parsed;
    }

    /**
     * Extract operation type from method name prefix.
     */
    private static MethodOperation extractOperation(String methodName) {
        for (String prefix : FIND_PREFIXES) {
            if (methodName.startsWith(prefix)) {
                return MethodOperation.FIND;
            }
        }
        if (methodName.startsWith(COUNT_PREFIX)) {
            return MethodOperation.COUNT;
        }
        if (methodName.startsWith(EXISTS_PREFIX)) {
            return MethodOperation.EXISTS;
        }
        for (String prefix : DELETE_PREFIXES) {
            if (methodName.startsWith(prefix)) {
                return MethodOperation.DELETE;
            }
        }
        throw new MethodParseException("Method must start with a valid operation prefix (find/read/get/query/stream/count/exists/delete/remove): " + methodName);
    }

    /**
     * Strip operation prefix from method name.
     */
    private static String stripOperationPrefix(String methodName, MethodOperation operation) {
        switch (operation) {
            case FIND:
                for (String prefix : FIND_PREFIXES) {
                    if (methodName.startsWith(prefix)) {
                        return methodName.substring(prefix.length());
                    }
                }
                break;
            case COUNT:
                return methodName.substring(COUNT_PREFIX.length());
            case EXISTS:
                return methodName.substring(EXISTS_PREFIX.length());
            case DELETE:
                for (String prefix : DELETE_PREFIXES) {
                    if (methodName.startsWith(prefix)) {
                        return methodName.substring(prefix.length());
                    }
                }
                break;
        }
        throw new MethodParseException("Unable to strip operation prefix: " + methodName);
    }

    /**
     * Parse field conditions from the "By" clause.
     * Handles AND/OR logical operators at word boundaries.
     * Word boundary = followed by uppercase (camelCase) or preceded by underscore.
     * Ignores underscores for readability.
     * Converts $ to dot notation for nested fields.
     * Validates and resolves field paths against entity type.
     */
    private static List<QueryPart> parseConditions(String conditionsPart, int parameterCount, Class<?> entityType) {
        if (conditionsPart.isEmpty()) {
            throw new MethodParseException("Empty condition part after 'By'");
        }

        List<QueryPart> parts = new ArrayList<>();
        int paramIndex = 0;
        int pos = 0;
        LogicalOperator pendingOperator = null; // Operator to attach to NEXT field

        while (pos < conditionsPart.length()) {
            String remaining = conditionsPart.substring(pos);

            // Find next And/Or keyword at a valid word boundary
            int andPos = findKeywordAtWordBoundary(remaining, AND_KEYWORD);
            int orPos = findKeywordAtWordBoundary(remaining, OR_KEYWORD);

            int nextOperatorPos = -1;
            LogicalOperator nextOperator = null;
            int keywordLength = 0;

            if ((andPos >= 0) && ((orPos < 0) || (andPos < orPos))) {
                nextOperatorPos = andPos;
                nextOperator = LogicalOperator.AND;
                keywordLength = AND_KEYWORD.length();
            } else if (orPos >= 0) {
                nextOperatorPos = orPos;
                nextOperator = LogicalOperator.OR;
                keywordLength = OR_KEYWORD.length();
            }

            String rawFieldName;
            if (nextOperatorPos >= 0) {
                rawFieldName = remaining.substring(0, nextOperatorPos);
            } else {
                rawFieldName = remaining;
            }

            if (rawFieldName.isEmpty()) {
                throw new MethodParseException("Empty field name in conditions: " + conditionsPart);
            }

            // Resolve field path with validation and subfield discovery
            String fieldPath = resolveFieldPath(rawFieldName, entityType);

            // Use pending operator from previous iteration (null for first field)
            parts.add(new QueryPart(fieldPath, paramIndex++, pendingOperator));

            // Save this operator for the next field
            pendingOperator = nextOperator;

            if (nextOperatorPos >= 0) {
                pos = pos + nextOperatorPos + keywordLength;
            } else {
                break;
            }
        }

        if (parts.isEmpty()) {
            throw new MethodParseException("No field conditions found after 'By': " + conditionsPart);
        }

        return parts;
    }

    /**
     * Find a keyword (And/Or) at a valid word boundary.
     * Valid boundary means:
     * - Followed by uppercase letter (camelCase: CategoryAndLevel)
     * - OR preceded by underscore (underscore style: category_and_level)
     *
     * @param text    the text to search in
     * @param keyword the keyword to find (And or Or)
     * @return position of keyword, or -1 if not found at valid boundary
     */
    private static int findKeywordAtWordBoundary(String text, String keyword) {
        int searchPos = 0;
        while (searchPos < text.length()) {
            // Find next occurrence (case-insensitive)
            int pos = indexOfIgnoreCase(text, keyword, searchPos);
            if (pos < 0) {
                return -1;
            }

            int afterKeyword = pos + keyword.length();

            // Check if this is a valid word boundary:
            // 1. Preceded by underscore: _And, _Or
            boolean precededByUnderscore = (pos > 0) && (text.charAt(pos - 1) == '_');

            // 2. Followed by uppercase letter (camelCase boundary) or underscore or end of string
            boolean validFollowing = (afterKeyword >= text.length()) ||
                Character.isUpperCase(text.charAt(afterKeyword)) ||
                (text.charAt(afterKeyword) == '_');

            if (precededByUnderscore || validFollowing) {
                return pos;
            }

            // Not a valid boundary, continue searching after this position
            searchPos = pos + 1;
        }
        return -1;
    }

    /**
     * Case-insensitive indexOf.
     */
    private static int indexOfIgnoreCase(String text, String keyword, int fromIndex) {
        String textUpper = text.toUpperCase();
        String keywordUpper = keyword.toUpperCase();
        return textUpper.indexOf(keywordUpper, fromIndex);
    }

    /**
     * Normalize field name from method name to actual field path.
     * - Removes underscores
     * - Converts $ to . (for nested fields)
     * - Converts to camelCase (first letter lowercase)
     * <p>
     * Note: This is the basic normalization without entity type validation.
     * Use resolveFieldPath() for full resolution with subfield discovery.
     */
    private static String normalizeFieldName(String fieldName) {
        // Remove underscores
        String cleaned = fieldName.replace("_", "");

        // Convert $ to . for nested fields
        String[] parts = cleaned.split("\\$");
        StringBuilder result = new StringBuilder();

        for (String part : parts) {
            if (part.isEmpty()) {
                continue;
            }

            // Convert first letter to lowercase
            String normalized = Character.toLowerCase(part.charAt(0)) + part.substring(1);

            if (result.length() > 0) {
                result.append(".");
            }
            result.append(normalized);
        }

        return result.toString();
    }

    /**
     * Resolve a field path from the method name part, with automatic subfield discovery.
     * Uses ConfigDeclaration API for field introspection.
     * Tries:
     * 1. Exact match after basic normalization (e.g., "name" → "name", "profile$age" → "profile.age")
     * 2. Subfield discovery without $ (e.g., "profileAge" → "profile.age" if profile.age exists)
     *
     * @param rawFieldName the raw field name from the method (e.g., "ProfileAge", "Meta$Name")
     * @param entityType   the entity class to validate against
     * @return the resolved field path (e.g., "profile.age", "meta.name")
     * @throws MethodParseException if field cannot be resolved
     */
    private static String resolveFieldPath(String rawFieldName, Class<?> entityType) {
        ConfigDeclaration declaration = ConfigDeclaration.of(entityType);

        // First, try basic normalization (handles $ separator)
        String normalized = normalizeFieldName(rawFieldName);

        // Check if the normalized path exists
        if (fieldPathExists(normalized, declaration)) {
            return normalized;
        }

        // If normalized path doesn't exist and doesn't contain dots,
        // try to discover nested fields by matching camelCase parts to entity structure
        if (!normalized.contains(".")) {
            String discovered = discoverNestedFieldPath(normalized, declaration);
            if (discovered != null) {
                return discovered;
            }
        }

        // Field not found - throw descriptive error
        throw new MethodParseException(
            "Unknown field '" + normalized + "' in entity " + entityType.getSimpleName() +
                ". Available fields: " + getAvailableFields(declaration)
        );
    }

    /**
     * Check if a field path exists in the entity type using ConfigDeclaration.
     * Supports nested paths like "profile.age".
     */
    private static boolean fieldPathExists(String path, ConfigDeclaration declaration) {
        String[] parts = path.split("\\.");

        ConfigDeclaration current = declaration;
        for (String part : parts) {
            Optional<FieldDeclaration> field = current.getField(part);
            if (!field.isPresent()) {
                return false;
            }

            // For nested paths, get declaration of the field type
            if (parts.length > 1) {
                GenericsDeclaration fieldType = field.get().getType();
                Class<?> rawType = fieldType.getType();
                if (OkaeriConfig.class.isAssignableFrom(rawType)) {
                    current = ConfigDeclaration.of(rawType);
                } else {
                    // Non-config type can't have subfields
                    return parts[parts.length - 1].equals(part);
                }
            }
        }
        return true;
    }

    /**
     * Try to discover a nested field path from a camelCase field name.
     * E.g., "profileAge" → "profile.age" if entity has field "profile" with subfield "age".
     */
    private static String discoverNestedFieldPath(String camelCaseName, ConfigDeclaration declaration) {
        // Try to match the beginning of camelCaseName to a field
        for (FieldDeclaration field : declaration.getFields()) {
            String fieldName = field.getName();

            // Check if camelCaseName starts with this field name
            if ((camelCaseName.length() > fieldName.length()) &&
                camelCaseName.startsWith(fieldName) &&
                Character.isUpperCase(camelCaseName.charAt(fieldName.length()))) {

                // Get the remaining part after the field name
                String remaining = camelCaseName.substring(fieldName.length());
                // Convert first letter to lowercase for the subfield
                String subFieldName = Character.toLowerCase(remaining.charAt(0)) + remaining.substring(1);

                // Check if this field type has the subfield
                Class<?> fieldType = field.getType().getType();
                if (OkaeriConfig.class.isAssignableFrom(fieldType)) {
                    ConfigDeclaration subDeclaration = ConfigDeclaration.of(fieldType);

                    // Recursively try to resolve the remaining path
                    String subPath = discoverNestedFieldPath(subFieldName, subDeclaration);
                    if (subPath != null) {
                        return fieldName + "." + subPath;
                    }

                    // Or check if it's a direct field
                    if (subDeclaration.getField(subFieldName).isPresent()) {
                        return fieldName + "." + subFieldName;
                    }
                }
            }
        }

        return null;
    }

    /**
     * Get a comma-separated list of available fields for error messages.
     */
    private static String getAvailableFields(ConfigDeclaration declaration) {
        StringBuilder sb = new StringBuilder();

        for (FieldDeclaration field : declaration.getFields()) {
            if (sb.length() > 0) {
                sb.append(", ");
            }
            sb.append(field.getName());

            // If field is an OkaeriConfig, show its subfields too
            Class<?> fieldType = field.getType().getType();
            if (OkaeriConfig.class.isAssignableFrom(fieldType)) {
                ConfigDeclaration subDeclaration = ConfigDeclaration.of(fieldType);
                List<String> subFields = subDeclaration.getFields().stream()
                    .map(FieldDeclaration::getName)
                    .collect(Collectors.toList());
                if (!subFields.isEmpty()) {
                    sb.append(" (").append(String.join(", ", subFields)).append(")");
                }
            }
        }

        return sb.toString();
    }

    /**
     * Parse ordering directives from "OrderBy" clause.
     * Supports multiple orderings: OrderByLevelDescNameAsc
     * Validates and resolves field paths against entity type.
     */
    private static List<OrderPart> parseOrderBy(String orderByPart, Class<?> entityType) {
        if (orderByPart.isEmpty()) {
            throw new MethodParseException("Empty OrderBy clause");
        }

        List<OrderPart> parts = new ArrayList<>();
        int pos = 0;

        while (pos < orderByPart.length()) {
            // Find next direction keyword
            int ascPos = orderByPart.indexOf(ASC_KEYWORD, pos);
            int descPos = orderByPart.indexOf(DESC_KEYWORD, pos);

            int nextDirPos = -1;
            boolean isAscending = true;

            if ((ascPos >= 0) && ((descPos < 0) || (ascPos < descPos))) {
                nextDirPos = ascPos;
                isAscending = true;
            } else if (descPos >= 0) {
                nextDirPos = descPos;
                isAscending = false;
            }

            String fieldName;
            if (nextDirPos >= 0) {
                fieldName = orderByPart.substring(pos, nextDirPos);
                String keyword = isAscending ? ASC_KEYWORD : DESC_KEYWORD;
                pos = nextDirPos + keyword.length();
            } else {
                // No direction specified, default to ascending
                fieldName = orderByPart.substring(pos);
                isAscending = true;
                pos = orderByPart.length();
            }

            if (fieldName.isEmpty()) {
                throw new MethodParseException("Empty field name in OrderBy: " + orderByPart);
            }

            // Resolve field path with validation and subfield discovery
            String fieldPath = resolveFieldPath(fieldName, entityType);

            parts.add(new OrderPart(fieldPath, isAscending));
        }

        if (parts.isEmpty()) {
            throw new MethodParseException("No ordering fields found in OrderBy: " + orderByPart);
        }

        return parts;
    }

    /**
     * Validate parsed method against its signature and entity type.
     */
    private static void validate(ParsedMethod parsed, Class<? extends Document> entityType) {
        Method method = parsed.getMethod();
        Parameter[] parameters = method.getParameters();

        // Validate parameter count
        int expectedParams = parsed.getQueryParts().size();
        if (parameters.length != expectedParams) {
            throw new MethodParseException(
                "Parameter count mismatch: method " + method.getName() +
                    " has " + parameters.length + " parameters but query requires " + expectedParams
            );
        }

        // Validate return type for stream prefix
        if (parsed.isRequiresStreamReturn() && (method.getReturnType() != Stream.class)) {
            throw new MethodParseException(
                "Methods with 'stream' prefix must return Stream<T>: " + method.getName()
            );
        }

        // Validate return type for operation
        validateReturnType(parsed);
    }

    /**
     * Validate return type matches operation type.
     */
    private static void validateReturnType(ParsedMethod parsed) {
        Class<?> returnType = parsed.getReturnType();
        MethodOperation operation = parsed.getOperation();

        switch (operation) {
            case COUNT:
                if ((returnType != long.class) && (returnType != Long.class)) {
                    throw new MethodParseException(
                        "Count methods must return long: " + parsed.getMethod().getName()
                    );
                }
                break;
            case EXISTS:
                if ((returnType != boolean.class) && (returnType != Boolean.class)) {
                    throw new MethodParseException(
                        "Exists methods must return boolean: " + parsed.getMethod().getName()
                    );
                }
                break;
            case DELETE:
                if ((returnType != long.class) && (returnType != Long.class) && (returnType != void.class) && (returnType != Void.class)) {
                    throw new MethodParseException(
                        "Delete methods must return long or void: " + parsed.getMethod().getName()
                    );
                }
                break;
            case FIND:
                // Validate acceptable return types for FIND
                if ((returnType != Optional.class) &&
                    (returnType != Stream.class) &&
                    (returnType != List.class) &&
                    (returnType != Collection.class) &&
                    (returnType != Set.class) &&
                    !Document.class.isAssignableFrom(returnType)) {
                    throw new MethodParseException(
                        "Find methods must return Optional<T>, Stream<T>, List<T>, Collection<T>, Set<T>, or T: " + parsed.getMethod().getName()
                    );
                }
                break;
        }
    }
}
