package eu.okaeri.persistence.repository;

import eu.okaeri.persistence.PersistenceCollection;
import eu.okaeri.persistence.PersistenceEntity;
import eu.okaeri.persistence.document.Document;
import eu.okaeri.persistence.document.DocumentPersistence;
import eu.okaeri.persistence.filter.DeleteFilter;
import eu.okaeri.persistence.filter.FindFilter;
import eu.okaeri.persistence.filter.FindFilterBuilder;
import eu.okaeri.persistence.filter.OrderBy;
import eu.okaeri.persistence.filter.condition.Condition;
import eu.okaeri.persistence.filter.condition.LogicalOperator;
import eu.okaeri.persistence.filter.predicate.equality.EqPredicate;
import eu.okaeri.persistence.repository.query.*;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class RepositoryDeclaration<T extends DocumentRepository> {

    private final Class<T> type;
    private final Map<Method, RepositoryMethodCaller> methods;
    private final Class<?> pathType;
    private final Class<? extends Document> entityType;

    @SuppressWarnings("unchecked")
    public static <A extends DocumentRepository> RepositoryDeclaration<A> of(@NonNull Class<A> clazz) {

        Map<Method, RepositoryMethodCaller> methods = new HashMap<>();
        Type[] types = ((ParameterizedType) clazz.getGenericInterfaces()[0]).getActualTypeArguments();
        Class<?> pathType = (Class<?>) types[0];
        Class<? extends Document> entityType = (Class<? extends Document>) types[1];

        for (Method method : clazz.getDeclaredMethods()) {
            // Skip default methods - they're handled by the proxy
            if (method.isDefault()) {
                continue;
            }

            // Skip synthetic methods (e.g., lambda implementations in default methods)
            if (method.isSynthetic()) {
                continue;
            }

            // Skip methods that exist in DefaultDocumentRepository (like save, findAll, etc.)
            if (isDefaultRepositoryMethod(method)) {
                continue;
            }

            // Parse method name - fail fast if invalid
            ParsedMethod parsed = MethodNameParser.parse(method, entityType);

            // Create method caller based on parsed method
            RepositoryMethodCaller caller = createCaller(parsed, entityType);
            methods.put(method, caller);
        }

        return new RepositoryDeclaration<A>(clazz, methods, pathType, entityType);
    }

    /**
     * Create a RepositoryMethodCaller from a ParsedMethod.
     */
    private static RepositoryMethodCaller createCaller(ParsedMethod parsed, Class<? extends Document> entityType) {
        MethodOperation operation = parsed.getOperation();

        switch (operation) {
            case FIND:
                return createFindCaller(parsed, entityType);
            case COUNT:
                return createCountCaller(parsed, entityType);
            case EXISTS:
                return createExistsCaller(parsed, entityType);
            case DELETE:
                return createDeleteCaller(parsed);
            default:
                throw new IllegalArgumentException("Unknown operation: " + operation);
        }
    }

    /**
     * Build a Condition from ParsedMethod's query parts and arguments.
     */
    private static Condition buildCondition(ParsedMethod parsed, Object[] args) {
        List<QueryPart> parts = parsed.getQueryParts();
        if (parts.isEmpty()) {
            return null;
        }

        // Build conditions for each query part
        List<Condition> conditions = new ArrayList<>();
        for (QueryPart part : parts) {
            Object value = args[part.getParameterIndex()];
            Condition condition = Condition.on(part.getField(), new EqPredicate(value));
            conditions.add(condition);
        }

        // Combine conditions with logical operators (AND has precedence over OR)
        if (conditions.size() == 1) {
            return conditions.get(0);
        }

        // Check if all operators are the same (all AND or all OR)
        LogicalOperator commonOperator = null;
        for (int i = 1; i < parts.size(); i++) {
            LogicalOperator op = parts.get(i).getLogicalOperator();
            if (commonOperator == null) {
                commonOperator = op;
            } else if (commonOperator != op) {
                // Mixed operators - need to handle precedence
                commonOperator = null;
                break;
            }
        }

        if (commonOperator == LogicalOperator.AND) {
            return Condition.and(conditions.toArray(new Condition[0]));
        } else if (commonOperator == LogicalOperator.OR) {
            return Condition.or(conditions.toArray(new Condition[0]));
        } else {
            // Mixed operators - AND has precedence over OR
            // Algorithm: group consecutive ANDs first, then OR them together
            // Example: A OR B AND C OR D → A OR (B AND C) OR D
            return buildWithPrecedence(conditions, parts);
        }
    }

    /**
     * Build condition tree with proper operator precedence (AND > OR).
     * Groups consecutive AND conditions first, then combines with OR.
     */
    private static Condition buildWithPrecedence(List<Condition> conditions, List<QueryPart> parts) {
        List<Condition> orOperands = new ArrayList<>();
        List<Condition> currentAndGroup = new ArrayList<>();
        currentAndGroup.add(conditions.get(0));

        for (int i = 1; i < conditions.size(); i++) {
            LogicalOperator op = parts.get(i).getLogicalOperator();

            if (op == LogicalOperator.AND) {
                // Continue building AND group
                currentAndGroup.add(conditions.get(i));
            } else {
                // OR operator - flush current AND group and start new one
                if (currentAndGroup.size() == 1) {
                    orOperands.add(currentAndGroup.get(0));
                } else {
                    orOperands.add(Condition.and(currentAndGroup.toArray(new Condition[0])));
                }
                currentAndGroup = new ArrayList<>();
                currentAndGroup.add(conditions.get(i));
            }
        }

        // Flush final AND group
        if (currentAndGroup.size() == 1) {
            orOperands.add(currentAndGroup.get(0));
        } else {
            orOperands.add(Condition.and(currentAndGroup.toArray(new Condition[0])));
        }

        // Combine all OR operands
        if (orOperands.size() == 1) {
            return orOperands.get(0);
        }
        return Condition.or(orOperands.toArray(new Condition[0]));
    }

    /**
     * Build OrderBy array from ParsedMethod's order parts.
     */
    private static OrderBy[] buildOrderBy(ParsedMethod parsed) {
        List<OrderPart> parts = parsed.getOrderParts();
        if (parts.isEmpty()) {
            return new OrderBy[0];
        }

        return parts.stream()
            .map(part -> part.isAscending()
                ? OrderBy.asc(part.getField())
                : OrderBy.desc(part.getField())
            )
            .toArray(OrderBy[]::new);
    }

    /**
     * Build FindFilter from ParsedMethod and arguments.
     */
    private static FindFilter buildFindFilter(ParsedMethod parsed, Object[] args) {
        FindFilterBuilder builder = FindFilter.builder();

        Condition condition = buildCondition(parsed, args);
        if (condition != null) {
            builder.where(condition);
        }

        OrderBy[] orderBy = buildOrderBy(parsed);
        if (orderBy.length > 0) {
            builder.orderBy(orderBy);
        }

        Integer limit = parsed.getResultLimit();
        if (limit != null) {
            builder.limit(limit);
        }

        return builder.build();
    }

    /**
     * Create caller for FIND operations.
     */
    private static RepositoryMethodCaller createFindCaller(ParsedMethod parsed, Class<? extends Document> entityType) {
        Class<?> returnType = parsed.getReturnType();
        Class<?> insideType = getInsideTypeFromParsed(parsed);

        if (returnType == Optional.class) {
            if (insideType == PersistenceEntity.class) {
                return (persistence, collection, args) -> {
                    FindFilter filter = buildFindFilter(parsed, args);
                    return persistence.find(collection, filter)
                        .findFirst()
                        .map(entity -> entity.into(entityType));
                };
            } else {
                return (persistence, collection, args) -> {
                    FindFilter filter = buildFindFilter(parsed, args);
                    return persistence.find(collection, filter)
                        .findFirst()
                        .map(entity -> entity.into(entityType))
                        .map(PersistenceEntity::getValue);
                };
            }
        }

        if (returnType == Stream.class) {
            if (insideType == PersistenceEntity.class) {
                return (persistence, collection, args) -> {
                    FindFilter filter = buildFindFilter(parsed, args);
                    return persistence.find(collection, filter)
                        .map(entity -> entity.into(entityType));
                };
            } else {
                return (persistence, collection, args) -> {
                    FindFilter filter = buildFindFilter(parsed, args);
                    return persistence.find(collection, filter)
                        .map(entity -> entity.into(entityType))
                        .map(PersistenceEntity::getValue);
                };
            }
        }

        if ((returnType == List.class) || (returnType == Collection.class)) {
            if (insideType == PersistenceEntity.class) {
                return (persistence, collection, args) -> {
                    FindFilter filter = buildFindFilter(parsed, args);
                    return persistence.find(collection, filter)
                        .map(entity -> entity.into(entityType))
                        .collect(Collectors.toList());
                };
            } else {
                return (persistence, collection, args) -> {
                    FindFilter filter = buildFindFilter(parsed, args);
                    return persistence.find(collection, filter)
                        .map(entity -> entity.into(entityType))
                        .map(PersistenceEntity::getValue)
                        .collect(Collectors.toList());
                };
            }
        }

        if (returnType == Set.class) {
            if (insideType == PersistenceEntity.class) {
                return (persistence, collection, args) -> {
                    FindFilter filter = buildFindFilter(parsed, args);
                    return persistence.find(collection, filter)
                        .map(entity -> entity.into(entityType))
                        .collect(Collectors.toSet());
                };
            } else {
                return (persistence, collection, args) -> {
                    FindFilter filter = buildFindFilter(parsed, args);
                    return persistence.find(collection, filter)
                        .map(entity -> entity.into(entityType))
                        .map(PersistenceEntity::getValue)
                        .collect(Collectors.toSet());
                };
            }
        }

        // Direct entity return (naked T)
        if (Document.class.isAssignableFrom(returnType)) {
            return (persistence, collection, args) -> {
                FindFilter filter = buildFindFilter(parsed, args);
                return persistence.find(collection, filter)
                    .findFirst()
                    .map(entity -> entity.into(entityType))
                    .map(PersistenceEntity::getValue)
                    .orElse(null);
            };
        }

        throw new IllegalArgumentException("Unsupported return type for FIND: " + returnType);
    }

    /**
     * Create caller for COUNT operations.
     */
    private static RepositoryMethodCaller createCountCaller(ParsedMethod parsed, Class<? extends Document> entityType) {
        return (persistence, collection, args) -> {
            FindFilter filter = buildFindFilter(parsed, args);
            return persistence.find(collection, filter).count();
        };
    }

    /**
     * Create caller for EXISTS operations.
     */
    private static RepositoryMethodCaller createExistsCaller(ParsedMethod parsed, Class<? extends Document> entityType) {
        return (persistence, collection, args) -> {
            // Use limit 1 for efficiency
            FindFilterBuilder builder = FindFilter.builder();

            Condition condition = buildCondition(parsed, args);
            if (condition != null) {
                builder.where(condition);
            }
            builder.limit(1);

            return persistence.find(collection, builder.build()).findAny().isPresent();
        };
    }

    /**
     * Create caller for DELETE operations.
     */
    private static RepositoryMethodCaller createDeleteCaller(ParsedMethod parsed) {
        return (persistence, collection, args) -> {
            Condition condition = buildCondition(parsed, args);
            if (condition == null) {
                throw new IllegalStateException("DELETE operations require a WHERE condition");
            }

            DeleteFilter filter = DeleteFilter.builder().where(condition).build();
            return persistence.delete(collection, filter);
        };
    }

    /**
     * Get the inside type from a ParsedMethod's generic return type.
     */
    private static Class<?> getInsideTypeFromParsed(ParsedMethod parsed) {
        Type genericReturnType = parsed.getGenericReturnType();

        if (!(genericReturnType instanceof ParameterizedType)) {
            return null;
        }

        ParameterizedType paramType = (ParameterizedType) genericReturnType;
        Type actualTypeArgument = paramType.getActualTypeArguments()[0];

        if (actualTypeArgument instanceof Class<?>) {
            return (Class<?>) actualTypeArgument;
        }

        if (actualTypeArgument instanceof ParameterizedType) {
            return (Class<?>) ((ParameterizedType) actualTypeArgument).getRawType();
        }

        return null;
    }

    /**
     * Check if a method exists in DefaultDocumentRepository.
     * These methods are provided by the framework and should not be parsed.
     */
    private static boolean isDefaultRepositoryMethod(Method method) {
        try {
            DefaultDocumentRepository.class.getMethod(method.getName(), method.getParameterTypes());
            return true;
        } catch (NoSuchMethodException e) {
            return false;
        }
    }

    @SuppressWarnings("unchecked")
    public T newProxy(@NonNull DocumentPersistence persistence, @NonNull PersistenceCollection collection, @NonNull ClassLoader classLoader) {

        DefaultDocumentRepository defaultRepository = new DefaultDocumentRepository(persistence, collection, this.entityType);
        Map<Method, Method> defaultRepositoryMethods = new HashMap<>();

        return (T) Proxy.newProxyInstance(classLoader, new Class[]{this.type}, (proxy, method, args) -> {

            // third party interface methods
            Class<?> dClass = method.getDeclaringClass();
            if (method.isDefault()) {
                try {
                    MethodType methodType = MethodType.methodType(method.getReturnType(), method.getParameterTypes());
                    return MethodHandles.lookup().findSpecial(dClass, method.getName(), methodType, dClass).bindTo(proxy).invokeWithArguments(args);
                } catch (IllegalAccessException ignored) {
                }
                // java 8 fallback
                Constructor<MethodHandles.Lookup> constructor = MethodHandles.Lookup.class.getDeclaredConstructor(Class.class);
                constructor.setAccessible(true);
                return constructor.newInstance(dClass).in(dClass).unreflectSpecial(method, dClass).bindTo(proxy).invokeWithArguments(args);
            }

            // okaeri-persistence provided impl
            try {
                Method defaultMethod;
                if (defaultRepositoryMethods.containsKey(method)) {
                    defaultMethod = defaultRepositoryMethods.get(method);
                } else {
                    defaultMethod = defaultRepository.getClass().getMethod(method.getName(), method.getParameterTypes());
                    defaultRepositoryMethods.put(method, defaultMethod);
                }
                if (defaultMethod != null) {
                    try {
                        return defaultMethod.invoke(defaultRepository, args);
                    } catch (InvocationTargetException exception) {
                        throw exception.getCause();
                    }
                }
            } catch (NoSuchMethodException | SecurityException ignored) {
                defaultRepositoryMethods.put(method, null);
            }

            // okaeri-persistence generated (parsed from method name)
            RepositoryMethodCaller caller = this.methods.get(method);
            if (caller == null) {
                throw new IllegalArgumentException("cannot proxy " + method);
            }

            return caller.call(persistence, collection, args);
        });
    }
}
