package eu.okaeri.persistence.repository.query;

import lombok.Builder;
import lombok.Data;
import lombok.NonNull;
import lombok.Singular;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.lang.reflect.Type;
import java.util.List;

/**
 * Structured representation of a parsed repository method.
 * Contains all information needed to generate backend queries.
 */
@Data
@Builder
public class ParsedMethod {
    /**
     * Original method being parsed.
     */
    @NonNull
    private final Method method;

    /**
     * Type of operation (FIND, COUNT, EXISTS, DELETE).
     */
    @NonNull
    private final MethodOperation operation;

    /**
     * Query conditions extracted from method name.
     * Empty list for methods like findAll() with no conditions.
     */
    @Singular
    private final List<QueryPart> queryParts;

    /**
     * Ordering directives extracted from OrderBy clause.
     * Empty list if no ordering specified.
     */
    @Singular
    private final List<OrderPart> orderParts;

    /**
     * Result limit for Top/First operations.
     * Null if no limit specified.
     * - First = limit 1
     * - Top10 = limit 10
     */
    private final Integer resultLimit;

    /**
     * Whether the method requires Stream<T> return type.
     * True for methods with 'stream' prefix.
     */
    private final boolean requiresStreamReturn;

    /**
     * Whether this method queries all documents (no 'By' clause).
     * True for findAll(), streamAll(), etc.
     */
    private final boolean queryAll;

    /**
     * Method parameters (from reflection).
     */
    public Parameter[] getParameters() {
        return this.method.getParameters();
    }

    /**
     * Method return type (from reflection).
     */
    public Class<?> getReturnType() {
        return this.method.getReturnType();
    }

    /**
     * Generic return type for collections (from reflection).
     */
    public Type getGenericReturnType() {
        return this.method.getGenericReturnType();
    }
}
