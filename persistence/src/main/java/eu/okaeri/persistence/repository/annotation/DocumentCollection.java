package eu.okaeri.persistence.repository.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a Document class as a persistence collection.
 * Defines collection name, key constraints, and indexes.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface DocumentCollection {

    /**
     * Collection/table name in the underlying storage.
     */
    String path();

    /**
     * Maximum length for document keys (primary key column size in SQL databases).
     * Only relevant for JDBC backends (PostgreSQL, H2). Ignored by MongoDB, Redis, and Flat Files.
     *
     * @return key length in characters (default: 255)
     */
    int keyLength() default 255;

    /**
     * Whether to automatically re-index existing documents when the collection is registered.
     * <p>
     * When true (default): Scans existing documents and creates index entries for any missing indexes.
     * Useful when adding new indexes to collections with existing data.
     * <p>
     * When false: Only new/updated documents will be indexed. Existing documents remain unindexed
     * until manually updated or until {@code fixIndexes()} is called explicitly.
     * Use this for large collections where automatic re-indexing would be too expensive.
     * <p>
     * Note: Only applies to emulated index backends (JDBC emulated, Redis, Flat Files).
     * Native index backends (MongoDB, PostgreSQL JSONB) create indexes immediately regardless of this setting.
     *
     * @return true to auto-fix indexes on registration (default: true)
     */
    boolean autofixIndexes() default true;

    /**
     * Index definitions for this collection.
     *
     * @return array of index definitions (default: empty)
     */
    DocumentIndex[] indexes() default {};
}
