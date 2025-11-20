package eu.okaeri.persistence.repository.annotation;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Defines an index on a document field.
 * Backends create native indexes where supported (MongoDB, PostgreSQL) or emulate them (Redis, Flat Files).
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface DocumentIndex {

    /**
     * Path to the indexed field. Supports nested paths using dot notation (e.g., "profile.email").
     */
    String path();

    /**
     * Maximum length for indexed values (column size in SQL databases).
     * Only relevant for JDBC backends (PostgreSQL, H2). Ignored by MongoDB, Redis, and Flat Files.
     *
     * @return max length in characters (default: 255)
     */
    int maxLength() default 255;
}
