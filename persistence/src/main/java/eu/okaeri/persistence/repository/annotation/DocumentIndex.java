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
     * Maximum length for indexed values in emulated index tables.
     * Used ONLY by H2 and MariaDB for VARCHAR column sizing in the separate index table.
     * <p>
     * NOT used by PostgreSQL (native JSONB GIN indexes), MongoDB (native indexes),
     * Redis, Flat Files, or In-Memory.
     * <p>
     * Default of 255 handles most strings. Increase for longer text fields.
     *
     * @return max length in characters (default: 255)
     */
    int maxLength() default 255;
}
