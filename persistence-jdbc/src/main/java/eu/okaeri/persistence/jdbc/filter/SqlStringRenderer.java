package eu.okaeri.persistence.jdbc.filter;

import eu.okaeri.persistence.filter.renderer.StringRenderer;
import lombok.NonNull;

/**
 * SQL-safe string renderer that uses SQL-standard escaping.
 * Single quotes are escaped by doubling them ('') as per SQL standard.
 * This works correctly in PostgreSQL, H2, MariaDB, and other SQL databases.
 */
public class SqlStringRenderer implements StringRenderer {

    @Override
    public String render(@NonNull String text) {
        return "'" + this.escape(text) + "'";
    }

    /**
     * Escape string for SQL using SQL-standard rules.
     * Single quotes are escaped by doubling: ' -> ''
     * Backslashes are NOT escaped - they are literal characters in SQL standard.
     * LIKE pattern escaping is handled separately by escapeLikePattern().
     */
    protected String escape(@NonNull String value) {
        if (value.indexOf('\0') >= 0) {
            throw new IllegalArgumentException("Null bytes are not supported in string values");
        }
        // SQL standard: escape single quotes by doubling them
        return value.replace("'", "''");
    }
}
