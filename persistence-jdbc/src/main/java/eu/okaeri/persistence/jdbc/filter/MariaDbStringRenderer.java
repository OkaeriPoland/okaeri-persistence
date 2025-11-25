package eu.okaeri.persistence.jdbc.filter;

import lombok.NonNull;

/**
 * MariaDB/MySQL-safe string renderer.
 *
 * MariaDB/MySQL by default use backslash as an escape character (unlike SQL standard).
 * This means '\'' is interpreted as an escaped quote, not a literal backslash followed by a quote.
 *
 * To prevent SQL injection, we must escape both:
 * - Backslashes: \ -> \\
 * - Single quotes: ' -> '' (SQL standard)
 *
 * The backslash must be escaped FIRST to avoid double-escaping the quote escapes.
 */
public class MariaDbStringRenderer extends SqlStringRenderer {

    @Override
    protected String escape(@NonNull String value) {
        // First escape backslashes (must be done first!)
        // Then escape single quotes using SQL standard
        return value.replace("\\", "\\\\").replace("'", "''");
    }
}
