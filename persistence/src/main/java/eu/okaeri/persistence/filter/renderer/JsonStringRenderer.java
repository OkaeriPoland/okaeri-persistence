package eu.okaeri.persistence.filter.renderer;

import lombok.NonNull;

/**
 * Renders strings as JSON string literals following RFC 8259.
 * Properly escapes special characters for JSON contexts.
 */
public class JsonStringRenderer implements StringRenderer {

    @Override
    public String render(@NonNull String text) {
        return "\"" + this.escape(text) + "\"";
    }

    /**
     * Escapes a string value for JSON according to RFC 8259.
     * Handles backslashes, quotes, and control characters.
     *
     * @param value The string to escape
     * @return Escaped string safe for JSON
     */
    protected String escape(@NonNull String value) {
        StringBuilder builder = new StringBuilder();
        for (char c : value.toCharArray()) {
            switch (c) {
                case '\\':
                    builder.append("\\\\");
                    break;
                case '"':
                    builder.append("\\\"");
                    break;
                case '\b':
                    builder.append("\\b");
                    break;
                case '\f':
                    builder.append("\\f");
                    break;
                case '\n':
                    builder.append("\\n");
                    break;
                case '\r':
                    builder.append("\\r");
                    break;
                case '\t':
                    builder.append("\\t");
                    break;
                default:
                    // Control characters (0x00-0x1F) must be escaped
                    if (c < 0x20) {
                        builder.append(String.format("\\u%04x", (int) c));
                    } else {
                        builder.append(c);
                    }
                    break;
            }
        }
        return builder.toString();
    }
}
