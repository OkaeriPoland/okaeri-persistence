package eu.okaeri.persistence.filter.renderer;

import lombok.NonNull;

public class DefaultStringRenderer implements StringRenderer {

    @Override
    public String render(@NonNull String text) {
        return "\"" + this.escape(text) + "\"";
    }

    protected String escape(@NonNull String value) {
        StringBuilder builder = new StringBuilder();
        for (char c : value.toCharArray()) {
            if (c == '\'') {
                builder.append("\\'");
            } else if (c == '\"') {
                builder.append("\\\"");
            } else if (c == '\r') {
                builder.append("\\r");
            } else if (c == '\n') {
                builder.append("\\n");
            } else if (c == '\t') {
                builder.append("\\t");
            } else if ((c < 32) || (c >= 127)) {
                builder.append(String.format("\\u%04x", (int) c));
            } else {
                builder.append(c);
            }
        }
        return builder.toString();
    }
}
