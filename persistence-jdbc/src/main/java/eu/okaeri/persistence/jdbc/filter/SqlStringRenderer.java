package eu.okaeri.persistence.jdbc.filter;

import eu.okaeri.persistence.filter.renderer.DefaultStringRenderer;
import lombok.NonNull;

public class SqlStringRenderer extends DefaultStringRenderer {

    @Override
    public String render(@NonNull String text) {
        return "'" + this.escape(text) + "'";
    }
}
