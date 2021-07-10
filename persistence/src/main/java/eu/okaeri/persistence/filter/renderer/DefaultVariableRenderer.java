package eu.okaeri.persistence.filter.renderer;

import eu.okaeri.persistence.PersistencePath;
import lombok.NonNull;

public class DefaultVariableRenderer implements VariableRenderer {

    @Override
    public String render(@NonNull PersistencePath path) {
        return "v_" + path.toSqlIdentifier();
    }
}
