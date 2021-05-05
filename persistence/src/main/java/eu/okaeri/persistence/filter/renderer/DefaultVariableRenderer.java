package eu.okaeri.persistence.filter.renderer;

import eu.okaeri.persistence.PersistencePath;

public class DefaultVariableRenderer implements VariableRenderer {

    @Override
    public String render(PersistencePath path) {
        return "v_" + path.toSqlIdentifier();
    }
}
