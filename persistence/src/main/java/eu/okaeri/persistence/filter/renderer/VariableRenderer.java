package eu.okaeri.persistence.filter.renderer;

import eu.okaeri.persistence.PersistencePath;

public interface VariableRenderer {
    String render(PersistencePath path);
}
