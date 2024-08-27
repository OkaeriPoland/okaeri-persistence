package eu.okaeri.persistence.mongo.filter;

import eu.okaeri.persistence.PersistencePath;
import eu.okaeri.persistence.filter.renderer.VariableRenderer;
import lombok.NonNull;

public class MongoVariableRenderer implements VariableRenderer {

    @Override
    public String render(@NonNull PersistencePath path) {
        return path.toMongoPath();
    }
}
