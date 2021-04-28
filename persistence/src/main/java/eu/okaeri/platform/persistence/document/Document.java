package eu.okaeri.platform.persistence.document;

import eu.okaeri.configs.ConfigManager;
import eu.okaeri.configs.OkaeriConfig;
import eu.okaeri.configs.annotation.Exclude;
import eu.okaeri.configs.exception.OkaeriException;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.logging.Logger;

@ToString(exclude = "cachedInto")
public class Document extends OkaeriConfig {

    @Exclude private static final boolean DEBUG = Boolean.parseBoolean(System.getProperty("okaeri.platform.debug", "false"));
    @Exclude private static final Logger LOGGER = Logger.getLogger(Document.class.getName());

    @Exclude @Getter @Setter private DocumentSaver saver;
    @Exclude private Document cachedInto = this;

    @Override
    public OkaeriConfig save() throws OkaeriException {

        if (this.saver == null) {
            throw new IllegalArgumentException("cannot #save() without saver");
        }

        long start = System.currentTimeMillis();
        this.saver.save(this);

        if (DEBUG) {
            long took = System.currentTimeMillis() - start;
            LOGGER.info("[" + this.getBindFile() + "] Document save took " + took + " ms");
        }

        return this;
    }

    @Override
    public OkaeriConfig load() throws OkaeriException {
        throw new RuntimeException("load() not available for ConfigDocument, use one of the specific methods instead");
    }

    @Override
    public OkaeriConfig load(boolean update) throws OkaeriException {
        throw new RuntimeException("load(update) not available for ConfigDocument, use one of the specific methods instead");
    }

    @Override
    public OkaeriConfig saveDefaults() throws OkaeriException {
        throw new RuntimeException("saveDefaults() not available for ConfigDocument");
    }

    @SuppressWarnings("unchecked")
    public <T extends Document> T into(Class<T> configClazz) {

        if (!configClazz.isInstance(this.cachedInto)) {
            T newEntity = ConfigManager.transformCopy(this.cachedInto, configClazz);
            newEntity.setSaver(this.cachedInto.getSaver());
            this.cachedInto = newEntity;
        }

        return (T) this.cachedInto;
    }
}
