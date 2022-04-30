package eu.okaeri.persistence.document;

import eu.okaeri.configs.ConfigManager;
import eu.okaeri.configs.OkaeriConfig;
import eu.okaeri.configs.annotation.Exclude;
import eu.okaeri.configs.exception.OkaeriException;
import eu.okaeri.persistence.PersistenceCollection;
import eu.okaeri.persistence.PersistencePath;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;

import java.util.Optional;
import java.util.logging.Logger;

@ToString(exclude = "cachedInto")
public class Document extends OkaeriConfig {

    @Exclude private static final boolean DEBUG = Boolean.parseBoolean(System.getProperty("okaeri.platform.debug", "false"));
    @Exclude private static final Logger LOGGER = Logger.getLogger(Document.class.getSimpleName());

    @Exclude @Getter @Setter private DocumentPersistence persistence;
    @Exclude @Getter @Setter private PersistencePath path;
    @Exclude @Getter @Setter private PersistenceCollection collection;
    @Exclude private Document cachedInto = this;

    @Override
    public Document save() throws OkaeriException {

        long start = System.currentTimeMillis();
        String logPath = DEBUG ? (((this.collection != null) && (this.path != null))
            ? this.collection.sub(this.path).getValue()
            : ("unknown/" + this.persistence)) : null;

        if (this.getBindFile() == null) {
            this.getPersistence().write(this.getCollection(), this.getPath(), this);
        } else {
            this.save(this.getBindFile());
        }

        if (DEBUG) {
            long took = System.currentTimeMillis() - start;
            LOGGER.info("[" + logPath + "] Document save took " + took + " ms");
        }

        return this;
    }

    @Override
    public OkaeriConfig load() throws OkaeriException {

        long start = System.currentTimeMillis();
        String logPath = DEBUG ? (((this.collection != null) && (this.path != null))
            ? this.collection.sub(this.path).getValue()
            : ("unknown/" + this.persistence)) : null;

        if (this.getBindFile() == null) {
            Optional<Document> document = this.getPersistence().read(this.getCollection(), this.getPath());
            if (!document.isPresent()) {
                throw new RuntimeException("Cannot #load, no result returned from persistence for path " + this.getPath());
            }
            this.load(document.get());
        } else {
            this.load(this.getBindFile());
        }

        return this;
    }

    @Override
    public OkaeriConfig saveDefaults() throws OkaeriException {
        throw new RuntimeException("saveDefaults() not available for ConfigDocument");
    }

    @SuppressWarnings("unchecked")
    public <T extends Document> T into(@NonNull Class<T> configClazz) {

        if (!configClazz.isInstance(this.cachedInto)) {
            T newEntity = ConfigManager.transformCopy(this.cachedInto, configClazz);
            newEntity.setPath(this.cachedInto.getPath());
            newEntity.setCollection(this.cachedInto.getCollection());
            newEntity.setPersistence(this.cachedInto.getPersistence());
            this.cachedInto = newEntity;
        }

        return (T) this.cachedInto;
    }
}
