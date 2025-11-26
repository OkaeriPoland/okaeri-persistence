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
import java.util.UUID;
import java.util.logging.Logger;

@ToString(exclude = "cachedInto")
public class Document extends OkaeriConfig {

    private static final @Exclude boolean DEBUG = Boolean.parseBoolean(System.getProperty("okaeri.platform.debug", "false"));
    private static final @Exclude Logger LOGGER = Logger.getLogger(Document.class.getSimpleName());

    private @Exclude @Getter @Setter DocumentPersistence persistence;
    private @Exclude @Getter PersistencePath path;
    private @Exclude @Getter @Setter PersistenceCollection collection;
    private @Exclude Document cachedInto = this;

    public void setPath(PersistencePath path) {
        this.path = path;
    }

    public void setPath(@NonNull UUID uuid) {
        if (this.path != null) {
            try {
                this.path.toUUID();
            } catch (IllegalArgumentException e) {
                throw new IllegalStateException("Cannot use setPath(UUID) when path is already set to non-UUID value: " + this.path.getValue() + ". Use setPath(PersistencePath) for explicit conversion.", e);
            }
        }
        this.path = PersistencePath.of(uuid);
    }

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
