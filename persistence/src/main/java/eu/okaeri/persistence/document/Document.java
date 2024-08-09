package eu.okaeri.persistence.document;

import eu.okaeri.configs.ConfigManager;
import eu.okaeri.configs.OkaeriConfig;
import eu.okaeri.configs.annotation.Exclude;
import eu.okaeri.configs.exception.OkaeriException;
import eu.okaeri.persistence.PersistenceCollection;
import eu.okaeri.persistence.PersistencePath;
import java.util.Optional;
import java.util.logging.Logger;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;

@ToString(exclude = "cachedInto")
public class Document extends OkaeriConfig {

  @Exclude
  private static final boolean DEBUG =
      Boolean.parseBoolean(System.getProperty("okaeri.platform.debug", "false"));

  @Exclude private static final Logger LOGGER = Logger.getLogger(Document.class.getSimpleName());

  @Exclude @Getter @Setter private DocumentPersistence persistence;
  @Exclude @Getter @Setter private PersistencePath path;
  @Exclude @Getter @Setter private PersistenceCollection collection;
  @Exclude private Document cachedInto = this;

  @Override
  public Document save() throws OkaeriException {

    final long start = System.currentTimeMillis();
    final String logPath =
        DEBUG
            ? (((this.collection != null) && (this.path != null))
                ? this.collection.sub(this.path).getValue()
                : ("unknown/" + this.persistence))
            : null;

    if (this.getBindFile() == null) {
      this.persistence.write(this.collection, this.path, this);
    } else {
      this.save(this.getBindFile());
    }

    if (DEBUG) {
      final long took = System.currentTimeMillis() - start;
      LOGGER.info("[" + logPath + "] Document save took " + took + " ms");
    }

    return this;
  }

  @Override
  public OkaeriConfig load() throws OkaeriException {

    final long start = System.currentTimeMillis();
    final String logPath =
        DEBUG
            ? (((this.collection != null) && (this.path != null))
                ? this.collection.sub(this.path).getValue()
                : ("unknown/" + this.persistence))
            : null;

    if (this.getBindFile() == null) {
      final Optional<Document> document = this.persistence.read(this.collection, this.path);
      if (!document.isPresent()) {
        throw new RuntimeException(
            "Cannot #load, no result returned from persistence for path " + this.path);
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
  public <T extends Document> T into(@NonNull final Class<T> configClazz) {

    if (!configClazz.isInstance(this.cachedInto)) {
      final T newEntity = ConfigManager.transformCopy(this.cachedInto, configClazz);
      newEntity.setPath(this.cachedInto.path);
      newEntity.setCollection(this.cachedInto.collection);
      newEntity.setPersistence(this.cachedInto.persistence);
      this.cachedInto = newEntity;
    }

    return (T) this.cachedInto;
  }
}
