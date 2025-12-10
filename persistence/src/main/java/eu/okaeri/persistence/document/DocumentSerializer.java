package eu.okaeri.persistence.document;

import eu.okaeri.configs.ConfigManager;
import eu.okaeri.configs.configurer.Configurer;
import eu.okaeri.configs.serdes.OkaeriSerdes;
import eu.okaeri.configs.serdes.SerdesRegistry;
import eu.okaeri.configs.serdes.commons.SerdesCommons;
import eu.okaeri.configs.serdes.commons.serializer.InstantSerializer;
import eu.okaeri.configs.serdes.standard.StandardSerdes;
import eu.okaeri.persistence.PersistenceCollection;
import eu.okaeri.persistence.PersistencePath;
import eu.okaeri.persistence.document.ref.EagerRefSerializer;
import eu.okaeri.persistence.document.ref.LazyRefSerializer;
import lombok.Getter;
import lombok.NonNull;

import java.time.Instant;
import java.util.Arrays;
import java.util.Map;

/**
 * Utility for serializing/deserializing Documents.
 * Shared by all backends to ensure consistent document handling.
 */
public class DocumentSerializer {

    @Getter
    private final DocumentSerializerConfig config;
    @Getter
    private final SerdesRegistry serdesRegistry;
    @Getter
    private final Configurer configurer;

    // Reference to persistence for LazyRef/EagerRef - set after construction
    private Object persistence;

    /**
     * Create with full configuration.
     */
    public DocumentSerializer(@NonNull DocumentSerializerConfig config) {
        this.config = config;
        this.configurer = config.getConfigurer();

        // Build shared serdes registry
        this.serdesRegistry = new SerdesRegistry();
        this.serdesRegistry.register(new StandardSerdes());
        this.serdesRegistry.register(new SerdesCommons());
        this.serdesRegistry.registerExclusive(Instant.class, new InstantSerializer(true));

        for (OkaeriSerdes pack : config.getSerdesPacks()) {
            this.serdesRegistry.register(pack);
        }

        // Set registry on the reusable configurer
        this.configurer.setRegistry(this.serdesRegistry);
    }

    /**
     * Convenience constructor for the most common use case.
     */
    public DocumentSerializer(@NonNull Configurer configurer, @NonNull OkaeriSerdes... serdes) {
        this(DocumentSerializerConfig.builder()
            .configurer(configurer)
            .serdesPacks(Arrays.asList(serdes))
            .build());
    }

    /**
     * Set the persistence instance for document references.
     * Must be called after persistence is constructed.
     */
    public void setPersistence(@NonNull Object persistence) {
        this.persistence = persistence;
        // Register ref serializers now that we have the persistence instance
        if (persistence instanceof DocumentPersistence) {
            this.serdesRegistry.register(new LazyRefSerializer((DocumentPersistence) persistence));
            this.serdesRegistry.register(new EagerRefSerializer((DocumentPersistence) persistence));
        }
    }

    /**
     * Serialize a document to JSON string.
     */
    public String serialize(@NonNull Document document) {
        return document.saveToString();
    }

    /**
     * Deserialize JSON string to a document.
     */
    public Document deserialize(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull String json) {
        Document document = this.createDocument(collection, path);
        document.load(json);
        return document;
    }

    /**
     * Create a new empty document for a collection.
     */
    public Document createDocument(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {
        Document document = ConfigManager.create(Document.class);
        this.setupDocument(document, collection, path);
        return document;
    }

    /**
     * Setup a document with configurer and collection info.
     */
    public void setupDocument(@NonNull Document document, @NonNull PersistenceCollection collection, @NonNull PersistencePath path) {
        // OkaeriConfig setup
        if (document.getConfigurer() == null) {
            document.setConfigurer(this.configurer);
        }

        // Document-specific setup
        document.setPath(path);
        document.setCollection(collection);
        if (this.persistence instanceof DocumentPersistence) {
            document.setPersistence((DocumentPersistence) this.persistence);
        }

        // Apply custom document configuration hook
        this.config.getDocumentConfigurator().configure(document);
    }

    /**
     * Convert document to a map for index extraction or other processing.
     */
    public Map<String, Object> toMap(@NonNull Document document) {
        return document.asMap(this.configurer, true);
    }

    /**
     * Deep copy a document.
     */
    public Document deepCopy(@NonNull Document source) {
        Document copy = ConfigManager.deepCopy(source, this.configurer, Document.class);
        copy.setPath(source.getPath());
        copy.setCollection(source.getCollection());
        if (this.persistence instanceof DocumentPersistence) {
            copy.setPersistence((DocumentPersistence) this.persistence);
        }
        return copy;
    }
}
