package eu.okaeri.persistence.document;

import eu.okaeri.configs.ConfigManager;
import eu.okaeri.configs.configurer.Configurer;
import eu.okaeri.configs.serdes.OkaeriSerdesPack;
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
import java.util.Map;
import java.util.stream.Stream;

/**
 * Utility for serializing/deserializing Documents.
 * Shared by all backends to ensure consistent document handling.
 */
public class DocumentSerializer {

    @Getter
    private final ConfigurerProvider configurerProvider;
    @Getter
    private final SerdesRegistry serdesRegistry;
    @Getter
    private final Configurer simplifier;

    // Reference to persistence for LazyRef/EagerRef - set after construction
    private Object persistence;

    public DocumentSerializer(@NonNull ConfigurerProvider configurerProvider, @NonNull OkaeriSerdesPack... serdesPacks) {
        this.configurerProvider = configurerProvider;

        // Build shared serdes registry
        this.serdesRegistry = new SerdesRegistry();
        Stream.concat(
                Stream.of(
                    new StandardSerdes(),
                    new SerdesCommons(),
                    registry -> registry.registerExclusive(Instant.class, new InstantSerializer(true))
                ),
                Stream.of(serdesPacks)
            )
            .forEach(pack -> pack.register(this.serdesRegistry));

        // Simplifier for document-to-map conversion
        this.simplifier = configurerProvider.get();
        this.simplifier.setRegistry(this.serdesRegistry);
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
        if (document.getDeclaration() == null) {
            document.updateDeclaration();
        }
        if (document.getConfigurer() == null) {
            document.setConfigurer(this.configurerProvider.get());
            document.getConfigurer().setRegistry(this.serdesRegistry);
        }

        // Document-specific setup
        document.setPath(path);
        document.setCollection(collection);
        if (this.persistence instanceof DocumentPersistence) {
            document.setPersistence((DocumentPersistence) this.persistence);
        }
    }

    /**
     * Convert document to a map for index extraction or other processing.
     */
    public Map<String, Object> toMap(@NonNull Document document) {
        return document.asMap(this.simplifier, true);
    }

    /**
     * Deep copy a document.
     */
    public Document deepCopy(@NonNull Document source) {
        Configurer copyConfigurer = this.configurerProvider.get();
        copyConfigurer.setRegistry(this.serdesRegistry);
        Document copy = ConfigManager.deepCopy(source, copyConfigurer, Document.class);
        copy.setPath(source.getPath());
        copy.setCollection(source.getCollection());
        if (this.persistence instanceof DocumentPersistence) {
            copy.setPersistence((DocumentPersistence) this.persistence);
        }
        return copy;
    }
}
