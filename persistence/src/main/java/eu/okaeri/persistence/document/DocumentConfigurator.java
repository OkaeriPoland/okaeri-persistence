package eu.okaeri.persistence.document;

import lombok.NonNull;

/**
 * Hook for applying additional configuration to documents when created.
 * Called after the document is created with its configurer and registry set.
 * <p>
 * This allows future-proof configuration without API changes when
 * okaeri-configs adds new options.
 */
@FunctionalInterface
public interface DocumentConfigurator {

    /**
     * Configure a newly created document.
     *
     * @param document The document to configure
     */
    void configure(@NonNull Document document);

    /**
     * Combines multiple configurators into one that applies them in order.
     */
    default DocumentConfigurator andThen(@NonNull DocumentConfigurator after) {
        return doc -> {
            this.configure(doc);
            after.configure(doc);
        };
    }

    /**
     * A no-op configurator.
     */
    static DocumentConfigurator identity() {
        return doc -> {};
    }
}
