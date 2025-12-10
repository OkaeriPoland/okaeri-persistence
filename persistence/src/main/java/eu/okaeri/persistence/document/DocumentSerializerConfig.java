package eu.okaeri.persistence.document;

import eu.okaeri.configs.configurer.Configurer;
import eu.okaeri.configs.serdes.OkaeriSerdes;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Singular;

import java.util.List;

/**
 * Configuration for DocumentSerializer.
 */
@Builder
@Getter
public class DocumentSerializerConfig {

    /**
     * The configurer determines the serialization format (JSON, YAML, etc).
     * This instance is reused for all documents.
     */
    @NonNull
    private final Configurer configurer;

    /**
     * Additional serdes packs to register.
     */
    @Singular
    private final List<OkaeriSerdes> serdesPacks;

    /**
     * Optional configurator hook for newly created documents.
     * Applied after document setup with configurer and registry.
     */
    @Builder.Default
    private final DocumentConfigurator documentConfigurator = DocumentConfigurator.identity();
}
