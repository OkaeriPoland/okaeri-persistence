package eu.okaeri.persistence.document;

import eu.okaeri.configs.OkaeriConfigOptions;
import eu.okaeri.configs.configurer.Configurer;
import eu.okaeri.configs.serdes.OkaeriSerdes;
import eu.okaeri.configs.validator.ConfigValidator;
import eu.okaeri.persistence.Persistence;
import eu.okaeri.persistence.PersistencePath;
import lombok.NonNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

/**
 * Base builder for persistence backends.
 * All backends should extend this to provide consistent configuration.
 *
 * @param <T> The builder type (for fluent API)
 * @param <P> The persistence type being built
 */
public abstract class PersistenceBuilder<T extends PersistenceBuilder<T, P>, P extends Persistence> {

    protected PersistencePath basePath;
    protected Configurer configurer;
    protected List<OkaeriSerdes> serdesPacks = new ArrayList<>();
    protected DocumentConfigurator documentConfigurator = DocumentConfigurator.identity();

    @SuppressWarnings("unchecked")
    protected T self() {
        return (T) this;
    }

    /**
     * Set the base path for this persistence backend.
     */
    public T basePath(@NonNull String basePath) {
        this.basePath = PersistencePath.of(basePath);
        return this.self();
    }

    /**
     * Set the base path for this persistence backend.
     */
    public T basePath(@NonNull PersistencePath basePath) {
        this.basePath = basePath;
        return this.self();
    }

    /**
     * Set the configurer (determines the serialization format: JSON, YAML, etc).
     * This instance will be reused for all documents.
     * <p>
     * Example: {@code .configurer(new JsonSimpleConfigurer())}
     */
    public T configurer(@NonNull Configurer configurer) {
        this.configurer = configurer;
        return this.self();
    }

    /**
     * Add serdes packs for custom type serialization.
     */
    public T serdes(@NonNull OkaeriSerdes... packs) {
        this.serdesPacks.addAll(Arrays.asList(packs));
        return this.self();
    }

    /**
     * Convenience method to set a validator for all documents.
     * <p>
     * Example: {@code .validator(new OkaeriValidator(true))}
     */
    public T validator(@NonNull ConfigValidator validator) {
        return this.injectOptions(opts -> opts.validator(validator));
    }

    /**
     * Inject configuration options into all created documents.
     * <p>
     * Example:
     * <pre>
     * .injectOptions(opt -> {
     *     opt.validator(new OkaeriValidator(true));
     *     opt.errorComments(true);
     * })
     * </pre>
     */
    public T injectOptions(@NonNull Consumer<OkaeriConfigOptions> optionsConsumer) {
        this.documentConfigurator = this.documentConfigurator.andThen(doc -> doc.configure(optionsConsumer));
        return this.self();
    }

    /**
     * Inject custom logic when documents are created.
     * <p>
     * Example:
     * <pre>
     * .inject(doc -> {
     *     doc.configure(opts -> opts.validator(new OkaeriValidator(true)));
     *     // Other document-level setup
     * })
     * </pre>
     */
    public T inject(@NonNull Consumer<Document> documentConsumer) {
        this.documentConfigurator = this.documentConfigurator.andThen(documentConsumer::accept);
        return this.self();
    }

    /**
     * Build the document serializer config.
     */
    protected DocumentSerializerConfig buildSerializerConfig() {
        if (this.configurer == null) {
            throw new IllegalStateException("configurer is required");
        }
        return DocumentSerializerConfig.builder()
            .configurer(this.configurer)
            .serdesPacks(this.serdesPacks)
            .documentConfigurator(this.documentConfigurator)
            .build();
    }

    /**
     * Resolve the base path, defaulting to empty if not set.
     */
    protected PersistencePath resolveBasePath() {
        return (this.basePath != null) ? this.basePath : PersistencePath.of("");
    }

    /**
     * Build the persistence backend.
     */
    public abstract P build();

    /**
     * Build and wrap the persistence backend with DocumentPersistence.
     * <p>
     * DocumentPersistence provides polyfills for features not natively supported
     * by the backend (filtering, updates, streaming) and enables repository creation.
     * <p>
     * Example: {@code H2Persistence.builder().hikariConfig(config).configurer(new JsonSimpleConfigurer()).polyfill()}
     */
    public DocumentPersistence polyfill() {
        return new DocumentPersistence(this.build());
    }
}
