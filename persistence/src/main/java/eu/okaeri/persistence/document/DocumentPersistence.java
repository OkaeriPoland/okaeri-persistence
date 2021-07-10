package eu.okaeri.persistence.document;

import eu.okaeri.configs.ConfigManager;
import eu.okaeri.configs.configurer.Configurer;
import eu.okaeri.configs.serdes.OkaeriSerdesPack;
import eu.okaeri.configs.serdes.TransformerRegistry;
import eu.okaeri.configs.serdes.commons.SerdesCommons;
import eu.okaeri.configs.serdes.standard.StandardSerdes;
import eu.okaeri.persistence.Persistence;
import eu.okaeri.persistence.PersistenceCollection;
import eu.okaeri.persistence.PersistenceEntity;
import eu.okaeri.persistence.PersistencePath;
import eu.okaeri.persistence.document.index.IndexProperty;
import eu.okaeri.persistence.document.ref.EagerRefSerializer;
import eu.okaeri.persistence.document.ref.LazyRefSerializer;
import eu.okaeri.persistence.raw.RawPersistence;
import lombok.Getter;
import lombok.NonNull;

import java.math.BigDecimal;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DocumentPersistence implements Persistence<Document> {

    private static final Logger LOGGER = Logger.getLogger(DocumentPersistence.class.getName());

    @Getter private final ConfigurerProvider configurerProvider;
    @Getter private final OkaeriSerdesPack[] serdesPacks;
    @Getter private final RawPersistence raw;
    private TransformerRegistry transformerRegistry;
    private Configurer simplifier;

    /**
     * @param rawPersistence Base persistence provider
     * @param configurerProvider Okaeri Config's provider (mostly json)
     * @param serdesPacks Additional serdes packs for the configurerProvider
     */
    public DocumentPersistence(@NonNull RawPersistence rawPersistence, @NonNull ConfigurerProvider configurerProvider, @NonNull OkaeriSerdesPack... serdesPacks) {
        this.raw = rawPersistence;
        this.configurerProvider = configurerProvider;
        this.serdesPacks = serdesPacks;
        // shared transform registry for faster transformations
        this.transformerRegistry = new TransformerRegistry();
        Stream.concat(Stream.of(new StandardSerdes(), new SerdesCommons()), Stream.of(this.serdesPacks)).forEach(pack -> pack.register(this.transformerRegistry));
        this.transformerRegistry.register(new LazyRefSerializer(this));
        this.transformerRegistry.register(new EagerRefSerializer(this));
        // simplifier for document mappings
        this.simplifier = configurerProvider.get();
        this.simplifier.setRegistry(this.transformerRegistry);
    }

    @Override
    public void setAutoFlush(boolean state) {
        this.getRaw().setAutoFlush(state);
    }

    @Override
    public void flush() {
        this.getRaw().flush();
    }

    @Override
    public PersistencePath getBasePath() {
        return this.getRaw().getBasePath();
    }

    @Override
    public void registerCollection(@NonNull PersistenceCollection collection) {
        this.getRaw().registerCollection(collection);
        if (!collection.isAutofixIndexes()) {
            return;
        }
        this.fixIndexes(collection);
    }

    @Override
    public long fixIndexes(@NonNull PersistenceCollection collection) {

        if (!this.getRaw().isNativeIndexes()) {
            return 0;
        }

        Set<IndexProperty> indexes = this.getRaw().getKnownIndexes().getOrDefault(collection.getValue(), new HashSet<>());
        Set<PersistencePath> withMissingIndexes = this.findMissingIndexes(collection, indexes);

        if (withMissingIndexes.isEmpty()) {
            return 0;
        }

        int total = withMissingIndexes.size();
        long start = System.currentTimeMillis();
        long lastInfo = System.currentTimeMillis();
        int updated = 0;
        LOGGER.warning("[" + this.getBasePath().sub(collection).getValue() + "] Found " + total + " entries with missing indexes, updating..");
        this.setAutoFlush(false);

        for (PersistencePath key : withMissingIndexes) {

            this.updateIndex(collection, key);
            updated++;

            if ((System.currentTimeMillis() - lastInfo) <= 5_000) {
                continue;
            }

            int percent = (int) (((double) updated / (double) total) * 100);
            LOGGER.warning("[" + this.getBasePath().sub(collection).getValue() + "] " + updated + " already done (" + percent + "%)");
            lastInfo = System.currentTimeMillis();
        }

        this.setAutoFlush(true);
        this.flush();
        LOGGER.warning("[" + this.getBasePath().sub(collection).getValue() + "] Finished creating indexes! (took: " + (System.currentTimeMillis() - start) + " ms)");
        return updated;
    }

    @Override
    public boolean updateIndex(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull IndexProperty property, @NonNull String identity) {
        return this.getRaw().isNativeIndexes() && this.getRaw().updateIndex(collection, path, property, identity);
    }

    @Override
    public boolean updateIndex(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull Document document) {

        if (!this.getRaw().isNativeIndexes()) {
            return false;
        }

        Set<IndexProperty> collectionIndexes = this.getRaw().getKnownIndexes().get(collection.getValue());
        if (collectionIndexes == null) {
            return false;
        }

        Map<String, Object> documentMap = document.asMap(this.simplifier, true);
        int changes = 0;

        for (IndexProperty index : collectionIndexes) {
            Object value = this.extractValue(documentMap, index.toParts());
            if ((value != null) && !this.getRaw().canUseToString(value)) {
                throw new RuntimeException("cannot transform " + value + " to index as string");
            }
            boolean changed = this.updateIndex(collection, path, index, (value == null) ? null : String.valueOf(value));
            if (changed) changes++;
        }

        return changes > 0;
    }

    @Override
    public boolean updateIndex(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {

        if (!this.getRaw().isNativeIndexes()) {
            return false;
        }

        Optional<Document> document = this.read(collection, path);
        return document.map(value -> this.updateIndex(collection, path, value)).isPresent();
    }

    @Override
    public boolean dropIndex(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull IndexProperty property) {
        return this.getRaw().isNativeIndexes() && this.getRaw().dropIndex(collection, path, property);
    }

    @Override
    public boolean dropIndex(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {
        return this.getRaw().isNativeIndexes() && this.getRaw().dropIndex(collection, path);
    }

    @Override
    public boolean dropIndex(@NonNull PersistenceCollection collection, @NonNull IndexProperty property) {
        return this.getRaw().isNativeIndexes() && this.getRaw().dropIndex(collection, property);
    }

    @Override
    public Set<PersistencePath> findMissingIndexes(@NonNull PersistenceCollection collection, @NonNull Set<IndexProperty> indexProperties) {
        return this.getRaw().findMissingIndexes(collection, indexProperties);
    }

    @Override
    public Document readOrEmpty(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {
        return this.read(collection, path).orElse(this.createDocument(collection, path));
    }

    @Override
    public Optional<Document> read(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {

        Optional<String> data = this.getRaw().read(collection, path);
        if (!data.isPresent()) {
            return Optional.empty();
        }

        Document document = this.createDocument(collection, path);
        return Optional.of((Document) document.load(data.get()));
    }

    @Override
    public Map<PersistencePath, Document> readOrEmpty(@NonNull PersistenceCollection collection, @NonNull Collection<PersistencePath> paths) {

        if (paths.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<PersistencePath, Document> map = new LinkedHashMap<>();
        Map<PersistencePath, Document> data = this.read(collection, paths);

        for (PersistencePath path : paths) {
            map.put(path, data.getOrDefault(path, this.createDocument(collection, path)));
        }

        return map;
    }

    @Override
    public Map<PersistencePath, Document> read(@NonNull PersistenceCollection collection, @NonNull Collection<PersistencePath> paths) {
        return paths.isEmpty() ? Collections.emptyMap() : this.getRaw().read(collection, paths).entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> {
                    Document document = this.createDocument(collection, entry.getKey());
                    return (Document) document.load(entry.getValue());
                }));

    }

    @Override
    public Map<PersistencePath, Document> readAll(@NonNull PersistenceCollection collection) {
        return this.getRaw().readAll(collection).entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> {
                    Document document = this.createDocument(collection, entry.getKey());
                    return (Document) document.load(entry.getValue());
                }));
    }

    @Override
    public Stream<PersistenceEntity<Document>> readByProperty(@NonNull PersistenceCollection collection, @NonNull PersistencePath property, Object propertyValue) {

        List<String> pathParts = property.toParts();
        Predicate<PersistenceEntity<Document>> documentFilter = entity -> {
            if (pathParts.size() == 1) {
                return this.compare(propertyValue, entity.getValue().get(pathParts.get(0)));
            }
            Map<String, Object> document = entity.getValue().asMap(this.simplifier, true);
            return this.compare(propertyValue, this.extractValue(document, pathParts));
        };

        // native read implementation may or may not filter entries
        // for every query, depending on the backend supported features
        // the goal is to allow extensibility - i trust but i verify
        if (this.getRaw().isNativeReadByProperty()) {
            return this.getRaw().readByProperty(collection, property, propertyValue)
                    .map(this.entityToDocumentMapper(collection))
                    .filter(documentFilter);
        }

        // streaming search optimzied with string search can
        // greatly reduce search time removing parsing overhead
        boolean stringSearch = this.getRaw().isUseStringSearch() && this.getRaw().canUseToString(propertyValue);
        return this.getRaw().streamAll(collection)
                .filter(entity -> !stringSearch || entity.getValue().contains(String.valueOf(propertyValue)))
                .map(this.entityToDocumentMapper(collection))
                .filter(documentFilter);
    }

    @Override
    public Stream<PersistenceEntity<Document>> streamAll(@NonNull PersistenceCollection collection) {
        return this.getRaw().streamAll(collection).map(this.entityToDocumentMapper(collection));
    }

    @Override
    public long count(@NonNull PersistenceCollection collection) {
        return this.getRaw().count(collection);
    }

    @Override
    public boolean exists(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {
        return this.getRaw().exists(collection, path);
    }

    @Override
    public boolean write(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull Document document) {
        this.updateIndex(collection, path, document);
        return this.getRaw().write(collection, path, document.saveToString());
    }

    @Override
    public long write(@NonNull PersistenceCollection collection, @NonNull Map<PersistencePath, Document> entities) {

        if (entities.isEmpty()) {
            return 0;
        }

        Map<PersistencePath, String> rawMap = new LinkedHashMap<>();

        for (Map.Entry<PersistencePath, Document> entry : entities.entrySet()) {
            this.updateIndex(collection, entry.getKey(), entry.getValue());
            rawMap.put(entry.getKey(), entry.getValue().saveToString());
        }

        return this.getRaw().write(collection, rawMap);
    }

    @Override
    public boolean delete(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {
        return this.getRaw().delete(collection, path);
    }

    @Override
    public long delete(@NonNull PersistenceCollection collection, @NonNull Collection<PersistencePath> paths) {
        return this.getRaw().delete(collection, paths);
    }

    @Override
    public boolean deleteAll(@NonNull PersistenceCollection collection) {
        return this.getRaw().deleteAll(collection);
    }

    @Override
    public long deleteAll() {
        return this.getRaw().deleteAll();
    }

    public Document createDocument(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {
        this.getRaw().checkCollectionRegistered(collection);
        Document config = ConfigManager.create(Document.class);
        config.withConfigurer(this.configurerProvider.get());
        config.getConfigurer().setRegistry(this.transformerRegistry);
        config.setSaver(document -> this.write(document.getCollection(), document.getPath(), document));
        config.setCollection(collection);
        config.setPersistence(this);
        config.setPath(path);
        return config;
    }

    private Function<PersistenceEntity<String>, PersistenceEntity<Document>> entityToDocumentMapper(PersistenceCollection collection) {
        return entity -> {
            Document document = this.createDocument(collection, entity.getPath());
            document.load(entity.getValue());
            return entity.into(document);
        };
    }

    protected Object extractValue(Map<?, ?> document, List<String> pathParts) {
        for (String part : pathParts) {
            Object element = document.get(part);
            if (element instanceof Map) {
                document = (Map<?, ?>) element;
                continue;
            }
            return element;
        }
        return null;
    }

    private boolean compare(Object object1, Object object2) {

        if ((object1 == null) && (object2 == null)) {
            return true;
        }

        if ((object1 == null) || (object2 == null)) {
            return false;
        }

        if ((object1 instanceof Number) && (object2 instanceof Number)) {
            return ((Number) object1).doubleValue() == ((Number) object2).doubleValue();
        }

        if (object1.getClass() == object2.getClass()) {
            return object1.equals(object2);
        }

        if (((object1 instanceof String) && (object2 instanceof Number)) || ((object1 instanceof Number) && (object2 instanceof String))) {
            try {
                return new BigDecimal(String.valueOf(object1)).compareTo(new BigDecimal(String.valueOf(object2))) == 0;
            } catch (NumberFormatException ignored) {
                return false;
            }
        }

        throw new IllegalArgumentException("cannot compare " + object1 + " [" + object1.getClass() + "] to " + object2 + " [" + object2.getClass() + "]");
    }
}
