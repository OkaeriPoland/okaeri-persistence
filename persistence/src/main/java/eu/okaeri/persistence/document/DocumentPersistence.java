package eu.okaeri.persistence.document;

import eu.okaeri.configs.ConfigManager;
import eu.okaeri.configs.configurer.Configurer;
import eu.okaeri.configs.serdes.OkaeriSerdesPack;
import eu.okaeri.configs.serdes.TransformerRegistry;
import eu.okaeri.configs.serdes.commons.SerdesCommons;
import eu.okaeri.configs.serdes.standard.StandardSerdes;
import eu.okaeri.persistence.index.IndexProperty;
import eu.okaeri.persistence.raw.RawPersistence;
import eu.okaeri.persistence.Persistence;
import eu.okaeri.persistence.PersistenceCollection;
import eu.okaeri.persistence.PersistenceEntity;
import eu.okaeri.persistence.PersistencePath;
import lombok.Getter;

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

    public DocumentPersistence(RawPersistence rawPersistence, ConfigurerProvider configurerProvider, OkaeriSerdesPack... serdesPacks) {
        this.raw = rawPersistence;
        this.configurerProvider = configurerProvider;
        this.serdesPacks = serdesPacks;
        // shared transform registry for faster transformations
        this.transformerRegistry = new TransformerRegistry();
        Stream.concat(Stream.of(new StandardSerdes(), new SerdesCommons()), Stream.of(this.serdesPacks)).forEach(pack -> pack.register(this.transformerRegistry));
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
    public void registerCollection(PersistenceCollection collection) {

        this.getRaw().registerCollection(collection);

        if (!this.getRaw().isNativeIndexes()) {
            return;
        }

        Set<IndexProperty> indexes = this.getRaw().getKnownIndexes().getOrDefault(collection, new HashSet<>());
        Set<PersistencePath> withMissingIndexes = this.findMissingIndexes(collection, indexes);

        if (withMissingIndexes.isEmpty()) {
            return;
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
    }

    @Override
    public boolean updateIndex(PersistenceCollection collection, IndexProperty property, PersistencePath path, String identity) {
        return this.getRaw().isNativeIndexes() && this.getRaw().updateIndex(collection, property, path, identity);
    }

    @Override
    public boolean updateIndex(PersistenceCollection collection, PersistencePath path, Document document) {

        if (!this.getRaw().isNativeIndexes()) {
            return false;
        }

        Set<IndexProperty> collectionIndexes = this.getRaw().getKnownIndexes().get(collection);
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
            boolean changed = this.updateIndex(collection, index, path, (value == null) ? null : String.valueOf(value));
            if (changed) changes++;
        }

        return changes > 0;
    }

    @Override
    public boolean updateIndex(PersistenceCollection collection, PersistencePath path) {

        if (!this.getRaw().isNativeIndexes()) {
            return false;
        }

        Optional<Document> document = this.read(collection, path);
        return document.map(value -> this.updateIndex(collection, path, value)).isPresent();
    }

    @Override
    public boolean dropIndex(PersistenceCollection collection, IndexProperty property, PersistencePath path) {
        return this.getRaw().isNativeIndexes() && this.getRaw().dropIndex(collection, property, path);
    }

    @Override
    public boolean dropIndex(PersistenceCollection collection, PersistencePath path) {
        return this.getRaw().isNativeIndexes() && this.getRaw().dropIndex(collection, path);
    }

    @Override
    public boolean dropIndex(PersistenceCollection collection, IndexProperty property) {
        return this.getRaw().isNativeIndexes() && this.getRaw().dropIndex(collection, property);
    }

    @Override
    public Set<PersistencePath> findMissingIndexes(PersistenceCollection collection, Set<IndexProperty> indexProperties) {
        return this.getRaw().findMissingIndexes(collection, indexProperties);
    }

    @Override
    public Document readOrEmpty(PersistenceCollection collection, PersistencePath path) {
        return this.read(collection, path).orElse(this.createDocument(collection, path));
    }

    @Override
    public Optional<Document> read(PersistenceCollection collection, PersistencePath path) {

        Optional<String> data = this.getRaw().read(collection, path);
        if (!data.isPresent()) {
            return Optional.empty();
        }

        Document document = this.createDocument(collection, path);
        return Optional.of((Document) document.load(data.get()));
    }

    @Override
    public Map<PersistencePath, Document> readOrEmpty(PersistenceCollection collection, Collection<PersistencePath> paths) {

        Map<PersistencePath, Document> map = new LinkedHashMap<>();
        Map<PersistencePath, Document> data = this.read(collection, paths);

        for (PersistencePath path : paths) {
            map.put(path, data.getOrDefault(path, this.createDocument(collection, path)));
        }

        return map;
    }

    @Override
    public Map<PersistencePath, Document> read(PersistenceCollection collection, Collection<PersistencePath> paths) {
        return this.getRaw().read(collection, paths).entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> {
                    Document document = this.createDocument(collection, entry.getKey());
                    return (Document) document.load(entry.getValue());
                }));
    }

    @Override
    public Map<PersistencePath, Document> readAll(PersistenceCollection collection) {
        return this.getRaw().readAll(collection).entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> {
                    Document document = this.createDocument(collection, entry.getKey());
                    return (Document) document.load(entry.getValue());
                }));
    }

    @Override
    public Stream<PersistenceEntity<Document>> readByProperty(PersistenceCollection collection, PersistencePath property, Object propertyValue) {

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
    public Stream<PersistenceEntity<Document>> streamAll(PersistenceCollection collection) {
        return this.getRaw().streamAll(collection).map(this.entityToDocumentMapper(collection));
    }

    @Override
    public long count(PersistenceCollection collection) {
        return this.getRaw().count(collection);
    }

    @Override
    public boolean exists(PersistenceCollection collection, PersistencePath path) {
        return this.getRaw().exists(collection, path);
    }

    @Override
    public boolean write(PersistenceCollection collection, PersistencePath path, Document document) {
        this.updateIndex(collection, path, document);
        return this.getRaw().write(collection, path, document.saveToString());
    }

    @Override
    public boolean delete(PersistenceCollection collection, PersistencePath path) {
        return this.getRaw().delete(collection, path);
    }

    @Override
    public long delete(PersistenceCollection collection, Collection<PersistencePath> paths) {
        return this.getRaw().delete(collection, paths);
    }

    @Override
    public boolean deleteAll(PersistenceCollection collection) {
        return this.getRaw().deleteAll(collection);
    }

    @Override
    public long deleteAll() {
        return this.getRaw().deleteAll();
    }

    public Document createDocument(PersistenceCollection collection, PersistencePath path) {
        this.getRaw().checkCollectionRegistered(collection);
        Document config = ConfigManager.create(Document.class);
        config.withConfigurer(this.configurerProvider.get());
        config.getConfigurer().setRegistry(this.transformerRegistry);
        config.setSaver(document -> this.write(collection, document.getPath(), document));
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

    private Object extractValue(Map<?, ?> document, List<String> pathParts) {
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
