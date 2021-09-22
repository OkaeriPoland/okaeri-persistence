package eu.okaeri.persistence.document;

import eu.okaeri.configs.ConfigManager;
import eu.okaeri.configs.configurer.InMemoryConfigurer;
import eu.okaeri.configs.serdes.OkaeriSerdesPack;
import eu.okaeri.persistence.PersistenceCollection;
import eu.okaeri.persistence.PersistenceEntity;
import eu.okaeri.persistence.PersistencePath;
import eu.okaeri.persistence.document.index.InMemoryIndex;
import eu.okaeri.persistence.document.index.IndexProperty;
import eu.okaeri.persistence.raw.RawPersistence;
import lombok.Getter;
import lombok.NonNull;
import lombok.SneakyThrows;

import java.io.IOException;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class InMemoryDocumentPersistence extends DocumentPersistence {

    @Getter private final PersistencePath basePath = PersistencePath.of("memory");
    private final Map<String, Map<String, InMemoryIndex>> indexMap = new ConcurrentHashMap<>();
    private final Map<String, Map<PersistencePath, Document>> documents = new ConcurrentHashMap<>();

    public InMemoryDocumentPersistence(@NonNull OkaeriSerdesPack... serdesPacks) {
        super(new RawPersistence(PersistencePath.of("memory"), true, true, false, true) {
            @Override
            public long count(PersistenceCollection collection) {
                return 0;
            }

            @Override
            public boolean exists(PersistenceCollection collection, PersistencePath path) {
                return false;
            }

            @Override
            public Optional<String> read(PersistenceCollection collection, PersistencePath path) {
                return Optional.empty();
            }

            @Override
            public Map<PersistencePath, String> read(PersistenceCollection collection, Collection<PersistencePath> paths) {
                return null;
            }

            @Override
            public Map<PersistencePath, String> readAll(PersistenceCollection collection) {
                return null;
            }

            @Override
            public Stream<PersistenceEntity<String>> streamAll(PersistenceCollection collection) {
                return null;
            }

            @Override
            public boolean write(PersistenceCollection collection, PersistencePath path, String entity) {
                return false;
            }

            @Override
            public boolean delete(PersistenceCollection collection, PersistencePath path) {
                return false;
            }

            @Override
            public long delete(PersistenceCollection collection, Collection<PersistencePath> paths) {
                return 0;
            }

            @Override
            public boolean deleteAll(PersistenceCollection collection) {
                return false;
            }

            @Override
            public long deleteAll() {
                return 0;
            }

            @Override
            public void close() throws IOException {
            }
        }, InMemoryConfigurer::new, serdesPacks);
    }

    @Override
    public void setAutoFlush(boolean state) {
    }

    @Override
    public void flush() {
    }

    @Override
    public void registerCollection(@NonNull PersistenceCollection collection) {

        this.getRaw().getKnownCollections().put(collection.getValue(), collection);
        this.getRaw().getKnownIndexes().put(collection.getValue(), collection.getIndexes());
        this.documents.put(collection.getValue(), new ConcurrentHashMap<>());

        Map<String, InMemoryIndex> indexes = this.indexMap.computeIfAbsent(collection.getValue(), col -> new ConcurrentHashMap<>());
        collection.getIndexes().forEach(index -> indexes.put(index.getValue(), ConfigManager.create(InMemoryIndex.class)));
    }

    @Override
    public boolean updateIndex(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull IndexProperty property, String identity) {

        // get index
        InMemoryIndex flatIndex = this.indexMap.get(collection.getValue()).get(property.getValue());
        if (flatIndex == null) throw new IllegalArgumentException("non-indexed property used: " + property);

        // get current value by key and remove from mapping
        String currentValue = flatIndex.getKeyToValue().remove(path.getValue());

        // remove from old set value_to_keys
        if (currentValue != null) {
            flatIndex.getValueToKeys().get(currentValue).remove(path.getValue());
        }

        // add to new value_to_keys
        Set<String> keys = flatIndex.getValueToKeys().computeIfAbsent(identity, s -> new HashSet<>());
        boolean changed = keys.add(path.getValue());

        // update key to value
        return (flatIndex.getKeyToValue().put(path.getValue(), identity) != null) || changed;
    }

    @Override
    public boolean dropIndex(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull IndexProperty property) {

        // get index
        InMemoryIndex flatIndex = this.indexMap.get(collection.getValue()).get(property.getValue());
        if (flatIndex == null) throw new IllegalArgumentException("non-indexed property used: " + property);

        // get current value by key and remove from mapping
        String currentValue = flatIndex.getKeyToValue().remove(path.getValue());

        // delete from value to set
        return (currentValue != null) && flatIndex.getValueToKeys().get(currentValue).remove(path.getValue());
    }

    @Override
    public boolean dropIndex(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {
        return this.getRaw().getKnownIndexes().getOrDefault(collection.getValue(), Collections.emptySet()).stream()
                .map(index -> this.dropIndex(collection, path, index))
                .anyMatch(Predicate.isEqual(true));
    }

    @Override
    @SneakyThrows
    public boolean dropIndex(@NonNull PersistenceCollection collection, @NonNull IndexProperty property) {

        // remove from list and get index
        InMemoryIndex flatIndex = this.indexMap.get(collection.getValue()).remove(property.getValue());

        // delete index file
        return (flatIndex != null) && Files.deleteIfExists(flatIndex.getBindFile());
    }

    @Override
    @SneakyThrows
    public Set<PersistencePath> findMissingIndexes(@NonNull PersistenceCollection collection, @NonNull Set<IndexProperty> indexProperties) {

        Map<String, InMemoryIndex> collectionIndexes = this.indexMap.get(collection.getValue());
        if (collectionIndexes.isEmpty()) {
            return Collections.emptySet();
        }

        return this.streamAll(collection)
                .map(PersistenceEntity::getValue)
                .map(entity -> collectionIndexes.values().stream()
                        .allMatch(flatIndex -> flatIndex.getKeyToValue().containsKey(entity.getPath().getValue()))
                        ? null : entity.getPath())
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
    }

    @Override
    public Document readOrEmpty(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {
        return this.read(collection, path).orElse(this.createDocument(collection, path));
    }

    @Override
    public Optional<Document> read(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {
        this.getRaw().checkCollectionRegistered(collection);
        return Optional.ofNullable(this.documents.get(collection.getValue()).get(path));
    }

    @Override
    public Map<PersistencePath, Document> readOrEmpty(@NonNull PersistenceCollection collection, @NonNull Collection<PersistencePath> paths) {

        this.getRaw().checkCollectionRegistered(collection);
        Map<PersistencePath, Document> map = new LinkedHashMap<>();
        Map<PersistencePath, Document> data = this.read(collection, paths);

        for (PersistencePath path : paths) {
            map.put(path, data.getOrDefault(path, this.createDocument(collection, path)));
        }

        return map;
    }

    @Override
    public Map<PersistencePath, Document> read(@NonNull PersistenceCollection collection, @NonNull Collection<PersistencePath> paths) {
        this.getRaw().checkCollectionRegistered(collection);
        return paths.stream()
                .map(path -> this.documents.get(collection.getValue()).get(path))
                .filter(Objects::nonNull)
                .collect(Collectors.toMap(Document::getPath, Function.identity()));
    }

    @Override
    public Map<PersistencePath, Document> readAll(@NonNull PersistenceCollection collection) {
        return new HashMap<>(this.documents.get(collection.getValue()));
    }

    @Override
    public Stream<PersistenceEntity<Document>> readByProperty(@NonNull PersistenceCollection collection, @NonNull PersistencePath property, Object propertyValue) {

        InMemoryIndex flatIndex = this.indexMap.get(collection.getValue()).get(property.getValue());
        if (flatIndex == null) return this.streamAll(collection);

        Set<String> keys = flatIndex.getValueToKeys().get(String.valueOf(propertyValue));
        if ((keys == null) || keys.isEmpty()) {
            return Stream.of();
        }

        return new ArrayList<>(keys).stream()
                .map(key -> {
                    PersistencePath path = PersistencePath.of(key);
                    return this.read(collection, path)
                            .map(data -> new PersistenceEntity<>(path, data))
                            .orElseGet(() -> {
                                this.dropIndex(collection, path);
                                return null;
                            });
                })
                .filter(Objects::nonNull);
    }

    @Override
    public Stream<PersistenceEntity<Document>> streamAll(@NonNull PersistenceCollection collection) {
        this.getRaw().checkCollectionRegistered(collection);
        Collection<Document> docList = this.documents.get(collection.getValue()).values();
        return docList.stream().map(document -> new PersistenceEntity<>(document.getPath(), document));
    }

    @Override
    public long count(@NonNull PersistenceCollection collection) {
        this.getRaw().checkCollectionRegistered(collection);
        return this.documents.get(collection.getValue()).size();
    }

    @Override
    public boolean exists(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {
        this.getRaw().checkCollectionRegistered(collection);
        return this.documents.get(collection.getValue()).containsKey(path);
    }

    @Override
    public boolean write(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull Document document) {
        this.getRaw().checkCollectionRegistered(collection);
        this.updateIndex(collection, path, document);
        return this.documents.get(collection.getValue()).put(path, document) != null;
    }

    @Override
    public long write(@NonNull PersistenceCollection collection, @NonNull Map<PersistencePath, Document> entities) {
        return entities.entrySet().stream()
                .map(entity -> this.write(collection, entity.getKey(), entity.getValue()))
                .filter(Predicate.isEqual(true))
                .count();
    }

    @Override
    public boolean delete(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {
        this.getRaw().checkCollectionRegistered(collection);
        return this.documents.get(collection.getValue()).remove(path) != null;
    }

    @Override
    public long delete(@NonNull PersistenceCollection collection, @NonNull Collection<PersistencePath> paths) {
        return paths.stream().map(path -> this.delete(collection, path)).filter(Predicate.isEqual(true)).count();
    }

    @Override
    public boolean deleteAll(@NonNull PersistenceCollection collection) {
        this.getRaw().checkCollectionRegistered(collection);
        Map<PersistencePath, Document> data = this.documents.get(collection.getValue());
        boolean changed = !data.isEmpty();
        data.clear();
        return changed;
    }

    @Override
    public long deleteAll() {
        return this.documents.values().stream()
                .peek(Map::clear)
                .count();
    }
}
