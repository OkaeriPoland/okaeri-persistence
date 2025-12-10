package eu.okaeri.persistence.redis;

import eu.okaeri.configs.configurer.Configurer;
import eu.okaeri.configs.serdes.OkaeriSerdes;
import eu.okaeri.persistence.*;
import eu.okaeri.persistence.document.Document;
import eu.okaeri.persistence.document.DocumentSerializer;
import eu.okaeri.persistence.document.DocumentSerializerConfig;
import eu.okaeri.persistence.document.PersistenceBuilder;
import io.lettuce.core.*;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import eu.okaeri.persistence.util.ConnectionRetry;
import lombok.Getter;
import lombok.NonNull;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Redis persistence backend using Lettuce client.
 * Stores documents as JSON in Redis hashes.
 * <p>
 * Note: Redis doesn't support native filtering or indexing.
 * Use {@link eu.okaeri.persistence.document.DocumentPersistence} wrapper
 * for filtering/update support with in-memory fallback.
 */
public class RedisPersistence implements Persistence {

    private static final Logger LOGGER = Logger.getLogger(RedisPersistence.class.getSimpleName());

    private final @Getter PersistencePath basePath;
    private @Getter StatefulRedisConnection<String, String> connection;
    private @Getter RedisClient client;

    private final @Getter DocumentSerializer serializer;
    private final Map<String, PersistenceCollection> knownCollections = new ConcurrentHashMap<>();

    public RedisPersistence(@NonNull PersistencePath basePath, @NonNull RedisClient client,
                            @NonNull Configurer configurer, @NonNull OkaeriSerdes... serdes) {
        this.basePath = basePath;
        this.serializer = new DocumentSerializer(configurer, serdes);
        this.connect(client);
    }

    public RedisPersistence(@NonNull RedisClient client, @NonNull Configurer configurer,
                            @NonNull OkaeriSerdes... serdes) {
        this(PersistencePath.of(""), client, configurer, serdes);
    }

    public RedisPersistence(@NonNull PersistencePath basePath, @NonNull RedisClient client,
                            @NonNull DocumentSerializerConfig serializerConfig) {
        this.basePath = basePath;
        this.serializer = new DocumentSerializer(serializerConfig);
        this.connect(client);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends PersistenceBuilder<Builder, RedisPersistence> {
        private RedisClient client;

        public Builder client(@NonNull RedisClient client) {
            this.client = client;
            return this;
        }

        @Override
        public RedisPersistence build() {
            if (this.client == null) {
                throw new IllegalStateException("client is required");
            }

            DocumentSerializerConfig serializerConfig = this.buildSerializerConfig();
            PersistencePath path = this.resolveBasePath();

            return new RedisPersistence(path, this.client, serializerConfig);
        }
    }

    private void connect(RedisClient client) {
        this.client = client;
        this.connection = this.createConnection(StringCodec.UTF8);
    }

    public <K, V> StatefulRedisConnection<K, V> createConnection(RedisCodec<K, V> codec) {
        if (this.client == null) {
            throw new RuntimeException("Cannot create connection - client not initialized");
        }
        return ConnectionRetry.of(this.basePath.getValue())
            .connector(() -> this.client.connect(codec))
            .connect();
    }

    public <K, V> StatefulRedisPubSubConnection<K, V> createPubSubConnection(RedisCodec<K, V> codec) {
        if (this.client == null) {
            throw new RuntimeException("Cannot create connection - client not initialized");
        }
        return ConnectionRetry.of(this.basePath.getValue() + " pubsub")
            .connector(() -> this.client.connectPubSub(codec))
            .connect();
    }

    // ==================== COLLECTION MANAGEMENT ====================

    @Override
    public void registerCollection(@NonNull PersistenceCollection collection) {
        this.knownCollections.put(collection.getValue(), collection);
    }

    private String hashKey(@NonNull PersistenceCollection collection) {
        return this.basePath.sub(collection).getValue();
    }

    private void checkCollectionRegistered(@NonNull PersistenceCollection collection) {
        if (!this.knownCollections.containsKey(collection.getValue())) {
            throw new IllegalArgumentException("Collection not registered: " + collection.getValue());
        }
    }

    // ==================== READ OPERATIONS ====================

    @Override
    public boolean exists(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {
        this.checkCollectionRegistered(collection);
        return this.connection.sync().hexists(this.hashKey(collection), path.getValue());
    }

    @Override
    public long count(@NonNull PersistenceCollection collection) {
        this.checkCollectionRegistered(collection);
        return this.connection.sync().hlen(this.hashKey(collection));
    }

    @Override
    public Optional<Document> read(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {
        this.checkCollectionRegistered(collection);
        String json = this.connection.sync().hget(this.hashKey(collection), path.getValue());
        if (json == null) {
            return Optional.empty();
        }
        return Optional.of(this.serializer.deserialize(collection, path, json));
    }

    @Override
    public Map<PersistencePath, Document> read(@NonNull PersistenceCollection collection, @NonNull Collection<PersistencePath> paths) {
        this.checkCollectionRegistered(collection);
        if (paths.isEmpty()) {
            return Collections.emptyMap();
        }

        RedisCommands<String, String> sync = this.connection.sync();
        String hKey = this.hashKey(collection);

        // Use Lua script for atomic batch read
        String script = "local collection = ARGV[1]\n" +
            "local result = {}\n" +
            "for _, key in ipairs(KEYS) do\n" +
            "    result[#result+1] = key\n" +
            "    result[#result+1] = redis.call('hget', collection, key)\n" +
            "end\n" +
            "return result\n";

        String[] keys = paths.stream().map(PersistencePath::getValue).toArray(String[]::new);
        List<String> result = sync.eval(script, ScriptOutputType.MULTI, keys, hKey);

        Map<PersistencePath, Document> map = new LinkedHashMap<>();
        for (int i = 0; i < result.size(); i += 2) {
            String key = result.get(i);
            String json = result.get(i + 1);
            if (json != null) {
                PersistencePath path = PersistencePath.of(key);
                map.put(path, this.serializer.deserialize(collection, path, json));
            }
        }
        return map;
    }

    @Override
    public Map<PersistencePath, Document> readAll(@NonNull PersistenceCollection collection) {
        this.checkCollectionRegistered(collection);
        Map<String, String> all = this.connection.sync().hgetall(this.hashKey(collection));
        return all.entrySet().stream()
            .collect(Collectors.toMap(
                e -> PersistencePath.of(e.getKey()),
                e -> this.serializer.deserialize(collection, PersistencePath.of(e.getKey()), e.getValue())
            ));
    }

    // ==================== STREAMING ====================

    @Override
    public Stream<PersistenceEntity<Document>> streamAll(@NonNull PersistenceCollection collection) {
        this.checkCollectionRegistered(collection);
        RedisCommands<String, String> sync = this.connection.sync();
        String hKey = this.hashKey(collection);

        ScanIterator<KeyValue<String, String>> iterator = ScanIterator.hscan(sync, hKey, ScanArgs.Builder.limit(100));
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(new Iterator<PersistenceEntity<Document>>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public PersistenceEntity<Document> next() {
                KeyValue<String, String> kv = iterator.next();
                PersistencePath path = PersistencePath.of(kv.getKey());
                Document doc = RedisPersistence.this.serializer.deserialize(collection, path, kv.getValue());
                return new PersistenceEntity<>(path, doc);
            }
        }, Spliterator.ORDERED), false);
    }

    // ==================== WRITE OPERATIONS ====================

    @Override
    public boolean write(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull Document document) {
        this.checkCollectionRegistered(collection);
        this.serializer.setupDocument(document, collection, path);
        String json = this.serializer.serialize(document);
        this.connection.sync().hset(this.hashKey(collection), path.getValue(), json);
        return true;
    }

    @Override
    public long write(@NonNull PersistenceCollection collection, @NonNull Map<PersistencePath, Document> documents) {
        if (documents.isEmpty()) {
            return 0;
        }
        this.checkCollectionRegistered(collection);

        Map<String, String> map = new LinkedHashMap<>();
        for (Map.Entry<PersistencePath, Document> entry : documents.entrySet()) {
            this.serializer.setupDocument(entry.getValue(), collection, entry.getKey());
            map.put(entry.getKey().getValue(), this.serializer.serialize(entry.getValue()));
        }

        this.connection.sync().hset(this.hashKey(collection), map);
        return documents.size();
    }

    // ==================== DELETE OPERATIONS ====================

    @Override
    public boolean delete(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {
        this.checkCollectionRegistered(collection);
        return this.connection.sync().hdel(this.hashKey(collection), path.getValue()) > 0;
    }

    @Override
    public long delete(@NonNull PersistenceCollection collection, @NonNull Collection<PersistencePath> paths) {
        this.checkCollectionRegistered(collection);
        if (paths.isEmpty()) {
            return 0;
        }
        String[] keys = paths.stream().map(PersistencePath::getValue).toArray(String[]::new);
        return this.connection.sync().hdel(this.hashKey(collection), keys);
    }

    @Override
    public boolean deleteAll(@NonNull PersistenceCollection collection) {
        this.checkCollectionRegistered(collection);
        this.cleanupLegacyIndexes(collection);
        return this.connection.sync().del(this.hashKey(collection)) > 0;
    }

    @Override
    public long deleteAll() {
        this.knownCollections.values().forEach(this::cleanupLegacyIndexes);
        String[] keys = this.knownCollections.values().stream()
            .map(this::hashKey)
            .toArray(String[]::new);
        if (keys.length == 0) {
            return 0;
        }
        return this.connection.sync().del(keys);
    }

    /**
     * Cleanup legacy emulated index keys from pre-v3 persistence.
     * Legacy keys follow pattern: basePath:collection:index:*
     */
    private void cleanupLegacyIndexes(@NonNull PersistenceCollection collection) {
        RedisCommands<String, String> sync = this.connection.sync();
        String indexPattern = this.basePath.sub(collection).sub("index").getValue() + ":*";

        // Use SCAN to find all legacy index keys
        ScanIterator<String> iterator = ScanIterator.scan(sync, ScanArgs.Builder.matches(indexPattern).limit(1000));
        List<String> keysToDelete = new ArrayList<>();

        while (iterator.hasNext()) {
            keysToDelete.add(iterator.next());
            // Delete in batches to avoid memory issues
            if (keysToDelete.size() >= 1000) {
                sync.del(keysToDelete.toArray(new String[0]));
                keysToDelete.clear();
            }
        }

        // Delete remaining keys
        if (!keysToDelete.isEmpty()) {
            sync.del(keysToDelete.toArray(new String[0]));
        }
    }

    @Override
    public void close() throws IOException {
        this.connection.close();
        this.client.shutdown();
    }
}
