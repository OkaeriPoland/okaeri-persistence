package eu.okaeri.persistence.redis;

import eu.okaeri.persistence.PersistenceCollection;
import eu.okaeri.persistence.PersistenceEntity;
import eu.okaeri.persistence.PersistencePath;
import eu.okaeri.persistence.document.index.IndexProperty;
import eu.okaeri.persistence.raw.PersistenceIndexMode;
import eu.okaeri.persistence.raw.PersistencePropertyMode;
import eu.okaeri.persistence.raw.RawPersistence;
import io.lettuce.core.*;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import lombok.Getter;
import lombok.NonNull;
import lombok.SneakyThrows;

import java.io.IOException;
import java.util.*;
import java.util.function.Predicate;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class RedisPersistence extends RawPersistence {

    private static final Logger LOGGER = Logger.getLogger(RedisPersistence.class.getSimpleName());
    @Getter private StatefulRedisConnection<String, String> connection;
    @Getter private RedisClient client;

    public RedisPersistence(@NonNull PersistencePath basePath, @NonNull RedisClient client) {
        super(basePath, PersistencePropertyMode.NATIVE, PersistenceIndexMode.EMULATED);
        this.connect(client);
    }

    public RedisPersistence(@NonNull RedisClient client) {
        this(PersistencePath.of(""), client);
    }

    private static <T> List<List<T>> partition(Collection<T> members, int maxSize) {

        List<List<T>> res = new ArrayList<>();
        List<T> internal = new ArrayList<>();

        for (T member : members) {
            internal.add(member);
            if (internal.size() == maxSize) {
                res.add(internal);
                internal = new ArrayList<>();
            }
        }

        if (!internal.isEmpty()) {
            res.add(internal);
        }

        return res;
    }

    @SneakyThrows
    private void connect(RedisClient client) {
        this.client = client;
        this.connection = this.createConnection(StringCodec.UTF8);
    }

    @SneakyThrows
    public <K, V> StatefulRedisConnection<K, V> createConnection(RedisCodec<K, V> codec) {
        if (this.client == null) {
            throw new RuntimeException("Cannot create connection! Make sure connect(RedisClient) is called before creating additional connections.");
        }
        StatefulRedisConnection<K, V> localConnection = null;
        do {
            try {
                localConnection = this.client.connect(codec);
            } catch (Exception exception) {
                if (exception.getCause() != null) {
                    LOGGER.severe("[" + this.getBasePath().getValue() + "] Cannot connect with redis (waiting 30s): " + exception.getMessage() + " caused by " + exception.getCause().getMessage());
                } else {
                    LOGGER.severe("[" + this.getBasePath().getValue() + "] Cannot connect with redis (waiting 30s): " + exception.getMessage());
                }
                Thread.sleep(30_000);
            }
        } while (localConnection == null);
        return localConnection;
    }

    @SneakyThrows
    public <K, V> StatefulRedisPubSubConnection<K, V> createPubSubConnection(RedisCodec<K, V> codec) {
        if (this.client == null) {
            throw new RuntimeException("Cannot create connection! Make sure connect(RedisClient) is called before creating additional connections.");
        }
        StatefulRedisPubSubConnection<K, V> localConnection = null;
        do {
            try {
                localConnection = this.client.connectPubSub(codec);
            } catch (Exception exception) {
                if (exception.getCause() != null) {
                    LOGGER.severe("[" + this.getBasePath().getValue() + "] Cannot connect with redis pubsub (waiting 30s): " + exception.getMessage() + " caused by " + exception.getCause().getMessage());
                } else {
                    LOGGER.severe("[" + this.getBasePath().getValue() + "] Cannot connect with redis pubsub (waiting 30s): " + exception.getMessage());
                }
                Thread.sleep(30_000);
            }
        } while (localConnection == null);
        return localConnection;
    }

    @Override
    public boolean updateIndex(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull IndexProperty property, String identity) {

        // remove from old set value_to_keys
        RedisCommands<String, String> sync = this.getConnection().sync();
        this.dropIndex(collection, path, property);
        String indexSet = this.toIndexValueToKeys(collection, property, identity).getValue();

        // register new value
        String valuesSet = this.toValuesSet(collection, property).getValue();
        sync.sadd(valuesSet, identity);

        // add to new value_to_keys
        sync.sadd(indexSet, path.getValue());

        // update key_to_value
        String keyToValue = this.toIndexKeyToValue(collection, property).getValue();
        return sync.hset(keyToValue, path.getValue(), identity);
    }

    @Override
    public boolean dropIndex(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull IndexProperty property) {

        // get current value by key
        String keyToValue = this.toIndexKeyToValue(collection, property).getValue();
        RedisCommands<String, String> sync = this.getConnection().sync();
        String currentValue = sync.hget(keyToValue, path.getValue());

        // delete old value mapping
        if (currentValue == null) return false;
        sync.hdel(keyToValue, path.getValue());

        // delete from values set
        String valuesSet = this.toValuesSet(collection, property).getValue();
        sync.srem(valuesSet, currentValue);

        // delete from value to set
        PersistencePath indexSet = this.toIndexValueToKeys(collection, property, currentValue);
        return sync.srem(indexSet.getValue(), path.getValue()) > 0;
    }

    @Override
    public boolean dropIndex(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {
        return this.getKnownIndexes().getOrDefault(collection.getValue(), Collections.emptySet()).stream()
            .map(index -> this.dropIndex(collection, path, index))
            .anyMatch(Predicate.isEqual(true));
    }

    @Override
    public boolean dropIndex(@NonNull PersistenceCollection collection, @NonNull IndexProperty property) {

        RedisCommands<String, String> sync = this.getConnection().sync();
        long changes = 0;

        // delete key to value mappings
        String keyToValue = this.toIndexKeyToValue(collection, property).getValue();
        changes += sync.del(keyToValue);

        // gather all used values and delete set
        String valuesSet = this.toValuesSet(collection, property).getValue();
        Set<String> propertyValues = sync.smembers(valuesSet);
        changes += sync.del(valuesSet);

        // delete all value to keys mappings
        if (!propertyValues.isEmpty()) {
            changes += sync.del(propertyValues.stream()
                .map(value -> this.toIndexValueToKeys(collection, property, value))
                .map(PersistencePath::getValue)
                .toArray(String[]::new));
        }

        return changes > 0;
    }

    @Override
    public Set<PersistencePath> findMissingIndexes(@NonNull PersistenceCollection collection, @NonNull Set<IndexProperty> indexProperties) {

        String[] args = indexProperties.stream()
            .map(index -> this.toIndexKeyToValue(collection, index))
            .map(PersistencePath::getValue)
            .toArray(String[]::new);

        String script = "local collection = ARGV[1]\n" +
            "local allKeys = redis.call('hkeys', collection)\n" +
            "local indexes = KEYS\n" +
            "local result = {}\n" +
            "\n" +
            "for _, key in ipairs(allKeys) do\n" +
            "\n" +
            "    local present = true\n" +
            "\n" +
            "    for _, index in ipairs(indexes) do\n" +
            "        if (redis.call('hexists', index, key) == 0) then\n" +
            "            present = false\n" +
            "            break\n" +
            "        end\n" +
            "    end\n" +
            "\n" +
            "    if not present then\n" +
            "        result[#result+1] = key\n" +
            "    end\n" +
            "end\n" +
            "\n" +
            "return result\n";

        String hashKey = this.getBasePath().sub(collection).getValue();
        List<String> out = this.getConnection().sync().eval(script, ScriptOutputType.MULTI, args, hashKey);

        return out.stream()
            .map(PersistencePath::of)
            .collect(Collectors.toSet());
    }

    @Override
    public Stream<PersistenceEntity<String>> readByProperty(@NonNull PersistenceCollection collection, @NonNull PersistencePath property, Object propertyValue) {

        if (!this.canUseToString(propertyValue) || !this.isIndexed(collection, property)) {
            return this.streamAll(collection);
        }

        String hashKeyString = this.getBasePath().sub(collection).getValue();
        PersistencePath indexSet = this.toIndexValueToKeys(collection, property, String.valueOf(propertyValue));

        RedisCommands<String, String> sync = this.getConnection().sync();
        Set<String> members = sync.smembers(indexSet.getValue());

        if (members.isEmpty()) {
            return Stream.of();
        }

        int totalKeys = members.size();
        long step = totalKeys / 100;
        if (step < 50) step = 50;

        String script = sync.scriptLoad("local collection = ARGV[1]\n" +
            "local result = {}\n" +
            "\n" +
            "for _, key in ipairs(KEYS) do\n" +
            "    result[#result+1] = key\n" +
            "    result[#result+1] = redis.call('hget', collection, key)\n" +
            "end\n" +
            "\n" +
            "return result\n");

        return partition(members, Math.toIntExact(step)).stream()
            .flatMap(part -> {

                String[] keys = part.toArray(new String[part.size()]);
                List<String> result = sync.evalsha(script, ScriptOutputType.MULTI, keys, hashKeyString);
                List<PersistenceEntity<String>> out = new ArrayList<>();

                for (int i = 0; i < result.size(); i += 2) {
                    String key = result.get(i);
                    String value = result.get(i + 1);
                    if (value != null) {
                        out.add(new PersistenceEntity<>(PersistencePath.of(key), value));
                    }
                }

                return out.stream();
            });
    }

    @Override
    public Optional<String> read(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {
        this.checkCollectionRegistered(collection);
        String hKey = this.getBasePath().sub(collection).getValue();
        return Optional.ofNullable(this.getConnection().sync().hget(hKey, path.getValue()));
    }

    @Override
    public Map<PersistencePath, String> read(@NonNull PersistenceCollection collection, @NonNull Collection<PersistencePath> paths) {

        this.checkCollectionRegistered(collection);
        String hKey = this.getBasePath().sub(collection).getValue();
        RedisCommands<String, String> sync = this.getConnection().sync();
        Map<PersistencePath, String> map = new LinkedHashMap<>();

        String script = "local collection = ARGV[1]\n" +
            "local result = {}\n" +
            "\n" +
            "for _, key in ipairs(KEYS) do\n" +
            "    result[#result+1] = key\n" +
            "    result[#result+1] = redis.call('hget', collection, key)\n" +
            "end\n" +
            "\n" +
            "return result\n";

        String[] keys = paths.stream().map(PersistencePath::getValue).toArray(String[]::new);
        List<String> result = sync.eval(script, ScriptOutputType.MULTI, keys, hKey);

        for (int i = 0; i < result.size(); i += 2) {
            String key = result.get(i);
            String value = result.get(i + 1);
            if (value != null) {
                map.put(PersistencePath.of(key), value);
            }
        }

        return map;
    }

    @Override
    public Map<PersistencePath, String> readAll(@NonNull PersistenceCollection collection) {
        this.checkCollectionRegistered(collection);
        String hKey = this.getBasePath().sub(collection).getValue();
        return this.getConnection().sync().hgetall(hKey).entrySet().stream()
            .collect(Collectors.toMap(entry -> PersistencePath.of(entry.getKey()), Map.Entry::getValue));
    }

    @Override
    public Stream<PersistenceEntity<String>> streamAll(@NonNull PersistenceCollection collection) {
        // Delegate to stream() to ensure batched fetching instead of loading all at once
        // Redis is single-threaded, so fetching large hashes all at once can cause issues
        return this.stream(collection, 100);
    }

    @Override
    public Stream<PersistenceEntity<String>> stream(@NonNull PersistenceCollection collection, int batchSize) {

        this.checkCollectionRegistered(collection);
        RedisCommands<String, String> sync = this.getConnection().sync();
        String hKey = this.getBasePath().sub(collection).getValue();

        ScanIterator<KeyValue<String, String>> iterator = ScanIterator.hscan(sync, hKey, ScanArgs.Builder.limit(batchSize));
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(new Iterator<PersistenceEntity<String>>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public PersistenceEntity<String> next() {
                KeyValue<String, String> next = iterator.next();
                return new PersistenceEntity<>(PersistencePath.of(next.getKey()), next.getValue());
            }
        }, Spliterator.ORDERED), false);
    }

    @Override
    public long count(@NonNull PersistenceCollection collection) {
        this.checkCollectionRegistered(collection);
        String hKey = this.getBasePath().sub(collection).getValue();
        return this.getConnection().sync().hlen(hKey);
    }

    @Override
    public boolean exists(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {
        this.checkCollectionRegistered(collection);
        String hKey = this.getBasePath().sub(collection).getValue();
        return this.getConnection().sync().hexists(hKey, path.getValue());
    }

    @Override
    public boolean write(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull String raw) {
        this.checkCollectionRegistered(collection);
        String hKey = this.getBasePath().sub(collection).getValue();
        this.getConnection().sync().hset(hKey, path.getValue(), raw);
        return true;
    }

    @Override
    public boolean delete(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {

        this.checkCollectionRegistered(collection);
        Set<IndexProperty> collectionIndexes = this.getKnownIndexes().get(collection.getValue());

        if (collectionIndexes != null) {
            collectionIndexes.forEach(index -> this.dropIndex(collection, path));
        }

        String hKey = this.getBasePath().sub(collection).getValue();
        return this.getConnection().sync().hdel(hKey, path.getValue()) > 0;
    }

    @Override
    public long delete(@NonNull PersistenceCollection collection, @NonNull Collection<PersistencePath> paths) {

        this.checkCollectionRegistered(collection);
        Set<IndexProperty> collectionIndexes = this.getKnownIndexes().get(collection.getValue());

        if (collectionIndexes != null) {
            for (PersistencePath path : paths) {
                collectionIndexes.forEach(index -> this.dropIndex(collection, path));
            }
        }

        String hKey = this.getBasePath().sub(collection).getValue();
        String[] keysToDelete = paths.stream().map(PersistencePath::getValue).toArray(String[]::new);

        if (keysToDelete.length == 0) {
            return 0;
        }

        return this.getConnection().sync().hdel(hKey, keysToDelete);
    }

    @Override
    public boolean deleteAll(@NonNull PersistenceCollection collection) {

        this.checkCollectionRegistered(collection);
        Set<IndexProperty> collectionIndexes = this.getKnownIndexes().get(collection.getValue());

        if (collectionIndexes != null) {
            collectionIndexes.forEach(index -> this.dropIndex(collection, index));
        }

        String hKey = this.getBasePath().sub(collection).getValue();
        return this.getConnection().sync().del(hKey) > 0;
    }

    @Override
    public long deleteAll() {
        return this.getConnection().sync().del(this.getKnownCollections().keySet().toArray(new String[0]));
    }

    @Override
    public void close() throws IOException {
        this.getConnection().close();
        this.getClient().shutdown();
    }

    private PersistencePath toIndexValueToKeys(PersistenceCollection collection, PersistencePath property, String propertyValue) {
        return this.getBasePath().sub(collection).sub("index").sub(property).sub("value_to_keys").sub(propertyValue);
    }

    private PersistencePath toIndexKeyToValue(PersistenceCollection collection, PersistencePath property) {
        return this.getBasePath().sub(collection).sub("index").sub(property).sub("key_to_value");
    }

    private PersistencePath toValuesSet(PersistenceCollection collection, PersistencePath property) {
        return this.getBasePath().sub(collection).sub("index").sub(property).sub("values");
    }
}
