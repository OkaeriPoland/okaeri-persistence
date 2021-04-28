package eu.okaeri.platform.persistence.redis;

import eu.okaeri.platform.persistence.PersistenceCollection;
import eu.okaeri.platform.persistence.PersistenceEntity;
import eu.okaeri.platform.persistence.PersistencePath;
import eu.okaeri.platform.persistence.index.IndexProperty;
import eu.okaeri.platform.persistence.raw.RawPersistence;
import io.lettuce.core.*;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import lombok.Getter;
import lombok.SneakyThrows;

import java.util.*;
import java.util.function.Predicate;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class RedisPersistence extends RawPersistence {

    private static final Logger LOGGER = Logger.getLogger(RedisPersistence.class.getName());
    @Getter private StatefulRedisConnection<String, String> connection;

    public RedisPersistence(PersistencePath basePath, RedisClient client) {
        super(basePath, true, true, true, true);
        this.connect(client);
    }

    @SneakyThrows
    private void connect(RedisClient client) {
        do {
            try {
                this.connection = client.connect();
            } catch (Exception exception) {
                if (exception.getCause() != null) {
                    LOGGER.severe("[" + this.getBasePath().getValue() + "] Cannot connect with redis (waiting 30s): " + exception.getMessage() + " caused by " + exception.getCause().getMessage());
                } else {
                    LOGGER.severe("[" + this.getBasePath().getValue() + "] Cannot connect with redis (waiting 30s): " + exception.getMessage());
                }
                Thread.sleep(30_000);
            }
        } while (this.connection == null);
    }

    @Override
    public boolean updateIndex(PersistenceCollection collection, IndexProperty property, PersistencePath path, String identity) {

        // remove from old set value_to_keys
        RedisCommands<String, String> sync = this.connection.sync();
        this.dropIndex(collection, property, path);
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
    public boolean dropIndex(PersistenceCollection collection, IndexProperty property, PersistencePath path) {

        // get current value by key
        String keyToValue = this.toIndexKeyToValue(collection, property).getValue();
        RedisCommands<String, String> sync = this.connection.sync();
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
    public boolean dropIndex(PersistenceCollection collection, PersistencePath path) {
        return this.getKnownIndexes().getOrDefault(collection, Collections.emptySet()).stream()
                .map(index -> this.dropIndex(collection, index, path))
                .anyMatch(Predicate.isEqual(true));
    }

    @Override
    public boolean dropIndex(PersistenceCollection collection, IndexProperty property) {

        RedisCommands<String, String> sync = this.connection.sync();
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
    public Set<PersistencePath> findMissingIndexes(PersistenceCollection collection, Set<IndexProperty> indexProperties) {

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
        List<String> out = this.connection.sync().eval(script, ScriptOutputType.MULTI, args, hashKey);

        return out.stream()
                .map(PersistencePath::of)
                .collect(Collectors.toSet());
    }

    @Override
    public Stream<PersistenceEntity<String>> readByProperty(PersistenceCollection collection, PersistencePath property, Object propertyValue) {

        if (!this.isIndexed(collection, property)) {
            return this.streamAll(collection);
        }

        String hashKeyString = this.getBasePath().sub(collection).getValue();
        PersistencePath indexSet = this.toIndexValueToKeys(collection, property, String.valueOf(propertyValue));

        RedisCommands<String, String> sync = this.connection.sync();
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
                        out.add(new PersistenceEntity<>(PersistencePath.of(key), value));
                    }

                    return out.stream();
                });
    }

    @Override
    public String read(PersistenceCollection collection, PersistencePath path) {
        this.checkCollectionRegistered(collection);
        String hKey = this.getBasePath().sub(collection).getValue();
        return this.connection.sync().hget(hKey, path.getValue());
    }

    @Override
    public Map<PersistencePath, String> readAll(PersistenceCollection collection) {
        this.checkCollectionRegistered(collection);
        String hKey = this.getBasePath().sub(collection).getValue();
        return this.connection.sync().hgetall(hKey).entrySet().stream()
                .collect(Collectors.toMap(entry -> PersistencePath.of(entry.getKey()), Map.Entry::getValue));
    }

    @Override
    public Stream<PersistenceEntity<String>> streamAll(PersistenceCollection collection) {

        this.checkCollectionRegistered(collection);
        RedisCommands<String, String> sync = this.connection.sync();
        String hKey = this.getBasePath().sub(collection).getValue();

        long totalKeys = sync.hlen(hKey);
        long step = totalKeys / 100;
        if (step < 50) step = 50;

        ScanIterator<KeyValue<String, String>> iterator = ScanIterator.hscan(sync, hKey, ScanArgs.Builder.limit(step));
        return StreamSupport.stream(Spliterators.spliterator(new Iterator<PersistenceEntity<String>>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public PersistenceEntity<String> next() {
                KeyValue<String, String> next = iterator.next();
                return new PersistenceEntity<>(PersistencePath.of(next.getKey()), next.getValue());
            }
        }, totalKeys, Spliterator.NONNULL), false);
    }

    @Override
    public boolean exists(PersistenceCollection collection, PersistencePath path) {
        this.checkCollectionRegistered(collection);
        String hKey = this.getBasePath().sub(collection).getValue();
        return this.connection.sync().hexists(hKey, path.getValue());
    }

    @Override
    public boolean write(PersistenceCollection collection, PersistencePath path, String raw) {
        this.checkCollectionRegistered(collection);
        String hKey = this.getBasePath().sub(collection).getValue();
        this.connection.sync().hset(hKey, path.getValue(), raw);
        return true;
    }

    @Override
    public boolean delete(PersistenceCollection collection, PersistencePath path) {

        this.checkCollectionRegistered(collection);
        Set<IndexProperty> collectionIndexes = this.getKnownIndexes().get(collection);

        if (collectionIndexes != null) {
            collectionIndexes.forEach(index -> this.dropIndex(collection, path));
        }

        String hKey = this.getBasePath().sub(collection).getValue();
        return this.connection.sync().hdel(hKey, path.getValue()) > 0;
    }

    @Override
    public boolean deleteAll(PersistenceCollection collection) {

        this.checkCollectionRegistered(collection);
        Set<IndexProperty> collectionIndexes = this.getKnownIndexes().get(collection);

        if (collectionIndexes != null) {
            collectionIndexes.forEach(index -> this.dropIndex(collection, index));
        }

        String hKey = this.getBasePath().sub(collection).getValue();
        return this.connection.sync().del(hKey) > 0;
    }

    @Override
    public long deleteAll() {
        return this.connection.sync().del(this.getKnownCollections().keySet().toArray(new String[0]));
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
}
