package eu.okaeri.persistence.redis;

import eu.okaeri.persistence.PersistenceCollection;
import eu.okaeri.persistence.PersistenceEntity;
import eu.okaeri.persistence.PersistencePath;
import eu.okaeri.persistence.document.index.IndexProperty;
import eu.okaeri.persistence.raw.RawPersistence;
import io.lettuce.core.*;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import java.io.IOException;
import java.util.*;
import java.util.function.Predicate;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import lombok.Getter;
import lombok.NonNull;
import lombok.SneakyThrows;

public class RedisPersistence extends RawPersistence {

  private static final Logger LOGGER = Logger.getLogger(RedisPersistence.class.getSimpleName());
  @Getter private StatefulRedisConnection<String, String> connection;
  @Getter private RedisClient client;

  public RedisPersistence(
      @NonNull final PersistencePath basePath, @NonNull final RedisClient client) {
    super(basePath, true, true, false, true, true);
    this.connect(client);
  }

  private static <T> List<List<T>> partition(final Collection<T> members, final int maxSize) {

    final List<List<T>> res = new ArrayList<>();
    List<T> internal = new ArrayList<>();

    for (final T member : members) {
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
  private void connect(final RedisClient client) {
    this.client = client;
    this.connection = this.createConnection(StringCodec.UTF8);
  }

  @SneakyThrows
  public <K, V> StatefulRedisConnection<K, V> createConnection(final RedisCodec<K, V> codec) {
    if (this.client == null) {
      throw new RuntimeException(
          "Cannot create connection! Make sure connect(RedisClient) is called before creating additional connections.");
    }
    StatefulRedisConnection<K, V> localConnection = null;
    do {
      try {
        localConnection = this.client.connect(codec);
      } catch (final Exception exception) {
        if (exception.getCause() != null) {
          LOGGER.severe(
              "["
                  + this.getBasePath().getValue()
                  + "] Cannot connect with redis (waiting 30s): "
                  + exception.getMessage()
                  + " caused by "
                  + exception.getCause().getMessage());
        } else {
          LOGGER.severe(
              "["
                  + this.getBasePath().getValue()
                  + "] Cannot connect with redis (waiting 30s): "
                  + exception.getMessage());
        }
        Thread.sleep(30_000);
      }
    } while (localConnection == null);
    return localConnection;
  }

  @SneakyThrows
  public <K, V> StatefulRedisPubSubConnection<K, V> createPubSubConnection(
      final RedisCodec<K, V> codec) {
    if (this.client == null) {
      throw new RuntimeException(
          "Cannot create connection! Make sure connect(RedisClient) is called before creating additional connections.");
    }
    StatefulRedisPubSubConnection<K, V> localConnection = null;
    do {
      try {
        localConnection = this.client.connectPubSub(codec);
      } catch (final Exception exception) {
        if (exception.getCause() != null) {
          LOGGER.severe(
              "["
                  + this.getBasePath().getValue()
                  + "] Cannot connect with redis pubsub (waiting 30s): "
                  + exception.getMessage()
                  + " caused by "
                  + exception.getCause().getMessage());
        } else {
          LOGGER.severe(
              "["
                  + this.getBasePath().getValue()
                  + "] Cannot connect with redis pubsub (waiting 30s): "
                  + exception.getMessage());
        }
        Thread.sleep(30_000);
      }
    } while (localConnection == null);
    return localConnection;
  }

  @Override
  public boolean updateIndex(
      @NonNull final PersistenceCollection collection,
      @NonNull final PersistencePath path,
      @NonNull final IndexProperty property,
      final String identity) {

    // remove from old set value_to_keys
    final RedisCommands<String, String> sync = this.connection.sync();
    this.dropIndex(collection, path, property);
    final String indexSet = this.toIndexValueToKeys(collection, property, identity).getValue();

    // register new value
    final String valuesSet = this.toValuesSet(collection, property).getValue();
    sync.sadd(valuesSet, identity);

    // add to new value_to_keys
    sync.sadd(indexSet, path.getValue());

    // update key_to_value
    final String keyToValue = this.toIndexKeyToValue(collection, property).getValue();
    return sync.hset(keyToValue, path.getValue(), identity);
  }

  @Override
  public boolean dropIndex(
      @NonNull final PersistenceCollection collection,
      @NonNull final PersistencePath path,
      @NonNull final IndexProperty property) {

    // get current value by key
    final String keyToValue = this.toIndexKeyToValue(collection, property).getValue();
    final RedisCommands<String, String> sync = this.connection.sync();
    final String currentValue = sync.hget(keyToValue, path.getValue());

    // delete old value mapping
    if (currentValue == null) return false;
    sync.hdel(keyToValue, path.getValue());

    // delete from values set
    final String valuesSet = this.toValuesSet(collection, property).getValue();
    sync.srem(valuesSet, currentValue);

    // delete from value to set
    final PersistencePath indexSet = this.toIndexValueToKeys(collection, property, currentValue);
    return sync.srem(indexSet.getValue(), path.getValue()) > 0;
  }

  @Override
  public boolean dropIndex(
      @NonNull final PersistenceCollection collection, @NonNull final PersistencePath path) {
    return this.getKnownIndexes()
        .getOrDefault(collection.getValue(), Collections.emptySet())
        .stream()
        .map(index -> this.dropIndex(collection, path, index))
        .anyMatch(Predicate.isEqual(true));
  }

  @Override
  public boolean dropIndex(
      @NonNull final PersistenceCollection collection, @NonNull final IndexProperty property) {

    final RedisCommands<String, String> sync = this.connection.sync();
    long changes = 0;

    // delete key to value mappings
    final String keyToValue = this.toIndexKeyToValue(collection, property).getValue();
    changes += sync.del(keyToValue);

    // gather all used values and delete set
    final String valuesSet = this.toValuesSet(collection, property).getValue();
    final Set<String> propertyValues = sync.smembers(valuesSet);
    changes += sync.del(valuesSet);

    // delete all value to keys mappings
    if (!propertyValues.isEmpty()) {
      changes +=
          sync.del(
              propertyValues.stream()
                  .map(value -> this.toIndexValueToKeys(collection, property, value))
                  .map(PersistencePath::getValue)
                  .toArray(String[]::new));
    }

    return changes > 0;
  }

  @Override
  public Set<PersistencePath> findMissingIndexes(
      @NonNull final PersistenceCollection collection,
      @NonNull final Set<IndexProperty> indexProperties) {

    final String[] args =
        indexProperties.stream()
            .map(index -> this.toIndexKeyToValue(collection, index))
            .map(PersistencePath::getValue)
            .toArray(String[]::new);

    final String script =
        "local collection = ARGV[1]\n"
            + "local allKeys = redis.call('hkeys', collection)\n"
            + "local indexes = KEYS\n"
            + "local result = {}\n"
            + "\n"
            + "for _, key in ipairs(allKeys) do\n"
            + "\n"
            + "    local present = true\n"
            + "\n"
            + "    for _, index in ipairs(indexes) do\n"
            + "        if (redis.call('hexists', index, key) == 0) then\n"
            + "            present = false\n"
            + "            break\n"
            + "        end\n"
            + "    end\n"
            + "\n"
            + "    if not present then\n"
            + "        result[#result+1] = key\n"
            + "    end\n"
            + "end\n"
            + "\n"
            + "return result\n";

    final String hashKey = this.getBasePath().sub(collection).getValue();
    final List<String> out =
        this.connection.sync().eval(script, ScriptOutputType.MULTI, args, hashKey);

    return out.stream().map(PersistencePath::of).collect(Collectors.toSet());
  }

  @Override
  public Stream<PersistenceEntity<String>> readByProperty(
      @NonNull final PersistenceCollection collection,
      @NonNull final PersistencePath property,
      final Object propertyValue) {

    if (!this.canUseToString(propertyValue) || !this.isIndexed(collection, property)) {
      return this.streamAll(collection);
    }

    final String hashKeyString = this.getBasePath().sub(collection).getValue();
    final PersistencePath indexSet =
        this.toIndexValueToKeys(collection, property, String.valueOf(propertyValue));

    final RedisCommands<String, String> sync = this.connection.sync();
    final Set<String> members = sync.smembers(indexSet.getValue());

    if (members.isEmpty()) {
      return Stream.of();
    }

    final int totalKeys = members.size();
    long step = totalKeys / 100;
    if (step < 50) step = 50;

    final String script =
        sync.scriptLoad(
            "local collection = ARGV[1]\n"
                + "local result = {}\n"
                + "\n"
                + "for _, key in ipairs(KEYS) do\n"
                + "    result[#result+1] = key\n"
                + "    result[#result+1] = redis.call('hget', collection, key)\n"
                + "end\n"
                + "\n"
                + "return result\n");

    return partition(members, Math.toIntExact(step)).stream()
        .flatMap(
            part -> {
              final String[] keys = part.toArray(new String[part.size()]);
              final List<String> result =
                  sync.evalsha(script, ScriptOutputType.MULTI, keys, hashKeyString);
              final List<PersistenceEntity<String>> out = new ArrayList<>();

              for (int i = 0; i < result.size(); i += 2) {
                final String key = result.get(i);
                final String value = result.get(i + 1);
                out.add(new PersistenceEntity<>(PersistencePath.of(key), value));
              }

              return out.stream();
            });
  }

  @Override
  public Optional<String> read(
      @NonNull final PersistenceCollection collection, @NonNull final PersistencePath path) {
    this.checkCollectionRegistered(collection);
    final String hKey = this.getBasePath().sub(collection).getValue();
    return Optional.ofNullable(this.connection.sync().hget(hKey, path.getValue()));
  }

  @Override
  public Map<PersistencePath, String> read(
      @NonNull final PersistenceCollection collection,
      @NonNull final Collection<PersistencePath> paths) {

    this.checkCollectionRegistered(collection);
    final String hKey = this.getBasePath().sub(collection).getValue();
    final RedisCommands<String, String> sync = this.connection.sync();
    final Map<PersistencePath, String> map = new LinkedHashMap<>();

    final String script =
        "local collection = ARGV[1]\n"
            + "local result = {}\n"
            + "\n"
            + "for _, key in ipairs(KEYS) do\n"
            + "    result[#result+1] = key\n"
            + "    result[#result+1] = redis.call('hget', collection, key)\n"
            + "end\n"
            + "\n"
            + "return result\n";

    final String[] keys = paths.stream().map(PersistencePath::getValue).toArray(String[]::new);
    final List<String> result = sync.eval(script, ScriptOutputType.MULTI, keys, hKey);

    for (int i = 0; i < result.size(); i += 2) {
      final String key = result.get(i);
      final String value = result.get(i + 1);
      map.put(PersistencePath.of(key), value);
    }

    return map;
  }

  @Override
  public Map<PersistencePath, String> readAll(@NonNull final PersistenceCollection collection) {
    this.checkCollectionRegistered(collection);
    final String hKey = this.getBasePath().sub(collection).getValue();
    return this.connection.sync().hgetall(hKey).entrySet().stream()
        .collect(
            Collectors.toMap(entry -> PersistencePath.of(entry.getKey()), Map.Entry::getValue));
  }

  @Override
  public Stream<PersistenceEntity<String>> streamAll(
      @NonNull final PersistenceCollection collection) {

    this.checkCollectionRegistered(collection);
    final RedisCommands<String, String> sync = this.connection.sync();
    final String hKey = this.getBasePath().sub(collection).getValue();

    final long totalKeys = sync.hlen(hKey);
    long step = totalKeys / 100;
    if (step < 50) step = 50;

    final ScanIterator<KeyValue<String, String>> iterator =
        ScanIterator.hscan(sync, hKey, ScanArgs.Builder.limit(step));
    return StreamSupport.stream(
        Spliterators.spliterator(
            new Iterator<PersistenceEntity<String>>() {
              @Override
              public boolean hasNext() {
                return iterator.hasNext();
              }

              @Override
              public PersistenceEntity<String> next() {
                final KeyValue<String, String> next = iterator.next();
                return new PersistenceEntity<>(PersistencePath.of(next.getKey()), next.getValue());
              }
            },
            totalKeys,
            Spliterator.NONNULL),
        false);
  }

  @Override
  public long count(@NonNull final PersistenceCollection collection) {
    this.checkCollectionRegistered(collection);
    final String hKey = this.getBasePath().sub(collection).getValue();
    return this.connection.sync().hlen(hKey);
  }

  @Override
  public boolean exists(
      @NonNull final PersistenceCollection collection, @NonNull final PersistencePath path) {
    this.checkCollectionRegistered(collection);
    final String hKey = this.getBasePath().sub(collection).getValue();
    return this.connection.sync().hexists(hKey, path.getValue());
  }

  @Override
  public boolean write(
      @NonNull final PersistenceCollection collection,
      @NonNull final PersistencePath path,
      @NonNull final String raw) {
    this.checkCollectionRegistered(collection);
    final String hKey = this.getBasePath().sub(collection).getValue();
    this.connection.sync().hset(hKey, path.getValue(), raw);
    return true;
  }

  @Override
  public boolean delete(
      @NonNull final PersistenceCollection collection, @NonNull final PersistencePath path) {

    this.checkCollectionRegistered(collection);
    final Set<IndexProperty> collectionIndexes = this.getKnownIndexes().get(collection.getValue());

    if (collectionIndexes != null) {
      collectionIndexes.forEach(index -> this.dropIndex(collection, path));
    }

    final String hKey = this.getBasePath().sub(collection).getValue();
    return this.connection.sync().hdel(hKey, path.getValue()) > 0;
  }

  @Override
  public long delete(
      @NonNull final PersistenceCollection collection,
      @NonNull final Collection<PersistencePath> paths) {

    this.checkCollectionRegistered(collection);
    final Set<IndexProperty> collectionIndexes = this.getKnownIndexes().get(collection.getValue());

    if (collectionIndexes != null) {
      for (final PersistencePath path : paths) {
        collectionIndexes.forEach(index -> this.dropIndex(collection, path));
      }
    }

    final String hKey = this.getBasePath().sub(collection).getValue();
    final String[] keysToDelete =
        paths.stream().map(PersistencePath::getValue).toArray(String[]::new);

    return this.connection.sync().hdel(hKey, keysToDelete);
  }

  @Override
  public boolean deleteAll(@NonNull final PersistenceCollection collection) {

    this.checkCollectionRegistered(collection);
    final Set<IndexProperty> collectionIndexes = this.getKnownIndexes().get(collection.getValue());

    if (collectionIndexes != null) {
      collectionIndexes.forEach(index -> this.dropIndex(collection, index));
    }

    final String hKey = this.getBasePath().sub(collection).getValue();
    return this.connection.sync().del(hKey) > 0;
  }

  @Override
  public long deleteAll() {
    return this.connection.sync().del(this.getKnownCollections().keySet().toArray(new String[0]));
  }

  @Override
  public void close() throws IOException {
    this.connection.close();
    this.client.shutdown();
  }

  private PersistencePath toIndexValueToKeys(
      final PersistenceCollection collection,
      final PersistencePath property,
      final String propertyValue) {
    return this.getBasePath()
        .sub(collection)
        .sub("index")
        .sub(property)
        .sub("value_to_keys")
        .sub(propertyValue);
  }

  private PersistencePath toIndexKeyToValue(
      final PersistenceCollection collection, final PersistencePath property) {
    return this.getBasePath().sub(collection).sub("index").sub(property).sub("key_to_value");
  }

  private PersistencePath toValuesSet(
      final PersistenceCollection collection, final PersistencePath property) {
    return this.getBasePath().sub(collection).sub("index").sub(property).sub("values");
  }
}
