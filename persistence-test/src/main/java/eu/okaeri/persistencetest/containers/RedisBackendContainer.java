package eu.okaeri.persistencetest.containers;

import eu.okaeri.configs.json.simple.JsonSimpleConfigurer;
import eu.okaeri.persistence.document.DocumentPersistence;
import eu.okaeri.persistence.redis.RedisPersistence;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Redis backend container using testcontainers.
 */
public class RedisBackendContainer implements BackendContainer {

    private static final GenericContainer<?> REDIS;

    static {
        REDIS = new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379)
            .withReuse(true);

        REDIS.start();
    }

    @Override
    public String getName() {
        return "Redis 7";
    }

    @Override
    public DocumentPersistence createPersistence() {
        RedisURI redisUri = RedisURI.builder()
            .withHost(REDIS.getHost())
            .withPort(REDIS.getMappedPort(6379))
            .build();

        RedisClient redisClient = RedisClient.create(redisUri);

        return new DocumentPersistence(new RedisPersistence(redisClient, new JsonSimpleConfigurer()));
    }

    @Override
    public boolean requiresContainer() {
        return true;
    }

    @Override
    public BackendType getType() {
        return BackendType.REDIS;
    }

    @Override
    public void close() throws Exception {
        // Container is reused across tests, no cleanup needed
    }

    @Override
    public String toString() {
        return getName();
    }
}
