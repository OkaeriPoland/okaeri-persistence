package eu.okaeri.persistence.redis;

import eu.okaeri.configs.json.simple.JsonSimpleConfigurer;
import eu.okaeri.persistence.PersistencePath;
import io.lettuce.core.RedisClient;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

class RedisPersistenceBuilderTest {

    @Test
    void build_throws_when_client_missing() {
        assertThatThrownBy(() -> RedisPersistence.builder()
            .basePath("myapp")
            .configurer(JsonSimpleConfigurer::new)
            .build())
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("client");
    }

    @Test
    void build_throws_when_configurer_missing() {
        RedisClient mockClient = mock(RedisClient.class);

        assertThatThrownBy(() -> RedisPersistence.builder()
            .basePath("myapp")
            .basePath(PersistencePath.of("myapp"))
            .client(mockClient)
            .serdes()
            .build())
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("configurer");
    }
}
