package eu.okaeri.persistence.mongo;

import com.mongodb.client.MongoClient;
import eu.okaeri.configs.json.simple.JsonSimpleConfigurer;
import eu.okaeri.persistence.PersistencePath;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

class MongoPersistenceBuilderTest {

    @Test
    void build_throws_when_client_missing() {
        assertThatThrownBy(() -> MongoPersistence.builder()
            .basePath("myapp")
            .databaseName("testdb")
            .configurer(new JsonSimpleConfigurer())
            .build())
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("client");
    }

    @Test
    void build_throws_when_databaseName_missing() {
        MongoClient mockClient = mock(MongoClient.class);

        assertThatThrownBy(() -> MongoPersistence.builder()
            .basePath("myapp")
            .client(mockClient)
            .configurer(new JsonSimpleConfigurer())
            .build())
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("databaseName");
    }

    @Test
    void build_throws_when_configurer_missing() {
        MongoClient mockClient = mock(MongoClient.class);

        assertThatThrownBy(() -> MongoPersistence.builder()
            .basePath("myapp")
            .basePath(PersistencePath.of("myapp"))
            .client(mockClient)
            .databaseName("testdb")
            .serdes()
            .build())
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("configurer");
    }
}
