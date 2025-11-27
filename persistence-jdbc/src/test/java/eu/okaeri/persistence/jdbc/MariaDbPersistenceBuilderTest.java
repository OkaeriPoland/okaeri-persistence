package eu.okaeri.persistence.jdbc;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import eu.okaeri.configs.json.simple.JsonSimpleConfigurer;
import eu.okaeri.persistence.PersistencePath;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

class MariaDbPersistenceBuilderTest {

    @Test
    void build_throws_when_hikariConfig_and_dataSource_missing() {
        assertThatThrownBy(() -> MariaDbPersistence.builder()
            .basePath("myapp")
            .configurer(JsonSimpleConfigurer::new)
            .build())
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("hikariConfig or dataSource is required");
    }

    @Test
    void build_throws_when_hikariConfig_and_dataSource_both_provided() {
        HikariConfig mockConfig = mock(HikariConfig.class);
        HikariDataSource mockDataSource = mock(HikariDataSource.class);

        assertThatThrownBy(() -> MariaDbPersistence.builder()
            .hikariConfig(mockConfig)
            .dataSource(mockDataSource)
            .configurer(JsonSimpleConfigurer::new)
            .build())
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("mutually exclusive");
    }

    @Test
    void build_throws_when_configurer_missing_with_hikariConfig() {
        HikariConfig mockConfig = mock(HikariConfig.class);

        assertThatThrownBy(() -> MariaDbPersistence.builder()
            .basePath("myapp")
            .basePath(PersistencePath.of("myapp"))
            .hikariConfig(mockConfig)
            .serdes()
            .build())
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("configurer");
    }

    @Test
    void build_throws_when_configurer_missing_with_dataSource() {
        HikariDataSource mockDataSource = mock(HikariDataSource.class);

        assertThatThrownBy(() -> MariaDbPersistence.builder()
            .dataSource(mockDataSource)
            .build())
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("configurer");
    }
}
