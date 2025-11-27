package eu.okaeri.persistence.flat;

import eu.okaeri.configs.json.simple.JsonSimpleConfigurer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FlatPersistenceBuilderTest {

    @TempDir
    Path tempDir;

    @Test
    void build_with_file_storageDir() {
        FlatPersistence persistence = FlatPersistence.builder()
            .storageDir(this.tempDir.toFile())
            .configurer(JsonSimpleConfigurer::new)
            .build();

        assertThat(persistence).isNotNull();
        assertThat(persistence.getBasePath().toFile()).isEqualTo(this.tempDir.toFile());
    }

    @Test
    void build_with_path_storageDir() {
        FlatPersistence persistence = FlatPersistence.builder()
            .storageDir(this.tempDir)
            .configurer(JsonSimpleConfigurer::new)
            .build();

        assertThat(persistence).isNotNull();
        assertThat(persistence.getBasePath().toFile()).isEqualTo(this.tempDir.toFile());
    }

    @Test
    void build_with_suffix() {
        FlatPersistence persistence = FlatPersistence.builder()
            .storageDir(this.tempDir)
            .configurer(JsonSimpleConfigurer::new)
            .suffix(".data")
            .build();

        assertThat(persistence.getFileSuffix()).isEqualTo(".data");
    }

    @Test
    void build_with_extension() {
        FlatPersistence persistence = FlatPersistence.builder()
            .storageDir(this.tempDir)
            .configurer(JsonSimpleConfigurer::new)
            .extension("yaml")
            .build();

        assertThat(persistence.getFileSuffix()).isEqualTo(".yaml");
    }

    @Test
    void build_uses_configurer_extension_as_default() {
        FlatPersistence persistence = FlatPersistence.builder()
            .storageDir(this.tempDir)
            .configurer(JsonSimpleConfigurer::new)
            .build();

        assertThat(persistence.getFileSuffix()).isEqualTo(".json");
    }

    @Test
    void build_throws_when_storageDir_missing() {
        assertThatThrownBy(() -> FlatPersistence.builder()
            .configurer(JsonSimpleConfigurer::new)
            .build())
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("storageDir");
    }

    @Test
    void build_throws_when_configurer_missing() {
        assertThatThrownBy(() -> FlatPersistence.builder()
            .storageDir(this.tempDir)
            .build())
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("configurer");
    }

    @Test
    void constructor_with_file_and_configurer() {
        FlatPersistence persistence = new FlatPersistence(
            this.tempDir.toFile(),
            JsonSimpleConfigurer::new
        );

        assertThat(persistence).isNotNull();
        assertThat(persistence.getFileSuffix()).isEqualTo(".json");
    }
}
