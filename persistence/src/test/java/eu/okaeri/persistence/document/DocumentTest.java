package eu.okaeri.persistence.document;

import eu.okaeri.persistence.PersistencePath;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DocumentTest {

    @Test
    void setPath_uuid_when_path_is_null() {
        Document doc = new Document();
        UUID uuid = UUID.randomUUID();

        doc.setPath(uuid);

        assertThat(doc.getPath().getValue()).isEqualTo(uuid.toString());
    }

    @Test
    void setPath_uuid_when_path_is_already_uuid() {
        Document doc = new Document();
        UUID firstUuid = UUID.randomUUID();
        UUID secondUuid = UUID.randomUUID();

        doc.setPath(firstUuid);
        doc.setPath(secondUuid);

        assertThat(doc.getPath().getValue()).isEqualTo(secondUuid.toString());
    }

    @Test
    void setPath_uuid_throws_when_path_is_non_uuid() {
        Document doc = new Document();
        doc.setPath(PersistencePath.of("non-uuid-path"));

        assertThatThrownBy(() -> doc.setPath(UUID.randomUUID()))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("non-uuid-path")
            .hasMessageContaining("setPath(PersistencePath)");
    }

    @Test
    void setPath_persistence_path_accepts_any_value() {
        Document doc = new Document();

        doc.setPath(PersistencePath.of("any-string"));
        assertThat(doc.getPath().getValue()).isEqualTo("any-string");

        doc.setPath(PersistencePath.of("another/path"));
        assertThat(doc.getPath().getValue()).isEqualTo("another/path");
    }

    @Test
    void saveDefaults_throws() {
        Document doc = new Document();

        assertThatThrownBy(doc::saveDefaults)
            .isInstanceOf(RuntimeException.class)
            .hasMessageContaining("not available");
    }
}
