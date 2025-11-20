package eu.okaeri.persistence;

import eu.okaeri.persistence.document.Document;
import eu.okaeri.persistence.repository.DocumentRepository;
import eu.okaeri.persistence.repository.annotation.DocumentCollection;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

public class PersistenceCollectionTest {

    @DocumentCollection(path = "users_uuid")
    interface UserRepositoryWithUuid extends DocumentRepository<UUID, UserDocument> {
    }

    @DocumentCollection(path = "users_string")
    interface UserRepositoryWithString extends DocumentRepository<String, UserDocument> {
    }

    @DocumentCollection(path = "users_integer")
    interface UserRepositoryWithInteger extends DocumentRepository<Integer, UserDocument> {
    }

    @DocumentCollection(path = "users_long")
    interface UserRepositoryWithLong extends DocumentRepository<Long, UserDocument> {
    }

    @DocumentCollection(path = "users_uuid_explicit", keyLength = 50)
    interface UserRepositoryWithUuidExplicitLength extends DocumentRepository<UUID, UserDocument> {
    }

    static class UserDocument extends Document {
    }

    @Test
    public void test_uuid_path_auto_detection() {
        PersistenceCollection collection = PersistenceCollection.of(UserRepositoryWithUuid.class);

        assertThat(collection.getValue()).isEqualTo("users_uuid");
        assertThat(collection.getKeyLength()).isEqualTo(36);
    }

    @Test
    public void test_integer_path_auto_detection() {
        PersistenceCollection collection = PersistenceCollection.of(UserRepositoryWithInteger.class);

        assertThat(collection.getValue()).isEqualTo("users_integer");
        assertThat(collection.getKeyLength()).isEqualTo(11);
    }

    @Test
    public void test_long_path_auto_detection() {
        PersistenceCollection collection = PersistenceCollection.of(UserRepositoryWithLong.class);

        assertThat(collection.getValue()).isEqualTo("users_long");
        assertThat(collection.getKeyLength()).isEqualTo(20);
    }

    @Test
    public void test_string_path_keeps_default() {
        PersistenceCollection collection = PersistenceCollection.of(UserRepositoryWithString.class);

        assertThat(collection.getValue()).isEqualTo("users_string");
        assertThat(collection.getKeyLength()).isEqualTo(255);
    }

    @Test
    public void test_explicit_keyLength_overrides_uuid_detection() {
        PersistenceCollection collection = PersistenceCollection.of(UserRepositoryWithUuidExplicitLength.class);

        assertThat(collection.getValue()).isEqualTo("users_uuid_explicit");
        assertThat(collection.getKeyLength()).isEqualTo(50);
    }

    @Test
    public void test_manual_creation_with_default_keyLength() {
        PersistenceCollection collection = PersistenceCollection.of("manual_path");

        assertThat(collection.getValue()).isEqualTo("manual_path");
        assertThat(collection.getKeyLength()).isEqualTo(255);
    }

    @Test
    public void test_manual_creation_with_explicit_keyLength() {
        PersistenceCollection collection = PersistenceCollection.of("manual_path", 100);

        assertThat(collection.getValue()).isEqualTo("manual_path");
        assertThat(collection.getKeyLength()).isEqualTo(100);
    }
}
