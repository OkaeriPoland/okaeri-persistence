package eu.okaeri.persistencetestmem.basic;

import eu.okaeri.persistence.PersistenceCollection;
import eu.okaeri.persistence.PersistencePath;
import eu.okaeri.persistence.document.Document;
import eu.okaeri.persistence.document.DocumentPersistence;
import eu.okaeri.persistence.document.InMemoryDocumentPersistence;
import eu.okaeri.persistence.repository.RepositoryDeclaration;
import eu.okaeri.persistencetestmem.basic.repository.UserRepository;
import lombok.NonNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertThrows;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestPersistenceExceptions {

    @Test
    public void test_proxy_error_rethrow() {
        PersistenceCollection collection = PersistenceCollection.of(UserRepository.class);
        DocumentPersistence persistence = new BrokenPersistence();
        persistence.registerCollection(collection);
        UserRepository userRepository = RepositoryDeclaration.of(UserRepository.class).newProxy(persistence, collection, TestPersistenceExceptions.class.getClassLoader());
        assertThrows(CustomException.class, () -> userRepository.findByPath(UUID.randomUUID()));
    }

    class CustomException extends RuntimeException {
    }

    class BrokenPersistence extends InMemoryDocumentPersistence {
        @Override
        public Optional<Document> read(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {
            throw new CustomException();
        }
    }
}
