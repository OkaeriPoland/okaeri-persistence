//package eu.okaeri.persistencetest.memory;
//
//import eu.okaeri.persistence.PersistenceCollection;
//import eu.okaeri.persistence.PersistencePath;
//import eu.okaeri.persistence.document.Document;
//import eu.okaeri.persistence.document.DocumentPersistence;
//import eu.okaeri.persistence.document.InMemoryPersistence;
//import eu.okaeri.persistencetest.TestContext;
//import lombok.NonNull;
//import org.junit.jupiter.api.Test;
//
//import java.util.Optional;
//import java.util.UUID;
//
//import static org.assertj.core.api.Assertions.assertThatThrownBy;
//
///**
// * Tests exception handling in repository proxies.
// * Verifies that custom exceptions from persistence backends are properly propagated.
// */
//public class InMemoryExceptionTest {
//
//    @Test
//    public void test_proxy_exception_propagation() {
//        DocumentPersistence brokenPersistence = new BrokenPersistence();
//        TestContext.UserRepository repo = brokenPersistence.createRepository(TestContext.UserRepository.class);
//
//        assertThatThrownBy(() -> repo.findByPath(UUID.randomUUID()))
//            .isInstanceOf(CustomTestException.class)
//            .hasMessage("Broken persistence!");
//    }
//
//    static class CustomTestException extends RuntimeException {
//        public CustomTestException(String message) {
//            super(message);
//        }
//    }
//
//    static class BrokenPersistence extends InMemoryPersistence {
//        @Override
//        public Optional<Document> read(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {
//            throw new CustomTestException("Broken persistence!");
//        }
//    }
//}
