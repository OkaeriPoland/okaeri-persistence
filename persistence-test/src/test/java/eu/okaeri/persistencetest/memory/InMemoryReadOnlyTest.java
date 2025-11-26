//package eu.okaeri.persistencetest.memory;
//
//import eu.okaeri.persistence.document.DocumentPersistence;
//import eu.okaeri.persistence.document.InMemoryPersistence;
//import eu.okaeri.persistence.document.ReadOnlyDocumentPersistence;
//import eu.okaeri.persistencetest.TestContext;
//import eu.okaeri.persistencetest.fixtures.User;
//import org.junit.jupiter.api.BeforeEach;
//import org.junit.jupiter.api.Test;
//
//import java.util.UUID;
//
//import static org.assertj.core.api.Assertions.assertThat;
//import static org.assertj.core.api.Assertions.assertThatThrownBy;
//
///**
// * Tests for ReadOnlyDocumentPersistence wrapper.
// * Verifies that read operations work but write operations fail.
// */
//public class InMemoryReadOnlyTest {
//
//    private DocumentPersistence memory;
//    private TestContext.UserRepository readOnlyRepo;
//    private TestContext.UserRepository writeableRepo;
//
//    @BeforeEach
//    public void setup() {
//        // Create in-memory backend
//        memory = new InMemoryPersistence();
//
//        // Create writeable repository (direct access to memory)
//        writeableRepo = memory.createRepository(TestContext.UserRepository.class);
//
//        // Create read-only repository (wrapped)
//        DocumentPersistence readOnlyPersistence = new ReadOnlyDocumentPersistence(memory);
//        readOnlyRepo = readOnlyPersistence.createRepository(TestContext.UserRepository.class);
//
//        // Setup test data via writeable repo
//        writeableRepo.deleteAll();
//        writeableRepo.save(new User("alice", 100));
//        writeableRepo.save(new User("bob", 200));
//        writeableRepo.save(new User("charlie", 300));
//    }
//
//    @Test
//    public void test_readonly_can_read() {
//        assertThat(readOnlyRepo.count()).isEqualTo(3);
//        assertThat(readOnlyRepo.findAll()).hasSize(3);
//    }
//
//    @Test
//    public void test_readonly_save_returns_false() {
//        User user = new User("diana", 400);
//        user.setPath(UUID.randomUUID());
//
//        // Read-only persistence returns false for write operations
//        readOnlyRepo.save(user);
//
//        // Verify it wasn't actually saved
//        assertThat(readOnlyRepo.count()).isEqualTo(3);  // Still 3, not 4
//    }
//
//    @Test
//    public void test_readonly_delete_returns_false() {
//        User user = writeableRepo.findAll().iterator().next();
//
//        // Read-only persistence returns false for delete operations
//        boolean deleted = readOnlyRepo.deleteByPath(user.getPath().toUUID());
//
//        assertThat(deleted).isFalse();
//        assertThat(readOnlyRepo.count()).isEqualTo(3);  // Still 3
//    }
//
//    @Test
//    public void test_readonly_deleteAll_returns_false() {
//        boolean deleted = readOnlyRepo.deleteAll();
//
//        assertThat(deleted).isFalse();
//        assertThat(readOnlyRepo.count()).isEqualTo(3);  // Still 3
//    }
//
//    @Test
//    public void test_readonly_reflects_underlying_changes() {
//        // Read initial count
//        assertThat(readOnlyRepo.count()).isEqualTo(3);
//
//        // Modify via writeable repo
//        writeableRepo.save(new User("diana", 400));
//
//        // Read-only repo should see the change
//        assertThat(readOnlyRepo.count()).isEqualTo(4);
//        assertThat(readOnlyRepo.findAll()).hasSize(4);
//    }
//}
