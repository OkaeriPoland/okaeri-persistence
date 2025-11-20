package eu.okaeri.persistencetest.e2e;

import eu.okaeri.persistence.document.DocumentPersistence;
import eu.okaeri.persistencetest.TestContext;
import eu.okaeri.persistencetest.containers.BackendContainer;
import lombok.Getter;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Wrapper that provides both BackendContainer and TestContext for E2E tests.
 * Automatic cleanup handled by @AfterEach in E2ETestBase.
 *
 * Usage in tests:
 * <pre>
 * @ParameterizedTest(name = "{0}")
 * @MethodSource("allBackendsWithContext")
 * void test_something(BackendTestContext btc) {
 *     btc.getUserRepository().save(new User(...));
 *     // test code here - zero boilerplate!
 * }
 * </pre>
 */
@Getter
public class BackendTestContext implements AutoCloseable {

    private static final ThreadLocal<BackendTestContext> CURRENT = new ThreadLocal<>();

    private final BackendContainer backend;
    private final TestContext context;
    private final Map<String, Object> testData = new ConcurrentHashMap<>();

    private BackendTestContext(BackendContainer backend) {
        this.backend = backend;

        // Create persistence and repositories
        DocumentPersistence persistence = backend.createPersistence();
        TestContext.UserRepository userRepo = persistence.createRepository(TestContext.UserRepository.class);
        TestContext.UserProfileRepository profileRepo = persistence.createRepository(TestContext.UserProfileRepository.class);

        // Clean slate for each test
        userRepo.deleteAll();
        profileRepo.deleteAll();

        this.context = new TestContext(userRepo, profileRepo);

        // Register for cleanup
        CURRENT.set(this);
    }

    public static BackendTestContext create(BackendContainer backend) {
        return new BackendTestContext(backend);
    }

    static BackendTestContext getCurrent() {
        return CURRENT.get();
    }

    static void clearCurrent() {
        CURRENT.remove();
    }

    // Convenience delegates to TestContext
    public TestContext.UserRepository getUserRepository() {
        return this.context.getUserRepository();
    }

    public TestContext.UserProfileRepository getProfileRepository() {
        return this.context.getProfileRepository();
    }

    @Override
    public String toString() {
        return this.backend.getName();
    }

    @Override
    public void close() throws Exception {
        this.backend.close();
        clearCurrent();
    }
}
