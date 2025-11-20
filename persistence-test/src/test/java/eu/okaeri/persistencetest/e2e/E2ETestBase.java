package eu.okaeri.persistencetest.e2e;

import eu.okaeri.persistencetest.containers.*;
import org.junit.jupiter.api.AfterEach;

import java.util.stream.Stream;

/**
 * Base class for all E2E tests providing common infrastructure.
 * All backends are tested with the same test methods to ensure
 * complete compatibility across the persistence abstraction layer.
 */
public abstract class E2ETestBase {

    /**
     * Provides all backend implementations for parameterized testing.
     * ALL backends MUST support ALL repository features.
     */
    protected static Stream<BackendContainer> allBackends() {
        return Stream.of(
            new InMemoryBackendContainer(),
            new H2BackendContainer(),
            new PostgresBackendContainer(),
            new MariaDbBackendContainer(),
            new MongoBackendContainer(),
            new RedisBackendContainer(),
            new FlatFileBackendContainer()
        );
    }

    /**
     * Automatically cleanup BackendTestContext after each test.
     */
    @AfterEach
    void cleanupBackendContext() throws Exception {
        BackendTestContext btc = BackendTestContext.getCurrent();
        if (btc != null) {
            btc.close();
        }
    }
}
