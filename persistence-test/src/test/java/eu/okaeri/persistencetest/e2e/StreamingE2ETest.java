package eu.okaeri.persistencetest.e2e;

import eu.okaeri.persistencetest.fixtures.User;
import lombok.Cleanup;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E tests comparing stream() vs streamAll() behavior.
 * Both methods should return the same data, but stream() uses batching for better memory efficiency.
 * Note: stream() requires proper resource management (@Cleanup or try-with-resources), streamAll() does not.
 */
class StreamingE2ETest extends E2ETestBase {

    // ===== TEST CONTEXT =====

    protected static Stream<BackendTestContext> smallStreamTestContext() {
        return allBackends().map(backend -> {
            BackendTestContext btc = BackendTestContext.create(backend);

            // Small dataset: 10 users
            IntStream.range(0, 10).forEach(i ->
                btc.getUserRepository().save(new User("user" + i, i * 100, i % 2 == 0))
            );

            return btc;
        });
    }

    protected static Stream<BackendTestContext> largeStreamTestContext() {
        return allBackends().map(backend -> {
            BackendTestContext btc = BackendTestContext.create(backend);

            // Large dataset: 250 users (to test batching with default batch size of 100)
            IntStream.range(0, 250).forEach(i ->
                btc.getUserRepository().save(new User("user" + i, i * 100, i % 2 == 0))
            );

            return btc;
        });
    }

    // ===== BASIC STREAMING TESTS =====

    @ParameterizedTest(name = "{0}")
    @MethodSource("smallStreamTestContext")
    void test_stream_returns_all_data(BackendTestContext btc) {
        @Cleanup Stream<User> stream = btc.getUserRepository().stream(5);
        List<String> streamedNames = stream
            .map(User::getName)
            .sorted()
            .toList();

        assertThat(streamedNames).hasSize(10);
        assertThat(streamedNames).containsExactly(
            "user0", "user1", "user2", "user3", "user4",
            "user5", "user6", "user7", "user8", "user9"
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("smallStreamTestContext")
    void test_stream_default_batch_size(BackendTestContext btc) {
        // Test default batch size (100)
        @Cleanup Stream<User> stream = btc.getUserRepository().stream();
        List<String> streamedNames = stream
            .map(User::getName)
            .sorted()
            .toList();

        assertThat(streamedNames).hasSize(10);
        assertThat(streamedNames).containsExactly(
            "user0", "user1", "user2", "user3", "user4",
            "user5", "user6", "user7", "user8", "user9"
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("smallStreamTestContext")
    void test_streamAll_returns_same_data_as_stream(BackendTestContext btc) {
        // streamAll() does not require @Cleanup
        List<String> streamAllNames = btc.getUserRepository().streamAll()
            .map(User::getName)
            .sorted()
            .toList();

        @Cleanup Stream<User> stream = btc.getUserRepository().stream(5);
        List<String> streamNames = stream
            .map(User::getName)
            .sorted()
            .toList();

        assertThat(streamNames).isEqualTo(streamAllNames);
    }

    // ===== LARGE DATASET TESTS =====

    @ParameterizedTest(name = "{0}")
    @MethodSource("largeStreamTestContext")
    void test_stream_large_dataset(BackendTestContext btc) {
        // Test with batch size 25 - should fetch in 10 batches
        @Cleanup Stream<User> stream = btc.getUserRepository().stream(25);
        long count = stream.count();
        assertThat(count).isEqualTo(250);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("largeStreamTestContext")
    void test_stream_large_dataset_with_default_batch_size(BackendTestContext btc) {
        // Test with default batch size (100) - should fetch in 3 batches
        @Cleanup Stream<User> stream = btc.getUserRepository().stream();
        long count = stream.count();
        assertThat(count).isEqualTo(250);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("largeStreamTestContext")
    void test_streamAll_vs_stream_large_dataset(BackendTestContext btc) {
        List<String> streamAllNames = btc.getUserRepository().streamAll()
            .map(User::getName)
            .sorted()
            .toList();

        @Cleanup Stream<User> stream = btc.getUserRepository().stream(50);
        List<String> streamNames = stream
            .map(User::getName)
            .sorted()
            .toList();

        assertThat(streamNames).hasSize(250);
        assertThat(streamNames).isEqualTo(streamAllNames);
    }

    // ===== STREAM OPERATIONS =====

    @ParameterizedTest(name = "{0}")
    @MethodSource("smallStreamTestContext")
    void test_stream_with_filter(BackendTestContext btc) {
        @Cleanup Stream<User> stream = btc.getUserRepository().stream(5);
        List<String> evenUsers = stream
            .filter(User::isVerified) // verified = true for even indexes
            .map(User::getName)
            .sorted()
            .toList();

        assertThat(evenUsers).containsExactly("user0", "user2", "user4", "user6", "user8");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("smallStreamTestContext")
    void test_stream_with_map(BackendTestContext btc) {
        @Cleanup Stream<User> stream = btc.getUserRepository().stream(5);
        List<Integer> expValues = stream
            .map(User::getExp)
            .sorted()
            .toList();

        assertThat(expValues).containsExactly(0, 100, 200, 300, 400, 500, 600, 700, 800, 900);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("smallStreamTestContext")
    void test_stream_with_limit(BackendTestContext btc) {
        @Cleanup Stream<User> stream = btc.getUserRepository().stream(5);
        long count = stream
            .limit(3)
            .count();

        assertThat(count).isEqualTo(3);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("smallStreamTestContext")
    void test_stream_with_skip(BackendTestContext btc) {
        @Cleanup Stream<User> stream = btc.getUserRepository().stream(5);
        List<String> skippedNames = stream
            .map(User::getName)
            .sorted()
            .skip(5)
            .toList();

        assertThat(skippedNames).containsExactly("user5", "user6", "user7", "user8", "user9");
    }

    // ===== EDGE CASES =====

    protected static Stream<BackendTestContext> emptyCollectionContext() {
        return allBackends().map(BackendTestContext::create);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("emptyCollectionContext")
    void test_stream_empty_collection(BackendTestContext btc) {
        @Cleanup Stream<User> stream = btc.getUserRepository().stream(10);
        long count = stream.count();
        assertThat(count).isEqualTo(0);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("emptyCollectionContext")
    void test_streamAll_empty_collection(BackendTestContext btc) {
        long count = btc.getUserRepository().streamAll().count();
        assertThat(count).isEqualTo(0);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("smallStreamTestContext")
    void test_stream_with_batch_size_larger_than_collection(BackendTestContext btc) {
        // Batch size 1000 but only 10 items
        @Cleanup Stream<User> stream = btc.getUserRepository().stream(1000);
        long count = stream.count();
        assertThat(count).isEqualTo(10);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("smallStreamTestContext")
    void test_stream_with_batch_size_one(BackendTestContext btc) {
        // Minimum batch size - should fetch one at a time
        @Cleanup Stream<User> stream = btc.getUserRepository().stream(1);
        long count = stream.count();
        assertThat(count).isEqualTo(10);
    }
}
