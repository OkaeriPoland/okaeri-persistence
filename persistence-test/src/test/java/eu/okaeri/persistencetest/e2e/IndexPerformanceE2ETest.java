package eu.okaeri.persistencetest.e2e;

import eu.okaeri.persistence.document.DocumentPersistence;
import eu.okaeri.persistencetest.IndexPerformanceTestContext;
import eu.okaeri.persistencetest.containers.*;
import eu.okaeri.persistencetest.fixtures.IndexTestEntity;
import lombok.Getter;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Performance tests comparing indexed vs non-indexed query execution.
 * <p>
 * Tests that indexes provide significant speedup for supported query patterns.
 * Only tests InMemory and FlatFile backends since they use our in-memory indexing.
 * <p>
 * Test data: 100 unique categories × factor copies (factor varies by backend)
 */
@DisplayName("Index Performance E2E Tests")
public class IndexPerformanceE2ETest extends E2ETestBase {

    /**
     * Cached contexts to avoid re-populating data for each test method.
     * Data is read-only during tests, so sharing is safe.
     */
    private static List<IndexPerfContext> cachedContexts;

    /**
     * Number of unique categories/levels (different entity "types")
     */
    private static final int UNIQUE_ENTITIES = 100;

    /**
     * Default copies per unique entity for in-memory backends
     */
    private static final int FACTOR_DEFAULT = 20;

    /**
     * Higher factor for native DB backends (need more data to show index benefit)
     */
    private static final int FACTOR_LENIENT = 2_000;

    /**
     * Maximum ratio of indexed time to non-indexed time for fully indexed queries.
     * 0.5 means indexed should be at most 50% of non-indexed time.
     */
    private static final double SPEEDUP_RATIO_DEFAULT = 0.5;

    /**
     * More lenient speedup ratio for native DB backends due to network/query overhead.
     */
    private static final double SPEEDUP_RATIO_LENIENT = 0.95;

    /**
     * Maximum ratio for mixed queries (partially indexed).
     * More lenient since index only narrows candidates, remaining filter applied in-memory.
     */
    private static final double SPEEDUP_MIXED_DEFAULT = 0.7;

    /**
     * Mixed query ratio for native DB backends.
     */
    private static final double SPEEDUP_MIXED_LENIENT = 0.99;

    /**
     * Number of warmup iterations before timing
     */
    private static final int WARMUP_ITERATIONS = 1;

    /**
     * Number of timed iterations for averaging
     */
    private static final int TIMED_ITERATIONS = 2;

    /**
     * Context holder for index performance tests
     */
    @Getter
    public static class IndexPerfContext implements AutoCloseable {
        private final BackendContainer backend;
        private final IndexPerformanceTestContext.IndexedRepository indexedRepo;
        private final IndexPerformanceTestContext.NonIndexedRepository nonIndexedRepo;
        private final int factor;
        private final double speedupRatio;
        private final double speedupMixed;
        private boolean dataPopulated = false;

        IndexPerfContext(BackendContainer backend) {
            this.backend = backend;
            boolean lenientBackend = isLenientBackend(backend);
            this.factor = lenientBackend ? FACTOR_LENIENT : FACTOR_DEFAULT;
            this.speedupRatio = lenientBackend ? SPEEDUP_RATIO_LENIENT : SPEEDUP_RATIO_DEFAULT;
            this.speedupMixed = lenientBackend ? SPEEDUP_MIXED_LENIENT : SPEEDUP_MIXED_DEFAULT;
            DocumentPersistence persistence = backend.createPersistence();
            this.indexedRepo = persistence.createRepository(IndexPerformanceTestContext.IndexedRepository.class);
            this.nonIndexedRepo = persistence.createRepository(IndexPerformanceTestContext.NonIndexedRepository.class);
        }

        private static boolean isLenientBackend(BackendContainer backend) {
            String name = backend.getName().toLowerCase();
            return name.contains("mongo") || name.contains("postgres") || name.contains("maria") || name.contains("h2");
        }

        void populateData() {
            if (this.dataPopulated) return;

            // Clear existing data
            this.indexedRepo.deleteAll();
            this.nonIndexedRepo.deleteAll();

            // Generate test data: 100 unique entities × factor copies each
            List<IndexTestEntity> indexedEntities = new ArrayList<>();
            List<IndexTestEntity> nonIndexedEntities = new ArrayList<>();

            for (int i = 0; i < UNIQUE_ENTITIES; i++) {
                String category = "category_" + i;
                int level = i;
                boolean active = (i == 15) || (i == 85); // only levels 15 and 85 are active (2%)
                String description = "desc_" + i;
                int score = i * 10;
                double rating = i * 1.5; // 0.0, 1.5, 3.0, ..., 148.5

                for (int seq = 0; seq < this.factor; seq++) {
                    IndexTestEntity indexed = new IndexTestEntity();
                    indexed.setCategory(category);
                    indexed.setLevel(level);
                    indexed.setActive(active);
                    indexed.setDescription(description);
                    indexed.setScore(score);
                    indexed.setRating(rating);
                    indexed.setSequence(seq);
                    indexedEntities.add(indexed);

                    IndexTestEntity nonIndexed = new IndexTestEntity();
                    nonIndexed.setCategory(category);
                    nonIndexed.setLevel(level);
                    nonIndexed.setActive(active);
                    nonIndexed.setDescription(description);
                    nonIndexed.setScore(score);
                    nonIndexed.setRating(rating);
                    nonIndexed.setSequence(seq);
                    nonIndexedEntities.add(nonIndexed);
                }
            }

            // Batch save for better DB performance
            this.indexedRepo.saveAll(indexedEntities);
            this.nonIndexedRepo.saveAll(nonIndexedEntities);

            this.dataPopulated = true;
        }

        IndexPerformanceTestContext.IndexedRepository indexed() {
            return this.indexedRepo;
        }

        IndexPerformanceTestContext.NonIndexedRepository nonIndexed() {
            return this.nonIndexedRepo;
        }

        @Override
        public String toString() {
            return this.backend.getName();
        }

        @Override
        public void close() throws Exception {
            this.backend.close();
        }
    }

    /**
     * Only test backends that use our in-memory indexing (InMemory and FlatFile).
     * Contexts are cached so data is populated only once per test class run.
     */
    protected static Stream<IndexPerfContext> indexableBackends() {
        if (cachedContexts == null) {
            cachedContexts = new ArrayList<>();
            for (BackendContainer backend : List.of(
                new InMemoryBackendContainer(),
                //new H2BackendContainer() // no performance benefit
                new PostgresBackendContainer(),
                new MariaDbBackendContainer(),
                new MongoBackendContainer()
//                new RedisBackendContainer(),
//                new FlatFileBackendContainer()
            )) {
                IndexPerfContext ctx = new IndexPerfContext(backend);
                ctx.populateData();
                cachedContexts.add(ctx);
            }
        }
        return cachedContexts.stream();
    }

    @AfterAll
    static void cleanupContexts() throws Exception {
        if (cachedContexts != null) {
            for (IndexPerfContext ctx : cachedContexts) {
                ctx.close();
            }
            cachedContexts = null;
        }
    }

    // ========================================================================
    // PRECISE TESTS: Single document lookups (1-3 results)
    // ========================================================================

    @ParameterizedTest(name = "{0}")
    @MethodSource("indexableBackends")
    @DisplayName("PRECISE: Single doc lookup by category + sequence")
    void test_precise_findByCategoryAndSequence(IndexPerfContext ctx) {
        String category = "category_42";
        int sequence = 7; // exactly 1 document

        TimingResult result = this.timeQueries(
            () -> ctx.indexed().findByCategoryAndSequence(category, sequence),
            () -> ctx.nonIndexed().findByCategoryAndSequence(category, sequence),
            1
        );

        this.assertSpeedup(result, ctx.getSpeedupRatio(), "findByCategoryAndSequence (1 doc)");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("indexableBackends")
    @DisplayName("PRECISE: Single doc lookup by level + sequence")
    void test_precise_findByLevelAndSequence(IndexPerfContext ctx) {
        int level = 33;
        int sequence = 5; // exactly 1 document

        TimingResult result = this.timeQueries(
            () -> ctx.indexed().findByLevelAndSequence(level, sequence),
            () -> ctx.nonIndexed().findByLevelAndSequence(level, sequence),
            1
        );

        this.assertSpeedup(result, ctx.getSpeedupRatio(), "findByLevelAndSequence (1 doc)");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("indexableBackends")
    @DisplayName("PRECISE: Two doc lookup by level range (99 only)")
    void test_precise_findByLevelEquals99(IndexPerfContext ctx) {
        int level = 99; // only level 99 - exactly factor documents

        TimingResult result = this.timeQueries(
            () -> ctx.indexed().findByLevel(level),
            () -> ctx.nonIndexed().findByLevel(level),
            ctx.getFactor()
        );

        this.assertSpeedup(result, ctx.getSpeedupRatio(), "findByLevel=99 (factor docs)");
    }

    // ========================================================================
    // IDEAL CASE TESTS: Fully indexed queries (expect significant speedup)
    // ========================================================================

    @ParameterizedTest(name = "{0}")
    @MethodSource("indexableBackends")
    @DisplayName("IDEAL: Simple equality on indexed field (category)")
    void test_ideal_findByCategory(IndexPerfContext ctx) {
        String category = "category_5";

        TimingResult result = this.timeQueries(
            () -> ctx.indexed().findByCategory(category),
            () -> ctx.nonIndexed().findByCategory(category),
            ctx.getFactor()
        );

        this.assertSpeedup(result, ctx.getSpeedupRatio(), "findByCategory");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("indexableBackends")
    @DisplayName("IDEAL: Simple equality on indexed numeric field (level)")
    void test_ideal_findByLevel(IndexPerfContext ctx) {
        int level = 10;

        TimingResult result = this.timeQueries(
            () -> ctx.indexed().findByLevel(level),
            () -> ctx.nonIndexed().findByLevel(level),
            ctx.getFactor()
        );

        this.assertSpeedup(result, ctx.getSpeedupRatio(), "findByLevel");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("indexableBackends")
    @DisplayName("IDEAL: Simple equality on indexed boolean (active)")
    void test_ideal_findByActive(IndexPerfContext ctx) {
        boolean active = true;
        // only levels 15 and 85 are active (2%)
        int expectedCount = 2 * ctx.getFactor();

        TimingResult result = this.timeQueries(
            () -> ctx.indexed().findByActive(active),
            () -> ctx.nonIndexed().findByActive(active),
            expectedCount
        );

        this.assertSpeedup(result, ctx.getSpeedupRatio(), "findByActive");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("indexableBackends")
    @DisplayName("IDEAL: AND of two indexed fields (level AND active)")
    void test_ideal_findByLevelAndActive(IndexPerfContext ctx) {
        int level = 15; // level 15 is active
        boolean active = true;

        TimingResult result = this.timeQueries(
            () -> ctx.indexed().findByLevelAndActive(level, active),
            () -> ctx.nonIndexed().findByLevelAndActive(level, active),
            ctx.getFactor()
        );

        this.assertSpeedup(result, ctx.getSpeedupRatio(), "findByLevelAndActive");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("indexableBackends")
    @DisplayName("IDEAL: AND of three indexed fields")
    void test_ideal_findByCategoryAndLevelAndActive(IndexPerfContext ctx) {
        String category = "category_15";
        int level = 15;
        boolean active = true;

        TimingResult result = this.timeQueries(
            () -> ctx.indexed().findByCategoryAndLevelAndActive(category, level, active),
            () -> ctx.nonIndexed().findByCategoryAndLevelAndActive(category, level, active),
            ctx.getFactor()
        );

        this.assertSpeedup(result, ctx.getSpeedupRatio(), "findByCategoryAndLevelAndActive");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("indexableBackends")
    @DisplayName("IDEAL: OR of two indexed fields (level OR active)")
    void test_ideal_findByLevelOrActive(IndexPerfContext ctx) {
        int level = 15; // level 15 is active, so overlaps with active=true
        boolean active = true; // covers levels 15 and 85
        // level=15 overlaps with active=true, so total is just 2*factor (levels 15 and 85)
        int expectedCount = 2 * ctx.getFactor();

        TimingResult result = this.timeQueries(
            () -> ctx.indexed().findByLevelOrActive(level, active),
            () -> ctx.nonIndexed().findByLevelOrActive(level, active),
            expectedCount
        );

        this.assertSpeedup(result, ctx.getSpeedupRatio(), "findByLevelOrActive");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("indexableBackends")
    @DisplayName("IDEAL: Range query - greater than")
    void test_ideal_findByLevelGreaterThan(IndexPerfContext ctx) {
        int level = 97; // levels 98-99 match (2% selectivity)
        int expectedCount = 2 * ctx.getFactor();

        TimingResult result = this.timeQueries(
            () -> ctx.indexed().findByLevelGreaterThan(level),
            () -> ctx.nonIndexed().findByLevelGreaterThan(level),
            expectedCount
        );

        this.assertSpeedup(result, ctx.getSpeedupRatio(), "findByLevelGreaterThan");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("indexableBackends")
    @DisplayName("IDEAL: Range query - less than")
    void test_ideal_findByLevelLessThan(IndexPerfContext ctx) {
        int level = 2; // levels 0-1 match (2% selectivity)
        int expectedCount = 2 * ctx.getFactor();

        TimingResult result = this.timeQueries(
            () -> ctx.indexed().findByLevelLessThan(level),
            () -> ctx.nonIndexed().findByLevelLessThan(level),
            expectedCount
        );

        this.assertSpeedup(result, ctx.getSpeedupRatio(), "findByLevelLessThan");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("indexableBackends")
    @DisplayName("IDEAL: Range query - between")
    void test_ideal_findByLevelBetween(IndexPerfContext ctx) {
        int minLevel = 48;
        int maxLevel = 49; // levels 48-49 match (2% selectivity)
        int expectedCount = 2 * ctx.getFactor();

        TimingResult result = this.timeQueries(
            () -> ctx.indexed().findByLevelBetween(minLevel, maxLevel),
            () -> ctx.nonIndexed().findByLevelBetween(minLevel, maxLevel),
            expectedCount
        );

        this.assertSpeedup(result, ctx.getSpeedupRatio(), "findByLevelBetween");
    }

    // ========================================================================
    // DOUBLE FIELD TESTS: Verify floating-point indexing works correctly
    // ========================================================================

    @ParameterizedTest(name = "{0}")
    @MethodSource("indexableBackends")
    @DisplayName("IDEAL: Simple equality on indexed double field (rating)")
    void test_ideal_findByRating(IndexPerfContext ctx) {
        double rating = 15.0; // level 10 has rating 10 * 1.5 = 15.0
        int expectedCount = ctx.getFactor();

        TimingResult result = this.timeQueries(
            () -> ctx.indexed().findByRating(rating),
            () -> ctx.nonIndexed().findByRating(rating),
            expectedCount
        );

        this.assertSpeedup(result, ctx.getSpeedupRatio(), "findByRating (double)");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("indexableBackends")
    @DisplayName("IDEAL: Range query on double - greater than")
    void test_ideal_findByRatingGreaterThan(IndexPerfContext ctx) {
        double rating = 145.5; // levels 98-99 have ratings 147.0, 148.5 (2% selectivity)
        int expectedCount = 2 * ctx.getFactor();

        TimingResult result = this.timeQueries(
            () -> ctx.indexed().findByRatingGreaterThan(rating),
            () -> ctx.nonIndexed().findByRatingGreaterThan(rating),
            expectedCount
        );

        this.assertSpeedup(result, ctx.getSpeedupRatio(), "findByRatingGreaterThan (double)");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("indexableBackends")
    @DisplayName("IDEAL: Range query on double - between")
    void test_ideal_findByRatingBetween(IndexPerfContext ctx) {
        double minRating = 72.0; // level 48 = 72.0
        double maxRating = 73.5; // level 49 = 73.5 (2% selectivity)
        int expectedCount = 2 * ctx.getFactor();

        TimingResult result = this.timeQueries(
            () -> ctx.indexed().findByRatingBetween(minRating, maxRating),
            () -> ctx.nonIndexed().findByRatingBetween(minRating, maxRating),
            expectedCount
        );

        this.assertSpeedup(result, ctx.getSpeedupRatio(), "findByRatingBetween (double)");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("indexableBackends")
    @DisplayName("IDEAL: Count on indexed field")
    void test_ideal_countByCategory(IndexPerfContext ctx) {
        String category = "category_7";

        TimingResult result = this.timeCountQueries(
            () -> ctx.indexed().countByCategory(category),
            () -> ctx.nonIndexed().countByCategory(category),
            ctx.getFactor()
        );

        this.assertSpeedup(result, ctx.getSpeedupRatio(), "countByCategory");
    }

    // ========================================================================
    // MIXED CASE TESTS: Partially indexed (expect some speedup)
    // ========================================================================

    @ParameterizedTest(name = "{0}")
    @MethodSource("indexableBackends")
    @DisplayName("MIXED: AND with one indexed, one non-indexed field")
    void test_mixed_findByCategoryAndDescription(IndexPerfContext ctx) {
        String category = "category_3";
        String description = "desc_3";

        TimingResult result = this.timeQueries(
            () -> ctx.indexed().findByCategoryAndDescription(category, description),
            () -> ctx.nonIndexed().findByCategoryAndDescription(category, description),
            ctx.getFactor()
        );

        // Mixed queries should still benefit from index narrowing down candidates
        // Using a more lenient ratio since post-filtering adds overhead
        this.assertSpeedup(result, ctx.getSpeedupMixed(), "findByCategoryAndDescription (mixed)");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("indexableBackends")
    @DisplayName("MIXED: AND with indexed numeric + non-indexed")
    void test_mixed_findByLevelAndScore(IndexPerfContext ctx) {
        int level = 8;
        int score = 80; // score = level * 10

        TimingResult result = this.timeQueries(
            () -> ctx.indexed().findByLevelAndScore(level, score),
            () -> ctx.nonIndexed().findByLevelAndScore(level, score),
            ctx.getFactor()
        );

        this.assertSpeedup(result, ctx.getSpeedupMixed(), "findByLevelAndScore (mixed)");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("indexableBackends")
    @DisplayName("MIXED: AND with indexed boolean + non-indexed")
    void test_mixed_findByActiveAndDescription(IndexPerfContext ctx) {
        boolean active = true; // only levels 15 and 85
        String description = "desc_15"; // only level 15

        TimingResult result = this.timeQueries(
            () -> ctx.indexed().findByActiveAndDescription(active, description),
            () -> ctx.nonIndexed().findByActiveAndDescription(active, description),
            ctx.getFactor()
        );

        this.assertSpeedup(result, ctx.getSpeedupMixed(), "findByActiveAndDescription (mixed)");
    }

    // ========================================================================
    // NOT COVERED TESTS: Full scan for both (expect roughly equal time)
    // ========================================================================

    @ParameterizedTest(name = "{0}")
    @MethodSource("indexableBackends")
    @DisplayName("NOT COVERED: Query on non-indexed field only")
    void test_notCovered_findByDescription(IndexPerfContext ctx) {
        String description = "desc_11";

        TimingResult result = this.timeQueries(
            () -> ctx.indexed().findByDescription(description),
            () -> ctx.nonIndexed().findByDescription(description),
            ctx.getFactor()
        );

        // Both should do full scan, so times should be similar (ratio ~1.0)
        // Allow 50% variance in either direction
        this.assertSimilarPerformance(result, "findByDescription (not covered)");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("indexableBackends")
    @DisplayName("NOT COVERED: AND on two non-indexed fields")
    void test_notCovered_findByDescriptionAndScore(IndexPerfContext ctx) {
        String description = "desc_15";
        int score = 150;

        TimingResult result = this.timeQueries(
            () -> ctx.indexed().findByDescriptionAndScore(description, score),
            () -> ctx.nonIndexed().findByDescriptionAndScore(description, score),
            ctx.getFactor()
        );

        this.assertSimilarPerformance(result, "findByDescriptionAndScore (not covered)");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("indexableBackends")
    @DisplayName("NOT COVERED: OR with one non-indexed field (forces full scan)")
    void test_notCovered_findByCategoryOrDescription(IndexPerfContext ctx) {
        String category = "category_2";
        String description = "desc_17";
        // category_2 (factor) + desc_17 (factor), no overlap since different values
        int expectedCount = 2 * ctx.getFactor();

        TimingResult result = this.timeQueries(
            () -> ctx.indexed().findByCategoryOrDescription(category, description),
            () -> ctx.nonIndexed().findByCategoryOrDescription(category, description),
            expectedCount
        );

        this.assertSimilarPerformance(result, "findByCategoryOrDescription (not covered)");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("indexableBackends")
    @DisplayName("NOT COVERED: Count on non-indexed field")
    void test_notCovered_countByDescription(IndexPerfContext ctx) {
        String description = "desc_9";

        TimingResult result = this.timeCountQueries(
            () -> ctx.indexed().countByDescription(description),
            () -> ctx.nonIndexed().countByDescription(description),
            ctx.getFactor()
        );

        this.assertSimilarPerformance(result, "countByDescription (not covered)");
    }

    // ========================================================================
    // Helper classes and methods
    // ========================================================================

    static class TimingResult {
        final long indexedNanos;
        final long nonIndexedNanos;
        final int resultCount;
        final double ratio;

        TimingResult(long indexedNanos, long nonIndexedNanos, int resultCount) {
            this.indexedNanos = indexedNanos;
            this.nonIndexedNanos = nonIndexedNanos;
            this.resultCount = resultCount;
            this.ratio = (double) indexedNanos / nonIndexedNanos;
        }
    }

    @FunctionalInterface
    interface QuerySupplier {
        List<IndexTestEntity> query();
    }

    @FunctionalInterface
    interface CountSupplier {
        long count();
    }

    private TimingResult timeQueries(QuerySupplier indexed, QuerySupplier nonIndexed, int expectedCount) {
        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            indexed.query();
            nonIndexed.query();
        }
        // Time indexed (assertions outside loop to avoid AssertJ overhead)
        long indexedStart = System.nanoTime();
        int indexedCount = 0;
        List<IndexTestEntity> indexedResult = null;
        for (int i = 0; i < TIMED_ITERATIONS; i++) {
            indexedResult = indexed.query();
            indexedCount = indexedResult.size();
        }
        long indexedNanos = System.nanoTime() - indexedStart;
        assertThat(indexedResult).hasSize(expectedCount);

        // Time non-indexed (assertions outside loop to avoid AssertJ overhead)
        long nonIndexedStart = System.nanoTime();
        List<IndexTestEntity> nonIndexedResult = null;
        for (int i = 0; i < TIMED_ITERATIONS; i++) {
            nonIndexedResult = nonIndexed.query();
        }
        long nonIndexedNanos = System.nanoTime() - nonIndexedStart;
        assertThat(nonIndexedResult).hasSize(expectedCount);

        return new TimingResult(indexedNanos, nonIndexedNanos, indexedCount);
    }

    private TimingResult timeCountQueries(CountSupplier indexed, CountSupplier nonIndexed, long expectedCount) {
        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            indexed.count();
            nonIndexed.count();
        }

        // Time indexed
        long indexedStart = System.nanoTime();
        long indexedCount = 0;
        for (int i = 0; i < TIMED_ITERATIONS; i++) {
            indexedCount = indexed.count();
        }
        long indexedNanos = System.nanoTime() - indexedStart;
        assertThat(indexedCount).isEqualTo(expectedCount);

        // Time non-indexed
        long nonIndexedStart = System.nanoTime();
        long nonIndexedCount = 0;
        for (int i = 0; i < TIMED_ITERATIONS; i++) {
            nonIndexedCount = nonIndexed.count();
        }
        long nonIndexedNanos = System.nanoTime() - nonIndexedStart;
        assertThat(nonIndexedCount).isEqualTo(expectedCount);

        return new TimingResult(indexedNanos, nonIndexedNanos, (int) indexedCount);
    }

    private void assertSpeedup(TimingResult result, double maxRatio, String queryName) {
        System.out.printf("  %s: indexed=%dms, nonIndexed=%dms, ratio=%.2f, results=%d%n",
            queryName,
            result.indexedNanos / 1_000_000,
            result.nonIndexedNanos / 1_000_000,
            result.ratio,
            result.resultCount);

        assertThat(result.ratio)
            .as("Indexed %s should be at most %.0f%% of non-indexed time", queryName, maxRatio * 100)
            .isLessThanOrEqualTo(maxRatio);
    }

    private void assertSimilarPerformance(TimingResult result, String queryName) {
        System.out.printf("  %s: indexed=%dms, nonIndexed=%dms, ratio=%.2f, results=%d%n",
            queryName,
            result.indexedNanos / 1_000_000,
            result.nonIndexedNanos / 1_000_000,
            result.ratio,
            result.resultCount);

        // Allow variance between 0.5x and 2.0x (both do full scan)
        assertThat(result.ratio)
            .as("Non-covered %s should have similar performance (full scan for both)", queryName)
            .isBetween(0.5, 2.0);
    }
}
