package eu.okaeri.persistencetest;

import eu.okaeri.persistence.repository.DocumentRepository;
import eu.okaeri.persistence.repository.annotation.DocumentCollection;
import eu.okaeri.persistence.repository.annotation.DocumentIndex;
import eu.okaeri.persistencetest.fixtures.IndexTestEntity;

import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import static eu.okaeri.persistence.filter.condition.Condition.on;
import static eu.okaeri.persistence.filter.predicate.SimplePredicate.*;

/**
 * Test context for index performance tests.
 * Contains two repository types: indexed and non-indexed.
 */
public class IndexPerformanceTestContext {

    /**
     * Repository WITH indexes for performance comparison.
     * Indexes on: category, level, active, rating
     */
    @DocumentCollection(path = "indexed_entities", keyLength = 36, indexes = {
        @DocumentIndex(path = "category", maxLength = 255),
        @DocumentIndex(path = "level"),
        @DocumentIndex(path = "active"),
        @DocumentIndex(path = "rating")
    })
    public interface IndexedRepository extends DocumentRepository<UUID, IndexTestEntity> {

        // ===== IDEAL CASE: Fully indexed queries =====

        // Simple equality on indexed field
        List<IndexTestEntity> findByCategory(String category);

        List<IndexTestEntity> findByLevel(int level);

        List<IndexTestEntity> findByActive(boolean active);

        List<IndexTestEntity> findByRating(double rating);

        // AND of indexed fields
        List<IndexTestEntity> findByLevelAndActive(int level, boolean active);

        List<IndexTestEntity> findByCategoryAndLevel(String category, int level);

        List<IndexTestEntity> findByCategoryAndLevelAndActive(String category, int level, boolean active);

        // OR of indexed fields
        List<IndexTestEntity> findByLevelOrActive(int level, boolean active);

        List<IndexTestEntity> findByCategoryOrLevel(String category, int level);

        // Range queries via Query DSL
        default List<IndexTestEntity> findByLevelGreaterThan(int level) {
            return this.find(q -> q.where(on("level", gt(level)))).toList();
        }

        default List<IndexTestEntity> findByLevelLessThan(int level) {
            return this.find(q -> q.where(on("level", lt(level)))).toList();
        }

        default List<IndexTestEntity> findByLevelBetween(int minLevel, int maxLevel) {
            return this.find(q -> q.where(on("level", between(minLevel, maxLevel)))).toList();
        }

        // Range queries for double field (rating)
        default List<IndexTestEntity> findByRatingGreaterThan(double rating) {
            return this.find(q -> q.where(on("rating", gt(rating)))).toList();
        }

        default List<IndexTestEntity> findByRatingLessThan(double rating) {
            return this.find(q -> q.where(on("rating", lt(rating)))).toList();
        }

        default List<IndexTestEntity> findByRatingBetween(double minRating, double maxRating) {
            return this.find(q -> q.where(on("rating", between(minRating, maxRating)))).toList();
        }

        // ===== MIXED CASE: Partially indexed =====
        // AND with one indexed, one non-indexed → index narrows down, filter remaining

        List<IndexTestEntity> findByCategoryAndDescription(String category, String description);

        List<IndexTestEntity> findByLevelAndScore(int level, int score);

        List<IndexTestEntity> findByActiveAndDescription(boolean active, String description);

        // ===== NOT COVERED: Full scan required =====
        // All conditions on non-indexed fields

        List<IndexTestEntity> findByDescription(String description);

        List<IndexTestEntity> findByScore(int score);

        List<IndexTestEntity> findByDescriptionAndScore(String description, int score);

        // OR with non-indexed field (forces full scan)
        List<IndexTestEntity> findByCategoryOrDescription(String category, String description);

        // Streaming variants for completeness
        Stream<IndexTestEntity> streamByCategory(String category);

        Stream<IndexTestEntity> streamByDescription(String description);

        // Count variants
        long countByCategory(String category);

        long countByLevel(int level);

        long countByDescription(String description);
    }

    /**
     * Repository WITHOUT indexes for performance comparison baseline.
     * Same query patterns, but no indexes defined.
     */
    @DocumentCollection(path = "nonindexed_entities", keyLength = 36)
    public interface NonIndexedRepository extends DocumentRepository<UUID, IndexTestEntity> {

        // ===== Same queries as IndexedRepository but without index support =====

        // Simple equality
        List<IndexTestEntity> findByCategory(String category);

        List<IndexTestEntity> findByLevel(int level);

        List<IndexTestEntity> findByActive(boolean active);

        List<IndexTestEntity> findByRating(double rating);

        // AND combinations
        List<IndexTestEntity> findByLevelAndActive(int level, boolean active);

        List<IndexTestEntity> findByCategoryAndLevel(String category, int level);

        List<IndexTestEntity> findByCategoryAndLevelAndActive(String category, int level, boolean active);

        // OR combinations
        List<IndexTestEntity> findByLevelOrActive(int level, boolean active);

        List<IndexTestEntity> findByCategoryOrLevel(String category, int level);

        // Range queries via Query DSL
        default List<IndexTestEntity> findByLevelGreaterThan(int level) {
            return this.find(q -> q.where(on("level", gt(level)))).toList();
        }

        default List<IndexTestEntity> findByLevelLessThan(int level) {
            return this.find(q -> q.where(on("level", lt(level)))).toList();
        }

        default List<IndexTestEntity> findByLevelBetween(int minLevel, int maxLevel) {
            return this.find(q -> q.where(on("level", between(minLevel, maxLevel)))).toList();
        }

        // Range queries for double field (rating)
        default List<IndexTestEntity> findByRatingGreaterThan(double rating) {
            return this.find(q -> q.where(on("rating", gt(rating)))).toList();
        }

        default List<IndexTestEntity> findByRatingLessThan(double rating) {
            return this.find(q -> q.where(on("rating", lt(rating)))).toList();
        }

        default List<IndexTestEntity> findByRatingBetween(double minRating, double maxRating) {
            return this.find(q -> q.where(on("rating", between(minRating, maxRating)))).toList();
        }

        // Mixed (indexed field + non-indexed field)
        List<IndexTestEntity> findByCategoryAndDescription(String category, String description);

        List<IndexTestEntity> findByLevelAndScore(int level, int score);

        List<IndexTestEntity> findByActiveAndDescription(boolean active, String description);

        // Non-indexed only
        List<IndexTestEntity> findByDescription(String description);

        List<IndexTestEntity> findByScore(int score);

        List<IndexTestEntity> findByDescriptionAndScore(String description, int score);

        // OR with non-indexed
        List<IndexTestEntity> findByCategoryOrDescription(String category, String description);

        // Streaming
        Stream<IndexTestEntity> streamByCategory(String category);

        Stream<IndexTestEntity> streamByDescription(String description);

        // Count
        long countByCategory(String category);

        long countByLevel(int level);

        long countByDescription(String description);
    }
}
