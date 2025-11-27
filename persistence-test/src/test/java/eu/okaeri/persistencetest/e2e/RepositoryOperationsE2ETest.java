package eu.okaeri.persistencetest.e2e;

import eu.okaeri.persistencetest.fixtures.User;
import eu.okaeri.persistencetest.fixtures.UserProfile;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static eu.okaeri.persistence.filter.condition.Condition.on;
import static eu.okaeri.persistence.filter.predicate.SimplePredicate.eq;
import static eu.okaeri.persistence.filter.predicate.SimplePredicate.gt;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E Repository Operations Tests - runs ALL repository CRUD tests against ALL backends.
 * Tests basic operations like count, findByPath, deleteByPath, batch operations, etc.
 * <p>
 * Each test method runs against 6 backends:
 * - H2 (SQL)
 * - PostgreSQL (SQL with JSONB)
 * - MariaDB (SQL with JSON)
 * - MongoDB (NoSQL document)
 * - Redis (Key-value)
 * - FlatFile (File-based)
 */
public class RepositoryOperationsE2ETest extends E2ETestBase {

    protected static Stream<BackendTestContext> allBackendsWithContext() {
        return allBackends().map(backend -> {
            BackendTestContext btc = BackendTestContext.create(backend);
            List<UUID> userIds = new ArrayList<>();

            // User 1
            UUID id1 = UUID.randomUUID();
            User user1 = new User();
            user1.setPath(id1);
            user1.setName("alice");
            user1.setExp(100);
            btc.getUserRepository().save(user1);
            userIds.add(id1);

            // User 2
            UUID id2 = UUID.randomUUID();
            User user2 = new User();
            user2.setPath(id2);
            user2.setName("bob");
            user2.setExp(200);
            btc.getUserRepository().save(user2);
            userIds.add(id2);

            // User 3
            UUID id3 = UUID.randomUUID();
            User user3 = new User();
            user3.setPath(id3);
            user3.setName("charlie");
            user3.setExp(300);
            btc.getUserRepository().save(user3);
            userIds.add(id3);

            // Store user IDs for tests to access
            btc.getTestData().put("userIds", userIds);

            return btc;
        });
    }

    // Helper to get user IDs from context
    @SuppressWarnings("unchecked")
    private List<UUID> userIds(BackendTestContext btc) {
        return (List<UUID>) btc.getTestData().get("userIds");
    }

    // ========== Basic CRUD Tests ==========

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_count(BackendTestContext btc) {
        assertThat(btc.getUserRepository().count()).isEqualTo(3);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_find_by_path(BackendTestContext btc) {
        UUID id = userIds(btc).get(0);
        Optional<User> result = btc.getUserRepository().findByPath(id);

        assertThat(result).isPresent();
        assertThat(result.get().getName()).isEqualTo("alice");
        assertThat(result.get().getExp()).isEqualTo(100);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_find_by_path_missing(BackendTestContext btc) {
        UUID nonExistentId = UUID.randomUUID();
        Optional<User> result = btc.getUserRepository().findByPath(nonExistentId);

        assertThat(result).isEmpty();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_exists_by_path(BackendTestContext btc) {
        UUID existingId = userIds(btc).get(0);
        UUID nonExistentId = UUID.randomUUID();

        assertThat(btc.getUserRepository().existsByPath(existingId)).isTrue();
        assertThat(btc.getUserRepository().existsByPath(nonExistentId)).isFalse();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_delete_by_path(BackendTestContext btc) {
        UUID id = userIds(btc).get(0);

        assertThat(btc.getUserRepository().existsByPath(id)).isTrue();
        assertThat(btc.getUserRepository().deleteByPath(id)).isTrue();
        assertThat(btc.getUserRepository().existsByPath(id)).isFalse();
        assertThat(btc.getUserRepository().count()).isEqualTo(2);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_delete_by_path_missing(BackendTestContext btc) {
        UUID nonExistentId = UUID.randomUUID();
        long countBefore = btc.getUserRepository().count();

        assertThat(btc.getUserRepository().deleteByPath(nonExistentId)).isFalse();
        assertThat(btc.getUserRepository().count()).isEqualTo(countBefore);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_find_all(BackendTestContext btc) {
        Collection<User> users = btc.getUserRepository().findAll();

        assertThat(users).hasSize(3);
        assertThat(users.stream().map(User::getName).collect(Collectors.toList()))
            .containsExactlyInAnyOrder("alice", "bob", "charlie");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_stream_all(BackendTestContext btc) {
        List<User> users = btc.getUserRepository().streamAll().collect(Collectors.toList());

        assertThat(users).hasSize(3);
        assertThat(users.stream().map(User::getName).collect(Collectors.toList()))
            .containsExactlyInAnyOrder("alice", "bob", "charlie");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_find_one_by_condition(BackendTestContext btc) {
        Optional<User> user = btc.getUserRepository().findOne(on("name", eq("bob")));

        assertThat(user).isPresent();
        assertThat(user.get().getName()).isEqualTo("bob");
        assertThat(user.get().getExp()).isEqualTo(200);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_find_one_not_found(BackendTestContext btc) {
        Optional<User> user = btc.getUserRepository().findOne(on("name", eq("nonexistent")));
        assertThat(user).isEmpty();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_delete_all(BackendTestContext btc) {
        assertThat(btc.getUserRepository().count()).isEqualTo(3);
        assertThat(btc.getUserRepository().deleteAll()).isTrue();
        assertThat(btc.getUserRepository().count()).isEqualTo(0);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_find_or_create(BackendTestContext btc) {
        // Test with existing user
        UUID existingId = userIds(btc).get(0);
        User existing = btc.getUserRepository().findOrCreateByPath(existingId);
        assertThat(existing.getName()).isEqualTo("alice");
        assertThat(existing.getExp()).isEqualTo(100);

        // Test with new user
        UUID newId = UUID.randomUUID();
        User newUser = btc.getUserRepository().findOrCreateByPath(newId);
        assertThat(newUser).isNotNull();
        assertThat(newUser.getPath().getValue()).isEqualTo(newId.toString());
        assertThat(newUser.getName()).isNull(); // New empty user
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_find_or_create_all_by_path(BackendTestContext btc) {
        UUID existingId = userIds(btc).get(0);
        UUID newId1 = UUID.randomUUID();
        UUID newId2 = UUID.randomUUID();

        List<UUID> ids = Arrays.asList(existingId, newId1, newId2);
        Collection<User> users = btc.getUserRepository().findOrCreateAllByPath(ids);

        assertThat(users).hasSize(3);

        // Existing user should retain data
        User existing = users.stream()
            .filter(u -> u.getPath().getValue().equals(existingId.toString()))
            .findFirst().orElseThrow();
        assertThat(existing.getName()).isEqualTo("alice");

        // New users should be created but empty
        long newUserCount = users.stream()
            .filter(u -> u.getName() == null)
            .count();
        assertThat(newUserCount).isEqualTo(2);
    }

    // ========== Batch Operations Tests ==========

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_find_all_by_path(BackendTestContext btc) {
        List<UUID> idsToFind = Arrays.asList(userIds(btc).get(0), userIds(btc).get(2));
        Collection<User> users = btc.getUserRepository().findAllByPath(idsToFind);

        assertThat(users).hasSize(2);
        assertThat(users.stream().map(User::getName).collect(Collectors.toList()))
            .containsExactlyInAnyOrder("alice", "charlie");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_find_all_by_path_with_duplicates(BackendTestContext btc) {
        UUID id = userIds(btc).get(0);
        List<UUID> idsWithDuplicates = Arrays.asList(id, id, id);
        Collection<User> users = btc.getUserRepository().findAllByPath(idsWithDuplicates);

        // Should deduplicate - only return one user
        assertThat(users).hasSize(1);
        assertThat(users.iterator().next().getName()).isEqualTo("alice");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_delete_all_by_path(BackendTestContext btc) {
        List<UUID> idsToDelete = Arrays.asList(userIds(btc).get(0), userIds(btc).get(1));

        long deletedCount = btc.getUserRepository().deleteAllByPath(idsToDelete);
        assertThat(deletedCount).isEqualTo(2);
        assertThat(btc.getUserRepository().count()).isEqualTo(1);

        // Verify only charlie remains
        Collection<User> remaining = btc.getUserRepository().findAll();
        assertThat(remaining).hasSize(1);
        assertThat(remaining.iterator().next().getName()).isEqualTo("charlie");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_save_all_bulk(BackendTestContext btc) {
        // Create multiple new users
        List<User> newUsers = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            User user = new User();
            user.setPath(UUID.randomUUID());
            user.setName("bulk_user_" + i);
            user.setExp(i * 100);
            newUsers.add(user);
        }

        // Bulk save
        btc.getUserRepository().saveAll(newUsers);

        // Verify ALL were saved (not just the last one!)
        assertThat(btc.getUserRepository().count()).isEqualTo(13); // 3 initial + 10 new

        // Verify each user individually
        for (User user : newUsers) {
            Optional<User> found = btc.getUserRepository().findByPath(user.getPath().toUUID());
            assertThat(found).isPresent();
            assertThat(found.get().getName()).isEqualTo(user.getName());
            assertThat(found.get().getExp()).isEqualTo(user.getExp());
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_delete_by_filter(BackendTestContext btc) {
        // Delete users with exp > 150
        long deleted = btc.getUserRepository().delete(filter -> filter
            .where(on("exp", gt(150))));

        assertThat(deleted).isEqualTo(2); // bob (200) and charlie (300)
        assertThat(btc.getUserRepository().count()).isEqualTo(1);

        // Only alice should remain
        User remaining = btc.getUserRepository().findAll().iterator().next();
        assertThat(remaining.getName()).isEqualTo("alice");
    }

    // ========== Edge Cases Tests ==========

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_crud_lifecycle(BackendTestContext btc) {
        // Create
        UUID newId = UUID.randomUUID();
        User newUser = new User();
        newUser.setPath(newId);
        newUser.setName("diana");
        newUser.setExp(400);
        btc.getUserRepository().save(newUser);

        // Read
        User found = btc.getUserRepository().findByPath(newId).orElse(null);
        assertThat(found).isNotNull();
        assertThat(found.getName()).isEqualTo("diana");

        // Update
        found.setExp(500);
        btc.getUserRepository().save(found);

        User updated = btc.getUserRepository().findByPath(newId).orElse(null);
        assertThat(updated).isNotNull();
        assertThat(updated.getExp()).isEqualTo(500);

        // Delete
        assertThat(btc.getUserRepository().deleteByPath(newId)).isTrue();
        assertThat(btc.getUserRepository().existsByPath(newId)).isFalse();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_batch_empty_list(BackendTestContext btc) {
        // findAllByPath with empty list - should return empty, not throw
        // Note: Some backends may throw on empty list, that's acceptable behavior
        try {
            Collection<User> found = btc.getUserRepository().findAllByPath(Collections.emptyList());
            assertThat(found).isEmpty();
        } catch (IllegalArgumentException e) {
            // MongoDB throws IllegalArgumentException: "Fields must not be empty"
            // This is acceptable behavior for empty batch operations
            assertThat(e.getMessage()).contains("must not be empty");
        }

        // deleteAllByPath with empty list - should return 0, not throw
        try {
            long deleted = btc.getUserRepository().deleteAllByPath(Collections.emptyList());
            assertThat(deleted).isEqualTo(0);
        } catch (IllegalArgumentException e) {
            // MongoDB throws IllegalArgumentException for empty batch operations
            // This is acceptable behavior
            assertThat(e.getMessage()).contains("must not be empty");
        }

        // Verify count unchanged
        assertThat(btc.getUserRepository().count()).isEqualTo(3);
    }

    // ========== Document save/load Tests ==========

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_document_save_and_load(BackendTestContext btc) {
        UUID id = UUID.randomUUID();
        User user = btc.getUserRepository().findOrCreateByPath(id);
        user.setName("direct_save");
        user.setExp(999);

        // Use Document.save() directly
        user.save();

        // Verify via repository
        User found = btc.getUserRepository().findByPath(id).orElse(null);
        assertThat(found).isNotNull();
        assertThat(found.getName()).isEqualTo("direct_save");
        assertThat(found.getExp()).isEqualTo(999);

        // Modify in database via repository
        found.setExp(1000);
        btc.getUserRepository().save(found);

        // Use Document.load() to refresh
        user.load();
        assertThat(user.getExp()).isEqualTo(1000);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_persistence_delete_all_collections(BackendTestContext btc) {
        // Verify both collections have data
        assertThat(btc.getUserRepository().count()).isEqualTo(3);

        // Add data to profile repository
        UserProfile profile = new UserProfile();
        profile.setPath(UUID.randomUUID());
        profile.setName("test_profile");
        btc.getProfileRepository().save(profile);
        assertThat(btc.getProfileRepository().count()).isEqualTo(1);

        // Delete ALL collections via persistence
        long deleted = btc.getUserRepository().getPersistence().deleteAll();
        assertThat(deleted).isGreaterThanOrEqualTo(2); // At least 2 collections

        // Verify all collections are empty
        assertThat(btc.getUserRepository().count()).isEqualTo(0);
        assertThat(btc.getProfileRepository().count()).isEqualTo(0);
    }
}
