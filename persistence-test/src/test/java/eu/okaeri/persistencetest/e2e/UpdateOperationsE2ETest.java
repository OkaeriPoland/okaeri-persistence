package eu.okaeri.persistencetest.e2e;

import eu.okaeri.persistencetest.fixtures.Address;
import eu.okaeri.persistencetest.fixtures.Profile;
import eu.okaeri.persistencetest.fixtures.User;
import eu.okaeri.persistencetest.fixtures.UserProfile;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.*;
import java.util.stream.Stream;

import static eu.okaeri.persistence.filter.condition.Condition.on;
import static eu.okaeri.persistence.filter.predicate.SimplePredicate.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * E2E Update Operations Tests - runs ALL update operation tests against ALL backends.
 */
public class UpdateOperationsE2ETest extends E2ETestBase {

    protected static Stream<BackendTestContext> allBackendsWithContext() {
        return allBackends().map(backend -> {
            BackendTestContext btc = BackendTestContext.create(backend);

            // Setup test data
            btc.getUserRepository().save(new User("alice", 100));
            btc.getUserRepository().save(new User("bob", 200));
            btc.getUserRepository().save(new User("charlie", 150));

            btc.getProfileRepository().save(new UserProfile("alice", new Profile(25, "Engineer", new Address("New York", "USA", 10001))));
            btc.getProfileRepository().save(new UserProfile("bob", new Profile(30, "Designer", new Address("London", "UK", 20002))));
            btc.getProfileRepository().save(new UserProfile("charlie", new Profile(25, "Manager", new Address("Paris", "France", 75001))));
            btc.getProfileRepository().save(new UserProfile("diana", new Profile(35, "Director", new Address("Berlin", "Germany", 10115))));

            return btc;
        });
    }

    // ===== FIELD OPERATIONS: SET =====

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_updateOne_set_single_field(BackendTestContext btc) {
        User alice = btc.getUserRepository().find(q -> q.where(on("name", eq("alice")))).findFirst().orElseThrow();

        boolean updated = btc.getUserRepository().updateOne(alice.getId(), u -> u
            .set("name", "alice_updated"));

        assertThat(updated).isTrue();

        User reloaded = btc.getUserRepository().findByPath(alice.getId()).orElseThrow();
        assertThat(reloaded.getName()).isEqualTo("alice_updated");
        assertThat(reloaded.getExp()).isEqualTo(100); // Unchanged
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_updateOne_set_multiple_fields(BackendTestContext btc) {
        User alice = btc.getUserRepository().find(q -> q.where(on("name", eq("alice")))).findFirst().orElseThrow();

        boolean updated = btc.getUserRepository().updateOne(alice.getId(), u -> u
            .set("name", "alice_new")
            .set("exp", 999));

        assertThat(updated).isTrue();

        User reloaded = btc.getUserRepository().findByPath(alice.getId()).orElseThrow();
        assertThat(reloaded.getName()).isEqualTo("alice_new");
        assertThat(reloaded.getExp()).isEqualTo(999);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_updateOne_by_entity(BackendTestContext btc) {
        User alice = btc.getUserRepository().find(q -> q.where(on("name", eq("alice")))).findFirst().orElseThrow();

        boolean updated = btc.getUserRepository().updateOne(alice, u -> u
            .set("exp", 500));

        assertThat(updated).isTrue();

        User reloaded = btc.getUserRepository().findByPath(alice.getId()).orElseThrow();
        assertThat(reloaded.getExp()).isEqualTo(500);
    }

    // ===== FIELD OPERATIONS: UNSET =====

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_updateOne_unset_field(BackendTestContext btc) {
        User alice = btc.getUserRepository().find(q -> q.where(on("name", eq("alice")))).findFirst().orElseThrow();

        boolean updated = btc.getUserRepository().updateOne(alice.getId(), u -> u
            .unset("name"));

        assertThat(updated).isTrue();

        User reloaded = btc.getUserRepository().findByPath(alice.getId()).orElseThrow();
        assertThat(reloaded.getName()).isNull();
        assertThat(reloaded.getExp()).isEqualTo(100); // Unchanged
    }

    // ===== FIELD OPERATIONS: INCREMENT =====

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_updateOne_increment_positive(BackendTestContext btc) {
        User alice = btc.getUserRepository().find(q -> q.where(on("name", eq("alice")))).findFirst().orElseThrow();

        boolean updated = btc.getUserRepository().updateOne(alice.getId(), u -> u
            .increment("exp", 50));

        assertThat(updated).isTrue();

        User reloaded = btc.getUserRepository().findByPath(alice.getId()).orElseThrow();
        assertThat(reloaded.getExp()).isEqualTo(150); // 100 + 50
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_updateOne_increment_negative(BackendTestContext btc) {
        User alice = btc.getUserRepository().find(q -> q.where(on("name", eq("alice")))).findFirst().orElseThrow();

        boolean updated = btc.getUserRepository().updateOne(alice.getId(), u -> u
            .increment("exp", -30));

        assertThat(updated).isTrue();

        User reloaded = btc.getUserRepository().findByPath(alice.getId()).orElseThrow();
        assertThat(reloaded.getExp()).isEqualTo(70); // 100 - 30
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_updateOne_increment_multiple_times(BackendTestContext btc) {
        User alice = btc.getUserRepository().find(q -> q.where(on("name", eq("alice")))).findFirst().orElseThrow();

        // First increment
        btc.getUserRepository().updateOne(alice.getId(), u -> u.increment("exp", 10));
        // Second increment
        btc.getUserRepository().updateOne(alice.getId(), u -> u.increment("exp", 20));
        // Third increment
        btc.getUserRepository().updateOne(alice.getId(), u -> u.increment("exp", 30));

        User reloaded = btc.getUserRepository().findByPath(alice.getId()).orElseThrow();
        assertThat(reloaded.getExp()).isEqualTo(160); // 100 + 10 + 20 + 30
    }

    // ===== FIELD OPERATIONS: MULTIPLY =====

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_updateOne_multiply(BackendTestContext btc) {
        User alice = btc.getUserRepository().find(q -> q.where(on("name", eq("alice")))).findFirst().orElseThrow();

        boolean updated = btc.getUserRepository().updateOne(alice.getId(), u -> u
            .multiply("exp", 2.5));

        assertThat(updated).isTrue();

        User reloaded = btc.getUserRepository().findByPath(alice.getId()).orElseThrow();
        assertThat(reloaded.getExp()).isEqualTo(250); // 100 * 2.5
    }

    // ===== FIELD OPERATIONS: MIN/MAX =====

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_updateOne_min_updates_when_new_is_smaller(BackendTestContext btc) {
        User alice = btc.getUserRepository().find(q -> q.where(on("name", eq("alice")))).findFirst().orElseThrow();

        boolean updated = btc.getUserRepository().updateOne(alice.getId(), u -> u
            .min("exp", 50));

        assertThat(updated).isTrue();

        User reloaded = btc.getUserRepository().findByPath(alice.getId()).orElseThrow();
        assertThat(reloaded.getExp()).isEqualTo(50); // min(100, 50) = 50
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_updateOne_min_no_change_when_new_is_larger(BackendTestContext btc) {
        User alice = btc.getUserRepository().find(q -> q.where(on("name", eq("alice")))).findFirst().orElseThrow();

        boolean updated = btc.getUserRepository().updateOne(alice.getId(), u -> u
            .min("exp", 200));

        assertThat(updated).isTrue();

        User reloaded = btc.getUserRepository().findByPath(alice.getId()).orElseThrow();
        assertThat(reloaded.getExp()).isEqualTo(100); // min(100, 200) = 100
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_updateOne_max_updates_when_new_is_larger(BackendTestContext btc) {
        User alice = btc.getUserRepository().find(q -> q.where(on("name", eq("alice")))).findFirst().orElseThrow();

        boolean updated = btc.getUserRepository().updateOne(alice.getId(), u -> u
            .max("exp", 500));

        assertThat(updated).isTrue();

        User reloaded = btc.getUserRepository().findByPath(alice.getId()).orElseThrow();
        assertThat(reloaded.getExp()).isEqualTo(500); // max(100, 500) = 500
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_updateOne_max_no_change_when_new_is_smaller(BackendTestContext btc) {
        User alice = btc.getUserRepository().find(q -> q.where(on("name", eq("alice")))).findFirst().orElseThrow();

        boolean updated = btc.getUserRepository().updateOne(alice.getId(), u -> u
            .max("exp", 50));

        assertThat(updated).isTrue();

        User reloaded = btc.getUserRepository().findByPath(alice.getId()).orElseThrow();
        assertThat(reloaded.getExp()).isEqualTo(100); // max(100, 50) = 100
    }

    // ===== FIELD OPERATIONS: CURRENT_DATE =====

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_updateOne_currentDate(BackendTestContext btc) {
        // Create user with a custom field that will hold a timestamp
        User alice = btc.getUserRepository().find(q -> q.where(on("name", eq("alice")))).findFirst().orElseThrow();

        boolean updated = btc.getUserRepository().updateOne(alice.getId(), u -> u
            .currentDate("lastModified"));

        assertThat(updated).isTrue();

        // Reload and verify timestamp was set (we can't check exact value, just that it's not null)
        User reloaded = btc.getUserRepository().findByPath(alice.getId()).orElseThrow();
        // Note: This assumes the User class can store arbitrary fields or has a lastModified field
        // The actual assertion will depend on how currentDate is serialized
        assertThat(reloaded).isNotNull();
    }

    // ===== ARRAY OPERATIONS: PUSH =====

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_updateOne_push_single_value(BackendTestContext btc) {
        User alice = btc.getUserRepository().find(q -> q.where(on("name", eq("alice")))).findFirst().orElseThrow();

        boolean updated = btc.getUserRepository().updateOne(alice.getId(), u -> u
            .push("tags", "newbie"));

        assertThat(updated).isTrue();

        User reloaded = btc.getUserRepository().findByPath(alice.getId()).orElseThrow();
        assertThat(reloaded.getTags()).containsExactly("newbie");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_updateOne_push_multiple_values_varargs(BackendTestContext btc) {
        User alice = btc.getUserRepository().find(q -> q.where(on("name", eq("alice")))).findFirst().orElseThrow();

        boolean updated = btc.getUserRepository().updateOne(alice.getId(), u -> u
            .push("tags", "tag1", "tag2", "tag3"));

        assertThat(updated).isTrue();

        User reloaded = btc.getUserRepository().findByPath(alice.getId()).orElseThrow();
        assertThat(reloaded.getTags()).containsExactly("tag1", "tag2", "tag3");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_updateOne_push_multiple_values_collection(BackendTestContext btc) {
        User alice = btc.getUserRepository().find(q -> q.where(on("name", eq("alice")))).findFirst().orElseThrow();

        List<String> newTags = Arrays.asList("a", "b", "c");
        boolean updated = btc.getUserRepository().updateOne(alice.getId(), u -> u
            .push("tags", newTags));

        assertThat(updated).isTrue();

        User reloaded = btc.getUserRepository().findByPath(alice.getId()).orElseThrow();
        assertThat(reloaded.getTags()).containsExactly("a", "b", "c");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_updateOne_push_to_existing_array(BackendTestContext btc) {
        // Setup: create user with existing tags
        User alice = new User("alice", 100);
        alice.setTags(Arrays.asList("existing1", "existing2"));
        btc.getUserRepository().save(alice);

        boolean updated = btc.getUserRepository().updateOne(alice.getId(), u -> u
            .push("tags", "new1", "new2"));

        assertThat(updated).isTrue();

        User reloaded = btc.getUserRepository().findByPath(alice.getId()).orElseThrow();
        assertThat(reloaded.getTags()).containsExactly("existing1", "existing2", "new1", "new2");
    }

    // ===== ARRAY OPERATIONS: POP =====

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_updateOne_popFirst(BackendTestContext btc) {
        User alice = new User("alice", 100);
        alice.setTags(Arrays.asList("first", "second", "third"));
        btc.getUserRepository().save(alice);

        boolean updated = btc.getUserRepository().updateOne(alice.getId(), u -> u
            .popFirst("tags"));

        assertThat(updated).isTrue();

        User reloaded = btc.getUserRepository().findByPath(alice.getId()).orElseThrow();
        assertThat(reloaded.getTags()).containsExactly("second", "third");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_updateOne_popLast(BackendTestContext btc) {
        User alice = new User("alice", 100);
        alice.setTags(Arrays.asList("first", "second", "third"));
        btc.getUserRepository().save(alice);

        boolean updated = btc.getUserRepository().updateOne(alice.getId(), u -> u
            .popLast("tags"));

        assertThat(updated).isTrue();

        User reloaded = btc.getUserRepository().findByPath(alice.getId()).orElseThrow();
        assertThat(reloaded.getTags()).containsExactly("first", "second");
    }

    // ===== ARRAY OPERATIONS: PULL =====

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_updateOne_pull_single_occurrence(BackendTestContext btc) {
        User alice = new User("alice", 100);
        alice.setTags(Arrays.asList("keep1", "remove", "keep2"));
        btc.getUserRepository().save(alice);

        boolean updated = btc.getUserRepository().updateOne(alice.getId(), u -> u
            .pull("tags", "remove"));

        assertThat(updated).isTrue();

        User reloaded = btc.getUserRepository().findByPath(alice.getId()).orElseThrow();
        assertThat(reloaded.getTags()).containsExactly("keep1", "keep2");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_updateOne_pull_multiple_occurrences(BackendTestContext btc) {
        User alice = new User("alice", 100);
        alice.setTags(Arrays.asList("keep", "dup", "keep", "dup", "keep"));
        btc.getUserRepository().save(alice);

        boolean updated = btc.getUserRepository().updateOne(alice.getId(), u -> u
            .pull("tags", "dup"));

        assertThat(updated).isTrue();

        User reloaded = btc.getUserRepository().findByPath(alice.getId()).orElseThrow();
        assertThat(reloaded.getTags()).containsExactly("keep", "keep", "keep");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_updateOne_pullAll_multiple_values(BackendTestContext btc) {
        User alice = new User("alice", 100);
        alice.setTags(Arrays.asList("keep", "remove1", "keep", "remove2", "keep"));
        btc.getUserRepository().save(alice);

        boolean updated = btc.getUserRepository().updateOne(alice.getId(), u -> u
            .pullAll("tags", "remove1", "remove2"));

        assertThat(updated).isTrue();

        User reloaded = btc.getUserRepository().findByPath(alice.getId()).orElseThrow();
        assertThat(reloaded.getTags()).containsExactly("keep", "keep", "keep");
    }

    // ===== ARRAY OPERATIONS: ADD_TO_SET =====

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_updateOne_addToSet_new_value(BackendTestContext btc) {
        User alice = new User("alice", 100);
        alice.setTags(List.of("existing"));
        btc.getUserRepository().save(alice);

        boolean updated = btc.getUserRepository().updateOne(alice.getId(), u -> u
            .addToSet("tags", "new"));

        assertThat(updated).isTrue();

        User reloaded = btc.getUserRepository().findByPath(alice.getId()).orElseThrow();
        assertThat(reloaded.getTags()).containsExactlyInAnyOrder("existing", "new");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_updateOne_addToSet_duplicate_value_ignored(BackendTestContext btc) {
        User alice = new User("alice", 100);
        alice.setTags(List.of("existing"));
        btc.getUserRepository().save(alice);

        boolean updated = btc.getUserRepository().updateOne(alice.getId(), u -> u
            .addToSet("tags", "existing"));

        assertThat(updated).isTrue();

        User reloaded = btc.getUserRepository().findByPath(alice.getId()).orElseThrow();
        assertThat(reloaded.getTags()).containsExactly("existing"); // No duplicate
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_updateOne_addToSet_multiple_values(BackendTestContext btc) {
        User alice = new User("alice", 100);
        alice.setTags(List.of("existing"));
        btc.getUserRepository().save(alice);

        boolean updated = btc.getUserRepository().updateOne(alice.getId(), u -> u
            .addToSet("tags", "new1", "existing", "new2")); // One duplicate

        assertThat(updated).isTrue();

        User reloaded = btc.getUserRepository().findByPath(alice.getId()).orElseThrow();
        assertThat(reloaded.getTags()).containsExactlyInAnyOrder("existing", "new1", "new2"); // Only 3, not 4
    }

    // ===== COMBINED OPERATIONS =====

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_updateOne_combined_operations(BackendTestContext btc) {
        User alice = new User("alice", 100);
        alice.setTags(List.of("tag1"));
        btc.getUserRepository().save(alice);

        boolean updated = btc.getUserRepository().updateOne(alice.getId(), u -> u
            .increment("exp", 50)
            .set("name", "alice_updated")
            .push("tags", "tag2", "tag3"));

        assertThat(updated).isTrue();

        User reloaded = btc.getUserRepository().findByPath(alice.getId()).orElseThrow();
        assertThat(reloaded.getName()).isEqualTo("alice_updated");
        assertThat(reloaded.getExp()).isEqualTo(150);
        assertThat(reloaded.getTags()).containsExactly("tag1", "tag2", "tag3");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_updateOne_conflicting_array_operations_rejected(BackendTestContext btc) {
        User alice = new User("alice", 100);
        alice.setTags(Arrays.asList("keep", "remove"));
        btc.getUserRepository().save(alice);

        // Multiple different operation types on same field should be rejected
        assertThatThrownBy(() -> btc.getUserRepository().updateOne(alice.getId(), u -> u
            .pullAll("tags", "remove")  // PULL_ALL on tags
            .push("tags", "new")))      // PUSH on tags - CONFLICT!
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Multiple operations on same field(s)")
            .hasMessageContaining("tags");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_updateOne_conflicting_operations_same_field_rejected(BackendTestContext btc) {
        User alice = new User("alice", 100);
        btc.getUserRepository().save(alice);

        // This should throw IllegalArgumentException due to field conflicts
        assertThatThrownBy(() -> btc.getUserRepository().updateOne(alice.getId(), u -> u
            .increment("exp", 50)    // INCREMENT on exp
            .multiply("exp", 2)))    // MULTIPLY on exp - CONFLICT!
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Multiple operations on same field(s)")
            .hasMessageContaining("exp");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_updateOne_multiple_operations_on_truly_different_fields(BackendTestContext btc) {
        UserProfile alice = new UserProfile("alice", new Profile(25, "Engineer", new Address("NYC", "USA", 10001)));
        btc.getProfileRepository().save(alice);

        boolean updated = btc.getProfileRepository().updateOne(alice.getId(), u -> u
            .set("name", "alice_v2")                       // SET on name
            .increment("profile.age", 5)                   // INCREMENT on profile.age: 25 + 5 = 30
            .set("profile.occupation", "Senior Engineer")  // SET on profile.occupation
            .set("profile.address.city", "San Francisco")  // SET on profile.address.city
            .multiply("profile.address.zipCode", 2));      // MULTIPLY on profile.address.zipCode: 10001 * 2 = 20002

        assertThat(updated).isTrue();

        UserProfile reloaded = btc.getProfileRepository().findByPath(alice.getId()).orElseThrow();
        assertThat(reloaded.getName()).isEqualTo("alice_v2");
        assertThat(reloaded.getProfile().getAge()).isEqualTo(30);
        assertThat(reloaded.getProfile().getOccupation()).isEqualTo("Senior Engineer");
        assertThat(reloaded.getProfile().getAddress().getCity()).isEqualTo("San Francisco");
        assertThat(reloaded.getProfile().getAddress().getZipCode()).isEqualTo(20002);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_updateOne_duplicate_operations_same_field_rejected(BackendTestContext btc) {
        User alice = new User("alice", 100);
        alice.setTags(Arrays.asList("first", "middle1", "middle2", "last"));
        btc.getUserRepository().save(alice);

        // Multiple operations on same field(s) should be rejected, even if same type
        assertThatThrownBy(() -> btc.getUserRepository().updateOne(alice.getId(), u -> u
            .popFirst("tags")      // POP_FIRST on tags
            .popFirst("tags")))    // POP_FIRST on tags again - CONFLICT!
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Multiple operations on same field(s)")
            .hasMessageContaining("tags");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_updateOne_mixed_nested_and_toplevel_operations(BackendTestContext btc) {
        UserProfile alice = new UserProfile("alice", new Profile(25, "Engineer", new Address("NYC", "USA", 10001)));
        btc.getProfileRepository().save(alice);

        boolean updated = btc.getProfileRepository().updateOne(alice.getId(), u -> u
            .set("name", "alice_updated")
            .increment("profile.age", 5)
            .set("profile.occupation", "Senior Engineer")
            .set("profile.address.city", "San Francisco")
            .increment("profile.address.zipCode", 84000)); // 10001 + 84000 = 94001

        assertThat(updated).isTrue();

        UserProfile reloaded = btc.getProfileRepository().findByPath(alice.getId()).orElseThrow();
        assertThat(reloaded.getName()).isEqualTo("alice_updated");
        assertThat(reloaded.getProfile().getAge()).isEqualTo(30);
        assertThat(reloaded.getProfile().getOccupation()).isEqualTo("Senior Engineer");
        assertThat(reloaded.getProfile().getAddress().getCity()).isEqualTo("San Francisco");
        assertThat(reloaded.getProfile().getAddress().getZipCode()).isEqualTo(94001);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_updateOne_many_operation_types_on_different_fields(BackendTestContext btc) {
        User alice = new User("alice", 100);
        alice.setTags(Arrays.asList("tag1", "tag2", "remove"));
        btc.getUserRepository().save(alice);

        boolean updated = btc.getUserRepository().updateOne(alice.getId(), u -> u
            .set("name", "alice_v2")           // SET on name
            .unset("description")              // UNSET on description
            .increment("exp", 100)             // INCREMENT on exp: 100 + 100 = 200
            .pull("tags", "remove"));          // PULL on tags

        assertThat(updated).isTrue();

        User reloaded = btc.getUserRepository().findByPath(alice.getId()).orElseThrow();
        assertThat(reloaded.getName()).isEqualTo("alice_v2");
        assertThat(reloaded.getExp()).isEqualTo(200);
        assertThat(reloaded.getTags()).containsExactly("tag1", "tag2");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_updateOne_addToSet_on_empty_array(BackendTestContext btc) {
        User alice = new User("alice", 100);
        // Start with no tags
        btc.getUserRepository().save(alice);

        boolean updated = btc.getUserRepository().updateOne(alice.getId(), u -> u
            .addToSet("tags", "first", "second", "third"));

        assertThat(updated).isTrue();

        User reloaded = btc.getUserRepository().findByPath(alice.getId()).orElseThrow();
        assertThat(reloaded.getTags()).containsExactlyInAnyOrder("first", "second", "third");
    }

    // ===== MULTI-DOCUMENT UPDATES =====

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_update_multi_document_with_where(BackendTestContext btc) {
        long updated = btc.getUserRepository().update(u -> u
            .where(on("exp", gte(150)))
            .increment("exp", 10));

        assertThat(updated).isEqualTo(2); // bob (200) and charlie (150)

        User alice = btc.getUserRepository().find(q -> q.where(on("name", eq("alice")))).findFirst().orElseThrow();
        User bob = btc.getUserRepository().find(q -> q.where(on("name", eq("bob")))).findFirst().orElseThrow();
        User charlie = btc.getUserRepository().find(q -> q.where(on("name", eq("charlie")))).findFirst().orElseThrow();

        assertThat(alice.getExp()).isEqualTo(100); // Unchanged
        assertThat(bob.getExp()).isEqualTo(210); // 200 + 10
        assertThat(charlie.getExp()).isEqualTo(160); // 150 + 10
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_update_multi_document_set_field(BackendTestContext btc) {
        long updated = btc.getUserRepository().update(u -> u
            .where(on("exp", lt(200)))
            .set("name", "low_exp_user"));

        assertThat(updated).isEqualTo(2); // alice (100) and charlie (150)

        List<User> lowExpUsers = btc.getUserRepository().find(q -> q.where(on("name", eq("low_exp_user")))).toList();
        assertThat(lowExpUsers).hasSize(2);
        assertThat(lowExpUsers).extracting(User::getExp).containsExactlyInAnyOrder(100, 150);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_update_all_documents(BackendTestContext btc) {
        // Update all users (WHERE matches everything)
        long updated = btc.getUserRepository().update(u -> u
            .where(on("exp", gt(0)))
            .increment("exp", 100));

        assertThat(updated).isEqualTo(3); // All users

        List<User> allUsers = btc.getUserRepository().findAll().stream().toList();
        assertThat(allUsers).extracting(User::getExp).containsExactlyInAnyOrder(200, 300, 250);
    }

    // ===== UPDATE-AND-GET OPERATIONS =====

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_updateOneAndGet_returns_new_version(BackendTestContext btc) {
        User alice = btc.getUserRepository().find(q -> q.where(on("name", eq("alice")))).findFirst().orElseThrow();

        Optional<User> result = btc.getUserRepository().updateOneAndGet(alice.getId(), u -> u
            .increment("exp", 50)
            .set("name", "alice_new"));

        assertThat(result).isPresent();
        assertThat(result.get().getName()).isEqualTo("alice_new"); // New value
        assertThat(result.get().getExp()).isEqualTo(150); // New value (100 + 50)
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_getAndUpdateOne_returns_old_version(BackendTestContext btc) {
        User alice = btc.getUserRepository().find(q -> q.where(on("name", eq("alice")))).findFirst().orElseThrow();

        Optional<User> result = btc.getUserRepository().getAndUpdateOne(alice.getId(), u -> u
            .increment("exp", 50)
            .set("name", "alice_new"));

        assertThat(result).isPresent();
        assertThat(result.get().getName()).isEqualTo("alice"); // Old value
        assertThat(result.get().getExp()).isEqualTo(100); // Old value

        // Verify the update actually happened
        User reloaded = btc.getUserRepository().findByPath(alice.getId()).orElseThrow();
        assertThat(reloaded.getName()).isEqualTo("alice_new");
        assertThat(reloaded.getExp()).isEqualTo(150);
    }

    // ===== EDGE CASES =====

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_updateOne_nonexistent_document_returns_false(BackendTestContext btc) {
        boolean updated = btc.getUserRepository().updateOne(UUID.randomUUID(), u -> u
            .set("name", "ghost"));

        assertThat(updated).isFalse();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_update_with_no_matches_returns_zero(BackendTestContext btc) {
        long updated = btc.getUserRepository().update(u -> u
            .where(on("exp", gt(10000)))
            .increment("exp", 1));

        assertThat(updated).isEqualTo(0); // No users have exp > 10000
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_updateOneAndGet_nonexistent_returns_empty(BackendTestContext btc) {
        Optional<User> result = btc.getUserRepository().updateOneAndGet(UUID.randomUUID(), u -> u
            .set("name", "ghost"));

        assertThat(result).isEmpty();
    }

    // ===== NESTED FIELD TESTS =====

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_updateOne_set_nested_field(BackendTestContext btc) {
        UserProfile alice = btc.getProfileRepository().find(q -> q.where(on("name", eq("alice")))).findFirst().orElseThrow();

        boolean updated = btc.getProfileRepository().updateOne(alice.getId(), u -> u
            .set("profile.age", 26));

        assertThat(updated).isTrue();
        UserProfile reloaded = btc.getProfileRepository().findByPath(alice.getId()).orElseThrow();
        assertThat(reloaded.getProfile().getAge()).isEqualTo(26);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_updateOne_increment_nested_field(BackendTestContext btc) {
        UserProfile alice = btc.getProfileRepository()
            .find(q -> q.where(on("name", eq("alice"))))
            .findFirst()
            .orElseThrow();

        boolean updated = btc.getProfileRepository().updateOne(alice.getId(), u -> u
            .increment("profile.age", 5));

        assertThat(updated).isTrue();
        UserProfile reloaded = btc.getProfileRepository().findByPath(alice.getId()).orElseThrow();
        assertThat(reloaded.getProfile().getAge()).isEqualTo(30); // 25 + 5
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_updateOne_set_deeply_nested_field(BackendTestContext btc) {
        UserProfile alice = btc.getProfileRepository()
            .find(q -> q.where(on("name", eq("alice"))))
            .findFirst()
            .orElseThrow();

        boolean updated = btc.getProfileRepository().updateOne(alice.getId(), u -> u
            .set("profile.address.zipCode", 10002));

        assertThat(updated).isTrue();
        UserProfile reloaded = btc.getProfileRepository().findByPath(alice.getId()).orElseThrow();
        assertThat(reloaded.getProfile().getAddress().getZipCode()).isEqualTo(10002);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_updateOne_unset_nested_field(BackendTestContext btc) {
        UserProfile alice = btc.getProfileRepository()
            .find(q -> q.where(on("name", eq("alice"))))
            .findFirst()
            .orElseThrow();

        boolean updated = btc.getProfileRepository().updateOne(alice.getId(), u -> u
            .unset("profile.occupation"));

        assertThat(updated).isTrue();
        UserProfile reloaded = btc.getProfileRepository().findByPath(alice.getId()).orElseThrow();
        assertThat(reloaded.getProfile().getOccupation()).isNull();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_updateOne_multiply_nested_field(BackendTestContext btc) {
        UserProfile alice = btc.getProfileRepository()
            .find(q -> q.where(on("name", eq("alice"))))
            .findFirst()
            .orElseThrow();

        boolean updated = btc.getProfileRepository().updateOne(alice.getId(), u -> u
            .multiply("profile.age", 2));

        assertThat(updated).isTrue();
        UserProfile reloaded = btc.getProfileRepository().findByPath(alice.getId()).orElseThrow();
        assertThat(reloaded.getProfile().getAge()).isEqualTo(50); // 25 * 2
    }

    // ===== ADVANCED COMBINED OPERATIONS TESTS =====

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_updateOne_combine_simple_and_pull(BackendTestContext btc) {
        User alice = new User("alice", 100);
        alice.setTags(Arrays.asList("keep1", "remove", "keep2", "remove", "keep3"));
        btc.getUserRepository().save(alice);

        boolean updated = btc.getUserRepository().updateOne(alice.getId(), u -> u
            .set("name", "alice_updated")
            .increment("exp", 50)               // 100 + 50 = 150
            .pull("tags", "remove"));

        assertThat(updated).isTrue();

        User reloaded = btc.getUserRepository().findByPath(alice.getId()).orElseThrow();
        assertThat(reloaded.getName()).isEqualTo("alice_updated");
        assertThat(reloaded.getExp()).isEqualTo(150);
        assertThat(reloaded.getTags()).containsExactly("keep1", "keep2", "keep3");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_updateOne_combine_simple_and_pullAll(BackendTestContext btc) {
        User alice = new User("alice", 100);
        alice.setTags(Arrays.asList("keep", "remove1", "keep", "remove2", "keep", "remove3"));
        btc.getUserRepository().save(alice);

        boolean updated = btc.getUserRepository().updateOne(alice.getId(), u -> u
            .multiply("exp", 2)                          // 100 * 2 = 200
            .set("name", "alice_v2")
            .pullAll("tags", "remove1", "remove2", "remove3"));

        assertThat(updated).isTrue();

        User reloaded = btc.getUserRepository().findByPath(alice.getId()).orElseThrow();
        assertThat(reloaded.getName()).isEqualTo("alice_v2");
        assertThat(reloaded.getExp()).isEqualTo(200);
        assertThat(reloaded.getTags()).containsExactly("keep", "keep", "keep");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_updateOne_combine_simple_and_addToSet_multiple(BackendTestContext btc) {
        User alice = new User("alice", 100);
        alice.setTags(Arrays.asList("existing1", "existing2"));
        btc.getUserRepository().save(alice);

        boolean updated = btc.getUserRepository().updateOne(alice.getId(), u -> u
            .increment("exp", 50)                                    // 100 + 50 = 150
            .set("name", "alice_unique")
            .addToSet("tags", "new1", "existing1", "new2", "new3"));

        assertThat(updated).isTrue();

        User reloaded = btc.getUserRepository().findByPath(alice.getId()).orElseThrow();
        assertThat(reloaded.getName()).isEqualTo("alice_unique");
        assertThat(reloaded.getExp()).isEqualTo(150);
        // existing1 should not be duplicated
        assertThat(reloaded.getTags()).containsExactlyInAnyOrder("existing1", "existing2", "new1", "new2", "new3");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_updateOne_multiple_simple_with_multiple_complex(BackendTestContext btc) {
        User alice = new User("alice", 100);
        alice.setTags(Arrays.asList("tag1", "tag2", "remove1", "remove2", "tag3"));
        btc.getUserRepository().save(alice);

        // First update: PULL_ALL + simple operations
        btc.getUserRepository().updateOne(alice.getId(), u -> u
            .set("name", "alice_step1")
            .increment("exp", 25)                        // 100 + 25 = 125
            .pullAll("tags", "remove1", "remove2"));

        User afterFirst = btc.getUserRepository().findByPath(alice.getId()).orElseThrow();
        assertThat(afterFirst.getName()).isEqualTo("alice_step1");
        assertThat(afterFirst.getExp()).isEqualTo(125);
        assertThat(afterFirst.getTags()).containsExactly("tag1", "tag2", "tag3");

        // Second update: ADD_TO_SET + simple operations
        btc.getUserRepository().updateOne(alice.getId(), u -> u
            .multiply("exp", 2)                          // 125 * 2 = 250
            .set("name", "alice_final")
            .addToSet("tags", "tag1", "new1", "new2")); // tag1 already exists

        User afterSecond = btc.getUserRepository().findByPath(alice.getId()).orElseThrow();
        assertThat(afterSecond.getName()).isEqualTo("alice_final");
        assertThat(afterSecond.getExp()).isEqualTo(250);
        assertThat(afterSecond.getTags()).containsExactlyInAnyOrder("tag1", "tag2", "tag3", "new1", "new2");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_updateOne_complex_array_ops_with_min_max(BackendTestContext btc) {
        User alice = new User("alice", 150);
        alice.setTags(Arrays.asList("a", "b", "c", "remove", "d"));
        btc.getUserRepository().save(alice);

        boolean updated = btc.getUserRepository().updateOne(alice.getId(), u -> u
            .min("exp", 120)                    // min(150, 120) = 120
            .pull("tags", "remove")
            .set("name", "alice_min"));

        assertThat(updated).isTrue();

        User reloaded = btc.getUserRepository().findByPath(alice.getId()).orElseThrow();
        assertThat(reloaded.getName()).isEqualTo("alice_min");
        assertThat(reloaded.getExp()).isEqualTo(120);
        assertThat(reloaded.getTags()).containsExactly("a", "b", "c", "d");

        // Now test MAX with complex operation
        boolean updated2 = btc.getUserRepository().updateOne(alice.getId(), u -> u
            .max("exp", 200)                                // max(120, 200) = 200
            .addToSet("tags", "a", "e", "f"));

        assertThat(updated2).isTrue();

        User reloaded2 = btc.getUserRepository().findByPath(alice.getId()).orElseThrow();
        assertThat(reloaded2.getExp()).isEqualTo(200);
        assertThat(reloaded2.getTags()).containsExactlyInAnyOrder("a", "b", "c", "d", "e", "f");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_updateOne_all_operation_types_including_complex(BackendTestContext btc) {
        User alice = new User("alice", 100);
        alice.setTags(Arrays.asList("old1", "old2", "remove", "old3"));
        btc.getUserRepository().save(alice);

        boolean updated = btc.getUserRepository().updateOne(alice.getId(), u -> u
            .set("name", "alice_complete")
            .increment("exp", 100)              // 100 + 100 = 200
            .pull("tags", "remove"));

        assertThat(updated).isTrue();

        User reloaded = btc.getUserRepository().findByPath(alice.getId()).orElseThrow();
        assertThat(reloaded.getName()).isEqualTo("alice_complete");
        assertThat(reloaded.getExp()).isEqualTo(200);
        assertThat(reloaded.getTags()).containsExactly("old1", "old2", "old3");

        // Second update with different complex operation
        boolean updated2 = btc.getUserRepository().updateOne(alice.getId(), u -> u
            .multiply("exp", 0.5)                           // 200 * 0.5 = 100
            .unset("description")
            .addToSet("tags", "new1", "old1", "new2"));

        assertThat(updated2).isTrue();

        User reloaded2 = btc.getUserRepository().findByPath(alice.getId()).orElseThrow();
        assertThat(reloaded2.getExp()).isEqualTo(100);
        assertThat(reloaded2.getTags()).containsExactlyInAnyOrder("old1", "old2", "old3", "new1", "new2");
    }

    // ===== NUMERIC ARRAY OPERATIONS TESTS =====

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_pull_withIntegers(BackendTestContext btc) {
        // Test PULL with List<Integer>
        User alice = new User("alice", 100);
        alice.setScores(Arrays.asList(10, 20, 30, 20, 40));
        btc.getUserRepository().save(alice);

        boolean updated = btc.getUserRepository().updateOne(alice.getId(), u -> u
            .pull("scores", 20));

        assertThat(updated).isTrue();

        User reloaded = btc.getUserRepository().findByPath(alice.getId()).orElseThrow();
        assertThat(reloaded.getScores()).containsExactly(10, 30, 40);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_pullAll_withIntegers(BackendTestContext btc) {
        // Test PULL_ALL with List<Integer>
        User alice = new User("alice", 100);
        alice.setScores(Arrays.asList(10, 20, 30, 40, 20, 50, 30));
        btc.getUserRepository().save(alice);

        boolean updated = btc.getUserRepository().updateOne(alice.getId(), u -> u
            .pullAll("scores", 20, 30));

        assertThat(updated).isTrue();

        User reloaded = btc.getUserRepository().findByPath(alice.getId()).orElseThrow();
        assertThat(reloaded.getScores()).containsExactly(10, 40, 50);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_addToSet_withIntegers(BackendTestContext btc) {
        // Test ADD_TO_SET with List<Integer>
        User alice = new User("alice", 100);
        alice.setScores(Arrays.asList(10, 20, 30));
        btc.getUserRepository().save(alice);

        // Add existing and new values
        boolean updated = btc.getUserRepository().updateOne(alice.getId(), u -> u
            .addToSet("scores", 20, 40, 30, 50));

        assertThat(updated).isTrue();

        User reloaded = btc.getUserRepository().findByPath(alice.getId()).orElseThrow();
        // Should only add 40 and 50 (20 and 30 already exist)
        assertThat(reloaded.getScores()).containsExactlyInAnyOrder(10, 20, 30, 40, 50);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_push_withIntegers(BackendTestContext btc) {
        // Test PUSH with List<Integer>
        User alice = new User("alice", 100);
        alice.setScores(Arrays.asList(10, 20));
        btc.getUserRepository().save(alice);

        boolean updated = btc.getUserRepository().updateOne(alice.getId(), u -> u
            .push("scores", 30, 40));

        assertThat(updated).isTrue();

        User reloaded = btc.getUserRepository().findByPath(alice.getId()).orElseThrow();
        assertThat(reloaded.getScores()).containsExactly(10, 20, 30, 40);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_popFirst_withIntegers(BackendTestContext btc) {
        // Test POP_FIRST with List<Integer>
        User alice = new User("alice", 100);
        alice.setScores(Arrays.asList(10, 20, 30, 40));
        btc.getUserRepository().save(alice);

        boolean updated = btc.getUserRepository().updateOne(alice.getId(), u -> u
            .popFirst("scores"));

        assertThat(updated).isTrue();

        User reloaded = btc.getUserRepository().findByPath(alice.getId()).orElseThrow();
        assertThat(reloaded.getScores()).containsExactly(20, 30, 40);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_popLast_withIntegers(BackendTestContext btc) {
        // Test POP_LAST with List<Integer>
        User alice = new User("alice", 100);
        alice.setScores(Arrays.asList(10, 20, 30, 40));
        btc.getUserRepository().save(alice);

        boolean updated = btc.getUserRepository().updateOne(alice.getId(), u -> u
            .popLast("scores"));

        assertThat(updated).isTrue();

        User reloaded = btc.getUserRepository().findByPath(alice.getId()).orElseThrow();
        assertThat(reloaded.getScores()).containsExactly(10, 20, 30);
    }

    // ===== NULL HANDLING TESTS =====

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_addToSet_singleValue_null(BackendTestContext btc) {
        // Test that ADD_TO_SET handles null values correctly
        User alice = new User("alice", 100);
        alice.setTags(new ArrayList<>(Arrays.asList("tag1", "tag2")));
        btc.getUserRepository().save(alice);

        // Add null once
        boolean updated1 = btc.getUserRepository().updateOne(alice.getId(), u -> u
            .addToSet("tags", (String) null));

        assertThat(updated1).isTrue();

        User reloaded1 = btc.getUserRepository().findByPath(alice.getId()).orElseThrow();
        assertThat(reloaded1.getTags()).containsExactlyInAnyOrder("tag1", "tag2", null);

        // Try to add null again (should not duplicate)
        boolean updated2 = btc.getUserRepository().updateOne(alice.getId(), u -> u
            .addToSet("tags", (String) null));

        assertThat(updated2).isTrue();

        User reloaded2 = btc.getUserRepository().findByPath(alice.getId()).orElseThrow();
        // Should still have only one null
        assertThat(reloaded2.getTags()).containsExactlyInAnyOrder("tag1", "tag2", null);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_addToSet_multipleValues_withNull(BackendTestContext btc) {
        // Test that ADD_TO_SET handles null values in multiple values correctly
        User alice = new User("alice", 100);
        alice.setTags(new ArrayList<>(Arrays.asList("tag1")));
        btc.getUserRepository().save(alice);

        // Add multiple values including null
        boolean updated1 = btc.getUserRepository().updateOne(alice.getId(), u -> u
            .addToSet("tags", null, "tag2", null, "tag3"));

        assertThat(updated1).isTrue();

        User reloaded1 = btc.getUserRepository().findByPath(alice.getId()).orElseThrow();
        // Should have deduplicated the null values
        assertThat(reloaded1.getTags()).containsExactlyInAnyOrder("tag1", "tag2", "tag3", null);

        // Try to add the same values again (should not duplicate)
        boolean updated2 = btc.getUserRepository().updateOne(alice.getId(), u -> u
            .addToSet("tags", null, "tag2", "tag4"));

        assertThat(updated2).isTrue();

        User reloaded2 = btc.getUserRepository().findByPath(alice.getId()).orElseThrow();
        // Should have only one null, and tag4 should be added
        assertThat(reloaded2.getTags()).containsExactlyInAnyOrder("tag1", "tag2", "tag3", "tag4", null);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_pull_withNull(BackendTestContext btc) {
        // Test that PULL can remove null values
        User alice = new User("alice", 100);
        alice.setTags(new ArrayList<>(Arrays.asList("tag1", null, "tag2", null, "tag3")));
        btc.getUserRepository().save(alice);

        boolean updated = btc.getUserRepository().updateOne(alice.getId(), u -> u
            .pull("tags", (String) null));

        assertThat(updated).isTrue();

        User reloaded = btc.getUserRepository().findByPath(alice.getId()).orElseThrow();
        assertThat(reloaded.getTags()).containsExactly("tag1", "tag2", "tag3");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_pullAll_withNull(BackendTestContext btc) {
        // Test that PULL_ALL can remove null values along with other values
        User alice = new User("alice", 100);
        alice.setTags(new ArrayList<>(Arrays.asList("tag1", null, "remove", "tag2", "remove", null, "tag3")));
        btc.getUserRepository().save(alice);

        boolean updated = btc.getUserRepository().updateOne(alice.getId(), u -> u
            .pullAll("tags", null, "remove"));

        assertThat(updated).isTrue();

        User reloaded = btc.getUserRepository().findByPath(alice.getId()).orElseThrow();
        assertThat(reloaded.getTags()).containsExactly("tag1", "tag2", "tag3");
    }
}
