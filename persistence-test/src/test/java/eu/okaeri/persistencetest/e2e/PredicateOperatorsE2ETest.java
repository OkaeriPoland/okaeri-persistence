package eu.okaeri.persistencetest.e2e;

import eu.okaeri.persistencetest.fixtures.User;
import eu.okaeri.persistencetest.fixtures.User.Status;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.UUID;
import java.util.stream.Stream;

import static eu.okaeri.persistence.filter.condition.Condition.on;
import static eu.okaeri.persistence.filter.predicate.SimplePredicate.*;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E Predicate Operator Tests - runs ALL predicate operators against ALL backends.
 * Tests filter rendering by using delete operations which share the same rendering logic.
 */
public class PredicateOperatorsE2ETest extends E2ETestBase {

    protected static Stream<BackendTestContext> allBackendsWithContext() {
        return allBackends().map(backend -> {
            BackendTestContext btc = BackendTestContext.create(backend);

            // Setup test data - simple dataset with various values
            btc.getUserRepository().save(new User("alice", 100));
            btc.getUserRepository().save(new User("bob", 200));
            btc.getUserRepository().save(new User("charlie", 300));
            btc.getUserRepository().save(new User("diana", 200));
            btc.getUserRepository().save(new User("eve", 150));

            return btc;
        });
    }

    // ===== EQUALS (eq) =====

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_eq_string(BackendTestContext btc) {
        long deleted = btc.getUserRepository().delete(q -> q.where(on("name", eq("bob"))));
        assertThat(deleted).isEqualTo(1);

        var remaining = btc.getUserRepository().streamAll().map(User::getName).toList();
        assertThat(remaining).containsExactlyInAnyOrder("alice", "charlie", "diana", "eve");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_eq_number(BackendTestContext btc) {
        long deleted = btc.getUserRepository().delete(q -> q.where(on("exp", eq(200))));
        assertThat(deleted).isEqualTo(2); // bob and diana

        var remaining = btc.getUserRepository().streamAll().map(User::getName).toList();
        assertThat(remaining).containsExactlyInAnyOrder("alice", "charlie", "eve");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_eq_no_match(BackendTestContext btc) {
        long deleted = btc.getUserRepository().delete(q -> q.where(on("name", eq("nonexistent"))));
        assertThat(deleted).isEqualTo(0);

        var remaining = btc.getUserRepository().streamAll().map(User::getName).toList();
        assertThat(remaining).containsExactlyInAnyOrder("alice", "bob", "charlie", "diana", "eve");
    }

    // ===== CASE-INSENSITIVE EQUALS (eqi / eq().ignoreCase()) =====

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_eqi_fluent_api(BackendTestContext btc) {
        // Add mixed case data
        btc.getUserRepository().save(new User("Alice", 999)); // Note capital A

        long deleted = btc.getUserRepository().delete(q -> q.where(on("name", eq("ALICE").ignoreCase())));
        assertThat(deleted).isEqualTo(2); // Both "alice" and "Alice"

        var remaining = btc.getUserRepository().streamAll().map(User::getName).toList();
        assertThat(remaining).containsExactlyInAnyOrder("bob", "charlie", "diana", "eve");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_eqi_shorthand(BackendTestContext btc) {
        // Add mixed case data
        btc.getUserRepository().save(new User("BOB", 999));

        long deleted = btc.getUserRepository().delete(q -> q.where(on("name", eqi("bob"))));
        assertThat(deleted).isEqualTo(2); // Both "bob" and "BOB"

        var remaining = btc.getUserRepository().streamAll().map(User::getName).toList();
        assertThat(remaining).containsExactlyInAnyOrder("alice", "charlie", "diana", "eve");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_eqi_no_match(BackendTestContext btc) {
        long deleted = btc.getUserRepository().delete(q -> q.where(on("name", eqi("NONEXISTENT"))));
        assertThat(deleted).isEqualTo(0);

        var remaining = btc.getUserRepository().streamAll().map(User::getName).toList();
        assertThat(remaining).containsExactlyInAnyOrder("alice", "bob", "charlie", "diana", "eve");
    }

    // ===== BOOLEAN EQUALS (eq) =====

    protected static Stream<BackendTestContext> booleanTestContext() {
        return allBackends().map(backend -> {
            BackendTestContext btc = BackendTestContext.create(backend);

            // Test data with boolean values
            btc.getUserRepository().save(new User("alice", 100, true));
            btc.getUserRepository().save(new User("bob", 200, false));
            btc.getUserRepository().save(new User("charlie", 300, true));
            btc.getUserRepository().save(new User("diana", 400, false));
            btc.getUserRepository().save(new User("eve", 500, true));

            return btc;
        });
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("booleanTestContext")
    void test_eq_boolean_true(BackendTestContext btc) {
        long deleted = btc.getUserRepository().delete(q -> q.where(on("verified", eq(true))));
        assertThat(deleted).isEqualTo(3); // alice, charlie, eve

        var remaining = btc.getUserRepository().streamAll().map(User::getName).toList();
        assertThat(remaining).containsExactlyInAnyOrder("bob", "diana");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("booleanTestContext")
    void test_eq_boolean_false(BackendTestContext btc) {
        long deleted = btc.getUserRepository().delete(q -> q.where(on("verified", eq(false))));
        assertThat(deleted).isEqualTo(2); // bob, diana

        var remaining = btc.getUserRepository().streamAll().map(User::getName).toList();
        assertThat(remaining).containsExactlyInAnyOrder("alice", "charlie", "eve");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("booleanTestContext")
    void test_ne_boolean_true(BackendTestContext btc) {
        long deleted = btc.getUserRepository().delete(q -> q.where(on("verified", ne(true))));
        assertThat(deleted).isEqualTo(2); // bob, diana (false values)

        var remaining = btc.getUserRepository().streamAll().map(User::getName).toList();
        assertThat(remaining).containsExactlyInAnyOrder("alice", "charlie", "eve");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("booleanTestContext")
    void test_ne_boolean_false(BackendTestContext btc) {
        long deleted = btc.getUserRepository().delete(q -> q.where(on("verified", ne(false))));
        assertThat(deleted).isEqualTo(3); // alice, charlie, eve (true values)

        var remaining = btc.getUserRepository().streamAll().map(User::getName).toList();
        assertThat(remaining).containsExactlyInAnyOrder("bob", "diana");
    }

    // ===== NOT EQUALS (ne) =====

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_ne_string(BackendTestContext btc) {
        long deleted = btc.getUserRepository().delete(q -> q.where(on("name", ne("alice"))));
        assertThat(deleted).isEqualTo(4); // everyone except alice

        var remaining = btc.getUserRepository().streamAll().map(User::getName).toList();
        assertThat(remaining).containsExactly("alice");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_ne_number(BackendTestContext btc) {
        long deleted = btc.getUserRepository().delete(q -> q.where(on("exp", ne(200))));
        assertThat(deleted).isEqualTo(3); // alice (100), charlie (300), eve (150)

        var remaining = btc.getUserRepository().streamAll().map(User::getName).toList();
        assertThat(remaining).containsExactlyInAnyOrder("bob", "diana");
    }

    // ===== GREATER THAN (gt) =====

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_gt_basic(BackendTestContext btc) {
        long deleted = btc.getUserRepository().delete(q -> q.where(on("exp", gt(200))));
        assertThat(deleted).isEqualTo(1); // charlie (300)

        var remaining = btc.getUserRepository().streamAll().map(User::getName).toList();
        assertThat(remaining).containsExactlyInAnyOrder("alice", "bob", "diana", "eve");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_gt_boundary(BackendTestContext btc) {
        long deleted = btc.getUserRepository().delete(q -> q.where(on("exp", gt(150))));
        assertThat(deleted).isEqualTo(3); // bob (200), diana (200), charlie (300)

        var remaining = btc.getUserRepository().streamAll().map(User::getExp).toList();
        assertThat(remaining).containsExactlyInAnyOrder(100, 150); // alice and eve
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_gt_no_match(BackendTestContext btc) {
        long deleted = btc.getUserRepository().delete(q -> q.where(on("exp", gt(500))));
        assertThat(deleted).isEqualTo(0);

        var remaining = btc.getUserRepository().streamAll().map(User::getName).toList();
        assertThat(remaining).containsExactlyInAnyOrder("alice", "bob", "charlie", "diana", "eve");
    }

    // ===== GREATER THAN OR EQUAL (gte) =====

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_gte_basic(BackendTestContext btc) {
        long deleted = btc.getUserRepository().delete(q -> q.where(on("exp", gte(200))));
        assertThat(deleted).isEqualTo(3); // bob (200), diana (200), charlie (300)

        var remaining = btc.getUserRepository().streamAll().map(User::getExp).toList();
        assertThat(remaining).containsExactlyInAnyOrder(100, 150); // alice and eve
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_gte_boundary(BackendTestContext btc) {
        long deleted = btc.getUserRepository().delete(q -> q.where(on("exp", gte(150))));
        assertThat(deleted).isEqualTo(4); // eve (150), bob (200), diana (200), charlie (300)

        var remaining = btc.getUserRepository().streamAll().map(User::getName).toList();
        assertThat(remaining).containsExactly("alice");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_gte_all(BackendTestContext btc) {
        long deleted = btc.getUserRepository().delete(q -> q.where(on("exp", gte(0))));
        assertThat(deleted).isEqualTo(5); // all users

        assertThat(btc.getUserRepository().count()).isEqualTo(0);
    }

    // ===== LESS THAN (lt) =====

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_lt_basic(BackendTestContext btc) {
        long deleted = btc.getUserRepository().delete(q -> q.where(on("exp", lt(150))));
        assertThat(deleted).isEqualTo(1); // alice (100)

        var remaining = btc.getUserRepository().streamAll().map(User::getName).toList();
        assertThat(remaining).containsExactlyInAnyOrder("bob", "charlie", "diana", "eve");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_lt_boundary(BackendTestContext btc) {
        long deleted = btc.getUserRepository().delete(q -> q.where(on("exp", lt(200))));
        assertThat(deleted).isEqualTo(2); // alice (100), eve (150)

        var remaining = btc.getUserRepository().streamAll().map(User::getExp).toList();
        assertThat(remaining).containsExactlyInAnyOrder(200, 200, 300); // bob, diana, charlie
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_lt_no_match(BackendTestContext btc) {
        long deleted = btc.getUserRepository().delete(q -> q.where(on("exp", lt(50))));
        assertThat(deleted).isEqualTo(0);

        var remaining = btc.getUserRepository().streamAll().map(User::getName).toList();
        assertThat(remaining).containsExactlyInAnyOrder("alice", "bob", "charlie", "diana", "eve");
    }

    // ===== LESS THAN OR EQUAL (lte) =====

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_lte_basic(BackendTestContext btc) {
        long deleted = btc.getUserRepository().delete(q -> q.where(on("exp", lte(150))));
        assertThat(deleted).isEqualTo(2); // alice (100), eve (150)

        var remaining = btc.getUserRepository().streamAll().map(User::getName).toList();
        assertThat(remaining).containsExactlyInAnyOrder("bob", "charlie", "diana");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_lte_boundary(BackendTestContext btc) {
        long deleted = btc.getUserRepository().delete(q -> q.where(on("exp", lte(200))));
        assertThat(deleted).isEqualTo(4); // alice (100), eve (150), bob (200), diana (200)

        var remaining = btc.getUserRepository().streamAll().map(User::getName).toList();
        assertThat(remaining).containsExactly("charlie");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_lte_all(BackendTestContext btc) {
        long deleted = btc.getUserRepository().delete(q -> q.where(on("exp", lte(1000))));
        assertThat(deleted).isEqualTo(5); // all users

        assertThat(btc.getUserRepository().count()).isEqualTo(0);
    }

    // ===== EDGE CASES =====

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_eq_empty_string(BackendTestContext btc) {
        // First add a user with empty name
        btc.getUserRepository().save(new User("", 999));
        assertThat(btc.getUserRepository().count()).isEqualTo(6);

        long deleted = btc.getUserRepository().delete(q -> q.where(on("name", eq(""))));
        assertThat(deleted).isEqualTo(1);

        var remaining = btc.getUserRepository().streamAll().map(User::getName).toList();
        assertThat(remaining).containsExactlyInAnyOrder("alice", "bob", "charlie", "diana", "eve");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_eq_zero(BackendTestContext btc) {
        // First add a user with zero exp
        btc.getUserRepository().save(new User("zero", 0));
        assertThat(btc.getUserRepository().count()).isEqualTo(6);

        long deleted = btc.getUserRepository().delete(q -> q.where(on("exp", eq(0))));
        assertThat(deleted).isEqualTo(1);

        var remaining = btc.getUserRepository().streamAll().map(User::getName).toList();
        assertThat(remaining).containsExactlyInAnyOrder("alice", "bob", "charlie", "diana", "eve");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_gt_negative_boundary(BackendTestContext btc) {
        // Add users with negative values
        btc.getUserRepository().save(new User("negative1", -50));
        btc.getUserRepository().save(new User("negative2", -100));
        assertThat(btc.getUserRepository().count()).isEqualTo(7);

        long deleted = btc.getUserRepository().delete(q -> q.where(on("exp", gt(-75))));
        assertThat(deleted).isEqualTo(6); // negative1 (-50) and all positive values

        var remaining = btc.getUserRepository().streamAll().toList();
        assertThat(remaining).hasSize(1);
        assertThat(remaining.get(0).getName()).isEqualTo("negative2");
        assertThat(remaining.get(0).getExp()).isEqualTo(-100);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_string_case_sensitive(BackendTestContext btc) {
        long deleted = btc.getUserRepository().delete(q -> q.where(on("name", eq("Alice")))); // capital A
        assertThat(deleted).isEqualTo(0); // should not match "alice"

        var remaining = btc.getUserRepository().streamAll().map(User::getName).toList();
        assertThat(remaining).containsExactlyInAnyOrder("alice", "bob", "charlie", "diana", "eve");
    }

    // ===== BETWEEN (convenience method) =====

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_between_basic(BackendTestContext btc) {
        long deleted = btc.getUserRepository().delete(q -> q.where(on("exp", between(150, 200))));
        assertThat(deleted).isEqualTo(3); // eve (150), bob (200), diana (200)

        var remaining = btc.getUserRepository().streamAll().map(User::getExp).toList();
        assertThat(remaining).containsExactlyInAnyOrder(100, 300); // alice and charlie
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_between_exclusive_boundaries(BackendTestContext btc) {
        long deleted = btc.getUserRepository().delete(q -> q.where(on("exp", between(101, 299))));
        assertThat(deleted).isEqualTo(3); // eve (150), bob (200), diana (200)

        var remaining = btc.getUserRepository().streamAll().map(User::getExp).toList();
        assertThat(remaining).containsExactlyInAnyOrder(100, 300); // alice and charlie
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_between_single_value(BackendTestContext btc) {
        long deleted = btc.getUserRepository().delete(q -> q.where(on("exp", between(200, 200))));
        assertThat(deleted).isEqualTo(2); // bob and diana exactly 200

        var remaining = btc.getUserRepository().streamAll().map(User::getName).toList();
        assertThat(remaining).containsExactlyInAnyOrder("alice", "charlie", "eve");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_between_no_match(BackendTestContext btc) {
        long deleted = btc.getUserRepository().delete(q -> q.where(on("exp", between(400, 500))));
        assertThat(deleted).isEqualTo(0);

        var remaining = btc.getUserRepository().streamAll().map(User::getName).toList();
        assertThat(remaining).containsExactlyInAnyOrder("alice", "bob", "charlie", "diana", "eve");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_between_all(BackendTestContext btc) {
        long deleted = btc.getUserRepository().delete(q -> q.where(on("exp", between(0, 1000))));
        assertThat(deleted).isEqualTo(5); // all users

        assertThat(btc.getUserRepository().count()).isEqualTo(0);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_between_negative_range(BackendTestContext btc) {
        btc.getUserRepository().save(new User("negative", -50));
        assertThat(btc.getUserRepository().count()).isEqualTo(6);

        long deleted = btc.getUserRepository().delete(q -> q.where(on("exp", between(-100, 100))));
        assertThat(deleted).isEqualTo(2); // negative (-50) and alice (100)

        var remaining = btc.getUserRepository().streamAll().map(User::getName).toList();
        assertThat(remaining).containsExactlyInAnyOrder("bob", "charlie", "diana", "eve");
    }

    // ===== IS NULL / NOT NULL =====

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_isNull_no_null_values(BackendTestContext btc) {
        // None of the users have null names
        long deleted = btc.getUserRepository().delete(q -> q.where(on("name", isNull())));
        assertThat(deleted).isEqualTo(0);

        var remaining = btc.getUserRepository().streamAll().map(User::getName).toList();
        assertThat(remaining).containsExactlyInAnyOrder("alice", "bob", "charlie", "diana", "eve");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_isNull_with_null_values(BackendTestContext btc) {
        // Add users with null names
        btc.getUserRepository().save(new User(null, 999));
        btc.getUserRepository().save(new User(null, 888));

        long deleted = btc.getUserRepository().delete(q -> q.where(on("name", isNull())));
        assertThat(deleted).isEqualTo(2);

        var remaining = btc.getUserRepository().streamAll().map(User::getName).toList();
        assertThat(remaining).containsExactlyInAnyOrder("alice", "bob", "charlie", "diana", "eve");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_notNull_with_null_values(BackendTestContext btc) {
        // Add users with null names
        btc.getUserRepository().save(new User(null, 999));
        btc.getUserRepository().save(new User(null, 888));

        long deleted = btc.getUserRepository().delete(q -> q.where(on("name", notNull())));
        assertThat(deleted).isEqualTo(5); // only the original 5 with names

        assertThat(btc.getUserRepository().count()).isEqualTo(2); // 2 with null names remain
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_notNull_all_have_values(BackendTestContext btc) {
        // All users have non-null names
        long deleted = btc.getUserRepository().delete(q -> q.where(on("name", notNull())));
        assertThat(deleted).isEqualTo(5); // all users

        assertThat(btc.getUserRepository().count()).isEqualTo(0);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_notNull_all_have_exp_values(BackendTestContext btc) {
        // All users have non-null exp values
        long deleted = btc.getUserRepository().delete(q -> q.where(on("exp", notNull())));
        assertThat(deleted).isEqualTo(5); // all users

        assertThat(btc.getUserRepository().count()).isEqualTo(0);
    }

    // ===== IN =====

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_in_string_single_match(BackendTestContext btc) {
        long deleted = btc.getUserRepository().delete(q -> q.where(on("name", in("alice", "zoe", "frank"))));
        assertThat(deleted).isEqualTo(1); // only alice matches

        var remaining = btc.getUserRepository().streamAll().map(User::getName).toList();
        assertThat(remaining).containsExactlyInAnyOrder("bob", "charlie", "diana", "eve");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_in_string_multiple_matches(BackendTestContext btc) {
        long deleted = btc.getUserRepository().delete(q -> q.where(on("name", in("alice", "bob", "charlie"))));
        assertThat(deleted).isEqualTo(3); // alice, bob, charlie

        var remaining = btc.getUserRepository().streamAll().map(User::getName).toList();
        assertThat(remaining).containsExactlyInAnyOrder("diana", "eve");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_in_number_single_match(BackendTestContext btc) {
        long deleted = btc.getUserRepository().delete(q -> q.where(on("exp", in(100, 500, 1000))));
        assertThat(deleted).isEqualTo(1); // alice (100)

        var remaining = btc.getUserRepository().streamAll().map(User::getName).toList();
        assertThat(remaining).containsExactlyInAnyOrder("bob", "charlie", "diana", "eve");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_in_number_multiple_matches(BackendTestContext btc) {
        long deleted = btc.getUserRepository().delete(q -> q.where(on("exp", in(100, 200, 300))));
        assertThat(deleted).isEqualTo(4); // alice (100), bob (200), diana (200), charlie (300)

        var remaining = btc.getUserRepository().streamAll().map(User::getName).toList();
        assertThat(remaining).containsExactly("eve");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_in_no_match(BackendTestContext btc) {
        long deleted = btc.getUserRepository().delete(q -> q.where(on("name", in("zoe", "frank", "grace"))));
        assertThat(deleted).isEqualTo(0);

        var remaining = btc.getUserRepository().streamAll().map(User::getName).toList();
        assertThat(remaining).containsExactlyInAnyOrder("alice", "bob", "charlie", "diana", "eve");
    }

    // ===== NOT IN =====

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_notIn_string_excludes_one(BackendTestContext btc) {
        long deleted = btc.getUserRepository().delete(q -> q.where(on("name", notIn("alice"))));
        assertThat(deleted).isEqualTo(4); // everyone except alice

        var remaining = btc.getUserRepository().streamAll().map(User::getName).toList();
        assertThat(remaining).containsExactly("alice");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_notIn_string_excludes_multiple(BackendTestContext btc) {
        long deleted = btc.getUserRepository().delete(q -> q.where(on("name", notIn("alice", "bob", "charlie"))));
        assertThat(deleted).isEqualTo(2); // diana and eve

        var remaining = btc.getUserRepository().streamAll().map(User::getName).toList();
        assertThat(remaining).containsExactlyInAnyOrder("alice", "bob", "charlie");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_notIn_number_excludes_one(BackendTestContext btc) {
        long deleted = btc.getUserRepository().delete(q -> q.where(on("exp", notIn(100))));
        assertThat(deleted).isEqualTo(4); // everyone except alice (100)

        var remaining = btc.getUserRepository().streamAll().map(User::getExp).toList();
        assertThat(remaining).containsExactly(100); // just alice
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_notIn_number_excludes_multiple(BackendTestContext btc) {
        long deleted = btc.getUserRepository().delete(q -> q.where(on("exp", notIn(100, 200))));
        assertThat(deleted).isEqualTo(2); // charlie (300), eve (150)

        var remaining = btc.getUserRepository().streamAll().map(User::getExp).toList();
        assertThat(remaining).containsExactlyInAnyOrder(100, 200, 200); // alice, bob, diana
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_notIn_all_match(BackendTestContext btc) {
        long deleted = btc.getUserRepository().delete(q -> q.where(on("name", notIn("zoe", "frank"))));
        assertThat(deleted).isEqualTo(5); // all users (none match the exclusion list)

        assertThat(btc.getUserRepository().count()).isEqualTo(0);
    }

    // ===== STRING PREDICATES - Setup =====

    protected static Stream<BackendTestContext> stringTestContext() {
        return allBackends().map(backend -> {
            BackendTestContext btc = BackendTestContext.create(backend);

            // Test data for string predicates
            btc.getUserRepository().save(new User("admin@example.com", 100));
            btc.getUserRepository().save(new User("user@example.com", 200));
            btc.getUserRepository().save(new User("support@test.org", 300));
            btc.getUserRepository().save(new User("Admin@Company.COM", 400));  // Mixed case
            btc.getUserRepository().save(new User("temporary_user", 500));

            return btc;
        });
    }

    // ===== STARTS WITH (startsWith) =====

    @ParameterizedTest(name = "{0}")
    @MethodSource("stringTestContext")
    void test_startsWith_basic(BackendTestContext btc) {
        long deleted = btc.getUserRepository().delete(q -> q.where(on("name", startsWith("admin"))));
        assertThat(deleted).isEqualTo(1); // admin@example.com

        var remaining = btc.getUserRepository().streamAll().map(User::getName).toList();
        assertThat(remaining).containsExactlyInAnyOrder("user@example.com", "support@test.org", "Admin@Company.COM", "temporary_user");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("stringTestContext")
    void test_startsWith_ignoreCase(BackendTestContext btc) {
        long deleted = btc.getUserRepository().delete(q -> q.where(on("name", startsWith("admin").ignoreCase())));
        assertThat(deleted).isEqualTo(2); // admin@example.com and Admin@Company.COM

        var remaining = btc.getUserRepository().streamAll().map(User::getName).toList();
        assertThat(remaining).containsExactlyInAnyOrder("user@example.com", "support@test.org", "temporary_user");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("stringTestContext")
    void test_startsWith_no_match(BackendTestContext btc) {
        long deleted = btc.getUserRepository().delete(q -> q.where(on("name", startsWith("xyz"))));
        assertThat(deleted).isEqualTo(0);

        assertThat(btc.getUserRepository().count()).isEqualTo(5);
    }

    // ===== ENDS WITH (endsWith) =====

    @ParameterizedTest(name = "{0}")
    @MethodSource("stringTestContext")
    void test_endsWith_basic(BackendTestContext btc) {
        long deleted = btc.getUserRepository().delete(q -> q.where(on("name", endsWith(".com"))));
        assertThat(deleted).isEqualTo(2); // admin@example.com, user@example.com

        var remaining = btc.getUserRepository().streamAll().map(User::getName).toList();
        assertThat(remaining).containsExactlyInAnyOrder("support@test.org", "Admin@Company.COM", "temporary_user");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("stringTestContext")
    void test_endsWith_ignoreCase(BackendTestContext btc) {
        long deleted = btc.getUserRepository().delete(q -> q.where(on("name", endsWith(".com").ignoreCase())));
        assertThat(deleted).isEqualTo(3); // admin@example.com, user@example.com, Admin@Company.COM

        var remaining = btc.getUserRepository().streamAll().map(User::getName).toList();
        assertThat(remaining).containsExactlyInAnyOrder("support@test.org", "temporary_user");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("stringTestContext")
    void test_endsWith_no_match(BackendTestContext btc) {
        long deleted = btc.getUserRepository().delete(q -> q.where(on("name", endsWith(".net"))));
        assertThat(deleted).isEqualTo(0);

        assertThat(btc.getUserRepository().count()).isEqualTo(5);
    }

    // ===== CONTAINS (contains) =====

    @ParameterizedTest(name = "{0}")
    @MethodSource("stringTestContext")
    void test_contains_basic(BackendTestContext btc) {
        long deleted = btc.getUserRepository().delete(q -> q.where(on("name", contains("@example"))));
        assertThat(deleted).isEqualTo(2); // admin@example.com, user@example.com

        var remaining = btc.getUserRepository().streamAll().map(User::getName).toList();
        assertThat(remaining).containsExactlyInAnyOrder("support@test.org", "Admin@Company.COM", "temporary_user");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("stringTestContext")
    void test_contains_ignoreCase(BackendTestContext btc) {
        long deleted = btc.getUserRepository().delete(q -> q.where(on("name", contains("admin").ignoreCase())));
        assertThat(deleted).isEqualTo(2); // admin@example.com, Admin@Company.COM

        var remaining = btc.getUserRepository().streamAll().map(User::getName).toList();
        assertThat(remaining).containsExactlyInAnyOrder("user@example.com", "support@test.org", "temporary_user");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("stringTestContext")
    void test_contains_underscore(BackendTestContext btc) {
        long deleted = btc.getUserRepository().delete(q -> q.where(on("name", contains("_user"))));
        assertThat(deleted).isEqualTo(1); // temporary_user

        var remaining = btc.getUserRepository().streamAll().map(User::getName).toList();
        assertThat(remaining).containsExactlyInAnyOrder("admin@example.com", "user@example.com", "support@test.org", "Admin@Company.COM");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("stringTestContext")
    void test_contains_no_match(BackendTestContext btc) {
        long deleted = btc.getUserRepository().delete(q -> q.where(on("name", contains("xyz"))));
        assertThat(deleted).isEqualTo(0);

        assertThat(btc.getUserRepository().count()).isEqualTo(5);
    }

    // ===== ENUM EQUALS (eq) =====

    protected static Stream<BackendTestContext> enumTestContext() {
        return allBackends().map(backend -> {
            BackendTestContext btc = BackendTestContext.create(backend);

            // Test data with enum status values
            btc.getUserRepository().save(new User("alice", 100, Status.ACTIVE));
            btc.getUserRepository().save(new User("bob", 200, Status.INACTIVE));
            btc.getUserRepository().save(new User("charlie", 300, Status.ACTIVE));
            btc.getUserRepository().save(new User("diana", 400, Status.PENDING));
            btc.getUserRepository().save(new User("eve", 500, Status.BANNED));

            return btc;
        });
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("enumTestContext")
    void test_eq_enum_single_match(BackendTestContext btc) {
        long deleted = btc.getUserRepository().delete(q -> q.where(on("status", eq(Status.BANNED))));
        assertThat(deleted).isEqualTo(1); // eve

        var remaining = btc.getUserRepository().streamAll().map(User::getName).toList();
        assertThat(remaining).containsExactlyInAnyOrder("alice", "bob", "charlie", "diana");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("enumTestContext")
    void test_eq_enum_multiple_matches(BackendTestContext btc) {
        long deleted = btc.getUserRepository().delete(q -> q.where(on("status", eq(Status.ACTIVE))));
        assertThat(deleted).isEqualTo(2); // alice, charlie

        var remaining = btc.getUserRepository().streamAll().map(User::getName).toList();
        assertThat(remaining).containsExactlyInAnyOrder("bob", "diana", "eve");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("enumTestContext")
    void test_ne_enum(BackendTestContext btc) {
        long deleted = btc.getUserRepository().delete(q -> q.where(on("status", ne(Status.ACTIVE))));
        assertThat(deleted).isEqualTo(3); // bob, diana, eve

        var remaining = btc.getUserRepository().streamAll().map(User::getName).toList();
        assertThat(remaining).containsExactlyInAnyOrder("alice", "charlie");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("enumTestContext")
    void test_in_enum(BackendTestContext btc) {
        long deleted = btc.getUserRepository().delete(q -> q.where(on("status", in(Status.ACTIVE, Status.PENDING))));
        assertThat(deleted).isEqualTo(3); // alice, charlie, diana

        var remaining = btc.getUserRepository().streamAll().map(User::getName).toList();
        assertThat(remaining).containsExactlyInAnyOrder("bob", "eve");
    }

    // ===== UUID EQUALS (eq) =====

    private static final UUID SHARED_REF = UUID.fromString("11111111-1111-1111-1111-111111111111");
    private static final UUID ALICE_REF = UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
    private static final UUID BOB_REF = UUID.fromString("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb");

    protected static Stream<BackendTestContext> uuidTestContext() {
        return allBackends().map(backend -> {
            BackendTestContext btc = BackendTestContext.create(backend);

            // Test data with UUID referenceId values
            btc.getUserRepository().save(new User("alice", 100, ALICE_REF));
            btc.getUserRepository().save(new User("bob", 200, BOB_REF));
            btc.getUserRepository().save(new User("charlie", 300, SHARED_REF));
            btc.getUserRepository().save(new User("diana", 400, SHARED_REF));
            btc.getUserRepository().save(new User("eve", 500, (UUID) null));

            return btc;
        });
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("uuidTestContext")
    void test_eq_uuid_single_match(BackendTestContext btc) {
        long deleted = btc.getUserRepository().delete(q -> q.where(on("referenceId", eq(ALICE_REF))));
        assertThat(deleted).isEqualTo(1); // alice

        var remaining = btc.getUserRepository().streamAll().map(User::getName).toList();
        assertThat(remaining).containsExactlyInAnyOrder("bob", "charlie", "diana", "eve");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("uuidTestContext")
    void test_eq_uuid_multiple_matches(BackendTestContext btc) {
        long deleted = btc.getUserRepository().delete(q -> q.where(on("referenceId", eq(SHARED_REF))));
        assertThat(deleted).isEqualTo(2); // charlie, diana

        var remaining = btc.getUserRepository().streamAll().map(User::getName).toList();
        assertThat(remaining).containsExactlyInAnyOrder("alice", "bob", "eve");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("uuidTestContext")
    void test_ne_uuid(BackendTestContext btc) {
        long deleted = btc.getUserRepository().delete(q -> q.where(on("referenceId", ne(SHARED_REF))));
        // Deletes alice (ALICE_REF), bob (BOB_REF), eve (null) - document-first semantics: null != X is true
        assertThat(deleted).isEqualTo(3);

        var remaining = btc.getUserRepository().streamAll().map(User::getName).toList();
        assertThat(remaining).containsExactlyInAnyOrder("charlie", "diana");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("uuidTestContext")
    void test_eq_uuid_no_match(BackendTestContext btc) {
        UUID nonExistent = UUID.fromString("99999999-9999-9999-9999-999999999999");
        long deleted = btc.getUserRepository().delete(q -> q.where(on("referenceId", eq(nonExistent))));
        assertThat(deleted).isEqualTo(0);

        assertThat(btc.getUserRepository().count()).isEqualTo(5);
    }

    // ==================== Null Handling Tests ====================

    /**
     * Tests that ne() includes documents with null values.
     * Document-first semantics: null != X is true.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("uuidTestContext")
    void test_ne_includes_null_values(BackendTestContext btc) {
        // Eve has referenceId = null
        // ne(ALICE_REF) should match: bob, charlie, diana (different UUIDs) AND eve (null)
        long deleted = btc.getUserRepository().delete(q -> q.where(on("referenceId", ne(ALICE_REF))));
        assertThat(deleted).isEqualTo(4); // bob, charlie, diana, eve

        var remaining = btc.getUserRepository().streamAll().map(User::getName).toList();
        assertThat(remaining).containsExactlyInAnyOrder("alice");
    }

    /**
     * Tests that notIn() includes documents with null values.
     * Document-first semantics: null is not in any set.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("uuidTestContext")
    void test_notIn_includes_null_values(BackendTestContext btc) {
        // notIn(ALICE_REF, BOB_REF) should match: charlie, diana (SHARED_REF) AND eve (null)
        long deleted = btc.getUserRepository().delete(q -> q.where(on("referenceId", notIn(ALICE_REF, BOB_REF))));
        assertThat(deleted).isEqualTo(3); // charlie, diana, eve

        var remaining = btc.getUserRepository().streamAll().map(User::getName).toList();
        assertThat(remaining).containsExactlyInAnyOrder("alice", "bob");
    }
}
