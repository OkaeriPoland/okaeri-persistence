package eu.okaeri.persistencetest.e2e;

import eu.okaeri.persistencetest.fixtures.User;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

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
}
