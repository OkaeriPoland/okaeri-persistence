package eu.okaeri.persistencetest.e2e;

import eu.okaeri.persistencetest.fixtures.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static eu.okaeri.persistence.filter.OrderBy.asc;
import static eu.okaeri.persistence.filter.OrderBy.desc;
import static eu.okaeri.persistence.filter.condition.Condition.on;
import static eu.okaeri.persistence.filter.predicate.SimplePredicate.eq;
import static eu.okaeri.persistence.filter.predicate.SimplePredicate.gt;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E Filter Query Tests - runs ALL filter query tests against ALL backends.
 */
public class FilterQueryE2ETest extends E2ETestBase {

    protected static Stream<BackendTestContext> allBackendsWithContext() {
        return allBackends().map(backend -> {
            BackendTestContext btc = BackendTestContext.create(backend);

            // Setup test data for filter query tests
            btc.getUserRepository().save(new User("tester", 123));
            btc.getUserRepository().save(new User("tester2", 456));
            btc.getUserRepository().save(new User("tester3", 123));

            btc.getProfileRepository().save(new UserProfile("alice", new Profile(25, "Engineer", new Address("New York", "USA", 10001))));
            btc.getProfileRepository().save(new UserProfile("bob", new Profile(30, "Designer", new Address("London", "UK", 20002))));
            btc.getProfileRepository().save(new UserProfile("charlie", new Profile(25, "Manager", new Address("Paris", "France", 75001))));
            btc.getProfileRepository().save(new UserProfile("diana", new Profile(35, "Director", new Address("Berlin", "Germany", 10115))));

            return btc;
        });
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_filter_by_name(BackendTestContext btc) {
        List<User> users = btc.getUserRepository()
            .find(q -> q.where(on("name", eq("tester2"))))
            .toList();

        assertThat(users.size()).isEqualTo(1);
        assertThat(users.get(0).getName()).isEqualTo("tester2");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_filter_by_exp_greater_than(BackendTestContext btc) {
        List<User> users = btc.getUserRepository()
            .find(q -> q.where(on("exp", gt(200))))
            .toList();

        assertThat(users.size()).isEqualTo(1);
        assertThat(users.get(0).getExp()).isEqualTo(456);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_orderBy_asc(BackendTestContext btc) {

        List<User> users = btc.getUserRepository()
            .find(q -> q.where(on("exp", eq(123))).orderBy(asc("name")))
            .toList();

        assertThat(users.size()).isEqualTo(2);
        assertThat(users.get(0).getName()).isEqualTo("tester");
        assertThat(users.get(1).getName()).isEqualTo("tester3");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_orderBy_desc(BackendTestContext btc) {

        List<User> users = btc.getUserRepository()
            .find(q -> q.where(on("exp", eq(123))).orderBy(desc("name")))
            .toList();

        assertThat(users.size()).isEqualTo(2);
        assertThat(users.get(0).getName()).isEqualTo("tester3");
        assertThat(users.get(1).getName()).isEqualTo("tester");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_orderBy_exp_desc(BackendTestContext btc) {

        List<User> orderedUsers = btc.getUserRepository()
            .find(q -> q.where(on("exp", gt(0))).orderBy(desc("exp")))
            .toList();

        assertThat(orderedUsers.size()).isEqualTo(3);
        assertThat(orderedUsers.get(0).getExp()).isEqualTo(456); // tester2
        assertThat(orderedUsers.get(1).getExp()).isEqualTo(123); // tester or tester3
        assertThat(orderedUsers.get(2).getExp()).isEqualTo(123); // tester or tester3
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_orderBy_multiple_fields(BackendTestContext btc) {

        List<User> users = btc.getUserRepository()
            .find(q -> q.where(on("exp", gt(0))).orderBy(asc("exp"), asc("name")))
            .toList();

        assertThat(users.size()).isEqualTo(3);
        assertThat(users.get(0).getExp()).isEqualTo(123);
        assertThat(users.get(0).getName()).isEqualTo("tester");
        assertThat(users.get(1).getExp()).isEqualTo(123);
        assertThat(users.get(1).getName()).isEqualTo("tester3");
        assertThat(users.get(2).getExp()).isEqualTo(456);
        assertThat(users.get(2).getName()).isEqualTo("tester2");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_orderBy_with_limit(BackendTestContext btc) {

        List<User> users = btc.getUserRepository()
            .find(q -> q.where(on("exp", gt(0))).orderBy(desc("exp")).limit(1))
            .toList();

        assertThat(users.size()).isEqualTo(1);
        assertThat(users.get(0).getName()).isEqualTo("tester2");
        assertThat(users.get(0).getExp()).isEqualTo(456);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_orderBy_with_skip_and_limit(BackendTestContext btc) {

        List<User> users = btc.getUserRepository()
            .find(q -> q.where(on("exp", gt(0))).orderBy(desc("exp")).skip(1).limit(1))
            .toList();

        assertThat(users.size()).isEqualTo(1);
        assertThat(users.get(0).getExp()).isEqualTo(123);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_orderBy_fluent_api(BackendTestContext btc) {

        List<User> users = btc.getUserRepository()
            .find(q -> q.where(on("exp", gt(0))).orderBy(asc("exp"), asc("name")))
            .toList();

        assertThat(users.size()).isEqualTo(3);
        assertThat(users.get(0).getExp()).isEqualTo(123);
        assertThat(users.get(0).getName()).isEqualTo("tester");
        assertThat(users.get(1).getExp()).isEqualTo(123);
        assertThat(users.get(1).getName()).isEqualTo("tester3");
        assertThat(users.get(2).getExp()).isEqualTo(456);
        assertThat(users.get(2).getName()).isEqualTo("tester2");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_nested_where_profile_age(BackendTestContext btc) {

        List<UserProfile> profiles = btc.getProfileRepository()
            .find(q -> q.where(on("profile.age", eq(25))))
            .toList();

        assertThat(profiles.size()).isEqualTo(2); // alice and charlie
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_nested_where_profile_age_gt(BackendTestContext btc) {

        List<UserProfile> profiles = btc.getProfileRepository()
            .find(q -> q.where(on("profile.age", gt(25))))
            .toList();

        assertThat(profiles.size()).isEqualTo(2); // bob (30) and diana (35)
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_nested_where_address_city(BackendTestContext btc) {

        List<UserProfile> profiles = btc.getProfileRepository()
            .find(q -> q.where(on("profile.address.city", eq("London"))))
            .toList();

        assertThat(profiles.size()).isEqualTo(1);
        assertThat(profiles.get(0).getName()).isEqualTo("bob");
        assertThat(profiles.get(0).getProfile().getAddress().getCity()).isEqualTo("London");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_nested_orderBy_profile_age_asc(BackendTestContext btc) {

        List<UserProfile> profiles = btc.getProfileRepository()
            .find(q -> q.where(on("profile.age", gt(0))).orderBy(asc("profile.age")))
            .toList();

        assertThat(profiles.size()).isEqualTo(4);
        assertThat(profiles.get(0).getProfile().getAge()).isEqualTo(25); // alice or charlie
        assertThat(profiles.get(1).getProfile().getAge()).isEqualTo(25); // alice or charlie
        assertThat(profiles.get(2).getProfile().getAge()).isEqualTo(30); // bob
        assertThat(profiles.get(3).getProfile().getAge()).isEqualTo(35); // diana
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_nested_orderBy_profile_age_desc(BackendTestContext btc) {

        List<UserProfile> profiles = btc.getProfileRepository()
            .find(q -> q.where(on("profile.age", gt(0))).orderBy(desc("profile.age")))
            .toList();

        assertThat(profiles.size()).isEqualTo(4);
        assertThat(profiles.get(0).getProfile().getAge()).isEqualTo(35); // diana
        assertThat(profiles.get(1).getProfile().getAge()).isEqualTo(30); // bob
        assertThat(profiles.get(2).getProfile().getAge()).isEqualTo(25); // alice or charlie
        assertThat(profiles.get(3).getProfile().getAge()).isEqualTo(25); // alice or charlie
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_nested_orderBy_name_then_profile_age(BackendTestContext btc) {

        List<UserProfile> profiles = btc.getProfileRepository()
            .find(q -> q.where(on("profile.age", eq(25))).orderBy(asc("name")))
            .toList();

        assertThat(profiles.size()).isEqualTo(2);
        assertThat(profiles.get(0).getName()).isEqualTo("alice");
        assertThat(profiles.get(1).getName()).isEqualTo("charlie");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_nested_orderBy_address_city(BackendTestContext btc) {

        List<UserProfile> profiles = btc.getProfileRepository()
            .find(q -> q.where(on("profile.age", gt(0))).orderBy(asc("profile.address.city")))
            .toList();

        assertThat(profiles.size()).isEqualTo(4);
        // Alphabetical by city: Berlin, London, New York, Paris
        assertThat(profiles.get(0).getProfile().getAddress().getCity()).isEqualTo("Berlin");
        assertThat(profiles.get(1).getProfile().getAddress().getCity()).isEqualTo("London");
        assertThat(profiles.get(2).getProfile().getAddress().getCity()).isEqualTo("New York");
        assertThat(profiles.get(3).getProfile().getAddress().getCity()).isEqualTo("Paris");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_nested_complex_query(BackendTestContext btc) {

        List<UserProfile> profiles = btc.getProfileRepository()
            .find(q -> q.where(on("profile.age", gt(25))).orderBy(desc("profile.age")).limit(2))
            .toList();

        assertThat(profiles.size()).isEqualTo(2);
        assertThat(profiles.get(0).getName()).isEqualTo("diana");
        assertThat(profiles.get(0).getProfile().getAge()).isEqualTo(35);
        assertThat(profiles.get(1).getName()).isEqualTo("bob");
        assertThat(profiles.get(1).getProfile().getAge()).isEqualTo(30);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_nested_orderBy_fluent_api(BackendTestContext btc) {

        List<UserProfile> profiles = btc.getProfileRepository()
            .find(q -> q.where(on("profile.age", eq(25))).orderBy(asc("profile.occupation"), desc("name")))
            .toList();

        assertThat(profiles.size()).isEqualTo(2);
        // Both age 25: Engineer (alice), Manager (charlie) - sorted by occupation asc, then name desc
        assertThat(profiles.get(0).getProfile().getOccupation()).isEqualTo("Engineer");
        assertThat(profiles.get(0).getName()).isEqualTo("alice");
        assertThat(profiles.get(1).getProfile().getOccupation()).isEqualTo("Manager");
        assertThat(profiles.get(1).getName()).isEqualTo("charlie");
    }

    // ===== TESTS FOR QUERIES WITHOUT WHERE CLAUSE =====

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_find_with_orderBy_only_no_where(BackendTestContext btc) {
        // Query with orderBy but no WHERE clause - should return all records sorted
        List<UserProfile> profiles = btc.getProfileRepository()
            .find(q -> q.orderBy(asc("profile.age"), asc("name")))
            .toList();

        assertThat(profiles.size()).isEqualTo(4);
        // Sorted by age asc, then name asc: alice(25), charlie(25), bob(30), diana(35)
        assertThat(profiles.get(0).getName()).isEqualTo("alice");
        assertThat(profiles.get(0).getProfile().getAge()).isEqualTo(25);
        assertThat(profiles.get(1).getName()).isEqualTo("charlie");
        assertThat(profiles.get(1).getProfile().getAge()).isEqualTo(25);
        assertThat(profiles.get(2).getName()).isEqualTo("bob");
        assertThat(profiles.get(2).getProfile().getAge()).isEqualTo(30);
        assertThat(profiles.get(3).getName()).isEqualTo("diana");
        assertThat(profiles.get(3).getProfile().getAge()).isEqualTo(35);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_find_with_limit_only_no_where(BackendTestContext btc) {
        // Query with limit but no WHERE clause - should return first N records
        List<UserProfile> profiles = btc.getProfileRepository()
            .find(q -> q.limit(2))
            .toList();

        assertThat(profiles.size()).isEqualTo(2);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_find_with_orderBy_and_limit_no_where(BackendTestContext btc) {
        // Query with orderBy + limit but no WHERE clause - should return top N sorted records
        List<UserProfile> profiles = btc.getProfileRepository()
            .find(q -> q.orderBy(desc("profile.age")).limit(2))
            .toList();

        assertThat(profiles.size()).isEqualTo(2);
        // Top 2 by age descending: diana(35), bob(30)
        assertThat(profiles.get(0).getName()).isEqualTo("diana");
        assertThat(profiles.get(0).getProfile().getAge()).isEqualTo(35);
        assertThat(profiles.get(1).getName()).isEqualTo("bob");
        assertThat(profiles.get(1).getProfile().getAge()).isEqualTo(30);
    }
}
