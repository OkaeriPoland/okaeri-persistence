package eu.okaeri.persistencetest;

import eu.okaeri.persistence.repository.DocumentRepository;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static eu.okaeri.persistence.filter.OrderBy.asc;
import static eu.okaeri.persistence.filter.OrderBy.desc;
import static eu.okaeri.persistence.filter.condition.Condition.on;
import static eu.okaeri.persistence.filter.predicate.SimplePredicate.eq;
import static eu.okaeri.persistence.filter.predicate.SimplePredicate.gt;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Abstract base class for database-agnostic filter query tests.
 * Each database backend extends this class and provides the setup/teardown.
 * <p>
 * This ensures all backends have consistent behavior for:
 * - WHERE conditions (flat and nested paths)
 * - ORDER BY clauses (flat and nested paths)
 * - LIMIT and SKIP
 * - Complex combinations
 */
public abstract class AbstractFilterQueryTest<R extends DocumentRepository<UUID, ?>> {

    // ========== Repository Interfaces ==========

    public interface UserRepository extends DocumentRepository<UUID, User> {
    }

    public interface UserProfileRepository extends DocumentRepository<UUID, UserProfile> {
    }

    // ========== Abstract Methods (implemented by subclasses) ==========

    /**
     * Get the repository for flat User documents
     */
    protected abstract UserRepository getUserRepository();

    /**
     * Get the repository for nested UserProfile documents
     */
    protected abstract UserProfileRepository getProfileRepository();

    /**
     * Setup test data using repositories.
     * Call this from @BeforeAll after initializing repositories.
     */
    protected void setupTestData() {
        UserRepository userRepo = this.getUserRepository();
        UserProfileRepository profileRepo = this.getProfileRepository();

        // Setup flat user collection
        userRepo.deleteAll();
        userRepo.save(new User("tester", 123));
        userRepo.save(new User("tester2", 456));
        userRepo.save(new User("tester3", 123));

        // Setup nested profile collection
        profileRepo.deleteAll();
        profileRepo.save(new UserProfile("alice",
            new Profile(25, "Engineer",
                new Address("New York", "USA", 10001))));
        profileRepo.save(new UserProfile("bob",
            new Profile(30, "Designer",
                new Address("London", "UK", 20002))));
        profileRepo.save(new UserProfile("charlie",
            new Profile(25, "Manager",
                new Address("Paris", "France", 75001))));
        profileRepo.save(new UserProfile("diana",
            new Profile(35, "Director",
                new Address("Berlin", "Germany", 10115))));
    }

    // ========== Flat Field Tests ==========

    @Test
    public void test_filter_by_name() {
        List<User> users = this.getUserRepository()
            .find(q -> q.where(on("name", eq("tester2"))))
            .collect(Collectors.toList());

        assertEquals(1, users.size());
        assertEquals("tester2", users.get(0).getName());
    }

    @Test
    public void test_filter_by_exp_greater_than() {
        List<User> users = this.getUserRepository()
            .find(q -> q.where(on("exp", gt(200))))
            .collect(Collectors.toList());

        assertEquals(1, users.size());
        assertEquals(456, users.get(0).getExp());
    }

    @Test
    public void test_orderBy_asc() {
        List<User> users = this.getUserRepository()
            .find(q -> q
                .where(on("exp", eq(123)))
                .orderBy(asc("name")))
            .collect(Collectors.toList());

        assertEquals(2, users.size());
        assertEquals("tester", users.get(0).getName());
        assertEquals("tester3", users.get(1).getName());
    }

    @Test
    public void test_orderBy_desc() {
        List<User> users = this.getUserRepository()
            .find(q -> q
                .where(on("exp", eq(123)))
                .orderBy(desc("name")))
            .collect(Collectors.toList());

        assertEquals(2, users.size());
        assertEquals("tester3", users.get(0).getName());
        assertEquals("tester", users.get(1).getName());
    }

    @Test
    public void test_orderBy_exp_desc() {
        List<User> orderedUsers = this.getUserRepository()
            .find(q -> q
                .where(on("exp", gt(0)))
                .orderBy(desc("exp")))
            .collect(Collectors.toList());

        assertEquals(3, orderedUsers.size());
        assertEquals(456, orderedUsers.get(0).getExp()); // tester2
        assertEquals(123, orderedUsers.get(1).getExp()); // tester or tester3
        assertEquals(123, orderedUsers.get(2).getExp()); // tester or tester3
    }

    @Test
    public void test_orderBy_multiple_fields() {
        List<User> users = this.getUserRepository()
            .find(q -> q
                .where(on("exp", gt(0)))
                .orderBy(asc("exp"), asc("name")))
            .collect(Collectors.toList());

        assertEquals(3, users.size());
        // exp=123: tester, tester3 (ordered by name)
        assertEquals(123, users.get(0).getExp());
        assertEquals("tester", users.get(0).getName());
        assertEquals(123, users.get(1).getExp());
        assertEquals("tester3", users.get(1).getName());
        // exp=456: tester2
        assertEquals(456, users.get(2).getExp());
        assertEquals("tester2", users.get(2).getName());
    }

    @Test
    public void test_orderBy_with_limit() {
        List<User> users = this.getUserRepository()
            .find(q -> q
                .where(on("exp", gt(0)))
                .orderBy(desc("exp"))
                .limit(1))
            .collect(Collectors.toList());

        assertEquals(1, users.size());
        assertEquals("tester2", users.get(0).getName());
        assertEquals(456, users.get(0).getExp());
    }

    @Test
    public void test_orderBy_with_skip_and_limit() {
        List<User> users = this.getUserRepository()
            .find(q -> q
                .where(on("exp", gt(0)))
                .orderBy(desc("exp"))
                .skip(1)
                .limit(1))
            .collect(Collectors.toList());

        assertEquals(1, users.size());
        assertEquals(123, users.get(0).getExp());
    }

    @Test
    public void test_orderBy_fluent_api() {
        List<User> users = this.getUserRepository()
            .find(q -> q
                .where(on("exp", gt(0)))
                .orderBy(asc("exp"), asc("name")))
            .collect(Collectors.toList());

        assertEquals(3, users.size());
        // exp=123: tester, tester3 (ordered by name)
        assertEquals(123, users.get(0).getExp());
        assertEquals("tester", users.get(0).getName());
        assertEquals(123, users.get(1).getExp());
        assertEquals("tester3", users.get(1).getName());
        // exp=456: tester2
        assertEquals(456, users.get(2).getExp());
        assertEquals("tester2", users.get(2).getName());
    }

    // ========== Nested Path Tests ==========

    @Test
    public void test_nested_where_profile_age() {
        List<UserProfile> profiles = this.getProfileRepository()
            .find(q -> q.where(on("profile.age", eq(25))))
            .collect(Collectors.toList());

        assertEquals(2, profiles.size());
        // Should find alice and charlie (both age 25)
    }

    @Test
    public void test_nested_where_profile_age_gt() {
        List<UserProfile> profiles = this.getProfileRepository()
            .find(q -> q.where(on("profile.age", gt(25))))
            .collect(Collectors.toList());

        assertEquals(2, profiles.size());
        // Should find bob (30) and diana (35)
    }

    @Test
    public void test_nested_where_address_city() {
        List<UserProfile> profiles = this.getProfileRepository()
            .find(q -> q.where(on("profile.address.city", eq("London"))))
            .collect(Collectors.toList());

        assertEquals(1, profiles.size());
        assertEquals("bob", profiles.get(0).getName());
        assertEquals("London", profiles.get(0).getProfile().getAddress().getCity());
    }

    @Test
    public void test_nested_orderBy_profile_age_asc() {
        List<UserProfile> profiles = this.getProfileRepository()
            .find(q -> q
                .where(on("profile.age", gt(0)))
                .orderBy(asc("profile.age")))
            .collect(Collectors.toList());

        assertEquals(4, profiles.size());
        assertEquals(25, profiles.get(0).getProfile().getAge()); // alice or charlie
        assertEquals(25, profiles.get(1).getProfile().getAge()); // alice or charlie
        assertEquals(30, profiles.get(2).getProfile().getAge()); // bob
        assertEquals(35, profiles.get(3).getProfile().getAge()); // diana
    }

    @Test
    public void test_nested_orderBy_profile_age_desc() {
        List<UserProfile> profiles = this.getProfileRepository()
            .find(q -> q
                .where(on("profile.age", gt(0)))
                .orderBy(desc("profile.age")))
            .collect(Collectors.toList());

        assertEquals(4, profiles.size());
        assertEquals(35, profiles.get(0).getProfile().getAge()); // diana
        assertEquals(30, profiles.get(1).getProfile().getAge()); // bob
        assertEquals(25, profiles.get(2).getProfile().getAge()); // alice or charlie
        assertEquals(25, profiles.get(3).getProfile().getAge()); // alice or charlie
    }

    @Test
    public void test_nested_orderBy_name_then_profile_age() {
        List<UserProfile> profiles = this.getProfileRepository()
            .find(q -> q
                .where(on("profile.age", eq(25)))
                .orderBy(asc("name")))
            .collect(Collectors.toList());

        assertEquals(2, profiles.size());
        assertEquals("alice", profiles.get(0).getName());
        assertEquals("charlie", profiles.get(1).getName());
    }

    @Test
    public void test_nested_orderBy_address_city() {
        List<UserProfile> profiles = this.getProfileRepository()
            .find(q -> q
                .where(on("profile.age", gt(0)))
                .orderBy(asc("profile.address.city")))
            .collect(Collectors.toList());

        assertEquals(4, profiles.size());
        // Alphabetical by city: Berlin, London, New York, Paris
        assertEquals("Berlin", profiles.get(0).getProfile().getAddress().getCity());
        assertEquals("London", profiles.get(1).getProfile().getAddress().getCity());
        assertEquals("New York", profiles.get(2).getProfile().getAddress().getCity());
        assertEquals("Paris", profiles.get(3).getProfile().getAddress().getCity());
    }

    @Test
    public void test_nested_complex_query() {
        List<UserProfile> profiles = this.getProfileRepository()
            .find(q -> q
                .where(on("profile.age", gt(25)))
                .orderBy(desc("profile.age"))
                .limit(2))
            .collect(Collectors.toList());

        assertEquals(2, profiles.size());
        assertEquals("diana", profiles.get(0).getName());
        assertEquals(35, profiles.get(0).getProfile().getAge());
        assertEquals("bob", profiles.get(1).getName());
        assertEquals(30, profiles.get(1).getProfile().getAge());
    }

    @Test
    public void test_nested_orderBy_fluent_api() {
        List<UserProfile> profiles = this.getProfileRepository()
            .find(q -> q
                .where(on("profile.age", eq(25)))
                .orderBy(asc("profile.occupation"), desc("name")))
            .collect(Collectors.toList());

        assertEquals(2, profiles.size());
        // Both age 25: Engineer (alice), Manager (charlie) - sorted by occupation asc, then name desc
        assertEquals("Engineer", profiles.get(0).getProfile().getOccupation());
        assertEquals("alice", profiles.get(0).getName());
        assertEquals("Manager", profiles.get(1).getProfile().getOccupation());
        assertEquals("charlie", profiles.get(1).getName());
    }
}
