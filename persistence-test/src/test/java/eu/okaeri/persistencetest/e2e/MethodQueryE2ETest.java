package eu.okaeri.persistencetest.e2e;

import eu.okaeri.persistence.PersistenceEntity;
import eu.okaeri.persistence.document.DocumentPersistence;
import eu.okaeri.persistencetest.MethodQueryTestContext;
import eu.okaeri.persistencetest.containers.BackendContainer;
import eu.okaeri.persistencetest.fixtures.Address;
import eu.okaeri.persistencetest.fixtures.Profile;
import eu.okaeri.persistencetest.fixtures.User;
import eu.okaeri.persistencetest.fixtures.UserProfile;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E tests for method name query parsing.
 * Tests the hybrid approach: simple patterns parsed from method names.
 */
public class MethodQueryE2ETest extends E2ETestBase {

    /**
     * Context holder for method query tests.
     */
    static class MethodQueryBackendContext implements AutoCloseable {
        private static final ThreadLocal<MethodQueryBackendContext> CURRENT = new ThreadLocal<>();

        private final BackendContainer backend;
        private final MethodQueryTestContext.UserQueryRepository userRepository;
        private final MethodQueryTestContext.ProfileQueryRepository profileRepository;

        private MethodQueryBackendContext(BackendContainer backend) {
            this.backend = backend;

            DocumentPersistence persistence = backend.createPersistence();
            this.userRepository = persistence.createRepository(MethodQueryTestContext.UserQueryRepository.class);
            this.profileRepository = persistence.createRepository(MethodQueryTestContext.ProfileQueryRepository.class);

            // Clean slate
            this.userRepository.deleteAll();
            this.profileRepository.deleteAll();

            CURRENT.set(this);
        }

        static MethodQueryBackendContext create(BackendContainer backend) {
            return new MethodQueryBackendContext(backend);
        }

        static MethodQueryBackendContext getCurrent() {
            return CURRENT.get();
        }

        MethodQueryTestContext.UserQueryRepository getUserRepository() {
            return this.userRepository;
        }

        MethodQueryTestContext.ProfileQueryRepository getProfileRepository() {
            return this.profileRepository;
        }

        @Override
        public String toString() {
            return this.backend.getName();
        }

        @Override
        public void close() throws Exception {
            this.backend.close();
            CURRENT.remove();
        }
    }

    protected static Stream<MethodQueryBackendContext> allBackendsWithUserData() {
        return allBackends().map(backend -> {
            MethodQueryBackendContext ctx = MethodQueryBackendContext.create(backend);

            // Setup test data
            ctx.getUserRepository().save(new User("alice", 100, true));
            ctx.getUserRepository().save(new User("bob", 200, false));
            ctx.getUserRepository().save(new User("charlie", 100, true));
            ctx.getUserRepository().save(new User("diana", 300, false));

            return ctx;
        });
    }

    protected static Stream<MethodQueryBackendContext> allBackendsWithProfileData() {
        return allBackends().map(backend -> {
            MethodQueryBackendContext ctx = MethodQueryBackendContext.create(backend);

            ctx.getProfileRepository().save(new UserProfile("alice", new Profile(25, "Engineer", new Address("New York", "USA", 10001))));
            ctx.getProfileRepository().save(new UserProfile("bob", new Profile(30, "Designer", new Address("London", "UK", 20002))));
            ctx.getProfileRepository().save(new UserProfile("charlie", new Profile(25, "Manager", new Address("Paris", "France", 75001))));
            ctx.getProfileRepository().save(new UserProfile("diana", new Profile(35, "Engineer", new Address("Berlin", "Germany", 10115))));

            return ctx;
        });
    }

    // ===== SIMPLE EQUALITY =====

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithUserData")
    void test_findByName(MethodQueryBackendContext ctx) {
        Optional<User> user = ctx.getUserRepository().findByName("alice");

        assertThat(user).isPresent();
        assertThat(user.get().getName()).isEqualTo("alice");
        assertThat(user.get().getExp()).isEqualTo(100);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithUserData")
    void test_findByName_notFound(MethodQueryBackendContext ctx) {
        Optional<User> user = ctx.getUserRepository().findByName("nonexistent");
        assertThat(user).isEmpty();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithUserData")
    void test_findByExp(MethodQueryBackendContext ctx) {
        List<User> users = ctx.getUserRepository().findByExp(100);

        assertThat(users).hasSize(2);
        assertThat(users).extracting(User::getName).containsExactlyInAnyOrder("alice", "charlie");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithUserData")
    void test_findByVerified(MethodQueryBackendContext ctx) {
        List<User> verified = ctx.getUserRepository().findByVerified(true);
        List<User> unverified = ctx.getUserRepository().findByVerified(false);

        assertThat(verified).hasSize(2);
        assertThat(unverified).hasSize(2);
    }

    // ===== MULTIPLE AND =====

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithUserData")
    void test_findByNameAndExp(MethodQueryBackendContext ctx) {
        Optional<User> user = ctx.getUserRepository().findByNameAndExp("alice", 100);

        assertThat(user).isPresent();
        assertThat(user.get().getName()).isEqualTo("alice");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithUserData")
    void test_findByNameAndExp_noMatch(MethodQueryBackendContext ctx) {
        Optional<User> user = ctx.getUserRepository().findByNameAndExp("alice", 999);
        assertThat(user).isEmpty();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithUserData")
    void test_findByExpAndVerified(MethodQueryBackendContext ctx) {
        List<User> users = ctx.getUserRepository().findByExpAndVerified(100, true);

        assertThat(users).hasSize(2);
        assertThat(users).extracting(User::getName).containsExactlyInAnyOrder("alice", "charlie");
    }

    // ===== MULTIPLE OR =====

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithUserData")
    void test_findByNameOrExp(MethodQueryBackendContext ctx) {
        List<User> users = ctx.getUserRepository().findByNameOrExp("alice", 200);

        assertThat(users).hasSize(2);
        assertThat(users).extracting(User::getName).containsExactlyInAnyOrder("alice", "bob");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithUserData")
    void test_streamByExpOrVerified(MethodQueryBackendContext ctx) {
        List<User> users = ctx.getUserRepository().streamByExpOrVerified(300, true).toList();

        // diana has exp=300, alice and charlie are verified=true
        assertThat(users).hasSize(3);
        assertThat(users).extracting(User::getName).containsExactlyInAnyOrder("alice", "charlie", "diana");
    }

    // ===== MIXED AND/OR (AND has precedence over OR) =====

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithUserData")
    void test_findByNameOrExpAndVerified_precedence(MethodQueryBackendContext ctx) {
        // Test data: alice(100,true), bob(200,false), charlie(100,true), diana(300,false)
        // Query: name="bob" OR (exp=100 AND verified=true)
        // Expected: bob (matches name), alice (matches exp=100 AND verified=true), charlie (matches exp=100 AND verified=true)
        List<User> users = ctx.getUserRepository().findByNameOrExpAndVerified("bob", 100, true);

        assertThat(users).hasSize(3);
        assertThat(users).extracting(User::getName).containsExactlyInAnyOrder("alice", "bob", "charlie");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithUserData")
    void test_findByNameAndExpOrVerified_precedence(MethodQueryBackendContext ctx) {
        // Test data: alice(100,true), bob(200,false), charlie(100,true), diana(300,false)
        // Query: (name="alice" AND exp=100) OR verified=false
        // Expected: alice (matches name AND exp), bob (matches verified=false), diana (matches verified=false)
        List<User> users = ctx.getUserRepository().findByNameAndExpOrVerified("alice", 100, false);

        assertThat(users).hasSize(3);
        assertThat(users).extracting(User::getName).containsExactlyInAnyOrder("alice", "bob", "diana");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithUserData")
    void test_findByNameOrExpAndVerifiedOrExp_complex_precedence(MethodQueryBackendContext ctx) {
        // Test data: alice(100,true), bob(200,false), charlie(100,true), diana(300,false)
        // Query: name="diana" OR (exp=100 AND verified=true) OR exp=200
        // Expected: diana (matches name), alice (100 AND true), charlie (100 AND true), bob (exp=200)
        List<User> users = ctx.getUserRepository().findByNameOrExpAndVerifiedOrExp("diana", 100, true, 200);

        assertThat(users).hasSize(4);
        assertThat(users).extracting(User::getName).containsExactlyInAnyOrder("alice", "bob", "charlie", "diana");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithUserData")
    void test_findByNameOrExpAndVerified_no_and_match(MethodQueryBackendContext ctx) {
        // Test data: alice(100,true), bob(200,false), charlie(100,true), diana(300,false)
        // Query: name="bob" OR (exp=200 AND verified=true)
        // Expected: only bob (matches name); nobody matches exp=200 AND verified=true
        List<User> users = ctx.getUserRepository().findByNameOrExpAndVerified("bob", 200, true);

        assertThat(users).hasSize(1);
        assertThat(users.get(0).getName()).isEqualTo("bob");
    }

    // ===== ORDERBY =====

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithUserData")
    void test_findByVerifiedOrderByExpDesc(MethodQueryBackendContext ctx) {
        List<User> users = ctx.getUserRepository().findByVerifiedOrderByExpDesc(false);

        assertThat(users).hasSize(2);
        assertThat(users.get(0).getName()).isEqualTo("diana"); // exp=300
        assertThat(users.get(1).getName()).isEqualTo("bob");   // exp=200
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithUserData")
    void test_findByExpOrderByNameAsc(MethodQueryBackendContext ctx) {
        List<User> users = ctx.getUserRepository().findByExpOrderByNameAsc(100);

        assertThat(users).hasSize(2);
        assertThat(users.get(0).getName()).isEqualTo("alice");
        assertThat(users.get(1).getName()).isEqualTo("charlie");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithUserData")
    void test_streamAllOrderByExpDesc(MethodQueryBackendContext ctx) {
        List<User> users = ctx.getUserRepository().streamAllOrderByExpDesc().toList();

        assertThat(users).hasSize(4);
        assertThat(users.get(0).getExp()).isEqualTo(300); // diana
        assertThat(users.get(1).getExp()).isEqualTo(200); // bob
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithUserData")
    void test_findAllOrderByNameAsc(MethodQueryBackendContext ctx) {
        List<User> users = ctx.getUserRepository().findAllOrderByNameAsc();

        assertThat(users).hasSize(4);
        assertThat(users.get(0).getName()).isEqualTo("alice");
        assertThat(users.get(1).getName()).isEqualTo("bob");
        assertThat(users.get(2).getName()).isEqualTo("charlie");
        assertThat(users.get(3).getName()).isEqualTo("diana");
    }

    // ===== LIMITING =====

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithUserData")
    void test_findFirstByOrderByExpDesc(MethodQueryBackendContext ctx) {
        Optional<User> user = ctx.getUserRepository().findFirstByOrderByExpDesc();

        assertThat(user).isPresent();
        assertThat(user.get().getName()).isEqualTo("diana");
        assertThat(user.get().getExp()).isEqualTo(300);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithUserData")
    void test_findTop2ByVerifiedOrderByExpDesc(MethodQueryBackendContext ctx) {
        List<User> users = ctx.getUserRepository().findTop2ByVerifiedOrderByExpDesc(false);

        assertThat(users).hasSize(2);
        assertThat(users.get(0).getName()).isEqualTo("diana"); // exp=300
        assertThat(users.get(1).getName()).isEqualTo("bob");   // exp=200
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithUserData")
    void test_findFirstByVerified_nakedReturn(MethodQueryBackendContext ctx) {
        User user = ctx.getUserRepository().findFirstByVerified(true);

        assertThat(user).isNotNull();
        assertThat(user.isVerified()).isTrue();
    }

    // ===== COUNT =====

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithUserData")
    void test_countByVerified(MethodQueryBackendContext ctx) {
        long verified = ctx.getUserRepository().countByVerified(true);
        long unverified = ctx.getUserRepository().countByVerified(false);

        assertThat(verified).isEqualTo(2);
        assertThat(unverified).isEqualTo(2);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithUserData")
    void test_countByExp(MethodQueryBackendContext ctx) {
        long count100 = ctx.getUserRepository().countByExp(100);
        long count200 = ctx.getUserRepository().countByExp(200);
        long count999 = ctx.getUserRepository().countByExp(999);

        assertThat(count100).isEqualTo(2);
        assertThat(count200).isEqualTo(1);
        assertThat(count999).isEqualTo(0);
    }

    // ===== EXISTS =====

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithUserData")
    void test_existsByName(MethodQueryBackendContext ctx) {
        assertThat(ctx.getUserRepository().existsByName("alice")).isTrue();
        assertThat(ctx.getUserRepository().existsByName("nonexistent")).isFalse();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithUserData")
    void test_existsByExp(MethodQueryBackendContext ctx) {
        assertThat(ctx.getUserRepository().existsByExp(100)).isTrue();
        assertThat(ctx.getUserRepository().existsByExp(999)).isFalse();
    }

    // ===== DELETE =====

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithUserData")
    void test_deleteByVerified(MethodQueryBackendContext ctx) {
        long deleted = ctx.getUserRepository().deleteByVerified(false);

        assertThat(deleted).isEqualTo(2); // bob and diana
        assertThat(ctx.getUserRepository().countByVerified(false)).isEqualTo(0);
        assertThat(ctx.getUserRepository().countByVerified(true)).isEqualTo(2);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithUserData")
    void test_deleteByExp(MethodQueryBackendContext ctx) {
        long deleted = ctx.getUserRepository().deleteByExp(100);

        assertThat(deleted).isEqualTo(2); // alice and charlie
        assertThat(ctx.getUserRepository().existsByName("alice")).isFalse();
        assertThat(ctx.getUserRepository().existsByName("charlie")).isFalse();
        assertThat(ctx.getUserRepository().existsByName("bob")).isTrue();
    }

    // ===== ALTERNATIVE PREFIXES =====

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithUserData")
    void test_readByName(MethodQueryBackendContext ctx) {
        Optional<User> user = ctx.getUserRepository().readByName("bob");

        assertThat(user).isPresent();
        assertThat(user.get().getExp()).isEqualTo(200);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithUserData")
    void test_getByName(MethodQueryBackendContext ctx) {
        Optional<User> user = ctx.getUserRepository().getByName("charlie");

        assertThat(user).isPresent();
        assertThat(user.get().getExp()).isEqualTo(100);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithUserData")
    void test_queryByExp(MethodQueryBackendContext ctx) {
        List<User> users = ctx.getUserRepository().queryByExp(200);

        assertThat(users).hasSize(1);
        assertThat(users.get(0).getName()).isEqualTo("bob");
    }

    // ===== STREAM PREFIX =====

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithUserData")
    void test_streamByExp(MethodQueryBackendContext ctx) {
        List<User> users = ctx.getUserRepository().streamByExp(100).toList();

        assertThat(users).hasSize(2);
        assertThat(users).extracting(User::getName).containsExactlyInAnyOrder("alice", "charlie");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithUserData")
    void test_streamByName(MethodQueryBackendContext ctx) {
        List<User> users = ctx.getUserRepository().streamByName("diana").toList();

        assertThat(users).hasSize(1);
        assertThat(users.get(0).getExp()).isEqualTo(300);
    }

    // ===== UNDERSCORE READABILITY =====

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithUserData")
    void test_findBy_name(MethodQueryBackendContext ctx) {
        Optional<User> user = ctx.getUserRepository().findBy_name("alice");

        assertThat(user).isPresent();
        assertThat(user.get().getExp()).isEqualTo(100);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithUserData")
    void test_findBy_name_and_exp(MethodQueryBackendContext ctx) {
        List<User> users = ctx.getUserRepository().findBy_name_and_exp("alice", 100);

        assertThat(users).hasSize(1);
        assertThat(users.get(0).getName()).isEqualTo("alice");
    }

    // ===== NESTED FIELD QUERIES ($ separator) =====

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithProfileData")
    void test_findByProfile$Age(MethodQueryBackendContext ctx) {
        List<UserProfile> profiles = ctx.getProfileRepository().findByProfile$Age(25);

        assertThat(profiles).hasSize(2);
        assertThat(profiles).extracting(UserProfile::getName).containsExactlyInAnyOrder("alice", "charlie");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithProfileData")
    void test_findByProfile$Occupation(MethodQueryBackendContext ctx) {
        Optional<UserProfile> profile = ctx.getProfileRepository().findByProfile$Occupation("Designer");

        assertThat(profile).isPresent();
        assertThat(profile.get().getName()).isEqualTo("bob");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithProfileData")
    void test_findByProfile$Address$City(MethodQueryBackendContext ctx) {
        List<UserProfile> profiles = ctx.getProfileRepository().findByProfile$Address$City("London");

        assertThat(profiles).hasSize(1);
        assertThat(profiles.get(0).getName()).isEqualTo("bob");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithProfileData")
    void test_findByProfile$AgeOrderByNameAsc(MethodQueryBackendContext ctx) {
        List<UserProfile> profiles = ctx.getProfileRepository().findByProfile$AgeOrderByNameAsc(25);

        assertThat(profiles).hasSize(2);
        assertThat(profiles.get(0).getName()).isEqualTo("alice");
        assertThat(profiles.get(1).getName()).isEqualTo("charlie");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithProfileData")
    void test_streamAllOrderByProfile$AgeDesc(MethodQueryBackendContext ctx) {
        List<UserProfile> profiles = ctx.getProfileRepository().streamAllOrderByProfile$AgeDesc().toList();

        assertThat(profiles).hasSize(4);
        assertThat(profiles.get(0).getProfile().getAge()).isEqualTo(35); // diana
        assertThat(profiles.get(1).getProfile().getAge()).isEqualTo(30); // bob
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithProfileData")
    void test_findByNameAndProfile$Age(MethodQueryBackendContext ctx) {
        List<UserProfile> profiles = ctx.getProfileRepository().findByNameAndProfile$Age("alice", 25);

        assertThat(profiles).hasSize(1);
        assertThat(profiles.get(0).getName()).isEqualTo("alice");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithProfileData")
    void test_findByProfile$AgeOrProfile$Occupation(MethodQueryBackendContext ctx) {
        // age=25 (alice, charlie) OR occupation="Designer" (bob)
        List<UserProfile> profiles = ctx.getProfileRepository().findByProfile$AgeOrProfile$Occupation(25, "Designer");

        assertThat(profiles).hasSize(3);
        assertThat(profiles).extracting(UserProfile::getName).containsExactlyInAnyOrder("alice", "bob", "charlie");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithProfileData")
    void test_countByProfile$Age(MethodQueryBackendContext ctx) {
        long count25 = ctx.getProfileRepository().countByProfile$Age(25);
        long count30 = ctx.getProfileRepository().countByProfile$Age(30);
        long count99 = ctx.getProfileRepository().countByProfile$Age(99);

        assertThat(count25).isEqualTo(2);
        assertThat(count30).isEqualTo(1);
        assertThat(count99).isEqualTo(0);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithProfileData")
    void test_existsByProfile$Address$City(MethodQueryBackendContext ctx) {
        assertThat(ctx.getProfileRepository().existsByProfile$Address$City("London")).isTrue();
        assertThat(ctx.getProfileRepository().existsByProfile$Address$City("Tokyo")).isFalse();
    }

    // ===== AUTOMATIC SUBFIELD DISCOVERY (without $ separator) =====
    // These tests verify that camelCase method names automatically resolve to nested field paths

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithProfileData")
    void test_findByProfileAge_autoDiscovery(MethodQueryBackendContext ctx) {
        // findByProfileAge should resolve to profile.age (same as findByProfile$Age)
        List<UserProfile> profiles = ctx.getProfileRepository().findByProfileAge(25);

        assertThat(profiles).hasSize(2);
        assertThat(profiles).extracting(UserProfile::getName).containsExactlyInAnyOrder("alice", "charlie");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithProfileData")
    void test_findByProfileOccupation_autoDiscovery(MethodQueryBackendContext ctx) {
        // findByProfileOccupation should resolve to profile.occupation
        Optional<UserProfile> profile = ctx.getProfileRepository().findByProfileOccupation("Designer");

        assertThat(profile).isPresent();
        assertThat(profile.get().getName()).isEqualTo("bob");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithProfileData")
    void test_findByProfileAddressCity_autoDiscovery(MethodQueryBackendContext ctx) {
        // findByProfileAddressCity should resolve to profile.address.city (3-level nesting)
        List<UserProfile> profiles = ctx.getProfileRepository().findByProfileAddressCity("London");

        assertThat(profiles).hasSize(1);
        assertThat(profiles.get(0).getName()).isEqualTo("bob");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithProfileData")
    void test_findByProfileAgeOrderByNameAsc_autoDiscovery(MethodQueryBackendContext ctx) {
        // Both condition and ordering use automatic discovery
        List<UserProfile> profiles = ctx.getProfileRepository().findByProfileAgeOrderByNameAsc(25);

        assertThat(profiles).hasSize(2);
        assertThat(profiles.get(0).getName()).isEqualTo("alice");
        assertThat(profiles.get(1).getName()).isEqualTo("charlie");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithProfileData")
    void test_streamAllOrderByProfileAgeDesc_autoDiscovery(MethodQueryBackendContext ctx) {
        // Ordering on nested field with automatic discovery
        List<UserProfile> profiles = ctx.getProfileRepository().streamAllOrderByProfileAgeDesc().toList();

        assertThat(profiles).hasSize(4);
        assertThat(profiles.get(0).getProfile().getAge()).isEqualTo(35); // diana
        assertThat(profiles.get(1).getProfile().getAge()).isEqualTo(30); // bob
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithProfileData")
    void test_findByNameAndProfileAge_autoDiscovery(MethodQueryBackendContext ctx) {
        // AND with automatic discovery on second field
        List<UserProfile> profiles = ctx.getProfileRepository().findByNameAndProfileAge("alice", 25);

        assertThat(profiles).hasSize(1);
        assertThat(profiles.get(0).getName()).isEqualTo("alice");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithProfileData")
    void test_findByProfileAgeOrProfileOccupation_autoDiscovery(MethodQueryBackendContext ctx) {
        // OR with automatic discovery on both fields
        // age=25 (alice, charlie) OR occupation="Designer" (bob)
        List<UserProfile> profiles = ctx.getProfileRepository().findByProfileAgeOrProfileOccupation(25, "Designer");

        assertThat(profiles).hasSize(3);
        assertThat(profiles).extracting(UserProfile::getName).containsExactlyInAnyOrder("alice", "bob", "charlie");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithProfileData")
    void test_countByProfileAge_autoDiscovery(MethodQueryBackendContext ctx) {
        // Count with automatic discovery
        long count25 = ctx.getProfileRepository().countByProfileAge(25);
        long count30 = ctx.getProfileRepository().countByProfileAge(30);
        long count99 = ctx.getProfileRepository().countByProfileAge(99);

        assertThat(count25).isEqualTo(2);
        assertThat(count30).isEqualTo(1);
        assertThat(count99).isEqualTo(0);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithProfileData")
    void test_existsByProfileAddressCity_autoDiscovery(MethodQueryBackendContext ctx) {
        // Exists with 3-level nesting and automatic discovery
        assertThat(ctx.getProfileRepository().existsByProfileAddressCity("London")).isTrue();
        assertThat(ctx.getProfileRepository().existsByProfileAddressCity("Tokyo")).isFalse();
    }

    // ===== SET RETURN TYPE =====

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithUserData")
    void test_findByExpAndName_returnsSet(MethodQueryBackendContext ctx) {
        Set<User> users = ctx.getUserRepository().findByExpAndName(100, "alice");

        assertThat(users).hasSize(1);
        assertThat(users.iterator().next().getName()).isEqualTo("alice");
    }

    // ===== PERSISTENCE ENTITY WRAPPER RETURN TYPES =====

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithUserData")
    void test_findByNameAndVerified_returnsEntity(MethodQueryBackendContext ctx) {
        Optional<PersistenceEntity<User>> entity = ctx.getUserRepository()
            .findByNameAndVerified("alice", true);

        assertThat(entity).isPresent();
        assertThat(entity.get().getPath()).isNotNull();
        assertThat(entity.get().getValue().getName()).isEqualTo("alice");
        assertThat(entity.get().getValue().getExp()).isEqualTo(100);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithUserData")
    void test_findByNameAndVerified_notFound(MethodQueryBackendContext ctx) {
        Optional<PersistenceEntity<User>> entity = ctx.getUserRepository()
            .findByNameAndVerified("alice", false); // alice is verified=true
        assertThat(entity).isEmpty();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithUserData")
    void test_findByVerifiedAndExp_returnsEntityList(MethodQueryBackendContext ctx) {
        List<PersistenceEntity<User>> entities = ctx.getUserRepository()
            .findByVerifiedAndExp(true, 100);

        assertThat(entities).hasSize(2); // alice and charlie
        assertThat(entities).allSatisfy(e -> {
            assertThat(e.getPath()).isNotNull();
            assertThat(e.getValue()).isNotNull();
            assertThat(e.getValue().isVerified()).isTrue();
            assertThat(e.getValue().getExp()).isEqualTo(100);
        });
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithUserData")
    void test_streamByNameAndExp_returnsEntityStream(MethodQueryBackendContext ctx) {
        List<PersistenceEntity<User>> entities = ctx.getUserRepository()
            .streamByNameAndExp("alice", 100)
            .toList();

        assertThat(entities).hasSize(1);
        assertThat(entities.get(0).getPath()).isNotNull();
        assertThat(entities.get(0).getValue().getName()).isEqualTo("alice");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithUserData")
    void test_findByVerifiedAndName_returnsEntitySet(MethodQueryBackendContext ctx) {
        Set<PersistenceEntity<User>> entities = ctx.getUserRepository()
            .findByVerifiedAndName(true, "alice");

        assertThat(entities).hasSize(1);
        PersistenceEntity<User> entity = entities.iterator().next();
        assertThat(entity.getPath()).isNotNull();
        assertThat(entity.getValue().getName()).isEqualTo("alice");
        assertThat(entity.getValue().isVerified()).isTrue();
    }
}
