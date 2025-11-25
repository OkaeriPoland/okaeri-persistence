package eu.okaeri.persistencetest;

import eu.okaeri.persistence.repository.DocumentRepository;
import eu.okaeri.persistence.repository.annotation.DocumentCollection;
import eu.okaeri.persistence.repository.annotation.DocumentIndex;
import eu.okaeri.persistencetest.fixtures.User;
import eu.okaeri.persistencetest.fixtures.UserProfile;
import lombok.Getter;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

/**
 * Test context for method name query parsing E2E tests.
 * Tests the hybrid approach: simple patterns parsed from method names.
 */
@Getter
public class MethodQueryTestContext {

    private final UserQueryRepository userRepository;
    private final ProfileQueryRepository profileRepository;

    public MethodQueryTestContext(UserQueryRepository userRepository, ProfileQueryRepository profileRepository) {
        this.userRepository = userRepository;
        this.profileRepository = profileRepository;
    }

    /**
     * Repository with method name-based queries for User documents.
     */
    @DocumentCollection(path = "users_query", keyLength = 36, indexes = {
        @DocumentIndex(path = "name", maxLength = 255),
        @DocumentIndex(path = "exp"),
        @DocumentIndex(path = "verified")
    })
    public interface UserQueryRepository extends DocumentRepository<UUID, User> {

        // === Simple equality ===
        Optional<User> findByName(String name);

        List<User> findByExp(int exp);

        List<User> findByVerified(boolean verified);

        // === Multiple AND ===
        Optional<User> findByNameAndExp(String name, int exp);

        List<User> findByExpAndVerified(int exp, boolean verified);

        // === Multiple OR ===
        List<User> findByNameOrExp(String name, int exp);

        Stream<User> streamByExpOrVerified(int exp, boolean verified);

        // === Mixed AND/OR (AND has precedence over OR) ===
        // A OR B AND C → A OR (B AND C)
        List<User> findByNameOrExpAndVerified(String name, int exp, boolean verified);

        // A AND B OR C → (A AND B) OR C
        List<User> findByNameAndExpOrVerified(String name, int exp, boolean verified);

        // A OR B AND C OR D → A OR (B AND C) OR D
        List<User> findByNameOrExpAndVerifiedOrExp(String name, int exp, boolean verified, int exp2);

        // === OrderBy ===
        List<User> findByVerifiedOrderByExpDesc(boolean verified);

        List<User> findByExpOrderByNameAsc(int exp);

        Stream<User> streamAllOrderByExpDesc();

        List<User> findAllOrderByNameAsc();

        // === Limiting ===
        Optional<User> findFirstByOrderByExpDesc();

        List<User> findTop2ByVerifiedOrderByExpDesc(boolean verified);

        User findFirstByVerified(boolean verified);

        // === Count ===
        long countByVerified(boolean verified);

        long countByExp(int exp);

        // === Exists ===
        boolean existsByName(String name);

        boolean existsByExp(int exp);

        // === Delete ===
        long deleteByVerified(boolean verified);

        long deleteByExp(int exp);

        // === Alternative prefixes ===
        Optional<User> readByName(String name);

        Optional<User> getByName(String name);

        List<User> queryByExp(int exp);

        // === Stream prefix (enforces Stream<T> return) ===
        Stream<User> streamByExp(int exp);

        Stream<User> streamByName(String name);

        // === Underscore readability ===
        Optional<User> findBy_name(String name);

        List<User> findBy_name_and_exp(String name, int exp);
    }

    /**
     * Repository with nested field queries for UserProfile documents.
     */
    @DocumentCollection(path = "profiles_query", keyLength = 36, indexes = {
        @DocumentIndex(path = "name", maxLength = 255),
        @DocumentIndex(path = "profile.age"),
        @DocumentIndex(path = "profile.occupation", maxLength = 255),
        @DocumentIndex(path = "profile.address.city", maxLength = 255)
    })
    public interface ProfileQueryRepository extends DocumentRepository<UUID, UserProfile> {

        // === Nested field with $ separator ===
        List<UserProfile> findByProfile$Age(int age);

        Optional<UserProfile> findByProfile$Occupation(String occupation);

        List<UserProfile> findByProfile$Address$City(String city);

        // === Nested with ordering ===
        List<UserProfile> findByProfile$AgeOrderByNameAsc(int age);

        Stream<UserProfile> streamAllOrderByProfile$AgeDesc();

        // === Nested with AND/OR ===
        List<UserProfile> findByNameAndProfile$Age(String name, int age);

        List<UserProfile> findByProfile$AgeOrProfile$Occupation(int age, String occupation);

        // === Count/Exists on nested ===
        long countByProfile$Age(int age);

        boolean existsByProfile$Address$City(String city);

        // === Automatic subfield discovery (without $ separator) ===
        // These method names resolve to the same paths as $ variants through camelCase matching
        List<UserProfile> findByProfileAge(int age);

        Optional<UserProfile> findByProfileOccupation(String occupation);

        List<UserProfile> findByProfileAddressCity(String city);

        // === Automatic discovery with ordering ===
        List<UserProfile> findByProfileAgeOrderByNameAsc(int age);

        Stream<UserProfile> streamAllOrderByProfileAgeDesc();

        // === Automatic discovery with AND/OR ===
        List<UserProfile> findByNameAndProfileAge(String name, int age);

        List<UserProfile> findByProfileAgeOrProfileOccupation(int age, String occupation);

        // === Automatic discovery Count/Exists ===
        long countByProfileAge(int age);

        boolean existsByProfileAddressCity(String city);
    }
}
