package eu.okaeri.persistencetest;

import eu.okaeri.persistence.repository.DocumentRepository;
import eu.okaeri.persistence.repository.annotation.DocumentCollection;
import eu.okaeri.persistence.repository.annotation.DocumentIndex;
import eu.okaeri.persistencetest.fixtures.User;
import eu.okaeri.persistencetest.fixtures.UserProfile;
import lombok.Getter;

import java.util.UUID;

/**
 * Shared test context providing access to repositories and test data.
 * Create once per test backend, reuse across all test methods.
 */
@Getter
public class TestContext {

    private final UserRepository userRepository;
    private final UserProfileRepository profileRepository;

    public TestContext(UserRepository userRepository, UserProfileRepository profileRepository) {
        this.userRepository = userRepository;
        this.profileRepository = profileRepository;
    }

    /**
     * Repository interface for User documents with collection configuration.
     */
    @DocumentCollection(path = "users", keyLength = 36, indexes = {
        @DocumentIndex(path = "name", maxLength = 255),
        @DocumentIndex(path = "exp")
    })
    public interface UserRepository extends DocumentRepository<UUID, User> {
    }

    /**
     * Repository interface for UserProfile documents with collection configuration.
     */
    @DocumentCollection(path = "profiles", keyLength = 36, indexes = {
        @DocumentIndex(path = "name", maxLength = 255),
        @DocumentIndex(path = "profile.age")
    })
    public interface UserProfileRepository extends DocumentRepository<UUID, UserProfile> {
    }
}
