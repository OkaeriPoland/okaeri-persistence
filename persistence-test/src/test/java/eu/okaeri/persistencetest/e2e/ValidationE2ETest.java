package eu.okaeri.persistencetest.e2e;

import eu.okaeri.configs.exception.ValidationException;
import eu.okaeri.configs.json.simple.JsonSimpleConfigurer;
import eu.okaeri.configs.validator.okaeri.OkaeriValidator;
import eu.okaeri.persistence.Persistence;
import eu.okaeri.persistence.document.DocumentPersistence;
import eu.okaeri.persistence.document.PersistenceBuilder;
import eu.okaeri.persistence.repository.DocumentRepository;
import eu.okaeri.persistence.repository.annotation.DocumentCollection;
import eu.okaeri.persistencetest.containers.BackendContainer;
import eu.okaeri.persistencetest.fixtures.PlayerStats;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * E2E Validation Tests - verifies document injection API with validator.
 * Tests validation across all backends using the new `.validator()` injection API.
 */
public class ValidationE2ETest extends E2ETestBase {

    @DocumentCollection(path = "player_stats")
    public interface PlayerStatsRepository extends DocumentRepository<String, PlayerStats> {
    }

    public static class ValidationTestContext implements AutoCloseable {
        private static final ThreadLocal<ValidationTestContext> CURRENT = new ThreadLocal<>();

        private final String name;
        private final BackendContainer backend;
        private final PlayerStatsRepository repository;

        public ValidationTestContext(String name, BackendContainer backend, DocumentPersistence persistence) {
            this.name = name;
            this.backend = backend;
            this.repository = persistence.createRepository(PlayerStatsRepository.class);
            this.repository.deleteAll();

            CURRENT.set(this);
        }

        public PlayerStatsRepository getRepository() {
            return this.repository;
        }

        @Override
        public String toString() {
            return this.name;
        }

        @Override
        public void close() throws Exception {
            this.backend.close();
            CURRENT.remove();
        }

        static ValidationTestContext getCurrent() {
            return CURRENT.get();
        }
    }

    protected static Stream<ValidationTestContext> backendsWithValidation() {
        return allBackends().map(backend -> {

            Object builder = backend.createPersistenceBuilder();
            if (!(builder instanceof PersistenceBuilder)) {
                throw new IllegalStateException("Backend " + backend.getName() + " doesn't support builder");
            }

            @SuppressWarnings("unchecked")
            PersistenceBuilder<?, ?> typedBuilder = (PersistenceBuilder<?, ?>) builder;

            Object builtBackend = typedBuilder
                .configurer(new JsonSimpleConfigurer())
                .validator(new OkaeriValidator())
                .build();

            DocumentPersistence persistence = new DocumentPersistence((Persistence) builtBackend);

            return new ValidationTestContext(backend.getName(), backend, persistence);
        });
    }

    @AfterEach
    void cleanupValidationContext() throws Exception {
        ValidationTestContext ctx = ValidationTestContext.getCurrent();
        if (ctx != null) {
            ctx.close();
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("backendsWithValidation")
    void test_save_valid_document(ValidationTestContext ctx) {
        // Valid document - all constraints satisfied
        PlayerStats valid = new PlayerStats("alice", 50);
        valid.setExperience(1000);
        valid.setWinRate(75.5);

        // Should save without error
        ctx.getRepository().save(valid);

        // Verify it was saved
        assertThat(ctx.getRepository().findByPath(valid.getPath().getValue())).isPresent();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("backendsWithValidation")
    void test_save_invalid_username_blank(ValidationTestContext ctx) {
        // Invalid: blank username (violates @NotBlank)
        PlayerStats invalid = new PlayerStats("", 50);
        invalid.setExperience(100);
        invalid.setWinRate(50.0);

        // Should throw ValidationException on save
        assertThatThrownBy(() -> ctx.getRepository().save(invalid))
            .isInstanceOf(ValidationException.class);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("backendsWithValidation")
    void test_save_invalid_username_too_short(ValidationTestContext ctx) {
        // Invalid: username too short (violates @Size(min=3))
        PlayerStats invalid = new PlayerStats("ab", 50);
        invalid.setExperience(100);
        invalid.setWinRate(50.0);

        // Should throw ValidationException on save
        assertThatThrownBy(() -> ctx.getRepository().save(invalid))
            .isInstanceOf(ValidationException.class);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("backendsWithValidation")
    void test_save_invalid_username_too_long(ValidationTestContext ctx) {
        // Invalid: username too long (violates @Size(max=16))
        PlayerStats invalid = new PlayerStats("this_username_is_way_too_long", 50);
        invalid.setExperience(100);
        invalid.setWinRate(50.0);

        // Should throw ValidationException on save
        assertThatThrownBy(() -> ctx.getRepository().save(invalid))
            .isInstanceOf(ValidationException.class);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("backendsWithValidation")
    void test_save_invalid_level_too_low(ValidationTestContext ctx) {
        // Invalid: level below minimum (violates @Min(1))
        PlayerStats invalid = new PlayerStats("alice", 0);
        invalid.setExperience(100);
        invalid.setWinRate(50.0);

        // Should throw ValidationException on save
        assertThatThrownBy(() -> ctx.getRepository().save(invalid))
            .isInstanceOf(ValidationException.class);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("backendsWithValidation")
    void test_save_invalid_level_too_high(ValidationTestContext ctx) {
        // Invalid: level above maximum (violates @Max(100))
        PlayerStats invalid = new PlayerStats("alice", 101);
        invalid.setExperience(100);
        invalid.setWinRate(50.0);

        // Should throw ValidationException on save
        assertThatThrownBy(() -> ctx.getRepository().save(invalid))
            .isInstanceOf(ValidationException.class);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("backendsWithValidation")
    void test_save_invalid_experience_negative(ValidationTestContext ctx) {
        // Invalid: negative experience (violates @PositiveOrZero)
        PlayerStats invalid = new PlayerStats("alice", 50);
        invalid.setExperience(-100);
        invalid.setWinRate(50.0);

        // Should throw ValidationException on save
        assertThatThrownBy(() -> ctx.getRepository().save(invalid))
            .isInstanceOf(ValidationException.class);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("backendsWithValidation")
    void test_save_invalid_winRate_too_low(ValidationTestContext ctx) {
        // Invalid: winRate below minimum (violates @DecimalMin("0.0"))
        PlayerStats invalid = new PlayerStats("alice", 50);
        invalid.setExperience(100);
        invalid.setWinRate(-5.0);

        // Should throw ValidationException on save
        assertThatThrownBy(() -> ctx.getRepository().save(invalid))
            .isInstanceOf(ValidationException.class);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("backendsWithValidation")
    void test_save_invalid_winRate_too_high(ValidationTestContext ctx) {
        // Invalid: winRate above maximum (violates @DecimalMax("100.0"))
        PlayerStats invalid = new PlayerStats("alice", 50);
        invalid.setExperience(100);
        invalid.setWinRate(150.0);

        // Should throw ValidationException on save
        assertThatThrownBy(() -> ctx.getRepository().save(invalid))
            .isInstanceOf(ValidationException.class);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("backendsWithValidation")
    void test_save_valid_edge_cases(ValidationTestContext ctx) {
        // Valid edge cases - exactly at boundaries
        PlayerStats valid1 = new PlayerStats("abc", 1);  // Min username length, min level
        valid1.setExperience(0);  // Min experience
        valid1.setWinRate(0.0);  // Min winRate
        ctx.getRepository().save(valid1);

        PlayerStats valid2 = new PlayerStats("sixteencharsname", 100);  // Max username length, max level
        valid2.setExperience(999999);  // Large experience
        valid2.setWinRate(100.0);  // Max winRate
        ctx.getRepository().save(valid2);

        // Verify both saved
        assertThat(ctx.getRepository().count()).isEqualTo(2);
    }
}
