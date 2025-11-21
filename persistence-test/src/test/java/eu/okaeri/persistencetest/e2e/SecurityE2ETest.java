package eu.okaeri.persistencetest.e2e;

import eu.okaeri.persistencetest.fixtures.User;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static eu.okaeri.persistence.filter.condition.Condition.on;
import static eu.okaeri.persistence.filter.predicate.SimplePredicate.*;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E Security Tests - validates SQL injection prevention and proper string escaping.
 * Tests all backends to ensure consistent security behavior.
 */
public class SecurityE2ETest extends E2ETestBase {

    protected static Stream<BackendTestContext> allBackendsWithContext() {
        return allBackends().map(backend -> {
            BackendTestContext btc = BackendTestContext.create(backend);

            // Setup test data - simple dataset
            btc.getUserRepository().save(new User("alice", 100));
            btc.getUserRepository().save(new User("bob", 200));
            btc.getUserRepository().save(new User("charlie", 300));
            btc.getUserRepository().save(new User("diana", 200));
            btc.getUserRepository().save(new User("eve", 150));

            return btc;
        });
    }

    // ===== SQL INJECTION PREVENTION =====

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_sql_injection_single_quote(BackendTestContext btc) {
        // Try to inject SQL with single quote
        String maliciousInput = "alice' OR '1'='1";
        btc.getUserRepository().save(new User(maliciousInput, 999));
        assertThat(btc.getUserRepository().count()).isEqualTo(6);

        // Should match only the exact malicious string, not inject SQL
        long deleted = btc.getUserRepository().delete(q -> q.where(on("name", eq(maliciousInput))));
        assertThat(deleted).isEqualTo(1); // Only the exact match

        var remaining = btc.getUserRepository().streamAll().map(User::getName).toList();
        assertThat(remaining).containsExactlyInAnyOrder("alice", "bob", "charlie", "diana", "eve");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_sql_injection_comment(BackendTestContext btc) {
        // Try to inject SQL with comment
        String maliciousInput = "alice'--";
        btc.getUserRepository().save(new User(maliciousInput, 999));
        assertThat(btc.getUserRepository().count()).isEqualTo(6);

        long deleted = btc.getUserRepository().delete(q -> q.where(on("name", eq(maliciousInput))));
        assertThat(deleted).isEqualTo(1);

        var remaining = btc.getUserRepository().streamAll().map(User::getName).toList();
        assertThat(remaining).containsExactlyInAnyOrder("alice", "bob", "charlie", "diana", "eve");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_sql_injection_union(BackendTestContext btc) {
        // Try to inject UNION-based SQL injection
        String maliciousInput = "alice' UNION SELECT * FROM users--";
        btc.getUserRepository().save(new User(maliciousInput, 999));
        assertThat(btc.getUserRepository().count()).isEqualTo(6);

        long deleted = btc.getUserRepository().delete(q -> q.where(on("name", eq(maliciousInput))));
        assertThat(deleted).isEqualTo(1);

        var remaining = btc.getUserRepository().streamAll().map(User::getName).toList();
        assertThat(remaining).containsExactlyInAnyOrder("alice", "bob", "charlie", "diana", "eve");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_multiple_single_quotes(BackendTestContext btc) {
        // Test string with multiple single quotes
        String stringWithQuotes = "O'Reilly's \"book\"";
        btc.getUserRepository().save(new User(stringWithQuotes, 999));
        assertThat(btc.getUserRepository().count()).isEqualTo(6);

        long deleted = btc.getUserRepository().delete(q -> q.where(on("name", eq(stringWithQuotes))));
        assertThat(deleted).isEqualTo(1);

        var remaining = btc.getUserRepository().streamAll().map(User::getName).toList();
        assertThat(remaining).containsExactlyInAnyOrder("alice", "bob", "charlie", "diana", "eve");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_in_predicate_injection(BackendTestContext btc) {
        // Test IN predicate with injection attempts
        String injection1 = "alice' OR '1'='1";
        String injection2 = "bob'--";

        long deleted = btc.getUserRepository().delete(q -> q.where(on("name", in(injection1, injection2))));
        assertThat(deleted).isEqualTo(0); // Should not match anything (injection strings don't exist)

        var remaining = btc.getUserRepository().streamAll().map(User::getName).toList();
        assertThat(remaining).containsExactlyInAnyOrder("alice", "bob", "charlie", "diana", "eve");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_semicolon_injection(BackendTestContext btc) {
        // Try to inject with semicolon to execute additional SQL
        String maliciousInput = "alice'; DROP TABLE users--";
        btc.getUserRepository().save(new User(maliciousInput, 999));
        assertThat(btc.getUserRepository().count()).isEqualTo(6);

        long deleted = btc.getUserRepository().delete(q -> q.where(on("name", eq(maliciousInput))));
        assertThat(deleted).isEqualTo(1);

        var remaining = btc.getUserRepository().streamAll().map(User::getName).toList();
        assertThat(remaining).containsExactlyInAnyOrder("alice", "bob", "charlie", "diana", "eve");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_escaped_quotes_in_search(BackendTestContext btc) {
        // Test searching for strings that contain quotes
        btc.getUserRepository().save(new User("It's", 999));
        btc.getUserRepository().save(new User("Who's", 888));
        assertThat(btc.getUserRepository().count()).isEqualTo(7);

        long deleted = btc.getUserRepository().delete(q -> q.where(on("name", eq("It's"))));
        assertThat(deleted).isEqualTo(1);

        var remaining = btc.getUserRepository().streamAll().map(User::getName).toList();
        assertThat(remaining).containsExactlyInAnyOrder("alice", "bob", "charlie", "diana", "eve", "Who's");
    }

    // ===== LIKE/REGEX PATTERN INJECTION PREVENTION =====

    protected static Stream<BackendTestContext> patternInjectionContext() {
        return allBackends().map(backend -> {
            BackendTestContext btc = BackendTestContext.create(backend);

            // Test data with special characters that could be exploited
            btc.getUserRepository().save(new User("50% discount", 100));
            btc.getUserRepository().save(new User("50X discount", 200));  // Should NOT match "50% discount"
            btc.getUserRepository().save(new User("test.file", 300));
            btc.getUserRepository().save(new User("testXfile", 400));  // Should NOT match "test.file" in regex
            btc.getUserRepository().save(new User("user_name", 500));
            btc.getUserRepository().save(new User("userXname", 600));  // Should NOT match "user_name"
            btc.getUserRepository().save(new User("data\\backup", 700));
            btc.getUserRepository().save(new User("pipe|separator", 800));

            return btc;
        });
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("patternInjectionContext")
    void test_like_percent_wildcard_escaped(BackendTestContext btc) {
        // Percent sign should be treated as literal, not wildcard
        long deleted = btc.getUserRepository().delete(q -> q.where(on("name", startsWith("50%"))));
        assertThat(deleted).isEqualTo(1); // Only "50% discount", NOT "50X discount"

        var remaining = btc.getUserRepository().streamAll().map(User::getName).toList();
        assertThat(remaining).containsExactlyInAnyOrder("50X discount", "test.file", "testXfile", "user_name", "userXname", "data\\backup", "pipe|separator");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("patternInjectionContext")
    void test_like_underscore_wildcard_escaped(BackendTestContext btc) {
        // Underscore should be treated as literal, not wildcard
        long deleted = btc.getUserRepository().delete(q -> q.where(on("name", contains("_name"))));
        assertThat(deleted).isEqualTo(1); // Only "user_name", NOT "userXname"

        var remaining = btc.getUserRepository().streamAll().map(User::getName).toList();
        assertThat(remaining).containsExactlyInAnyOrder("50% discount", "50X discount", "test.file", "testXfile", "userXname", "data\\backup", "pipe|separator");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("patternInjectionContext")
    void test_regex_dot_wildcard_escaped(BackendTestContext btc) {
        // Dot should be treated as literal in MongoDB regex, not wildcard
        long deleted = btc.getUserRepository().delete(q -> q.where(on("name", contains(".file"))));
        assertThat(deleted).isEqualTo(1); // Only "test.file", NOT "testXfile"

        var remaining = btc.getUserRepository().streamAll().map(User::getName).toList();
        assertThat(remaining).containsExactlyInAnyOrder("50% discount", "50X discount", "testXfile", "user_name", "userXname", "data\\backup", "pipe|separator");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("patternInjectionContext")
    void test_backslash_escape_literal(BackendTestContext btc) {
        // Backslash should be treated as literal character
        long deleted = btc.getUserRepository().delete(q -> q.where(on("name", contains("\\"))));
        assertThat(deleted).isEqualTo(1); // Only "data\\backup"

        var remaining = btc.getUserRepository().streamAll().map(User::getName).toList();
        assertThat(remaining).containsExactlyInAnyOrder("50% discount", "50X discount", "test.file", "testXfile", "user_name", "userXname", "pipe|separator");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("patternInjectionContext")
    void test_pipe_escape_character_literal(BackendTestContext btc) {
        // Pipe character should be treated as literal, not as escape character
        long deleted = btc.getUserRepository().delete(q -> q.where(on("name", contains("|"))));
        assertThat(deleted).isEqualTo(1); // Only "pipe|separator"

        var remaining = btc.getUserRepository().streamAll().map(User::getName).toList();
        assertThat(remaining).containsExactlyInAnyOrder("50% discount", "50X discount", "test.file", "testXfile", "user_name", "userXname", "data\\backup");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("patternInjectionContext")
    void test_combined_special_characters(BackendTestContext btc) {
        // Test multiple special characters in one pattern
        btc.getUserRepository().save(new User("price: 50% off!", 900));
        assertThat(btc.getUserRepository().count()).isEqualTo(9);

        long deleted = btc.getUserRepository().delete(q -> q.where(on("name", contains("50% off"))));
        assertThat(deleted).isEqualTo(1); // Only exact match

        var remaining = btc.getUserRepository().streamAll().map(User::getName).toList();
        assertThat(remaining).hasSize(8);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("patternInjectionContext")
    void test_regex_metacharacters_escaped(BackendTestContext btc) {
        // Test that regex metacharacters are properly escaped in MongoDB
        btc.getUserRepository().save(new User("test[abc]", 900));
        btc.getUserRepository().save(new User("test(xyz)", 1000));
        btc.getUserRepository().save(new User("test{1,3}", 1100));
        assertThat(btc.getUserRepository().count()).isEqualTo(11);

        // Should match literal brackets, not regex patterns
        long deleted = btc.getUserRepository().delete(q -> q.where(on("name", contains("[abc]"))));
        assertThat(deleted).isEqualTo(1); // Only "test[abc]"

        var remaining = btc.getUserRepository().streamAll().map(User::getName).toList();
        assertThat(remaining).contains("test(xyz)", "test{1,3}");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("patternInjectionContext")
    void test_like_multiple_wildcards_escaped(BackendTestContext btc) {
        // Test pattern with multiple SQL wildcards that should be escaped
        btc.getUserRepository().save(new User("100%_guaranteed", 900));
        assertThat(btc.getUserRepository().count()).isEqualTo(9);

        long deleted = btc.getUserRepository().delete(q -> q.where(on("name", startsWith("100%_"))));
        assertThat(deleted).isEqualTo(1); // Should match literally

        var remaining = btc.getUserRepository().streamAll().map(User::getName).toList();
        assertThat(remaining).hasSize(8);
    }
}
