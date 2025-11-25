package eu.okaeri.persistencetest.e2e;

import eu.okaeri.persistencetest.fixtures.User;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static eu.okaeri.persistence.filter.condition.Condition.on;
import static eu.okaeri.persistence.filter.predicate.SimplePredicate.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

    // ===== NOSQL/MONGODB OPERATOR INJECTION PREVENTION =====

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_nosql_operator_in_string_value_eq(BackendTestContext btc) {
        // MongoDB operator syntax in string value should be treated as literal
        String mongoOperator = "{$gt: 0}";
        btc.getUserRepository().save(new User(mongoOperator, 999));
        assertThat(btc.getUserRepository().count()).isEqualTo(6);

        // Should match only the exact string, not interpret as operator
        var found = btc.getUserRepository().find(q -> q.where(on("name", eq(mongoOperator)))).toList();
        assertThat(found).hasSize(1);
        assertThat(found.get(0).getName()).isEqualTo(mongoOperator);

        // Other users should NOT be returned (operator not executed)
        assertThat(found.get(0).getExp()).isEqualTo(999);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_nosql_ne_operator_injection(BackendTestContext btc) {
        // $ne operator that would match all if interpreted
        String mongoNe = "{$ne: null}";
        btc.getUserRepository().save(new User(mongoNe, 999));
        assertThat(btc.getUserRepository().count()).isEqualTo(6);

        var found = btc.getUserRepository().find(q -> q.where(on("name", eq(mongoNe)))).toList();
        assertThat(found).hasSize(1); // Only exact match, not all documents
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_nosql_regex_operator_injection(BackendTestContext btc) {
        // $regex operator that would match all if interpreted
        String mongoRegex = "{$regex: \".*\"}";
        btc.getUserRepository().save(new User(mongoRegex, 999));
        assertThat(btc.getUserRepository().count()).isEqualTo(6);

        var found = btc.getUserRepository().find(q -> q.where(on("name", eq(mongoRegex)))).toList();
        assertThat(found).hasSize(1); // Only exact match
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_nosql_json_structure_injection(BackendTestContext btc) {
        // Attempt to inject JSON structure to add operators
        String jsonInjection = "test\", \"$gt\": 0, \"x\": \"";
        btc.getUserRepository().save(new User(jsonInjection, 999));
        assertThat(btc.getUserRepository().count()).isEqualTo(6);

        var found = btc.getUserRepository().find(q -> q.where(on("name", eq(jsonInjection)))).toList();
        assertThat(found).hasSize(1);
        assertThat(found.get(0).getName()).isEqualTo(jsonInjection);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_nosql_operator_in_collection(BackendTestContext btc) {
        // MongoDB operator in IN collection should be treated as literal
        String operator1 = "{$ne: null}";
        String operator2 = "{$gt: 0}";

        var found = btc.getUserRepository().find(q -> q.where(on("name", in(operator1, operator2)))).toList();
        assertThat(found).isEmpty(); // None of these strings exist as names
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_nosql_where_operator_injection(BackendTestContext btc) {
        // $where operator (JavaScript execution) should be treated as literal
        String whereInjection = "{$where: \"this.exp > 0\"}";
        btc.getUserRepository().save(new User(whereInjection, 999));
        assertThat(btc.getUserRepository().count()).isEqualTo(6);

        var found = btc.getUserRepository().find(q -> q.where(on("name", eq(whereInjection)))).toList();
        assertThat(found).hasSize(1); // Only exact match, no JS execution
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_regex_wildcard_in_contains_escaped(BackendTestContext btc) {
        // Regex .* should be escaped, not match everything
        btc.getUserRepository().save(new User("test.*pattern", 999));
        assertThat(btc.getUserRepository().count()).isEqualTo(6);

        // Should match literal ".*", not use as regex wildcard
        var found = btc.getUserRepository().find(q -> q.where(on("name", contains(".*")))).toList();
        assertThat(found).hasSize(1);
        assertThat(found.get(0).getName()).isEqualTo("test.*pattern");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_regex_metacharacters_in_contains(BackendTestContext btc) {
        // Various regex metacharacters should be escaped
        btc.getUserRepository().save(new User("test^$pattern", 999));
        btc.getUserRepository().save(new User("test+pattern", 1000));
        btc.getUserRepository().save(new User("test?pattern", 1001));
        assertThat(btc.getUserRepository().count()).isEqualTo(8);

        // Each should match only its literal pattern
        assertThat(btc.getUserRepository().find(q -> q.where(on("name", contains("^$")))).toList()).hasSize(1);
        assertThat(btc.getUserRepository().find(q -> q.where(on("name", contains("+p")))).toList()).hasSize(1);
        assertThat(btc.getUserRepository().find(q -> q.where(on("name", contains("?p")))).toList()).hasSize(1);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_backslash_quote_injection(BackendTestContext btc) {
        // Backslash before quote - attempt to escape the escape
        String backslashQuote = "test\\\"injection";
        btc.getUserRepository().save(new User(backslashQuote, 999));
        assertThat(btc.getUserRepository().count()).isEqualTo(6);

        var found = btc.getUserRepository().find(q -> q.where(on("name", eq(backslashQuote)))).toList();
        assertThat(found).hasSize(1);
        assertThat(found.get(0).getName()).isEqualTo(backslashQuote);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_double_quote_in_string(BackendTestContext btc) {
        // Double quotes in string values should be handled correctly
        String withQuote = "test\"injection";
        btc.getUserRepository().save(new User(withQuote, 999));
        assertThat(btc.getUserRepository().count()).isEqualTo(6);

        var found = btc.getUserRepository().find(q -> q.where(on("name", eq(withQuote)))).toList();
        assertThat(found).hasSize(1);
        assertThat(found.get(0).getName()).isEqualTo(withQuote);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_null_byte_in_string_rejected(BackendTestContext btc) {
        // Null bytes are rejected for consistent behavior across all backends
        String nullByte = "test\u0000admin";
        assertThatThrownBy(() -> btc.getUserRepository().find(q -> q.where(on("name", eq(nullByte)))).toList())
            .hasMessageContaining("Null bytes are not supported");
    }

    // ===== SQL QUOTE ESCAPE BYPASS TESTS =====
    // These tests verify that pre-escaped quotes don't break out of string literals

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_pre_escaped_quote_bypass_attempt(BackendTestContext btc) {
        // User provides pre-escaped quotes hoping to break out
        // test'' OR 1=1-- -> after escaping: test'''' OR 1=1--
        // In SQL: 'test'''' OR 1=1--' should be a single string literal
        String preEscaped = "test'' OR 1=1--";
        btc.getUserRepository().save(new User(preEscaped, 999));
        assertThat(btc.getUserRepository().count()).isEqualTo(6);

        // Should match ONLY the exact string, not cause SQL injection
        var found = btc.getUserRepository().find(q -> q.where(on("name", eq(preEscaped)))).toList();
        assertThat(found).hasSize(1);
        assertThat(found.get(0).getName()).isEqualTo(preEscaped);
        assertThat(found.get(0).getExp()).isEqualTo(999);

        // Verify no other users were affected (would happen if OR 1=1 executed)
        var allUsers = btc.getUserRepository().streamAll().toList();
        assertThat(allUsers).hasSize(6);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_multiple_pre_escaped_quotes(BackendTestContext btc) {
        // Multiple pre-escaped quotes
        String multiQuote = "a''''b''''c";
        btc.getUserRepository().save(new User(multiQuote, 999));
        assertThat(btc.getUserRepository().count()).isEqualTo(6);

        var found = btc.getUserRepository().find(q -> q.where(on("name", eq(multiQuote)))).toList();
        assertThat(found).hasSize(1);
        assertThat(found.get(0).getName()).isEqualTo(multiQuote);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_odd_number_of_quotes(BackendTestContext btc) {
        // Odd number of quotes - attempts to leave unbalanced quotes
        String oddQuotes = "test''' OR 1=1--";
        btc.getUserRepository().save(new User(oddQuotes, 999));
        assertThat(btc.getUserRepository().count()).isEqualTo(6);

        var found = btc.getUserRepository().find(q -> q.where(on("name", eq(oddQuotes)))).toList();
        assertThat(found).hasSize(1);
        assertThat(found.get(0).getName()).isEqualTo(oddQuotes);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_backslash_before_quote(BackendTestContext btc) {
        // Backslash before quote - in SQL standard, backslash is literal
        String backslashQuote = "test\\' OR 1=1--";
        btc.getUserRepository().save(new User(backslashQuote, 999));
        assertThat(btc.getUserRepository().count()).isEqualTo(6);

        var found = btc.getUserRepository().find(q -> q.where(on("name", eq(backslashQuote)))).toList();
        assertThat(found).hasSize(1);
        assertThat(found.get(0).getName()).isEqualTo(backslashQuote);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_double_backslash_quote(BackendTestContext btc) {
        // Double backslash then quote
        String doubleBackslash = "test\\\\' OR 1=1--";
        btc.getUserRepository().save(new User(doubleBackslash, 999));
        assertThat(btc.getUserRepository().count()).isEqualTo(6);

        var found = btc.getUserRepository().find(q -> q.where(on("name", eq(doubleBackslash)))).toList();
        assertThat(found).hasSize(1);
        assertThat(found.get(0).getName()).isEqualTo(doubleBackslash);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_quote_at_end_of_string(BackendTestContext btc) {
        // Quote at the end - ensures string termination is handled
        String endQuote = "test'";
        btc.getUserRepository().save(new User(endQuote, 999));
        assertThat(btc.getUserRepository().count()).isEqualTo(6);

        var found = btc.getUserRepository().find(q -> q.where(on("name", eq(endQuote)))).toList();
        assertThat(found).hasSize(1);
        assertThat(found.get(0).getName()).isEqualTo(endQuote);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_quote_at_start_of_string(BackendTestContext btc) {
        // Quote at the start
        String startQuote = "'test";
        btc.getUserRepository().save(new User(startQuote, 999));
        assertThat(btc.getUserRepository().count()).isEqualTo(6);

        var found = btc.getUserRepository().find(q -> q.where(on("name", eq(startQuote)))).toList();
        assertThat(found).hasSize(1);
        assertThat(found.get(0).getName()).isEqualTo(startQuote);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_only_quotes_string(BackendTestContext btc) {
        // String of only quotes
        String onlyQuotes = "''''";
        btc.getUserRepository().save(new User(onlyQuotes, 999));
        assertThat(btc.getUserRepository().count()).isEqualTo(6);

        var found = btc.getUserRepository().find(q -> q.where(on("name", eq(onlyQuotes)))).toList();
        assertThat(found).hasSize(1);
        assertThat(found.get(0).getName()).isEqualTo(onlyQuotes);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_complex_injection_string(BackendTestContext btc) {
        // Complex injection attempt combining multiple techniques
        String complex = "admin'--; DROP TABLE users; SELECT * FROM secrets WHERE '1'='1";
        btc.getUserRepository().save(new User(complex, 999));
        assertThat(btc.getUserRepository().count()).isEqualTo(6);

        var found = btc.getUserRepository().find(q -> q.where(on("name", eq(complex)))).toList();
        assertThat(found).hasSize(1);
        assertThat(found.get(0).getName()).isEqualTo(complex);

        // Table should still exist and have correct count
        assertThat(btc.getUserRepository().count()).isEqualTo(6);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_injection_in_delete_operation(BackendTestContext btc) {
        // Ensure delete with injection doesn't delete wrong records
        String injection = "' OR '1'='1";

        // This should delete 0 records (no user has this exact name)
        long deleted = btc.getUserRepository().delete(q -> q.where(on("name", eq(injection))));
        assertThat(deleted).isEqualTo(0);

        // All original users should still exist
        assertThat(btc.getUserRepository().count()).isEqualTo(5);
        var names = btc.getUserRepository().streamAll().map(User::getName).toList();
        assertThat(names).containsExactlyInAnyOrder("alice", "bob", "charlie", "diana", "eve");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allBackendsWithContext")
    void test_always_true_condition_blocked(BackendTestContext btc) {
        // Attempt to create always-true condition via injection
        String alwaysTrue = "x' OR 'a'='a";

        var found = btc.getUserRepository().find(q -> q.where(on("name", eq(alwaysTrue)))).toList();
        // Should NOT return all users - injection should be blocked
        assertThat(found).isEmpty(); // No user has this name

        // Verify all users still exist (weren't affected)
        assertThat(btc.getUserRepository().count()).isEqualTo(5);
    }
}
