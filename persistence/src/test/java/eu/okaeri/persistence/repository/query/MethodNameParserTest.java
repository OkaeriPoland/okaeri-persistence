package eu.okaeri.persistence.repository.query;

import eu.okaeri.configs.OkaeriConfig;
import eu.okaeri.persistence.document.Document;
import eu.okaeri.persistence.filter.condition.LogicalOperator;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class MethodNameParserTest {

    // Mock Document class for testing
    public static class TestDocument extends Document {
        private String name;
        private String email;
        private int level;
        private boolean active;
        private TestMeta meta;
    }

    public static class TestMeta extends OkaeriConfig {
        private String name;
        private int score;
    }

    // Mock repository interface for method extraction
    interface TestRepository {
        // Simple equality
        Optional<TestDocument> findByName(String name);

        Stream<TestDocument> streamByLevel(int level);

        List<TestDocument> findByEmail(String email);

        TestDocument findByActive(boolean active);

        // Multiple AND
        Optional<TestDocument> findByNameAndLevel(String name, int level);

        List<TestDocument> findByEmailAndActive(String email, boolean active);

        // Multiple OR
        Stream<TestDocument> findByNameOrEmail(String name, String email);

        List<TestDocument> findByLevelOrActive(int level, boolean active);

        // Mixed AND/OR (for precedence testing)
        List<TestDocument> findByNameOrLevelAndActive(String name, int level, boolean active);

        List<TestDocument> findByNameAndLevelOrActive(String name, int level, boolean active);

        List<TestDocument> findByNameOrLevelAndActiveOrEmail(String name, int level, boolean active, String email);

        // OrderBy
        List<TestDocument> findByActiveOrderByLevelDesc(boolean active);

        Stream<TestDocument> findAllOrderByLevel();

        List<TestDocument> findAllOrderByLevelDescNameAsc();

        // Limiting
        Optional<TestDocument> findFirstByOrderByLevelDesc();

        List<TestDocument> findTop10ByActive(boolean active);

        List<TestDocument> findTop5ByActiveOrderByLevelDesc(boolean active);

        // Count
        long countByActive(boolean active);

        long countByLevel(int level);

        // Exists
        boolean existsByEmail(String email);

        boolean existsByName(String name);

        // Delete
        long deleteByLevel(int level);

        long deleteByActive(boolean active);

        // Query all
        Stream<TestDocument> findAll();

        List<TestDocument> streamAll();

        // Nested fields with $ separator
        Optional<TestDocument> findByMeta$Name(String metaName);

        List<TestDocument> findByMeta$Score(int score);

        // Underscores for readability
        Optional<TestDocument> findBy_name(String name);

        List<TestDocument> findBy_name_and_level(String name, int level);

        Stream<TestDocument> findBy_name_or_email(String name, String email);

        // Stream prefix enforcement
        Stream<TestDocument> streamByActive(boolean active);

        Stream<TestDocument> streamAllOrderByLevelDesc();

        // Alternative prefixes
        Optional<TestDocument> readByName(String name);

        Optional<TestDocument> getByEmail(String email);

        List<TestDocument> queryByLevel(int level);
    }

    @Test
    public void test_parse_simple_equality() throws Exception {
        Method method = TestRepository.class.getMethod("findByName", String.class);
        ParsedMethod parsed = MethodNameParser.parse(method, TestDocument.class);

        assertThat(parsed.getOperation()).isEqualTo(MethodOperation.FIND);
        assertThat(parsed.getQueryParts()).hasSize(1);
        assertThat(parsed.getQueryParts().get(0).getField()).isEqualTo("name");
        assertThat(parsed.getQueryParts().get(0).getParameterIndex()).isEqualTo(0);
        assertThat(parsed.getQueryParts().get(0).getLogicalOperator()).isNull();
        assertThat(parsed.isQueryAll()).isFalse();
        assertThat(parsed.getOrderParts()).isEmpty();
        assertThat(parsed.getResultLimit()).isNull();
    }

    @Test
    public void test_parse_stream_return_type() throws Exception {
        Method method = TestRepository.class.getMethod("streamByLevel", int.class);
        ParsedMethod parsed = MethodNameParser.parse(method, TestDocument.class);

        assertThat(parsed.getOperation()).isEqualTo(MethodOperation.FIND);
        assertThat(parsed.isRequiresStreamReturn()).isTrue();
        assertThat(parsed.getQueryParts()).hasSize(1);
        assertThat(parsed.getQueryParts().get(0).getField()).isEqualTo("level");
    }

    @Test
    public void test_parse_multiple_and_conditions() throws Exception {
        Method method = TestRepository.class.getMethod("findByNameAndLevel", String.class, int.class);
        ParsedMethod parsed = MethodNameParser.parse(method, TestDocument.class);

        assertThat(parsed.getOperation()).isEqualTo(MethodOperation.FIND);
        assertThat(parsed.getQueryParts()).hasSize(2);

        QueryPart part0 = parsed.getQueryParts().get(0);
        assertThat(part0.getField()).isEqualTo("name");
        assertThat(part0.getParameterIndex()).isEqualTo(0);
        assertThat(part0.getLogicalOperator()).isNull();

        QueryPart part1 = parsed.getQueryParts().get(1);
        assertThat(part1.getField()).isEqualTo("level");
        assertThat(part1.getParameterIndex()).isEqualTo(1);
        assertThat(part1.getLogicalOperator()).isEqualTo(LogicalOperator.AND);
    }

    @Test
    public void test_parse_multiple_or_conditions() throws Exception {
        Method method = TestRepository.class.getMethod("findByNameOrEmail", String.class, String.class);
        ParsedMethod parsed = MethodNameParser.parse(method, TestDocument.class);

        assertThat(parsed.getOperation()).isEqualTo(MethodOperation.FIND);
        assertThat(parsed.getQueryParts()).hasSize(2);

        QueryPart part0 = parsed.getQueryParts().get(0);
        assertThat(part0.getField()).isEqualTo("name");
        assertThat(part0.getLogicalOperator()).isNull();

        QueryPart part1 = parsed.getQueryParts().get(1);
        assertThat(part1.getField()).isEqualTo("email");
        assertThat(part1.getLogicalOperator()).isEqualTo(LogicalOperator.OR);
    }

    @Test
    public void test_parse_mixed_or_and_conditions() throws Exception {
        // A OR B AND C → should parse as: name(null), level(OR), active(AND)
        Method method = TestRepository.class.getMethod("findByNameOrLevelAndActive", String.class, int.class, boolean.class);
        ParsedMethod parsed = MethodNameParser.parse(method, TestDocument.class);

        assertThat(parsed.getOperation()).isEqualTo(MethodOperation.FIND);
        assertThat(parsed.getQueryParts()).hasSize(3);

        QueryPart part0 = parsed.getQueryParts().get(0);
        assertThat(part0.getField()).isEqualTo("name");
        assertThat(part0.getLogicalOperator()).isNull();

        QueryPart part1 = parsed.getQueryParts().get(1);
        assertThat(part1.getField()).isEqualTo("level");
        assertThat(part1.getLogicalOperator()).isEqualTo(LogicalOperator.OR);

        QueryPart part2 = parsed.getQueryParts().get(2);
        assertThat(part2.getField()).isEqualTo("active");
        assertThat(part2.getLogicalOperator()).isEqualTo(LogicalOperator.AND);
    }

    @Test
    public void test_parse_mixed_and_or_conditions() throws Exception {
        // A AND B OR C → should parse as: name(null), level(AND), active(OR)
        Method method = TestRepository.class.getMethod("findByNameAndLevelOrActive", String.class, int.class, boolean.class);
        ParsedMethod parsed = MethodNameParser.parse(method, TestDocument.class);

        assertThat(parsed.getOperation()).isEqualTo(MethodOperation.FIND);
        assertThat(parsed.getQueryParts()).hasSize(3);

        QueryPart part0 = parsed.getQueryParts().get(0);
        assertThat(part0.getField()).isEqualTo("name");
        assertThat(part0.getLogicalOperator()).isNull();

        QueryPart part1 = parsed.getQueryParts().get(1);
        assertThat(part1.getField()).isEqualTo("level");
        assertThat(part1.getLogicalOperator()).isEqualTo(LogicalOperator.AND);

        QueryPart part2 = parsed.getQueryParts().get(2);
        assertThat(part2.getField()).isEqualTo("active");
        assertThat(part2.getLogicalOperator()).isEqualTo(LogicalOperator.OR);
    }

    @Test
    public void test_parse_complex_mixed_operators() throws Exception {
        // A OR B AND C OR D → should parse as: name(null), level(OR), active(AND), email(OR)
        Method method = TestRepository.class.getMethod("findByNameOrLevelAndActiveOrEmail", String.class, int.class, boolean.class, String.class);
        ParsedMethod parsed = MethodNameParser.parse(method, TestDocument.class);

        assertThat(parsed.getOperation()).isEqualTo(MethodOperation.FIND);
        assertThat(parsed.getQueryParts()).hasSize(4);

        assertThat(parsed.getQueryParts().get(0).getField()).isEqualTo("name");
        assertThat(parsed.getQueryParts().get(0).getLogicalOperator()).isNull();

        assertThat(parsed.getQueryParts().get(1).getField()).isEqualTo("level");
        assertThat(parsed.getQueryParts().get(1).getLogicalOperator()).isEqualTo(LogicalOperator.OR);

        assertThat(parsed.getQueryParts().get(2).getField()).isEqualTo("active");
        assertThat(parsed.getQueryParts().get(2).getLogicalOperator()).isEqualTo(LogicalOperator.AND);

        assertThat(parsed.getQueryParts().get(3).getField()).isEqualTo("email");
        assertThat(parsed.getQueryParts().get(3).getLogicalOperator()).isEqualTo(LogicalOperator.OR);
    }

    @Test
    public void test_parse_order_by_single() throws Exception {
        Method method = TestRepository.class.getMethod("findByActiveOrderByLevelDesc", boolean.class);
        ParsedMethod parsed = MethodNameParser.parse(method, TestDocument.class);

        assertThat(parsed.getOperation()).isEqualTo(MethodOperation.FIND);
        assertThat(parsed.getQueryParts()).hasSize(1);
        assertThat(parsed.getOrderParts()).hasSize(1);

        OrderPart orderPart = parsed.getOrderParts().get(0);
        assertThat(orderPart.getField()).isEqualTo("level");
        assertThat(orderPart.isAscending()).isFalse();
    }

    @Test
    public void test_parse_order_by_multiple() throws Exception {
        Method method = TestRepository.class.getMethod("findAllOrderByLevelDescNameAsc");
        ParsedMethod parsed = MethodNameParser.parse(method, TestDocument.class);

        assertThat(parsed.getOperation()).isEqualTo(MethodOperation.FIND);
        assertThat(parsed.isQueryAll()).isTrue();
        assertThat(parsed.getQueryParts()).isEmpty();
        assertThat(parsed.getOrderParts()).hasSize(2);

        OrderPart order0 = parsed.getOrderParts().get(0);
        assertThat(order0.getField()).isEqualTo("level");
        assertThat(order0.isAscending()).isFalse();

        OrderPart order1 = parsed.getOrderParts().get(1);
        assertThat(order1.getField()).isEqualTo("name");
        assertThat(order1.isAscending()).isTrue();
    }

    @Test
    public void test_parse_order_by_default_ascending() throws Exception {
        Method method = TestRepository.class.getMethod("findAllOrderByLevel");
        ParsedMethod parsed = MethodNameParser.parse(method, TestDocument.class);

        assertThat(parsed.getOrderParts()).hasSize(1);
        OrderPart orderPart = parsed.getOrderParts().get(0);
        assertThat(orderPart.getField()).isEqualTo("level");
        assertThat(orderPart.isAscending()).isTrue(); // Default to ascending
    }

    @Test
    public void test_parse_limit_first() throws Exception {
        Method method = TestRepository.class.getMethod("findFirstByOrderByLevelDesc");
        ParsedMethod parsed = MethodNameParser.parse(method, TestDocument.class);

        assertThat(parsed.getOperation()).isEqualTo(MethodOperation.FIND);
        assertThat(parsed.getResultLimit()).isEqualTo(1);
        assertThat(parsed.isQueryAll()).isFalse();
        assertThat(parsed.getQueryParts()).isEmpty();
        assertThat(parsed.getOrderParts()).hasSize(1);
    }

    @Test
    public void test_parse_limit_top_n() throws Exception {
        Method method = TestRepository.class.getMethod("findTop10ByActive", boolean.class);
        ParsedMethod parsed = MethodNameParser.parse(method, TestDocument.class);

        assertThat(parsed.getOperation()).isEqualTo(MethodOperation.FIND);
        assertThat(parsed.getResultLimit()).isEqualTo(10);
        assertThat(parsed.getQueryParts()).hasSize(1);
    }

    @Test
    public void test_parse_count_operation() throws Exception {
        Method method = TestRepository.class.getMethod("countByActive", boolean.class);
        ParsedMethod parsed = MethodNameParser.parse(method, TestDocument.class);

        assertThat(parsed.getOperation()).isEqualTo(MethodOperation.COUNT);
        assertThat(parsed.getQueryParts()).hasSize(1);
        assertThat(parsed.getQueryParts().get(0).getField()).isEqualTo("active");
    }

    @Test
    public void test_parse_exists_operation() throws Exception {
        Method method = TestRepository.class.getMethod("existsByEmail", String.class);
        ParsedMethod parsed = MethodNameParser.parse(method, TestDocument.class);

        assertThat(parsed.getOperation()).isEqualTo(MethodOperation.EXISTS);
        assertThat(parsed.getQueryParts()).hasSize(1);
        assertThat(parsed.getQueryParts().get(0).getField()).isEqualTo("email");
    }

    @Test
    public void test_parse_delete_operation() throws Exception {
        Method method = TestRepository.class.getMethod("deleteByLevel", int.class);
        ParsedMethod parsed = MethodNameParser.parse(method, TestDocument.class);

        assertThat(parsed.getOperation()).isEqualTo(MethodOperation.DELETE);
        assertThat(parsed.getQueryParts()).hasSize(1);
        assertThat(parsed.getQueryParts().get(0).getField()).isEqualTo("level");
    }

    @Test
    public void test_parse_query_all() throws Exception {
        Method method = TestRepository.class.getMethod("findAll");
        ParsedMethod parsed = MethodNameParser.parse(method, TestDocument.class);

        assertThat(parsed.getOperation()).isEqualTo(MethodOperation.FIND);
        assertThat(parsed.isQueryAll()).isTrue();
        assertThat(parsed.getQueryParts()).isEmpty();
        assertThat(parsed.getOrderParts()).isEmpty();
    }

    @Test
    public void test_parse_nested_field_with_dollar() throws Exception {
        Method method = TestRepository.class.getMethod("findByMeta$Name", String.class);
        ParsedMethod parsed = MethodNameParser.parse(method, TestDocument.class);

        assertThat(parsed.getQueryParts()).hasSize(1);
        assertThat(parsed.getQueryParts().get(0).getField()).isEqualTo("meta.name");
    }

    @Test
    public void test_parse_underscore_ignored() throws Exception {
        Method method = TestRepository.class.getMethod("findBy_name", String.class);
        ParsedMethod parsed = MethodNameParser.parse(method, TestDocument.class);

        assertThat(parsed.getQueryParts()).hasSize(1);
        assertThat(parsed.getQueryParts().get(0).getField()).isEqualTo("name");
    }

    @Test
    public void test_parse_underscore_and_operator() throws Exception {
        Method method = TestRepository.class.getMethod("findBy_name_and_level", String.class, int.class);
        ParsedMethod parsed = MethodNameParser.parse(method, TestDocument.class);

        assertThat(parsed.getQueryParts()).hasSize(2);
        assertThat(parsed.getQueryParts().get(0).getField()).isEqualTo("name");
        assertThat(parsed.getQueryParts().get(1).getField()).isEqualTo("level");
        assertThat(parsed.getQueryParts().get(1).getLogicalOperator()).isEqualTo(LogicalOperator.AND);
    }

    @Test
    public void test_parse_alternative_prefixes() throws Exception {
        Method readMethod = TestRepository.class.getMethod("readByName", String.class);
        ParsedMethod readParsed = MethodNameParser.parse(readMethod, TestDocument.class);
        assertThat(readParsed.getOperation()).isEqualTo(MethodOperation.FIND);

        Method getMethod = TestRepository.class.getMethod("getByEmail", String.class);
        ParsedMethod getParsed = MethodNameParser.parse(getMethod, TestDocument.class);
        assertThat(getParsed.getOperation()).isEqualTo(MethodOperation.FIND);

        Method queryMethod = TestRepository.class.getMethod("queryByLevel", int.class);
        ParsedMethod queryParsed = MethodNameParser.parse(queryMethod, TestDocument.class);
        assertThat(queryParsed.getOperation()).isEqualTo(MethodOperation.FIND);
    }

    @Test
    public void test_validate_stream_prefix_requires_stream_return() throws Exception {
        Method method = TestRepository.class.getMethod("streamByActive", boolean.class);
        ParsedMethod parsed = MethodNameParser.parse(method, TestDocument.class);
        assertThat(parsed.isRequiresStreamReturn()).isTrue();
        // No exception thrown - valid
    }

    @Test
    public void test_validate_parameter_count_mismatch() throws Exception {
        interface BadRepository {
            Optional<TestDocument> findByNameAndLevel(String name); // Missing level parameter
        }

        Method method = BadRepository.class.getMethod("findByNameAndLevel", String.class);
        assertThatThrownBy(() -> MethodNameParser.parse(method, TestDocument.class))
            .isInstanceOf(MethodParseException.class)
            .hasMessageContaining("Parameter count mismatch");
    }

    @Test
    public void test_validate_count_return_type() throws Exception {
        Method method = TestRepository.class.getMethod("countByActive", boolean.class);
        ParsedMethod parsed = MethodNameParser.parse(method, TestDocument.class);
        assertThat(parsed.getReturnType()).isEqualTo(long.class);
        // No exception thrown - valid
    }

    @Test
    public void test_validate_exists_return_type() throws Exception {
        Method method = TestRepository.class.getMethod("existsByEmail", String.class);
        ParsedMethod parsed = MethodNameParser.parse(method, TestDocument.class);
        assertThat(parsed.getReturnType()).isEqualTo(boolean.class);
        // No exception thrown - valid
    }

    @Test
    public void test_error_invalid_prefix() {
        interface BadRepository {
            Optional<TestDocument> invalidByName(String name);
        }

        assertThatThrownBy(() -> {
            Method method = BadRepository.class.getMethod("invalidByName", String.class);
            MethodNameParser.parse(method, TestDocument.class);
        })
            .isInstanceOf(MethodParseException.class)
            .hasMessageContaining("must start with a valid operation prefix");
    }

    @Test
    public void test_error_missing_by_keyword() {
        interface BadRepository {
            Optional<TestDocument> find(String name);
        }

        assertThatThrownBy(() -> {
            Method method = BadRepository.class.getMethod("find", String.class);
            MethodNameParser.parse(method, TestDocument.class);
        })
            .isInstanceOf(MethodParseException.class)
            .hasMessageContaining("must start with operation prefix followed by 'By', 'All', or 'OrderBy'");
    }

    @Test
    public void test_error_empty_field_name() {
        interface BadRepository {
            Optional<TestDocument> findByAnd(String name);
        }

        assertThatThrownBy(() -> {
            Method method = BadRepository.class.getMethod("findByAnd", String.class);
            MethodNameParser.parse(method, TestDocument.class);
        })
            .isInstanceOf(MethodParseException.class)
            .hasMessageContaining("Empty field name");
    }

    @Test
    public void test_error_count_wrong_return_type() {
        interface BadRepository {
            String countByActive(boolean active); // Should return long
        }

        assertThatThrownBy(() -> {
            Method method = BadRepository.class.getMethod("countByActive", boolean.class);
            MethodNameParser.parse(method, TestDocument.class);
        })
            .isInstanceOf(MethodParseException.class)
            .hasMessageContaining("Count methods must return long");
    }

    @Test
    public void test_error_exists_wrong_return_type() {
        interface BadRepository {
            String existsByEmail(String email); // Should return boolean
        }

        assertThatThrownBy(() -> {
            Method method = BadRepository.class.getMethod("existsByEmail", String.class);
            MethodNameParser.parse(method, TestDocument.class);
        })
            .isInstanceOf(MethodParseException.class)
            .hasMessageContaining("Exists methods must return boolean");
    }

    @Test
    public void test_error_stream_prefix_wrong_return_type() {
        interface BadRepository {
            List<TestDocument> streamByActive(boolean active); // stream prefix requires Stream<T>
        }

        assertThatThrownBy(() -> {
            Method method = BadRepository.class.getMethod("streamByActive", boolean.class);
            MethodNameParser.parse(method, TestDocument.class);
        })
            .isInstanceOf(MethodParseException.class)
            .hasMessageContaining("Methods with 'stream' prefix must return Stream<T>");
    }
}
