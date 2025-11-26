package eu.okaeri.persistencetestmongo.filter;

import eu.okaeri.persistence.filter.OrderBy;
import eu.okaeri.persistence.filter.renderer.FilterRenderer;
import eu.okaeri.persistence.mongo.filter.MongoFilterRenderer;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Arrays;
import java.util.Collections;

import static eu.okaeri.persistence.filter.condition.Condition.and;
import static eu.okaeri.persistence.filter.condition.Condition.or;
import static eu.okaeri.persistence.filter.predicate.SimplePredicate.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Backend-specific test for MongoDB BSON query rendering.
 * Tests BSON document generation for WHERE and ORDER BY clauses.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class FilterRenderingTest {

    private FilterRenderer renderer;

    @BeforeAll
    public void prepare() {
        this.renderer = new MongoFilterRenderer();
    }

    @Test
    public void test_condition_0() {
        String condition = this.renderer.renderCondition(and("age", eq(55))); // age equal to 55
        assertThatCode(() -> Document.parse(condition)).doesNotThrowAnyException();
        // Single condition doesn't need $and wrapper
        assertThat(condition).isEqualTo("{ \"age\": { \"$eq\": 55 }}");
    }

    @Test
    public void test_condition_1() {
        String condition = this.renderer.renderCondition(and("distance", gte(100), lte(1000))); // distance between 100 and 1000
        assertThatCode(() -> Document.parse(condition)).doesNotThrowAnyException();
        assertThat(condition).isEqualTo("{\"$and\": [{ \"distance\": { \"$gte\": 100 }}, { \"distance\": { \"$lte\": 1000 }}]}");
    }

    @Test
    public void test_condition_2() {
        String condition = this.renderer.renderCondition(or(
            and("distance", gte(100), lte(1000)),
            and("age", eq(55))
        ));
        assertThatCode(() -> Document.parse(condition)).doesNotThrowAnyException();
        // Single condition in second branch doesn't need $and wrapper
        assertThat(condition).isEqualTo("{\"$or\": [{\"$and\": [{ \"distance\": { \"$gte\": 100 }}, { \"distance\": { \"$lte\": 1000 }}]}, { \"age\": { \"$eq\": 55 }}]}");
    }

    @Test
    public void test_condition_3() {
        String condition = this.renderer.renderCondition(and("name", eq("tester")));
        assertThatCode(() -> Document.parse(condition)).doesNotThrowAnyException();
        // Single condition doesn't need $and wrapper
        assertThat(condition).isEqualTo("{ \"name\": { \"$eq\": \"tester\" }}");
    }

    @Test
    public void test_orderBy_single_asc() {
        String orderBy = this.renderer.renderOrderBy(Collections.singletonList(OrderBy.asc("name")));
        assertThatCode(() -> Document.parse(orderBy)).doesNotThrowAnyException();
        assertThat(orderBy).isEqualTo("{\"name\": 1}");
    }

    @Test
    public void test_orderBy_single_desc() {
        String orderBy = this.renderer.renderOrderBy(Collections.singletonList(OrderBy.desc("score")));
        assertThatCode(() -> Document.parse(orderBy)).doesNotThrowAnyException();
        assertThat(orderBy).isEqualTo("{\"score\": -1}");
    }

    @Test
    public void test_orderBy_multiple() {
        String orderBy = this.renderer.renderOrderBy(Arrays.asList(
            OrderBy.desc("score"),
            OrderBy.asc("name"),
            OrderBy.desc("timestamp")
        ));
        assertThatCode(() -> Document.parse(orderBy)).doesNotThrowAnyException();
        assertThat(orderBy).isEqualTo("{\"score\": -1, \"name\": 1, \"timestamp\": -1}");
    }

    @Test
    public void test_orderBy_nested_path() {
        String orderBy = this.renderer.renderOrderBy(Collections.singletonList(OrderBy.asc("user.profile.age")));
        assertThatCode(() -> Document.parse(orderBy)).doesNotThrowAnyException();
        assertThat(orderBy).isEqualTo("{\"user.profile.age\": 1}");
    }

    // ==================== REGEX ESCAPING TESTS ====================

    @Test
    public void test_startsWith_with_dot() {
        String condition = this.renderer.renderCondition(and("domain", startsWith("api.example")));
        assertThatCode(() -> Document.parse(condition)).doesNotThrowAnyException();
        // Dot escaped as \. for regex, then \\ for JSON = \\. in output
        assertThat(condition).isEqualTo("{ \"domain\": { \"$regex\": \"^api\\\\.example\" }}");
    }

    @Test
    public void test_endsWith_with_special_chars() {
        String condition = this.renderer.renderCondition(and("filename", endsWith("file[1].txt")));
        assertThatCode(() -> Document.parse(condition)).doesNotThrowAnyException();
        // Square brackets and dot escaped for regex, then backslashes doubled for JSON
        assertThat(condition).isEqualTo("{ \"filename\": { \"$regex\": \"file\\\\[1\\\\]\\\\.txt$\" }}");
    }

    @Test
    public void test_contains_with_regex_metacharacters() {
        String condition = this.renderer.renderCondition(and("query", contains("a*b+c?")));
        assertThatCode(() -> Document.parse(condition)).doesNotThrowAnyException();
        // *, +, ? escaped for regex, then backslashes doubled for JSON
        assertThat(condition).isEqualTo("{ \"query\": { \"$regex\": \"a\\\\*b\\\\+c\\\\?\" }}");
    }

    @Test
    public void test_eq_ignoreCase_with_special_chars() {
        String condition = this.renderer.renderCondition(and("email", eqi("user@example.com")));
        assertThatCode(() -> Document.parse(condition)).doesNotThrowAnyException();
        // Dot escaped for regex, then backslash doubled for JSON
        assertThat(condition).isEqualTo("{ \"email\": { \"$regex\": \"^user@example\\\\.com$\", \"$options\": \"i\" }}");
    }

    @Test
    public void test_startsWith_ignoreCase_with_parentheses() {
        String condition = this.renderer.renderCondition(and("name", startsWith("test(1)").ignoreCase()));
        assertThatCode(() -> Document.parse(condition)).doesNotThrowAnyException();
        // Parentheses escaped for regex, then backslashes doubled for JSON
        assertThat(condition).isEqualTo("{ \"name\": { \"$regex\": \"^test\\\\(1\\\\)\", \"$options\": \"i\" }}");
    }
}
