package eu.okaeri.persistencetestjdbc.filter;

import eu.okaeri.persistence.filter.OrderBy;
import eu.okaeri.persistence.filter.renderer.FilterRenderer;
import eu.okaeri.persistence.jdbc.filter.PostgresFilterRenderer;
import eu.okaeri.persistence.jdbc.filter.SqlStringRenderer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Arrays;
import java.util.Collections;

import static eu.okaeri.persistence.filter.condition.Condition.and;
import static eu.okaeri.persistence.filter.condition.Condition.or;
import static eu.okaeri.persistence.filter.predicate.SimplePredicate.*;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Backend-specific test for PostgreSQL JSONB query rendering.
 * Tests SQL string generation for WHERE and ORDER BY clauses.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class FilterRenderingTest {

    private FilterRenderer renderer;

    @BeforeAll
    public void prepare() {
        this.renderer = new PostgresFilterRenderer(new SqlStringRenderer());
    }

    @Test
    public void test_condition_0() {
        String condition = this.renderer.renderCondition(and("age", eq(55))); // age equal to 55
        assertThat(condition).isEqualTo("((value->'age')::numeric = 55)");
    }

    @Test
    public void test_condition_1() {
        String condition = this.renderer.renderCondition(and("distance", gte(100), lte(1000))); // distance between 100 and 1000
        assertThat(condition).isEqualTo("(((value->'distance')::numeric >= 100) and ((value->'distance')::numeric <= 1000))");
    }

    @Test
    public void test_condition_2() {
        String condition = this.renderer.renderCondition(or(
            and("distance", gte(100), lte(1000)),
            and("age", eq(55))
        ));
        assertThat(condition).isEqualTo("((((value->'distance')::numeric >= 100) and ((value->'distance')::numeric <= 1000)) or ((value->'age')::numeric = 55))");
    }

    @Test
    public void test_condition_3() {
        String condition = this.renderer.renderCondition(and("name", eq("tester")));
        assertThat(condition).isEqualTo("(value->>'name'= 'tester')");
    }

    @Test
    public void test_orderBy_single_asc() {
        String orderBy = this.renderer.renderOrderBy(Collections.singletonList(OrderBy.asc("name")));
        assertThat(orderBy).isEqualTo("value->>'name' asc");
    }

    @Test
    public void test_orderBy_single_desc() {
        String orderBy = this.renderer.renderOrderBy(Collections.singletonList(OrderBy.desc("score")));
        assertThat(orderBy).isEqualTo("value->>'score' desc");
    }

    @Test
    public void test_orderBy_multiple() {
        String orderBy = this.renderer.renderOrderBy(Arrays.asList(
            OrderBy.desc("score"),
            OrderBy.asc("name"),
            OrderBy.desc("timestamp")
        ));
        assertThat(orderBy).isEqualTo("value->>'score' desc, value->>'name' asc, value->>'timestamp' desc");
    }

    @Test
    public void test_orderBy_nested_path() {
        String orderBy = this.renderer.renderOrderBy(Collections.singletonList(OrderBy.asc("user.profile.age")));
        assertThat(orderBy).isEqualTo("value->'user'->'profile'->>'age' asc");
    }
}
