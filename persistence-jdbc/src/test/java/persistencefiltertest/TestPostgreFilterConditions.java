package persistencefiltertest;

import eu.okaeri.persistence.filter.renderer.FilterRenderer;
import eu.okaeri.persistence.jdbc.filter.PostgresFilterRenderer;
import eu.okaeri.persistence.jdbc.filter.SqlStringRenderer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static eu.okaeri.persistence.filter.condition.Condition.and;
import static eu.okaeri.persistence.filter.condition.Condition.or;
import static eu.okaeri.persistence.filter.predicate.SimplePredicate.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestPostgreFilterConditions {

    private FilterRenderer renderer;

    @BeforeAll
    public void prepare() {
        this.renderer = new PostgresFilterRenderer(new SqlStringRenderer());
    }

    @Test
    public void test_condition_0() {
        String condition = this.renderer.renderCondition(and("age", eq(55))); // age equal to 55
        assertEquals("((value->'age')::numeric = 55)", condition);
    }

    @Test
    public void test_condition_1() {
        String condition = this.renderer.renderCondition(and("distance", ge(100), le(1000))); // distance between 100 and 1000
        assertEquals("(((value->'distance')::numeric >= 100) and ((value->'distance')::numeric <= 1000))", condition);
    }

    @Test
    public void test_condition_2() {
        String condition = this.renderer.renderCondition(or(
            and("distance", ge(100), le(1000)),
            and("age", eq(55))
        ));
        assertEquals("((((value->'distance')::numeric >= 100) and ((value->'distance')::numeric <= 1000)) or ((value->'age')::numeric = 55))", condition);
    }

    @Test
    public void test_condition_3() {
        String condition = this.renderer.renderCondition(and("name", eq("tester")));
        assertEquals("(value->>'name'= 'tester')", condition);
    }
}
