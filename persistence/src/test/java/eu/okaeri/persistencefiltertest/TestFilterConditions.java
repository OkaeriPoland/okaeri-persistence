package eu.okaeri.persistencefiltertest;

import eu.okaeri.persistence.filter.renderer.DefaultFilterRenderer;
import eu.okaeri.persistence.filter.renderer.DefaultStringRenderer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static eu.okaeri.persistence.filter.condition.Condition.and;
import static eu.okaeri.persistence.filter.condition.Condition.or;
import static eu.okaeri.persistence.filter.predicate.SimplePredicate.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestFilterConditions {

    private DefaultFilterRenderer renderer;

    @BeforeAll
    public void prepare() {
        this.renderer = new DefaultFilterRenderer(new DefaultStringRenderer());
    }

    @Test
    public void test_condition_0() {
        String condition = this.renderer.renderCondition(and("age", eq(55))); // age equal to 55
        assertEquals("(age == 55)", condition);
    }

    @Test
    public void test_condition_1() {
        String condition = this.renderer.renderCondition(and("distance", ge(100), le(1000))); // distance between 100 and 1000
        assertEquals("((distance >= 100) && (distance <= 1000))", condition);
    }

    @Test
    public void test_condition_2() {
        String condition = this.renderer.renderCondition(or(
            and("distance", ge(100), le(1000)),
            and("age", eq(55))
        ));
        assertEquals("(((distance >= 100) && (distance <= 1000)) || (age == 55))", condition);
    }

    @Test
    public void test_condition_3() {
        String condition = this.renderer.renderCondition(or(
            and("distance", ge(100), le(1000)),
            and(
                and("age", eq(55)),
                and("thing", gt(123), lt(999))
            )
        ));
        assertEquals("(((distance >= 100) && (distance <= 1000)) || ((age == 55) && ((thing > 123) && (thing < 999))))", condition);
    }

    @Test
    public void test_condition_4() {
        String condition = this.renderer.renderCondition(or(
            and("distance", ge(100), le(1000)),
            and(
                and("age", eq(55)),
                and("thing", gt(123), and("abc", lt(999)))
            )
        ));
        assertEquals("(((distance >= 100) && (distance <= 1000)) || ((age == 55) && ((thing > 123) && (abc < 999))))", condition);
    }

    @Test
    public void test_condition_5() {
        String condition = this.renderer.renderCondition(or(
            and("distance", ge(100), le(1000)),
            and(
                and("age", eq(55)),
                or(
                    and("thing", gt(123), and("abc", lt(999))),
                    and("abcd", eq(-1))
                )
            )
        ));
        assertEquals("(((distance >= 100) && (distance <= 1000)) || ((age == 55) && (((thing > 123) && (abc < 999)) || (abcd == -1))))", condition);
    }
}
