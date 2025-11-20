package eu.okaeri.persistence.filter;

import eu.okaeri.persistence.filter.renderer.DefaultFilterRenderer;
import eu.okaeri.persistence.filter.renderer.DefaultStringRenderer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static eu.okaeri.persistence.filter.condition.Condition.and;
import static eu.okaeri.persistence.filter.condition.Condition.or;
import static eu.okaeri.persistence.filter.predicate.SimplePredicate.*;
import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class FilterRenderingTest {

    private DefaultFilterRenderer renderer;

    @BeforeAll
    public void prepare() {
        this.renderer = new DefaultFilterRenderer(new DefaultStringRenderer());
    }

    @Test
    public void test_condition_0() {
        String condition = this.renderer.renderCondition(and("age", eq(55))); // age equal to 55
        assertThat(condition).isEqualTo("(age == 55)");
    }

    @Test
    public void test_condition_1() {
        String condition = this.renderer.renderCondition(and("distance", gte(100), lte(1000))); // distance between 100 and 1000
        assertThat(condition).isEqualTo("((distance >= 100) && (distance <= 1000))");
    }

    @Test
    public void test_condition_2() {
        String condition = this.renderer.renderCondition(or(
            and("distance", gte(100), lte(1000)),
            and("age", eq(55))
        ));
        assertThat(condition).isEqualTo("(((distance >= 100) && (distance <= 1000)) || (age == 55))");
    }

    @Test
    public void test_condition_3() {
        String condition = this.renderer.renderCondition(or(
            and("distance", gte(100), lte(1000)),
            and(
                and("age", eq(55)),
                and("thing", gt(123), lt(999))
            )
        ));
        assertThat(condition).isEqualTo("(((distance >= 100) && (distance <= 1000)) || ((age == 55) && ((thing > 123) && (thing < 999))))");
    }

    @Test
    public void test_condition_4() {
        String condition = this.renderer.renderCondition(or(
            and("distance", gte(100), lte(1000)),
            and(
                and("age", eq(55)),
                and("thing", gt(123), and("abc", lt(999)))
            )
        ));
        assertThat(condition).isEqualTo("(((distance >= 100) && (distance <= 1000)) || ((age == 55) && ((thing > 123) && (abc < 999))))");
    }

    @Test
    public void test_condition_5() {
        String condition = this.renderer.renderCondition(or(
            and("distance", gte(100), lte(1000)),
            and(
                and("age", eq(55)),
                or(
                    and("thing", gt(123), and("abc", lt(999))),
                    and("abcd", eq(-1))
                )
            )
        ));
        assertThat(condition).isEqualTo("(((distance >= 100) && (distance <= 1000)) || ((age == 55) && (((thing > 123) && (abc < 999)) || (abcd == -1))))");
    }
}
