package eu.okaeri.persistencefiltertest;

import eu.okaeri.persistence.filter.condition.Condition;
import eu.okaeri.persistence.filter.condition.renderer.DefaultConditionRenderer;
import eu.okaeri.persistence.filter.predicate.renderer.DefaultPredicateRenderer;
import eu.okaeri.persistence.filter.renderer.DefaultVariableRenderer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static eu.okaeri.persistence.filter.condition.Condition.*;
import static eu.okaeri.persistence.filter.predicate.Predicate.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestFilterConditions {

    private DefaultConditionRenderer renderer;

    @BeforeAll
    public void prepare() {
        this.renderer = new DefaultConditionRenderer(new DefaultVariableRenderer(), new DefaultPredicateRenderer());
    }

    @Test
    public void test_condition_simple() {
        Condition condition = on("age", eq(12)); // age equal to 12
        System.out.println(this.renderer.render(condition));
    }

    @Test
    public void test_condition_multi() {
        Condition condition = on("distance", ge(100), le(1000)); // distance between 100 and 1000
        System.out.println(this.renderer.render(condition));
    }
}
