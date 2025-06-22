package persistencefiltertest;

import eu.okaeri.persistence.filter.renderer.FilterRenderer;
import eu.okaeri.persistence.mongo.filter.MongoFilterRenderer;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static eu.okaeri.persistence.filter.condition.Condition.and;
import static eu.okaeri.persistence.filter.condition.Condition.or;
import static eu.okaeri.persistence.filter.predicate.SimplePredicate.*;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestFilterConditions {

    private FilterRenderer renderer;

    @BeforeAll
    public void prepare() {
        this.renderer = new MongoFilterRenderer();
    }

    @Test
    public void test_condition_0() {
        String condition = this.renderer.renderCondition(and("age", eq(55))); // age equal to 55
        assertDoesNotThrow(() -> Document.parse(condition), condition);
        assertEquals("{\"$and\": [{ \"age\": { \"$eq\": 55 }}]}", condition);
    }

    @Test
    public void test_condition_1() {
        String condition = this.renderer.renderCondition(and("distance", gte(100), lte(1000))); // distance between 100 and 1000
        assertDoesNotThrow(() -> Document.parse(condition), condition);
        assertEquals("{\"$and\": [{ \"distance\": { \"$gte\": 100 }}, { \"distance\": { \"$lte\": 1000 }}]}", condition);
    }

    @Test
    public void test_condition_2() {
        String condition = this.renderer.renderCondition(or(
            and("distance", gte(100), lte(1000)),
            and("age", eq(55))
        ));
        assertDoesNotThrow(() -> Document.parse(condition), condition);
        assertEquals("{\"$or\": [{\"$and\": [{ \"distance\": { \"$gte\": 100 }}, { \"distance\": { \"$lte\": 1000 }}]}, {\"$and\": [{ \"age\": { \"$eq\": 55 }}]}]}", condition);
    }

    @Test
    public void test_condition_3() {
        String condition = this.renderer.renderCondition(and("name", eq("tester")));
        assertDoesNotThrow(() -> Document.parse(condition), condition);
        assertEquals("{\"$and\": [{ \"name\": { \"$eq\": \"tester\" }}]}", condition);
    }
}
