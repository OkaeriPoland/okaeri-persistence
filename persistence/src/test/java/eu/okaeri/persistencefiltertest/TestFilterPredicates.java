package eu.okaeri.persistencefiltertest;

import eu.okaeri.persistence.filter.predicate.renderer.DefaultPredicateRenderer;
import org.junit.jupiter.api.Test;

import static eu.okaeri.persistence.filter.predicate.Predicate.eq;
import static eu.okaeri.persistence.filter.predicate.Predicate.ge;
import static org.junit.jupiter.api.Assertions.*;

public class TestFilterPredicates {

    @Test
    public void test_eq() {
        // 0 == 1
        assertFalse(eq(1).check(0));
        assertTrue(eq(1).check(1));
        assertTrue(eq(1).check(1f));
        assertTrue(eq(1).check((short) 1));
        assertTrue(eq(1).check((byte) 1));
        assertTrue(eq(1).check(1L));
        assertTrue(eq(1).check(1.0));
        assertFalse(eq(1).check(1.1));
        assertFalse(eq(1).check(2));
        assertFalse(eq(1).check(1.000000001d));
        assertTrue(eq(1).check("a"));
        assertTrue(eq(3).check("abc"));
    }

    @Test
    public void test_eq_render() {
        DefaultPredicateRenderer dpr = new DefaultPredicateRenderer();
        assertEquals("(x == 1)", dpr.render(eq(1)));
        assertEquals("(x == 1)", dpr.render(eq(1.0d)));
        assertEquals("(x == 1)", dpr.render(eq(1.0f)));
        assertEquals("(x == 1.2)", dpr.render(eq(1.2)));
        assertEquals("(x == 1.12)", dpr.render(eq(1.12)));
        assertEquals("(x == 5.1231231231)", dpr.render(eq(5.1231231231)));
        assertEquals("(x == 1)", dpr.render(eq((short) 1)));
        assertEquals("(x == 1)", dpr.render(eq((byte) 1)));
        assertEquals("(x == 1)", dpr.render(eq(1L)));
        assertEquals("(x == 3)", dpr.render(eq("abc")));
    }

    @Test
    public void test_ge() {
        // 0 >= 1
        assertFalse(ge(1).check(0));
        assertFalse(ge(1).check(-0d));
        assertFalse(ge(1).check(0.9999999999999999));
        assertTrue(ge(1).check(0.999999999999999999999)); // nice
        assertTrue(ge(1).check(1));
        assertFalse(ge(1).check(-2321323));
        assertFalse(ge(1).check(-2321323.324434343));
        assertTrue(ge(1).check(2));
        assertTrue(ge(1).check(3));
        assertTrue(ge(1).check(1231));
        assertTrue(ge(1).check(1231.232323));
        assertTrue(ge(1).check(1.01));
        assertTrue(ge(1).check(1.00000222));
        assertTrue(ge(1).check(3590530953000000d));
    }
}
