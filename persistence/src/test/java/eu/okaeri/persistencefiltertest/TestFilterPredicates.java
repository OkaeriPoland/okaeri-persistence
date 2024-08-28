package eu.okaeri.persistencefiltertest;

import eu.okaeri.persistence.PersistencePath;
import eu.okaeri.persistence.filter.renderer.DefaultFilterRenderer;
import eu.okaeri.persistence.filter.renderer.DefaultStringRenderer;
import org.junit.jupiter.api.Test;

import static eu.okaeri.persistence.filter.predicate.SimplePredicate.eq;
import static eu.okaeri.persistence.filter.predicate.SimplePredicate.ge;
import static org.junit.jupiter.api.Assertions.*;

public class TestFilterPredicates {

    private static final PersistencePath X = PersistencePath.of("x");

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
        DefaultFilterRenderer dpr = new DefaultFilterRenderer(new DefaultStringRenderer());
        assertEquals("(x == 1)", dpr.renderPredicate(X, eq(1)));
        assertEquals("(x == 1)", dpr.renderPredicate(X, eq(1.0d)));
        assertEquals("(x == 1)", dpr.renderPredicate(X, eq(1.0f)));
        assertEquals("(x == 1.2)", dpr.renderPredicate(X, eq(1.2)));
        assertEquals("(x == 1.12)", dpr.renderPredicate(X, eq(1.12)));
        assertEquals("(x == 5.1231231231)", dpr.renderPredicate(X, eq(5.1231231231)));
        assertEquals("(x == 1)", dpr.renderPredicate(X, eq((short) 1)));
        assertEquals("(x == 1)", dpr.renderPredicate(X, eq((byte) 1)));
        assertEquals("(x == 1)", dpr.renderPredicate(X, eq(1L)));
        assertEquals("(x == \"abc\")", dpr.renderPredicate(X, eq("abc")));
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
