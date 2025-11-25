package eu.okaeri.persistence.filter;

import eu.okaeri.persistence.PersistencePath;
import eu.okaeri.persistence.filter.renderer.DefaultFilterRenderer;
import eu.okaeri.persistence.filter.renderer.DefaultStringRenderer;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static eu.okaeri.persistence.filter.predicate.SimplePredicate.*;
import static org.assertj.core.api.Assertions.assertThat;

public class FilterPredicatesTest {

    private static final PersistencePath X = PersistencePath.of("x");

    @Test
    public void test_eq() {
        // 0 == 1
        assertThat(eq(1).check(0)).isFalse();
        assertThat(eq(1).check(1)).isTrue();
        assertThat(eq(1).check(1f)).isTrue();
        assertThat(eq(1).check((short) 1)).isTrue();
        assertThat(eq(1).check((byte) 1)).isTrue();
        assertThat(eq(1).check(1L)).isTrue();
        assertThat(eq(1).check(1.0)).isTrue();
        assertThat(eq(1).check(1.1)).isFalse();
        assertThat(eq(1).check(2)).isFalse();
        assertThat(eq(1).check(1.000000001d)).isFalse();
        // String to number comparison - should try to parse string as number
        assertThat(eq(1).check("a")).isFalse();        // "a" is not a number
        assertThat(eq(3).check("abc")).isFalse();      // "abc" is not a number
        assertThat(eq(1).check("1")).isTrue();         // "1" parses to 1
        assertThat(eq(42).check("42")).isTrue();       // "42" parses to 42
        assertThat(eq(3.14).check("3.14")).isTrue();   // "3.14" parses to 3.14
    }

    @Test
    public void test_eq_render() {
        DefaultFilterRenderer dpr = new DefaultFilterRenderer(new DefaultStringRenderer());
        assertThat(dpr.renderPredicate(X, eq(1))).isEqualTo("(x == 1)");
        assertThat(dpr.renderPredicate(X, eq(1.0d))).isEqualTo("(x == 1)");
        assertThat(dpr.renderPredicate(X, eq(1.0f))).isEqualTo("(x == 1)");
        assertThat(dpr.renderPredicate(X, eq(1.2))).isEqualTo("(x == 1.2)");
        assertThat(dpr.renderPredicate(X, eq(1.12))).isEqualTo("(x == 1.12)");
        assertThat(dpr.renderPredicate(X, eq(5.1231231231))).isEqualTo("(x == 5.1231231231)");
        assertThat(dpr.renderPredicate(X, eq((short) 1))).isEqualTo("(x == 1)");
        assertThat(dpr.renderPredicate(X, eq((byte) 1))).isEqualTo("(x == 1)");
        assertThat(dpr.renderPredicate(X, eq(1L))).isEqualTo("(x == 1)");
        assertThat(dpr.renderPredicate(X, eq("abc"))).isEqualTo("(x == \"abc\")");
    }

    @Test
    public void test_ge() {
        // 0 >= 1
        assertThat(gte(1).check(0)).isFalse();
        assertThat(gte(1).check(-0d)).isFalse();
        assertThat(gte(1).check(0.9999999999999999)).isFalse();
        assertThat(gte(1).check(0.999999999999999999999)).isTrue(); // nice
        assertThat(gte(1).check(1)).isTrue();
        assertThat(gte(1).check(-2321323)).isFalse();
        assertThat(gte(1).check(-2321323.324434343)).isFalse();
        assertThat(gte(1).check(2)).isTrue();
        assertThat(gte(1).check(3)).isTrue();
        assertThat(gte(1).check(1231)).isTrue();
        assertThat(gte(1).check(1231.232323)).isTrue();
        assertThat(gte(1).check(1.01)).isTrue();
        assertThat(gte(1).check(1.00000222)).isTrue();
        assertThat(gte(1).check(3590530953000000d)).isTrue();
    }

    @Test
    public void test_eq_uuid() {
        UUID uuid = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
        String uuidString = "550e8400-e29b-41d4-a716-446655440000";

        // eq(UUID) converts to string internally
        assertThat(eq(uuid).check(uuidString)).isTrue();
        assertThat(eq(uuid).check("550e8400-e29b-41d4-a716-446655440000")).isTrue();
        assertThat(eq(uuid).check("different-uuid")).isFalse();

        // ne(UUID) also works
        assertThat(ne(uuid).check(uuidString)).isFalse();
        assertThat(ne(uuid).check("different-uuid")).isTrue();
    }

    enum TestStatus {
        ACTIVE, INACTIVE, PENDING
    }

    @Test
    public void test_eq_enum() {
        // eq(Enum) converts to name() string internally
        // Document stores enum as string "ACTIVE", predicate compares against "ACTIVE"
        assertThat(eq(TestStatus.ACTIVE).check("ACTIVE")).isTrue();
        assertThat(eq(TestStatus.ACTIVE).check("INACTIVE")).isFalse();
        assertThat(eq(TestStatus.ACTIVE).check("active")).isFalse(); // case-sensitive

        assertThat(eq(TestStatus.PENDING).check("PENDING")).isTrue();
        assertThat(eq(TestStatus.INACTIVE).check("INACTIVE")).isTrue();

        // ne(Enum) also works
        assertThat(ne(TestStatus.ACTIVE).check("ACTIVE")).isFalse();
        assertThat(ne(TestStatus.ACTIVE).check("INACTIVE")).isTrue();
        assertThat(ne(TestStatus.ACTIVE).check("PENDING")).isTrue();
    }

    @Test
    public void test_eq_enum_render() {
        DefaultFilterRenderer dpr = new DefaultFilterRenderer(new DefaultStringRenderer());
        assertThat(dpr.renderPredicate(X, eq(TestStatus.ACTIVE))).isEqualTo("(x == \"ACTIVE\")");
        assertThat(dpr.renderPredicate(X, ne(TestStatus.PENDING))).isEqualTo("(x != \"PENDING\")");
    }

    @Test
    public void test_eq_uuid_render() {
        DefaultFilterRenderer dpr = new DefaultFilterRenderer(new DefaultStringRenderer());
        UUID uuid = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
        assertThat(dpr.renderPredicate(X, eq(uuid))).isEqualTo("(x == \"550e8400-e29b-41d4-a716-446655440000\")");
        assertThat(dpr.renderPredicate(X, ne(uuid))).isEqualTo("(x != \"550e8400-e29b-41d4-a716-446655440000\")");
    }
}
