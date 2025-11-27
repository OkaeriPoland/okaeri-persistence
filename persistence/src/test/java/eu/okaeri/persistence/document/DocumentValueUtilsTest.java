package eu.okaeri.persistence.document;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Month;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DocumentValueUtilsTest {

    @Nested
    class ExtractValue {

        @Test
        void returns_top_level_value() {
            Map<String, Object> map = Map.of("name", "alice");
            Object result = DocumentValueUtils.extractValue(map, List.of("name"));
            assertThat(result).isEqualTo("alice");
        }

        @Test
        void returns_nested_value() {
            Map<String, Object> map = Map.of(
                "user", Map.of(
                    "profile", Map.of("name", "alice")
                )
            );
            Object result = DocumentValueUtils.extractValue(map, List.of("user", "profile", "name"));
            assertThat(result).isEqualTo("alice");
        }

        @Test
        void returns_null_when_path_not_found() {
            Map<String, Object> map = Map.of("name", "alice");
            Object result = DocumentValueUtils.extractValue(map, List.of("missing"));
            assertThat(result).isNull();
        }

        @Test
        void returns_null_when_intermediate_path_not_found() {
            Map<String, Object> map = Map.of("user", Map.of("name", "alice"));
            Object result = DocumentValueUtils.extractValue(map, List.of("user", "profile", "name"));
            assertThat(result).isNull();
        }

        @Test
        void returns_null_when_navigating_through_non_map() {
            Map<String, Object> map = Map.of("user", "not-a-map");
            Object result = DocumentValueUtils.extractValue(map, List.of("user", "name"));
            assertThat(result).isNull();
        }

        @Test
        void returns_map_from_null_input() {
            Object result = DocumentValueUtils.extractValue(null, List.of("name"));
            assertThat(result).isNull();
        }

        @Test
        void returns_entire_map_for_empty_path() {
            Map<String, Object> map = Map.of("name", "alice");
            Object result = DocumentValueUtils.extractValue(map, List.of());
            assertThat(result).isEqualTo(map);
        }
    }

    @Nested
    class CompareEquals {

        @Test
        void both_null_returns_true() {
            assertThat(DocumentValueUtils.compareEquals(null, null)).isTrue();
        }

        @Test
        void one_null_returns_false() {
            assertThat(DocumentValueUtils.compareEquals(null, "value")).isFalse();
            assertThat(DocumentValueUtils.compareEquals("value", null)).isFalse();
        }

        @Test
        void same_strings_returns_true() {
            assertThat(DocumentValueUtils.compareEquals("hello", "hello")).isTrue();
        }

        @Test
        void different_strings_returns_false() {
            assertThat(DocumentValueUtils.compareEquals("hello", "world")).isFalse();
        }

        @Test
        void same_integers_returns_true() {
            assertThat(DocumentValueUtils.compareEquals(42, 42)).isTrue();
        }

        @Test
        void integer_and_long_same_value_returns_true() {
            assertThat(DocumentValueUtils.compareEquals(42, 42L)).isTrue();
        }

        @Test
        void integer_and_double_same_value_returns_true() {
            assertThat(DocumentValueUtils.compareEquals(42, 42.0)).isTrue();
        }

        @Test
        void string_number_and_number_returns_true() {
            assertThat(DocumentValueUtils.compareEquals("42", 42)).isTrue();
            assertThat(DocumentValueUtils.compareEquals(42, "42")).isTrue();
            assertThat(DocumentValueUtils.compareEquals("42.5", 42.5)).isTrue();
        }

        @Test
        void string_number_and_number_different_returns_false() {
            assertThat(DocumentValueUtils.compareEquals("42", 43)).isFalse();
        }

        @Test
        void non_numeric_string_and_number_returns_false() {
            assertThat(DocumentValueUtils.compareEquals("hello", 42)).isFalse();
        }

        @Test
        void uuid_and_string_returns_true() {
            UUID uuid = UUID.randomUUID();
            assertThat(DocumentValueUtils.compareEquals(uuid, uuid.toString())).isTrue();
            assertThat(DocumentValueUtils.compareEquals(uuid.toString(), uuid)).isTrue();
        }

        @Test
        void enum_and_string_returns_true() {
            assertThat(DocumentValueUtils.compareEquals("JANUARY", Month.JANUARY)).isTrue();
            assertThat(DocumentValueUtils.compareEquals(Month.JANUARY, "JANUARY")).isTrue();
        }

        @Test
        void enum_and_string_case_insensitive() {
            assertThat(DocumentValueUtils.compareEquals("january", Month.JANUARY)).isTrue();
            assertThat(DocumentValueUtils.compareEquals(Month.JANUARY, "january")).isTrue();
        }

        @Test
        void enum_and_string_different_returns_false() {
            assertThat(DocumentValueUtils.compareEquals("FEBRUARY", Month.JANUARY)).isFalse();
            assertThat(DocumentValueUtils.compareEquals(Month.JANUARY, "FEBRUARY")).isFalse();
        }

        @Test
        void incompatible_types_throws() {
            assertThatThrownBy(() -> DocumentValueUtils.compareEquals(List.of(1), Map.of("a", 1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot compare");
        }
    }

    @Nested
    class CompareForSort {

        @Test
        void both_null_returns_zero() {
            assertThat(DocumentValueUtils.compareForSort(null, null)).isEqualTo(0);
        }

        @Test
        void null_sorts_last() {
            assertThat(DocumentValueUtils.compareForSort(null, "value")).isGreaterThan(0);
            assertThat(DocumentValueUtils.compareForSort("value", null)).isLessThan(0);
        }

        @Test
        void numbers_compare_numerically() {
            assertThat(DocumentValueUtils.compareForSort(1, 2)).isLessThan(0);
            assertThat(DocumentValueUtils.compareForSort(2, 1)).isGreaterThan(0);
            assertThat(DocumentValueUtils.compareForSort(1, 1)).isEqualTo(0);
        }

        @Test
        void mixed_number_types_compare_numerically() {
            assertThat(DocumentValueUtils.compareForSort(1, 2L)).isLessThan(0);
            assertThat(DocumentValueUtils.compareForSort(1.5, 2)).isLessThan(0);
            assertThat(DocumentValueUtils.compareForSort(2.0, 2)).isEqualTo(0);
        }

        @Test
        void string_numbers_compare_numerically() {
            assertThat(DocumentValueUtils.compareForSort("1", 2)).isLessThan(0);
            assertThat(DocumentValueUtils.compareForSort(1, "2")).isLessThan(0);
            assertThat(DocumentValueUtils.compareForSort("10", "2")).isGreaterThan(0); // numeric, not lexical
        }

        @Test
        void non_numeric_strings_compare_lexically() {
            assertThat(DocumentValueUtils.compareForSort("apple", "banana")).isLessThan(0);
            assertThat(DocumentValueUtils.compareForSort("banana", "apple")).isGreaterThan(0);
            assertThat(DocumentValueUtils.compareForSort("apple", "apple")).isEqualTo(0);
        }

        @Test
        void mixed_number_and_non_numeric_string_falls_back_to_string() {
            // "abc" can't be parsed as number, falls through to string comparison
            // 123 → "123", compared to "abc" lexically: "1" < "a"
            assertThat(DocumentValueUtils.compareForSort(123, "abc")).isLessThan(0);
            assertThat(DocumentValueUtils.compareForSort("abc", 123)).isGreaterThan(0);
        }

        @Test
        void same_comparable_type_uses_natural_order() {
            assertThat(DocumentValueUtils.compareForSort(
                UUID.fromString("00000000-0000-0000-0000-000000000001"),
                UUID.fromString("00000000-0000-0000-0000-000000000002")
            )).isLessThan(0);
        }

        @Test
        void fallback_to_string_comparison() {
            // Different types fall back to string comparison
            assertThat(DocumentValueUtils.compareForSort("abc", List.of(1))).isNotEqualTo(0);
        }
    }
}
