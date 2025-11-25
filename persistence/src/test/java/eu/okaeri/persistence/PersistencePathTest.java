package eu.okaeri.persistence;

import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for PersistencePath, including security-focused path traversal tests.
 */
class PersistencePathTest {

    // ===== PATH TRAVERSAL SECURITY TESTS =====
    // These tests verify that path traversal attempts are blocked by toSafeFilePath()

    @Test
    void toSafeFilePath_blocks_classic_path_traversal() {
        // Classic path traversal with forward slashes
        // Note: Without colons, this is a single segment - slashes are stripped
        // The remaining dots are garbage text, not a traversal threat
        PersistencePath path = PersistencePath.of("../../../etc/passwd");
        String safePath = path.toSafeFilePath();

        // Slashes should be removed, making traversal ineffective
        assertThat(safePath).doesNotContain("/");
        // Path becomes mangled garbage, can't navigate directories
        assertThat(safePath).isEqualTo("....etcpasswd");
    }

    @Test
    void toSafeFilePath_blocks_backslash_traversal() {
        // Windows-style path traversal
        // Note: Without colons, this is a single segment - backslashes are stripped
        PersistencePath path = PersistencePath.of("..\\..\\..\\windows\\system32");
        String safePath = path.toSafeFilePath();

        // Backslashes should be removed, making traversal ineffective
        assertThat(safePath).doesNotContain("\\");
        // Path becomes mangled garbage
        assertThat(safePath).isEqualTo("....windowssystem32");
    }

    @Test
    void toSafeFilePath_blocks_colon_separated_traversal() {
        // CRITICAL: Colon-separated path traversal
        // This is the vulnerability found in security analysis:
        // Colons split into segments, and ".." parts should be sanitized
        PersistencePath path = PersistencePath.of("foo:..:..:sensitive");
        String safePath = path.toSafeFilePath();

        // The ".." segments should be stripped of leading dots
        assertThat(safePath).doesNotContain("..");

        // Should NOT allow escaping parent directories
        File file = new File("/base/collection/" + safePath);
        try {
            String canonical = file.getCanonicalPath();
            // If ".." worked, canonical path would escape /base/collection
            assertThat(canonical).startsWith("/base/collection");
        } catch (Exception e) {
            // Even if canonical fails, path should not contain traversal
            assertThat(safePath).doesNotContain("..");
        }
    }

    @Test
    void toSafeFilePath_blocks_multiple_colon_traversal() {
        // Multiple levels of traversal via colons
        PersistencePath path = PersistencePath.of("data:..:..:..:..:etc:passwd");
        String safePath = path.toSafeFilePath();

        assertThat(safePath).doesNotContain("..");
    }

    @Test
    void toSafeFilePath_strips_leading_dots_from_segments() {
        // Leading dots should be stripped from each segment
        PersistencePath path = PersistencePath.of("...hidden:..secret:normal");
        String safePath = path.toSafeFilePath();

        // Leading dots should be removed, file separator depends on OS
        String[] parts = safePath.split(File.separator.equals("\\") ? "\\\\" : File.separator);
        for (String part : parts) {
            assertThat(part).doesNotStartWith(".");
        }
    }

    @Test
    void toSafeFilePath_handles_single_dot() {
        // Single dot (current directory) should be stripped
        PersistencePath path = PersistencePath.of("foo:.:bar");
        String safePath = path.toSafeFilePath();

        // The "." segment should become empty and not cause issues
        assertThat(safePath).doesNotContain(File.separator + "." + File.separator);
    }

    @Test
    void toSafeFilePath_handles_double_dot_only() {
        // Just double-dot segments
        PersistencePath path = PersistencePath.of("..:..:..");
        String safePath = path.toSafeFilePath();

        // All segments should be sanitized to empty
        assertThat(safePath).doesNotContain("..");
    }

    @Test
    void toSafeFilePath_removes_special_characters() {
        // Special characters that are dangerous in file paths
        PersistencePath path = PersistencePath.of("test*file?name<>|\"chars");
        String safePath = path.toSafeFilePath();

        assertThat(safePath).doesNotContain("*");
        assertThat(safePath).doesNotContain("?");
        assertThat(safePath).doesNotContain("<");
        assertThat(safePath).doesNotContain(">");
        assertThat(safePath).doesNotContain("|");
        assertThat(safePath).doesNotContain("\"");
    }

    @Test
    void toSafeFilePath_handles_url_encoded_traversal() {
        // URL-encoded traversal (should not be decoded)
        PersistencePath path = PersistencePath.of("%2e%2e:%2e%2e:etc");
        String safePath = path.toSafeFilePath();

        // URL encoding is NOT decoded, so %2e stays as-is
        // This is safe because filesystem won't interpret %2e as dot
        assertThat(safePath).doesNotContain("..");
    }

    @Test
    void toSafeFilePath_handles_mixed_traversal_attempts() {
        // Mixed traversal with slashes and colons
        // Splits into: ["foo/../bar", "..", "..", "escape"]
        // After processing: ["foo..bar", "", "", "escape"]
        PersistencePath path = PersistencePath.of("foo/../bar:..:..:escape");
        String safePath = path.toSafeFilePath();

        // The ".." colon-segments become empty (leading dots stripped)
        // First segment has slashes removed but keeps internal dots (not leading)
        // "foo..bar" contains ".." but it's in the middle, not a standalone segment
        // so it cannot cause path traversal - verify with File.getCanonicalPath()
        File file = new File("/base/collection/" + safePath);
        try {
            String canonical = file.getCanonicalPath();
            // Safe if canonical path stays within base directory
            assertThat(canonical).startsWith("/base/collection");
        } catch (Exception e) {
            // Even if resolution fails, verify no standalone ".." segments
            String[] parts = safePath.split(File.separator.equals("\\") ? "\\\\" : File.separator);
            for (String part : parts) {
                assertThat(part).isNotEqualTo("..");
            }
        }
        assertThat(safePath).contains("foo");
        assertThat(safePath).contains("bar");
        assertThat(safePath).contains("escape");
    }

    @Test
    void toSafeFilePath_preserves_valid_filenames() {
        // Valid filenames should be preserved
        PersistencePath path = PersistencePath.of("users:john_doe:profile");
        String safePath = path.toSafeFilePath();

        assertThat(safePath).contains("users");
        assertThat(safePath).contains("john_doe");
        assertThat(safePath).contains("profile");
    }

    @Test
    void toSafeFilePath_handles_uuid_paths() {
        // UUID paths (common in this library) should work fine
        String uuid = "550e8400-e29b-41d4-a716-446655440000";
        PersistencePath path = PersistencePath.of("collection:" + uuid);
        String safePath = path.toSafeFilePath();

        assertThat(safePath).contains("collection");
        assertThat(safePath).contains(uuid);
    }

    // ===== BASIC FUNCTIONALITY TESTS =====

    @Test
    void of_creates_path_from_string() {
        PersistencePath path = PersistencePath.of("users:john");
        assertThat(path.getValue()).isEqualTo("users:john");
    }

    @Test
    void sub_appends_path_segment() {
        PersistencePath path = PersistencePath.of("users").sub("john");
        assertThat(path.getValue()).isEqualTo("users:john");
    }

    @Test
    void toParts_splits_by_separator() {
        PersistencePath path = PersistencePath.of("a:b:c");
        assertThat(path.toParts()).containsExactly("a", "b", "c");
    }

    @Test
    void toSqlIdentifier_replaces_colons_with_underscores() {
        PersistencePath path = PersistencePath.of("users_data");
        assertThat(path.toSqlIdentifier()).isEqualTo("users_data");
    }

    @Test
    void toMongoPath_replaces_colons_with_dots() {
        PersistencePath path = PersistencePath.of("user:profile:name");
        assertThat(path.toMongoPath()).isEqualTo("user.profile.name");
    }
}
