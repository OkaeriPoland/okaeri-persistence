package eu.okaeri.persistence;

import lombok.*;

import java.io.File;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.stream.Collectors;

@Getter
@ToString
@EqualsAndHashCode
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public class PersistencePath {

    public static final String SEPARATOR = ":";
    private String value;

    public static PersistencePath of(@NonNull File file) {
        return new PersistencePath(file.getPath().replace(File.separator, SEPARATOR));
    }

    public static PersistencePath of(@NonNull UUID uuid) {
        return new PersistencePath(String.valueOf(uuid));
    }

    public static PersistencePath of(@NonNull String path) {
        return new PersistencePath(path);
    }

    public static PersistencePath parse(@NonNull String source, @NonNull String separator) {
        return new PersistencePath(source.replace(separator, SEPARATOR));
    }

    public PersistencePath sub(@NonNull UUID sub) {
        return this.sub(String.valueOf(sub));
    }

    public PersistencePath sub(@NonNull PersistencePath sub) {
        if (this.value.isEmpty()) {
            return of(sub.getValue());
        }
        return this.sub(sub.getValue());
    }

    public PersistencePath sub(@NonNull String sub) {

        boolean startsWithSeparator = sub.startsWith(SEPARATOR);
        if (this.value.isEmpty()) {
            return of(startsWithSeparator ? sub.substring(1) : sub);
        }

        String separator = startsWithSeparator ? "" : SEPARATOR;
        return this.append(separator + sub);
    }

    public PersistencePath append(@NonNull String element) {
        return of(this.value + element);
    }

    public PersistencePath removeStart(@NonNull String part) {
        return this.value.startsWith(part) ? of(this.value.substring(part.length())) : this;
    }

    public PersistencePath removeStart(@NonNull PersistencePath path) {
        return this.removeStart(path.getValue());
    }

    public PersistencePath group() {
        String[] parts = this.value.split(SEPARATOR);
        return of(String.join(SEPARATOR, Arrays.copyOfRange(parts, 0, parts.length - 1)));
    }

    public File toFile() {
        return new File(this.toSafeFilePath());
    }

    public Path toPath() {
        return this.toFile().toPath();
    }

    public String toSqlIdentifier() {
        String identifier = this.value.replace(SEPARATOR, "_");
        if (!identifier.matches("^[a-zA-Z_][a-zA-Z0-9_]*$")) {
            throw new IllegalArgumentException("identifier '" + identifier + "' cannot be used as sql identifier");
        }
        return identifier;
    }

    public String toSqlJsonPath() {
        String identifier = "$." + this.value.replace(SEPARATOR, ".");
        if (identifier.contains("'") || identifier.contains("/") || identifier.contains("#") || identifier.contains("--")) {
            throw new IllegalArgumentException("identifier '" + identifier + "' cannot be used as sql json path");
        }
        return identifier;
    }

    public String toMongoPath() {
        return this.value.replace(SEPARATOR, ".");
    }

    public List<String> toParts() {
        return Arrays.asList(this.value.split(SEPARATOR));
    }

    public String toSafeFilePath() {
        // edge case and windows (restore drive letter)
        if ((this.value.length() >= 3) && (SEPARATOR + SEPARATOR).equals(this.value.substring(1, 3)) && System.getProperty("os.name").toLowerCase(Locale.ROOT).contains("windows")) {
            String suffix = this.toSafeFilePath(this.value.substring(3).split(SEPARATOR));
            return this.value.charAt(0) + ":" + File.separator + suffix;
        }
        // use standard procedure
        return this.toSafeFilePath(this.value.split(SEPARATOR));
    }

    private String toSafeFilePath(String[] parts) {
        return Arrays.stream(parts)
            .map(part -> part.replace("^\\.+", "").replaceAll("[\\\\/:*?\"<>|]", ""))
            .collect(Collectors.joining(File.separator));
    }

    public String toSafeFileName() {
        return this.toSafeFilePath().replace(File.separator, "_");
    }

    public UUID toUUID() {
        return UUID.fromString(this.value);
    }
}
