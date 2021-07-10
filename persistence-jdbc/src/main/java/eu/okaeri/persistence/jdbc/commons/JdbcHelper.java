package eu.okaeri.persistence.jdbc.commons;

import com.zaxxer.hikari.HikariConfig;
import lombok.NonNull;

public final class JdbcHelper {

    public static void initDriverQuietly(@NonNull String clazz) {
        try {
            Class.forName(clazz);
        } catch (ClassNotFoundException ignored) {
        }
    }

    public static HikariConfig configureHikari(@NonNull String jdbcUrl) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl);
        return config;
    }

    public static HikariConfig configureHikari(@NonNull String jdbcUrl, @NonNull String driverClazz) {
        initDriverQuietly(driverClazz);
        return configureHikari(jdbcUrl);
    }
}
