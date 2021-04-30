package eu.okaeri.persistence.jdbc.commons;

import com.zaxxer.hikari.HikariConfig;

public final class JdbcHelper {

    public static void initDriverQuietly(String clazz) {
        try {
            Class.forName(clazz);
        } catch (ClassNotFoundException ignored) {
        }
    }

    public static HikariConfig configureHikari(String jdbcUrl) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl);
        return config;
    }

    public static HikariConfig configureHikari(String jdbcUrl, String driverClazz) {
        initDriverQuietly(driverClazz);
        return configureHikari(jdbcUrl);
    }
}
