package eu.okaeri.persistence.jdbc.commons;

import com.zaxxer.hikari.HikariConfig;
import lombok.NonNull;

public final class JdbcHelper {

  public static void initDriverQuietly(@NonNull final String clazz) {
    try {
      Class.forName(clazz);
    } catch (final ClassNotFoundException ignored) {
    }
  }

  public static HikariConfig configureHikari(@NonNull final String jdbcUrl) {
    final HikariConfig config = new HikariConfig();
    config.setJdbcUrl(jdbcUrl);
    return config;
  }

  public static HikariConfig configureHikari(
      @NonNull final String jdbcUrl, @NonNull final String driverClazz) {
    initDriverQuietly(driverClazz);
    return configureHikari(jdbcUrl);
  }
}
