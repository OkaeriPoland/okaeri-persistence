package eu.okaeri.persistence.jdbc;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import eu.okaeri.persistence.PersistenceCollection;
import eu.okaeri.persistence.PersistenceEntity;
import eu.okaeri.persistence.PersistencePath;
import eu.okaeri.persistence.document.index.IndexProperty;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import lombok.NonNull;

public class H2Persistence extends JdbcPersistence {

  public H2Persistence(
      @NonNull final PersistencePath basePath, @NonNull final HikariConfig hikariConfig) {
    super(basePath, hikariConfig);
  }

  public H2Persistence(
      @NonNull final PersistencePath basePath, @NonNull final HikariDataSource dataSource) {
    super(basePath, dataSource);
  }

  @Override
  public boolean updateIndex(
      @NonNull final PersistenceCollection collection,
      @NonNull final PersistencePath path,
      @NonNull final IndexProperty property,
      final String identity) {

    this.checkCollectionRegistered(collection);
    final String indexTable = this.indexTable(collection);
    final String sql =
        "insert into `"
            + indexTable
            + "` (`key`, `property`, `identity`) values (?, ?, ?) on duplicate key update `identity` = ?";
    final String key = path.getValue();

    try (final Connection connection = this.getDataSource().getConnection()) {
      final PreparedStatement prepared = connection.prepareStatement(sql);
      prepared.setString(1, key);
      prepared.setString(2, property.getValue());
      prepared.setString(3, identity);
      prepared.setString(4, identity);
      return prepared.executeUpdate() > 0;
    } catch (final SQLException exception) {
      throw new RuntimeException(
          "cannot update index "
              + indexTable
              + "["
              + property.getValue()
              + "] -> "
              + key
              + " = "
              + identity,
          exception);
    }
  }

  @Override
  public Stream<PersistenceEntity<String>> readByProperty(
      @NonNull final PersistenceCollection collection,
      @NonNull final PersistencePath property,
      @NonNull final Object propertyValue) {
    return this.isIndexed(collection, property)
        ? this.readByPropertyIndexed(
            collection, IndexProperty.of(property.getValue()), propertyValue)
        : this.readByPropertyInstr(collection, property, propertyValue);
  }

  private Stream<PersistenceEntity<String>> readByPropertyInstr(
      @NonNull final PersistenceCollection collection,
      @NonNull final PersistencePath property,
      @NonNull final Object propertyValue) {

    if (!this.canUseToString(propertyValue)) {
      return this.streamAll(collection);
    }

    this.checkCollectionRegistered(collection);
    final String sql =
        "select `key`, `value` from `" + this.table(collection) + "` where instr(`value`, ?)";

    try (final Connection connection = this.getDataSource().getConnection()) {

      final PreparedStatement prepared = connection.prepareStatement(sql);
      prepared.setObject(1, String.valueOf(propertyValue));
      final ResultSet resultSet = prepared.executeQuery();
      final List<PersistenceEntity<String>> results = new ArrayList<>();

      while (resultSet.next()) {
        final String key = resultSet.getString("key");
        final String value = resultSet.getString("value");
        results.add(new PersistenceEntity<>(PersistencePath.of(key), value));
      }

      return results.stream();
    } catch (final SQLException exception) {
      throw new RuntimeException("cannot ready by property from " + collection, exception);
    }
  }

  @Override
  public boolean write(
      @NonNull final PersistenceCollection collection,
      @NonNull final PersistencePath path,
      @NonNull final String raw) {

    this.checkCollectionRegistered(collection);
    final String sql =
        "insert into `"
            + this.table(collection)
            + "` (`key`, `value`) values (?, ?) on duplicate key update `value` = ?";

    try (final Connection connection = this.getDataSource().getConnection()) {
      final PreparedStatement prepared = connection.prepareStatement(sql);
      prepared.setString(1, path.getValue());
      prepared.setString(2, raw);
      prepared.setString(3, raw);
      return prepared.executeUpdate() > 0;
    } catch (final SQLException exception) {
      throw new RuntimeException("cannot write " + path + " to " + collection, exception);
    }
  }

  @Override
  public long write(
      @NonNull final PersistenceCollection collection,
      @NonNull final Map<PersistencePath, String> entities) {

    this.checkCollectionRegistered(collection);
    final String sql =
        "insert into `"
            + this.table(collection)
            + "` (`key`, `value`) values (?, ?) on duplicate key update `value` = ?";

    try (final Connection connection = this.getDataSource().getConnection()) {
      final PreparedStatement prepared = connection.prepareStatement(sql);
      connection.setAutoCommit(false);
      for (final Map.Entry<PersistencePath, String> entry : entities.entrySet()) {
        prepared.setString(1, entry.getKey().getValue());
        prepared.setString(2, entry.getValue());
        prepared.setString(3, entry.getValue());
        prepared.addBatch();
      }
      final int changes = prepared.executeUpdate();
      connection.commit();
      return changes;
    } catch (final SQLException exception) {
      throw new RuntimeException("cannot write " + entities + " to " + collection, exception);
    }
  }
}
