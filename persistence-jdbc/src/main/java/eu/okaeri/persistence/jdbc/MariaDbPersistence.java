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
import java.util.Set;
import java.util.stream.Stream;
import lombok.NonNull;

public class MariaDbPersistence extends JdbcPersistence {

  public MariaDbPersistence(
      @NonNull final PersistencePath basePath, @NonNull final HikariConfig hikariConfig) {
    super(basePath, hikariConfig);
  }

  public MariaDbPersistence(
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
          "cannot update index " + indexTable + " -> " + key + " = " + identity, exception);
    }
  }

  @Override
  public void registerCollection(@NonNull final PersistenceCollection collection) {

    final String collectionTable = this.table(collection);
    final int keyLength = collection.getKeyLength();

    final String sql =
        "create table if not exists `"
            + collectionTable
            + "` ("
            + "`key` varchar("
            + keyLength
            + ") primary key not null,"
            + "`value` json not null)"
            + "engine = InnoDB character set = utf8mb4;";
    final String alterKeySql =
        "alter table `"
            + collectionTable
            + "` MODIFY COLUMN `key` varchar("
            + keyLength
            + ") not null";

    try (final Connection connection = this.getDataSource().getConnection()) {
      connection.createStatement().execute(sql);
      connection.createStatement().execute(alterKeySql);
    } catch (final SQLException exception) {
      throw new RuntimeException("cannot register collection", exception);
    }

    final Set<IndexProperty> indexes = collection.getIndexes();
    final int identityLength = collection.getMaxIndexIdentityLength();
    final int propertyLength = collection.getMaxIndexPropertyLength();
    indexes.forEach(index -> this.registerIndex(collection, index, identityLength, propertyLength));

    super.registerCollection(collection);
  }

  private void registerIndex(
      @NonNull final PersistenceCollection collection,
      @NonNull final IndexProperty property,
      final int identityLength,
      final int propertyLength) {

    final int keyLength = collection.getKeyLength();
    final String indexTable = this.indexTable(collection);

    final String tableSql =
        "create table if not exists `"
            + indexTable
            + "` ("
            + "`key` varchar("
            + keyLength
            + ") not null,"
            + "`property` varchar("
            + propertyLength
            + ") not null,"
            + "`identity` varchar("
            + identityLength
            + ") not null,"
            + "primary key(`key`, `property`),"
            + "index (`identity`),"
            + "index (`property`, `identity`))"
            + "engine = InnoDB character set = utf8mb4;";

    final String alterKeySql =
        "alter table `" + indexTable + "` MODIFY COLUMN `key` varchar(" + keyLength + ") not null";
    final String alterPropertySql =
        "alter table `"
            + indexTable
            + "` MODIFY COLUMN `property` varchar("
            + propertyLength
            + ") not null";
    final String alterIdentitySql =
        "alter table `"
            + indexTable
            + "` MODIFY COLUMN `identity` varchar("
            + identityLength
            + ") not null";

    try (final Connection connection = this.getDataSource().getConnection()) {
      connection.createStatement().execute(tableSql);
      connection.createStatement().execute(alterKeySql);
      connection.createStatement().execute(alterPropertySql);
      connection.createStatement().execute(alterIdentitySql);
    } catch (final SQLException exception) {
      throw new RuntimeException("cannot register collection", exception);
    }
  }

  @Override
  public Stream<PersistenceEntity<String>> readByProperty(
      @NonNull final PersistenceCollection collection,
      @NonNull final PersistencePath property,
      final Object propertyValue) {
    return this.isIndexed(collection, property)
        ? this.readByPropertyIndexed(
            collection, IndexProperty.of(property.getValue()), propertyValue)
        : this.readByPropertyJsonExtract(collection, property, propertyValue);
  }

  private Stream<PersistenceEntity<String>> readByPropertyJsonExtract(
      @NonNull final PersistenceCollection collection,
      @NonNull final PersistencePath property,
      final Object propertyValue) {

    this.checkCollectionRegistered(collection);
    final String sql =
        "select `key`, `value` from `"
            + this.table(collection)
            + "` where json_extract(`value`, ?) = ?";

    try (final Connection connection = this.getDataSource().getConnection()) {

      final PreparedStatement prepared = connection.prepareStatement(sql);
      prepared.setString(1, property.toSqlJsonPath());
      prepared.setObject(2, propertyValue);
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
