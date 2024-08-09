package eu.okaeri.persistence.jdbc;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import eu.okaeri.persistence.PersistenceCollection;
import eu.okaeri.persistence.PersistenceEntity;
import eu.okaeri.persistence.PersistencePath;
import eu.okaeri.persistence.document.index.IndexProperty;
import eu.okaeri.persistence.raw.RawPersistence;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Predicate;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Getter;
import lombok.NonNull;
import lombok.SneakyThrows;

public class JdbcPersistence extends RawPersistence {

  private static final Logger LOGGER = Logger.getLogger(JdbcPersistence.class.getSimpleName());
  @Getter protected HikariDataSource dataSource;

  public JdbcPersistence(
      @NonNull final PersistencePath basePath, @NonNull final HikariConfig hikariConfig) {
    super(basePath, true, true, false, true, true);
    this.connect(hikariConfig);
  }

  public JdbcPersistence(
      @NonNull final PersistencePath basePath, @NonNull final HikariDataSource dataSource) {
    super(basePath, true, true, false, true, true);
    this.dataSource = dataSource;
  }

  @SneakyThrows
  protected void connect(@NonNull final HikariConfig hikariConfig) {
    do {
      try {
        this.dataSource = new HikariDataSource(hikariConfig);
      } catch (final Exception exception) {
        if (exception.getCause() != null) {
          LOGGER.severe(
              "["
                  + this.getBasePath().getValue()
                  + "] Cannot connect with database (waiting 30s): "
                  + exception.getMessage()
                  + " caused by "
                  + exception.getCause().getMessage());
        } else {
          LOGGER.severe(
              "["
                  + this.getBasePath().getValue()
                  + "] Cannot connect with database (waiting 30s): "
                  + exception.getMessage());
        }
        Thread.sleep(30_000);
      }
    } while (this.dataSource == null);
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
            + "`value` text not null)";
    final String alterKeySql =
        "alter table `"
            + collectionTable
            + "` MODIFY COLUMN `key` varchar("
            + keyLength
            + ") not null";

    try (final Connection connection = this.dataSource.getConnection()) {
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
            + "primary key(`key`, `property`))";

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

    final String indexSql = "create index `identity` on `" + indexTable + "`(`identity`)";
    final String index2Sql =
        "create index `property` on `" + indexTable + "`(`property`, identity`)";

    try (final Connection connection = this.dataSource.getConnection()) {
      connection.createStatement().execute(tableSql);
      connection.createStatement().execute(alterKeySql);
      connection.createStatement().execute(alterPropertySql);
      connection.createStatement().execute(alterIdentitySql);
      try {
        connection.createStatement().execute(indexSql);
      } catch (final SQLException ignored) {
        // index already exists or worse cannot be created
        // and we don't know about that. heh
      }
      try {
        connection.createStatement().execute(index2Sql);
      } catch (final SQLException ignored) {
        // index already exists or worse cannot be created
        // and we don't know about that. heh
      }
    } catch (final SQLException exception) {
      throw new RuntimeException("cannot register collection", exception);
    }
  }

  @Override
  public Set<PersistencePath> findMissingIndexes(
      @NonNull final PersistenceCollection collection,
      @NonNull final Set<IndexProperty> indexProperties) {

    if (indexProperties.isEmpty()) {
      return Collections.emptySet();
    }

    final String table = this.table(collection);
    final String indexTable = this.indexTable(collection);
    final Set<PersistencePath> paths = new HashSet<>();

    final String params = indexProperties.stream().map(e -> "?").collect(Collectors.joining(", "));

    final String sql =
        "select `key` from `"
            + table
            + "` "
            + "where (select count(1) from "
            + indexTable
            + " where `key` = `"
            + table
            + "`.`key` and `property` in ("
            + params
            + ")) != ?";

    try (final Connection connection = this.dataSource.getConnection()) {
      final PreparedStatement prepared = connection.prepareStatement(sql);
      int currentPrepared = 1;
      for (final IndexProperty indexProperty : indexProperties) {
        prepared.setString(currentPrepared++, indexProperty.getValue());
      }
      prepared.setInt(currentPrepared, indexProperties.size());
      final ResultSet resultSet = prepared.executeQuery();
      while (resultSet.next()) {
        paths.add(PersistencePath.of(resultSet.getString("key")));
      }
    } catch (final SQLException exception) {
      throw new RuntimeException("cannot search missing indexes for " + collection, exception);
    }

    return paths;
  }

  @Override
  public boolean updateIndex(
      @NonNull final PersistenceCollection collection,
      @NonNull final PersistencePath path,
      @NonNull final IndexProperty property,
      final String identity) {

    this.checkCollectionRegistered(collection);
    final String indexTable = this.indexTable(collection);
    final String key = path.getValue();
    final boolean exists;

    try (final Connection connection = this.dataSource.getConnection()) {
      final String sql =
          "select count(1) from `" + indexTable + "` where `key` = ? and `property` = ?";
      final PreparedStatement prepared = connection.prepareStatement(sql);
      prepared.setString(1, key);
      prepared.setString(2, property.getValue());
      final ResultSet resultSet = prepared.executeQuery();
      exists = resultSet.next();
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

    if (exists) {
      final String sql =
          "update `" + indexTable + "` set `identity` = ? where `key` = ? and `property` = ?";
      try (final Connection connection = this.dataSource.getConnection()) {
        final PreparedStatement prepared = connection.prepareStatement(sql);
        prepared.setString(1, identity);
        prepared.setString(2, key);
        prepared.setString(3, property.getValue());
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

    final String sql =
        "insert into `" + indexTable + "` (`key`, `property`, `identity`) values (?, ?, ?)";
    try (final Connection connection = this.dataSource.getConnection()) {
      final PreparedStatement prepared = connection.prepareStatement(sql);
      prepared.setString(1, key);
      prepared.setString(2, property.getValue());
      prepared.setString(3, identity);
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
  public boolean dropIndex(
      @NonNull final PersistenceCollection collection,
      @NonNull final PersistencePath path,
      @NonNull final IndexProperty property) {

    this.checkCollectionRegistered(collection);
    final String indexTable = this.indexTable(collection);
    final String sql = "delete from `" + indexTable + "` where `property` = ? and `key` = ?";
    final String key = path.getValue();

    try (final Connection connection = this.dataSource.getConnection()) {
      final PreparedStatement prepared = connection.prepareStatement(sql);
      prepared.setString(1, property.getValue());
      prepared.setString(2, key);
      return prepared.executeUpdate() > 0;
    } catch (final SQLException exception) {
      throw new RuntimeException(
          "cannot delete from index " + indexTable + "[" + property.getValue() + "] key = " + key,
          exception);
    }
  }

  @Override
  public boolean dropIndex(
      @NonNull final PersistenceCollection collection, @NonNull final PersistencePath path) {

    this.checkCollectionRegistered(collection);
    final String indexTable = this.indexTable(collection);
    final String sql = "delete from `" + indexTable + "` where `key` = ?";
    final String key = path.getValue();

    try (final Connection connection = this.dataSource.getConnection()) {
      final PreparedStatement prepared = connection.prepareStatement(sql);
      prepared.setString(1, key);
      return prepared.executeUpdate() > 0;
    } catch (final SQLException exception) {
      throw new RuntimeException(
          "cannot delete from index " + indexTable + " key = " + key, exception);
    }
  }

  @Override
  public boolean dropIndex(
      @NonNull final PersistenceCollection collection, @NonNull final IndexProperty property) {

    this.checkCollectionRegistered(collection);
    final String indexTable = this.indexTable(collection);
    final String sql = "delete from `" + indexTable + "` where `property` = ?";

    try (final Connection connection = this.dataSource.getConnection()) {
      final PreparedStatement prepared = connection.prepareStatement(sql);
      prepared.setString(1, property.getValue());
      return prepared.executeUpdate() > 0;
    } catch (final SQLException exception) {
      throw new RuntimeException("cannot truncate " + indexTable, exception);
    }
  }

  @Override
  public Optional<String> read(
      @NonNull final PersistenceCollection collection, @NonNull final PersistencePath path) {

    this.checkCollectionRegistered(collection);
    final String sql =
        "select `value` from `" + this.table(collection) + "` where `key` = ? limit 1";

    try (final Connection connection = this.dataSource.getConnection()) {
      final PreparedStatement prepared = connection.prepareStatement(sql);
      prepared.setString(1, path.getValue());
      final ResultSet resultSet = prepared.executeQuery();
      if (resultSet.next()) {
        return Optional.ofNullable(resultSet.getString("value"));
      }
    } catch (final SQLException exception) {
      throw new RuntimeException("cannot read " + path + " from " + collection, exception);
    }

    return Optional.empty();
  }

  @Override
  public Map<PersistencePath, String> read(
      @NonNull final PersistenceCollection collection,
      @NonNull final Collection<PersistencePath> paths) {

    this.checkCollectionRegistered(collection);
    final String keys = paths.stream().map(path -> "`key` = ?").collect(Collectors.joining(" or "));
    final String sql = "select `key`, `value` from `" + this.table(collection) + "` where " + keys;
    final Map<PersistencePath, String> map = new LinkedHashMap<>();

    try (final Connection connection = this.dataSource.getConnection()) {
      final PreparedStatement prepared = connection.prepareStatement(sql);
      int currentIndex = 1;
      for (final PersistencePath path : paths) {
        prepared.setString(currentIndex++, path.getValue());
      }
      final ResultSet resultSet = prepared.executeQuery();
      while (resultSet.next()) {
        final String key = resultSet.getString("key");
        final String value = resultSet.getString("value");
        map.put(PersistencePath.of(key), value);
      }
    } catch (final SQLException exception) {
      throw new RuntimeException("cannot read " + paths + " from " + collection, exception);
    }

    return map;
  }

  @Override
  public Map<PersistencePath, String> readAll(@NonNull final PersistenceCollection collection) {

    this.checkCollectionRegistered(collection);
    final String sql = "select `key`, `value` from `" + this.table(collection) + "`";
    final Map<PersistencePath, String> map = new LinkedHashMap<>();

    try (final Connection connection = this.dataSource.getConnection()) {

      final PreparedStatement prepared = connection.prepareStatement(sql);
      final ResultSet resultSet = prepared.executeQuery();

      while (resultSet.next()) {
        final String key = resultSet.getString("key");
        final String value = resultSet.getString("value");
        map.put(PersistencePath.of(key), value);
      }
    } catch (final SQLException exception) {
      throw new RuntimeException("cannot read all from " + collection, exception);
    }

    return map;
  }

  @Override
  public Stream<PersistenceEntity<String>> readByProperty(
      @NonNull final PersistenceCollection collection,
      @NonNull final PersistencePath property,
      @NonNull final Object propertyValue) {
    return this.isIndexed(collection, property)
        ? this.readByPropertyIndexed(
            collection, IndexProperty.of(property.getValue()), propertyValue)
        : this.streamAll(collection);
  }

  protected Stream<PersistenceEntity<String>> readByPropertyIndexed(
      @NonNull final PersistenceCollection collection,
      @NonNull final IndexProperty indexProperty,
      @NonNull final Object propertyValue) {

    if (!this.canUseToString(propertyValue)) {
      return this.streamAll(collection);
    }

    this.checkCollectionRegistered(collection);
    final String table = this.table(collection);
    final String indexTable = this.indexTable(collection);

    final String sql =
        "select indexer.`key`, `value` from `"
            + table
            + "`"
            + " join `"
            + indexTable
            + "` indexer on `"
            + table
            + "`.`key` = indexer.`key`"
            + " where indexer.`property` = ? and indexer.`identity` = ?";

    try (final Connection connection = this.dataSource.getConnection()) {

      final PreparedStatement prepared = connection.prepareStatement(sql);
      prepared.setString(1, indexProperty.getValue());
      prepared.setString(2, String.valueOf(propertyValue));
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
  public Stream<PersistenceEntity<String>> streamAll(
      @NonNull final PersistenceCollection collection) {

    this.checkCollectionRegistered(collection);
    final String sql = "select `key`, `value` from `" + this.table(collection) + "`";

    try (final Connection connection = this.dataSource.getConnection()) {

      final PreparedStatement prepared = connection.prepareStatement(sql);
      final ResultSet resultSet = prepared.executeQuery();
      final List<PersistenceEntity<String>> results = new ArrayList<>();

      while (resultSet.next()) {
        final String key = resultSet.getString("key");
        final String value = resultSet.getString("value");
        results.add(new PersistenceEntity<>(PersistencePath.of(key), value));
      }

      return results.stream();
    } catch (final SQLException exception) {
      throw new RuntimeException("cannot stream all from " + collection, exception);
    }
  }

  @Override
  public long count(@NonNull final PersistenceCollection collection) {

    this.checkCollectionRegistered(collection);
    final String sql = "select count(1) from `" + this.table(collection) + "`";

    try (final Connection connection = this.dataSource.getConnection()) {
      final PreparedStatement prepared = connection.prepareStatement(sql);
      final ResultSet resultSet = prepared.executeQuery();
      if (resultSet.next()) {
        return resultSet.getLong(1);
      }
    } catch (final SQLException exception) {
      throw new RuntimeException("cannot count " + collection, exception);
    }

    return 0;
  }

  @Override
  public boolean exists(
      @NonNull final PersistenceCollection collection, @NonNull final PersistencePath path) {

    this.checkCollectionRegistered(collection);
    final String sql = "select 1 from `" + this.table(collection) + "` where `key` = ? limit 1";

    try (final Connection connection = this.dataSource.getConnection()) {
      final PreparedStatement prepared = connection.prepareStatement(sql);
      prepared.setString(1, path.getValue());
      final ResultSet resultSet = prepared.executeQuery();
      return resultSet.next();
    } catch (final SQLException exception) {
      throw new RuntimeException("cannot check if " + path + " exists in " + collection, exception);
    }
  }

  @Override
  public boolean write(
      @NonNull final PersistenceCollection collection,
      @NonNull final PersistencePath path,
      @NonNull final String raw) {

    if (this.read(collection, path).isPresent()) {
      final String sql = "update `" + this.table(collection) + "` set `value` = ? where `key` = ?";
      try (final Connection connection = this.dataSource.getConnection()) {
        final PreparedStatement prepared = connection.prepareStatement(sql);
        prepared.setString(1, raw);
        prepared.setString(2, path.getValue());
        return prepared.executeUpdate() > 0;
      } catch (final SQLException exception) {
        throw new RuntimeException("cannot write " + path + " to " + collection, exception);
      }
    }

    final String sql =
        "insert into `" + this.table(collection) + "` (`key`, `value`) values (?, ?)";
    try (final Connection connection = this.dataSource.getConnection()) {
      final PreparedStatement prepared = connection.prepareStatement(sql);
      prepared.setString(1, path.getValue());
      prepared.setString(2, raw);
      return prepared.executeUpdate() > 0;
    } catch (final SQLException exception) {
      throw new RuntimeException("cannot write " + path + " to " + collection, exception);
    }
  }

  @Override
  public boolean delete(
      @NonNull final PersistenceCollection collection, @NonNull final PersistencePath path) {

    this.checkCollectionRegistered(collection);
    final String sql = "delete from `" + this.table(collection) + "` where `key` = ?";
    final String key = path.getValue();

    final Set<IndexProperty> collectionIndexes = this.getKnownIndexes().get(collection.getValue());
    if (collectionIndexes != null) {
      collectionIndexes.forEach(index -> this.dropIndex(collection, path));
    }

    try (final Connection connection = this.dataSource.getConnection()) {
      final PreparedStatement prepared = connection.prepareStatement(sql);
      prepared.setString(1, key);
      return prepared.executeUpdate() > 0;
    } catch (final SQLException exception) {
      throw new RuntimeException("cannot delete " + path + " from " + collection, exception);
    }
  }

  @Override
  public long delete(
      @NonNull final PersistenceCollection collection,
      @NonNull final Collection<PersistencePath> paths) {

    this.checkCollectionRegistered(collection);
    if (paths.isEmpty()) {
      return 0;
    }

    this.checkCollectionRegistered(collection);
    final String keys = paths.stream().map(path -> "`key` = ?").collect(Collectors.joining(" or "));
    final String deleteSql = "delete from `" + this.table(collection) + "` where " + keys;
    final String deleteIndexSql = "delete from `" + this.indexTable(collection) + "` where " + keys;

    try (final Connection connection = this.dataSource.getConnection()) {
      final PreparedStatement prepared = connection.prepareStatement(deleteIndexSql);
      int currentIndex = 1;
      for (final PersistencePath path : paths) {
        prepared.setString(currentIndex++, path.getValue());
      }
      prepared.executeUpdate();
    } catch (final SQLException exception) {
      throw new RuntimeException("cannot delete " + paths + " from " + collection, exception);
    }

    try (final Connection connection = this.dataSource.getConnection()) {
      final PreparedStatement prepared = connection.prepareStatement(deleteSql);
      int currentIndex = 1;
      for (final PersistencePath path : paths) {
        prepared.setString(currentIndex++, path.getValue());
      }
      return prepared.executeUpdate();
    } catch (final SQLException exception) {
      throw new RuntimeException("cannot delete " + paths + " from " + collection, exception);
    }
  }

  @Override
  public boolean deleteAll(@NonNull final PersistenceCollection collection) {

    this.checkCollectionRegistered(collection);
    final String sql = "truncate table `" + this.table(collection) + "`";
    final String indexSql = "truncate table `" + this.indexTable(collection) + "`";

    try (final Connection connection = this.dataSource.getConnection()) {
      final PreparedStatement prepared = connection.prepareStatement(indexSql);
      prepared.executeUpdate();
    } catch (final SQLException exception) {
      throw new RuntimeException("cannot truncate " + collection, exception);
    }

    try (final Connection connection = this.dataSource.getConnection()) {
      final PreparedStatement prepared = connection.prepareStatement(sql);
      prepared.executeUpdate();
      return true;
    } catch (final SQLException exception) {
      throw new RuntimeException("cannot truncate " + collection, exception);
    }
  }

  @Override
  public long deleteAll() {
    return this.getKnownCollections().values().stream()
        .map(this::deleteAll)
        .filter(Predicate.isEqual(true))
        .count();
  }

  @Override
  public void close() throws IOException {
    this.dataSource.close();
  }

  protected String table(final PersistenceCollection collection) {
    return this.getBasePath().sub(collection).toSqlIdentifier();
  }

  protected String indexTable(final PersistenceCollection collection) {
    return this.getBasePath().sub(collection).sub("index").toSqlIdentifier();
  }
}
