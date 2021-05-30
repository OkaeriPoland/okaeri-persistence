package eu.okaeri.persistence.jdbc;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import eu.okaeri.persistence.PersistenceCollection;
import eu.okaeri.persistence.PersistenceEntity;
import eu.okaeri.persistence.PersistencePath;
import eu.okaeri.persistence.document.index.IndexProperty;
import eu.okaeri.persistence.raw.RawPersistence;
import lombok.Getter;
import lombok.SneakyThrows;

import java.sql.*;
import java.util.*;
import java.util.function.Predicate;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class JdbcPersistence extends RawPersistence {

    private static final Logger LOGGER = Logger.getLogger(JdbcPersistence.class.getName());
    @Getter private HikariDataSource dataSource;

    public JdbcPersistence(PersistencePath basePath, HikariConfig hikariConfig) {
        super(basePath, true, true, true, true);
        this.connect(hikariConfig);
    }

    @SneakyThrows
    private void connect(HikariConfig hikariConfig) {
        do {
            try {
                this.dataSource = new HikariDataSource(hikariConfig);
            } catch (Exception exception) {
                if (exception.getCause() != null) {
                    LOGGER.severe("[" + this.getBasePath().getValue() + "] Cannot connect with database (waiting 30s): " + exception.getMessage() + " caused by " + exception.getCause().getMessage());
                } else {
                    LOGGER.severe("[" + this.getBasePath().getValue() + "] Cannot connect with database (waiting 30s): " + exception.getMessage());
                }
                Thread.sleep(30_000);
            }
        } while (this.dataSource == null);
    }

    @Override
    public void registerCollection(PersistenceCollection collection) {

        String sql = "create table if not exists `" + this.table(collection) + "` (" +
                "`key` varchar(" + collection.getKeyLength() + ") primary key not null," +
                "`value` text not null)";

        try (Connection connection = this.dataSource.getConnection()) {
            connection.createStatement().execute(sql);
        } catch (SQLException exception) {
            throw new RuntimeException("cannot register collection", exception);
        }

        Set<IndexProperty> indexes = collection.getIndexes();
        int identityLength = collection.getMaxIndexIdentityLength();
        int propertyLength = collection.getMaxIndexPropertyLength();
        indexes.forEach(index -> this.registerIndex(collection, index, identityLength, propertyLength));

        super.registerCollection(collection);
    }

    private void registerIndex(PersistenceCollection collection, IndexProperty property, int identityLength, int propertyLength) {

        int keyLength = collection.getKeyLength();
        String indexTable = this.indexTable(collection);

        String tableSql = "create table if not exists `" + indexTable + "` (" +
                "`key` varchar(" + keyLength + ") not null," +
                "`property` varchar(" + propertyLength + ") not null," +
                "`identity` varchar(" + identityLength + ") not null," +
                "primary key(`key`, `property`))";
        String indexSql = "create index `identity` on `" + indexTable + "`(`identity`)";

        try (Connection connection = this.dataSource.getConnection()) {
            connection.createStatement().execute(tableSql);
            try {
                connection.createStatement().execute(indexSql);
            } catch (SQLException ignored) {
                // index already exists or worse cannot be created
                // and we don't know about that. heh
            }
        } catch (SQLException exception) {
            throw new RuntimeException("cannot register collection", exception);
        }
    }

    @Override
    public Set<PersistencePath> findMissingIndexes(PersistenceCollection collection, Set<IndexProperty> indexProperties) {

        if (indexProperties.isEmpty()) {
            return Collections.emptySet();
        }

        String table = this.table(collection);
        String indexTable = this.indexTable(collection);
        Set<PersistencePath> paths = new HashSet<>();

        String params = indexProperties.stream()
                .map(e -> "?")
                .collect(Collectors.joining(", "));

        String sql = "select `key` from `" + table + "` " +
                "where (select count(0) from " + indexTable + " where `key` = `" + table + "`.`key` and `property` in (" + params + ")) != ?";

        try (Connection connection = this.dataSource.getConnection()) {
            PreparedStatement prepared = connection.prepareStatement(sql);
            int currentPrepared = 1;
            for (IndexProperty indexProperty : indexProperties) {
                prepared.setString(currentPrepared++, indexProperty.getValue());
            }
            prepared.setInt(currentPrepared, indexProperties.size());
            ResultSet resultSet = prepared.executeQuery();
            while (resultSet.next()) {
                paths.add(PersistencePath.of(resultSet.getString("key")));
            }
        } catch (SQLException exception) {
            throw new RuntimeException("cannot search missing indexes for " + collection, exception);
        }

        return paths;
    }

    @Override
    public boolean updateIndex(PersistenceCollection collection, PersistencePath path, IndexProperty property, String identity) {

        this.checkCollectionRegistered(collection);
        String indexTable = this.indexTable(collection);
        String key = path.getValue();
        boolean exists;

        try (Connection connection = this.dataSource.getConnection()) {
            String sql = "select count(0) from `" + indexTable + "` where `key` = ? and `property` = ?";
            PreparedStatement prepared = connection.prepareStatement(sql);
            prepared.setString(1, key);
            prepared.setString(2, property.getValue());
            ResultSet resultSet = prepared.executeQuery();
            exists = resultSet.next();
        } catch (SQLException exception) {
            throw new RuntimeException("cannot update index " + indexTable + "[" + property.getValue() + "] -> " + key + " = " + identity, exception);
        }

        if (exists) {
            String sql = "update `" + indexTable + "` set `identity` = ? where `key` = ? and `property` = ?";
            try (Connection connection = this.dataSource.getConnection()) {
                PreparedStatement prepared = connection.prepareStatement(sql);
                prepared.setString(1, identity);
                prepared.setString(2, key);
                prepared.setString(3, property.getValue());
                return prepared.executeUpdate() > 0;
            } catch (SQLException exception) {
                throw new RuntimeException("cannot update index " + indexTable + "[" + property.getValue() + "] -> " + key + " = " + identity, exception);
            }
        }

        String sql = "insert into `" + indexTable + "` (`key`, `property`, `identity`) values (?, ?, ?)";
        try (Connection connection = this.dataSource.getConnection()) {
            PreparedStatement prepared = connection.prepareStatement(sql);
            prepared.setString(1, key);
            prepared.setString(2, property.getValue());
            prepared.setString(3, identity);
            return prepared.executeUpdate() > 0;
        } catch (SQLException exception) {
            throw new RuntimeException("cannot update index " + indexTable + "[" + property.getValue() + "] -> " + key + " = " + identity, exception);
        }
    }

    @Override
    public boolean dropIndex(PersistenceCollection collection, PersistencePath path, IndexProperty property) {

        this.checkCollectionRegistered(collection);
        String indexTable = this.indexTable(collection);
        String sql = "delete from `" + indexTable + "` where `property` = ? and `key` = ?";
        String key = path.getValue();

        try (Connection connection = this.dataSource.getConnection()) {
            PreparedStatement prepared = connection.prepareStatement(sql);
            prepared.setString(1, property.getValue());
            prepared.setString(2, key);
            return prepared.executeUpdate() > 0;
        } catch (SQLException exception) {
            throw new RuntimeException("cannot delete from index " + indexTable + "[" + property.getValue() + "] key = " + key, exception);
        }
    }

    @Override
    public boolean dropIndex(PersistenceCollection collection, PersistencePath path) {

        this.checkCollectionRegistered(collection);
        String indexTable = this.indexTable(collection);
        String sql = "delete from `" + indexTable + "` where `key` = ?";
        String key = path.getValue();

        try (Connection connection = this.dataSource.getConnection()) {
            PreparedStatement prepared = connection.prepareStatement(sql);
            prepared.setString(1, key);
            return prepared.executeUpdate() > 0;
        } catch (SQLException exception) {
            throw new RuntimeException("cannot delete from index " + indexTable + " key = " + key, exception);
        }
    }

    @Override
    public boolean dropIndex(PersistenceCollection collection, IndexProperty property) {

        this.checkCollectionRegistered(collection);
        String indexTable = this.indexTable(collection);
        String sql = "delete from `" + indexTable + "` where `property` = ?";

        try (Connection connection = this.dataSource.getConnection()) {
            PreparedStatement prepared = connection.prepareStatement(sql);
            prepared.setString(1, property.getValue());
            return prepared.executeUpdate() > 0;
        } catch (SQLException exception) {
            throw new RuntimeException("cannot truncate " + indexTable, exception);
        }
    }

    @Override
    public Optional<String> read(PersistenceCollection collection, PersistencePath path) {

        this.checkCollectionRegistered(collection);
        String sql = "select `value` from `" + this.table(collection) + "` where `key` = ? limit 1";

        try (Connection connection = this.dataSource.getConnection()) {
            PreparedStatement prepared = connection.prepareStatement(sql);
            prepared.setString(1, path.getValue());
            ResultSet resultSet = prepared.executeQuery();
            if (resultSet.next()) {
                return Optional.ofNullable(resultSet.getString("value"));
            }
        } catch (SQLException exception) {
            throw new RuntimeException("cannot read " + path + " from " + collection, exception);
        }

        return Optional.empty();
    }

    @Override
    public Map<PersistencePath, String> read(PersistenceCollection collection, Collection<PersistencePath> paths) {

        this.checkCollectionRegistered(collection);
        String keys = paths.stream().map(path -> "`key` = ?").collect(Collectors.joining(" or "));
        String sql = "select `key`, `value` from `" + this.table(collection) + "` where " + keys;
        Map<PersistencePath, String> map = new LinkedHashMap<>();

        try (Connection connection = this.dataSource.getConnection()) {
            PreparedStatement prepared = connection.prepareStatement(sql);
            int currentIndex = 1;
            for (PersistencePath path : paths) {
                prepared.setString(currentIndex++, path.getValue());
            }
            ResultSet resultSet = prepared.executeQuery();
            while (resultSet.next()) {
                String key = resultSet.getString("key");
                String value = resultSet.getString("value");
                map.put(PersistencePath.of(key), value);
            }
        } catch (SQLException exception) {
            throw new RuntimeException("cannot read " + paths + " from " + collection, exception);
        }

        return map;
    }

    @Override
    public Map<PersistencePath, String> readAll(PersistenceCollection collection) {

        this.checkCollectionRegistered(collection);
        String sql = "select `key`, `value` from `" + this.table(collection) + "`";
        Map<PersistencePath, String> map = new LinkedHashMap<>();

        try (Connection connection = this.dataSource.getConnection()) {

            PreparedStatement prepared = connection.prepareStatement(sql);
            ResultSet resultSet = prepared.executeQuery();

            while (resultSet.next()) {
                String key = resultSet.getString("key");
                String value = resultSet.getString("value");
                map.put(PersistencePath.of(key), value);
            }
        } catch (SQLException exception) {
            throw new RuntimeException("cannot read all from " + collection, exception);
        }

        return map;
    }

    @Override
    public Stream<PersistenceEntity<String>> readByProperty(PersistenceCollection collection, PersistencePath property, Object propertyValue) {
        return this.isIndexed(collection, property)
                ? this.readByPropertyIndexed(collection, IndexProperty.of(property.getValue()), propertyValue)
                : this.streamAll(collection);
    }

    protected Stream<PersistenceEntity<String>> readByPropertyIndexed(PersistenceCollection collection, IndexProperty indexProperty, Object propertyValue) {

        if (!this.canUseToString(propertyValue)) {
            return this.streamAll(collection);
        }

        this.checkCollectionRegistered(collection);
        String table = this.table(collection);
        String indexTable = this.indexTable(collection);

        String sql = "select indexer.`key`, `value` from `" + table + "`" +
                " join `" + indexTable + "` indexer on `" + table + "`.`key` = indexer.`key`" +
                " where indexer.`property` = ? and indexer.`identity` = ?";

        try (Connection connection = this.dataSource.getConnection()) {

            PreparedStatement prepared = connection.prepareStatement(sql);
            prepared.setString(1, indexProperty.getValue());
            prepared.setString(2, String.valueOf(propertyValue));
            ResultSet resultSet = prepared.executeQuery();
            List<PersistenceEntity<String>> results = new ArrayList<>();

            while (resultSet.next()) {
                String key = resultSet.getString("key");
                String value = resultSet.getString("value");
                results.add(new PersistenceEntity<>(PersistencePath.of(key), value));
            }

            return StreamSupport.stream(Spliterators.spliterator(results.iterator(), resultSet.getFetchSize(), Spliterator.NONNULL), false);
        } catch (SQLException exception) {
            throw new RuntimeException("cannot ready by property from " + collection, exception);
        }
    }

    @Override
    public Stream<PersistenceEntity<String>> streamAll(PersistenceCollection collection) {

        this.checkCollectionRegistered(collection);
        String sql = "select `key`, `value` from `" + this.table(collection) + "`";

        try (Connection connection = this.dataSource.getConnection()) {

            PreparedStatement prepared = connection.prepareStatement(sql);
            ResultSet resultSet = prepared.executeQuery();
            List<PersistenceEntity<String>> results = new ArrayList<>();

            while (resultSet.next()) {
                String key = resultSet.getString("key");
                String value = resultSet.getString("value");
                results.add(new PersistenceEntity<>(PersistencePath.of(key), value));
            }

            return StreamSupport.stream(Spliterators.spliterator(results.iterator(), resultSet.getFetchSize(), Spliterator.NONNULL), false);
        } catch (SQLException exception) {
            throw new RuntimeException("cannot stream all from " + collection, exception);
        }
    }

    @Override
    public long count(PersistenceCollection collection) {

        this.checkCollectionRegistered(collection);
        String sql = "select count(0) from `" + this.table(collection) + "`";

        try (Connection connection = this.dataSource.getConnection()) {
            PreparedStatement prepared = connection.prepareStatement(sql);
            ResultSet resultSet = prepared.executeQuery();
            if (resultSet.next()) {
                return resultSet.getLong(1);
            }
        } catch (SQLException exception) {
            throw new RuntimeException("cannot count " + collection, exception);
        }

        return 0;
    }

    @Override
    public boolean exists(PersistenceCollection collection, PersistencePath path) {

        this.checkCollectionRegistered(collection);
        String sql = "select 1 from `" + this.table(collection) + "` where `key` = ? limit 1";

        try (Connection connection = this.dataSource.getConnection()) {
            PreparedStatement prepared = connection.prepareStatement(sql);
            prepared.setString(1, path.getValue());
            ResultSet resultSet = prepared.executeQuery();
            return resultSet.next();
        } catch (SQLException exception) {
            throw new RuntimeException("cannot check if " + path + " exists in " + collection, exception);
        }
    }

    @Override
    public boolean write(PersistenceCollection collection, PersistencePath path, String raw) {

        if (this.read(collection, path).isPresent()) {
            String sql = "update `" + this.table(collection) + "` set `value` = ? where `key` = ?";
            try (Connection connection = this.dataSource.getConnection()) {
                PreparedStatement prepared = connection.prepareStatement(sql);
                prepared.setString(1, raw);
                prepared.setString(2, path.getValue());
                return prepared.executeUpdate() > 0;
            } catch (SQLException exception) {
                throw new RuntimeException("cannot write " + path + " to " + collection, exception);
            }
        }

        String sql = "insert into `" + this.table(collection) + "` (`key`, `value`) values (?, ?)";
        try (Connection connection = this.dataSource.getConnection()) {
            PreparedStatement prepared = connection.prepareStatement(sql);
            prepared.setString(1, path.getValue());
            prepared.setString(2, raw);
            return prepared.executeUpdate() > 0;
        } catch (SQLException exception) {
            throw new RuntimeException("cannot write " + path + " to " + collection, exception);
        }
    }

    @Override
    public boolean delete(PersistenceCollection collection, PersistencePath path) {

        this.checkCollectionRegistered(collection);
        String sql = "delete from `" + this.table(collection) + "` where `key` = ?";
        String key = path.getValue();

        Set<IndexProperty> collectionIndexes = this.getKnownIndexes().get(collection.getValue());
        if (collectionIndexes != null) {
            collectionIndexes.forEach(index -> this.dropIndex(collection, path));
        }

        try (Connection connection = this.dataSource.getConnection()) {
            PreparedStatement prepared = connection.prepareStatement(sql);
            prepared.setString(1, key);
            return prepared.executeUpdate() > 0;
        } catch (SQLException exception) {
            throw new RuntimeException("cannot delete " + path + " from " + collection, exception);
        }
    }

    @Override
    public long delete(PersistenceCollection collection, Collection<PersistencePath> paths) {

        this.checkCollectionRegistered(collection);
        if (paths.isEmpty()) {
            return 0;
        }

        this.checkCollectionRegistered(collection);
        String keys = paths.stream().map(path -> "`key` = ?").collect(Collectors.joining(" or "));
        String deleteSql = "delete from `" + this.table(collection) + "` where " + keys;
        String deleteIndexSql = "delete from `" + this.indexTable(collection) + "` where " + keys;

        try (Connection connection = this.dataSource.getConnection()) {
            PreparedStatement prepared = connection.prepareStatement(deleteIndexSql);
            int currentIndex = 1;
            for (PersistencePath path : paths) {
                prepared.setString(currentIndex++, path.getValue());
            }
            prepared.executeUpdate();
        } catch (SQLException exception) {
            throw new RuntimeException("cannot delete " + paths + " from " + collection, exception);
        }

        try (Connection connection = this.dataSource.getConnection()) {
            PreparedStatement prepared = connection.prepareStatement(deleteSql);
            int currentIndex = 1;
            for (PersistencePath path : paths) {
                prepared.setString(currentIndex++, path.getValue());
            }
            return prepared.executeUpdate();
        } catch (SQLException exception) {
            throw new RuntimeException("cannot delete " + paths + " from " + collection, exception);
        }
    }

    @Override
    public boolean deleteAll(PersistenceCollection collection) {

        this.checkCollectionRegistered(collection);
        String sql = "truncate table `" + this.table(collection) + "`";
        String indexSql = "truncate table `" + this.indexTable(collection) + "`";

        try (Connection connection = this.dataSource.getConnection()) {
            PreparedStatement prepared = connection.prepareStatement(indexSql);
            prepared.executeUpdate();
        } catch (SQLException exception) {
            throw new RuntimeException("cannot truncate " + collection, exception);
        }

        try (Connection connection = this.dataSource.getConnection()) {
            PreparedStatement prepared = connection.prepareStatement(sql);
            prepared.executeUpdate();
            return true;
        } catch (SQLException exception) {
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

    protected String table(PersistenceCollection collection) {
        return this.getBasePath().sub(collection).toSqlIdentifier();
    }

    protected String indexTable(PersistenceCollection collection) {
        return this.getBasePath().sub(collection).sub("index").toSqlIdentifier();
    }
}
