package eu.okaeri.platform.persistence.jdbc;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import eu.okaeri.platform.persistence.PersistenceCollection;
import eu.okaeri.platform.persistence.PersistenceEntity;
import eu.okaeri.platform.persistence.PersistencePath;
import eu.okaeri.platform.persistence.index.IndexProperty;
import eu.okaeri.platform.persistence.raw.RawPersistence;
import lombok.Getter;
import lombok.SneakyThrows;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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
                "`key` varchar(" + collection.getKeyLength() + ") unique primary key not null," +
                "`value` json not null)" +
                "engine = InnoDB character set = utf8mb4;";

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

        String sql = "create table if not exists `" + indexTable + "` (" +
                "`key` varchar(" + keyLength + ") not null," +
                "`property` varchar(" + propertyLength + ") not null," +
                "`identity` varchar(" + identityLength + ") not null," +
                "primary key(`key`, `property`)," +
                "index (`identity`))" +
                "engine = InnoDB character set = utf8mb4;";

        try (Connection connection = this.dataSource.getConnection()) {
            connection.createStatement().execute(sql);
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
    public boolean updateIndex(PersistenceCollection collection, IndexProperty property, PersistencePath path, String identity) {

        this.checkCollectionRegistered(collection);
        String indexTable = this.indexTable(collection);
        String sql = "insert into `" + indexTable + "` (`key`, `property`, `identity`) values (?, ?, ?) on duplicate key update `identity` = ?";
        String key = path.getValue();

        try (Connection connection = this.dataSource.getConnection()) {
            PreparedStatement prepared = connection.prepareStatement(sql);
            prepared.setString(1, key);
            prepared.setString(2, property.getValue());
            prepared.setString(3, identity);
            prepared.setString(4, identity);
            return prepared.executeUpdate() > 0;
        } catch (SQLException exception) {
            throw new RuntimeException("cannot update index " + indexTable + " -> " + key + " = " + identity, exception);
        }
    }

    @Override
    public boolean dropIndex(PersistenceCollection collection, IndexProperty property, PersistencePath path) {

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
            throw new RuntimeException("cannot delete from index " + indexTable + "property = " + property.getValue() + " , key = " + key, exception);
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
    public String read(PersistenceCollection collection, PersistencePath path) {

        this.checkCollectionRegistered(collection);
        String sql = "select `value` from `" + this.table(collection) + "` where `key` = ? limit 1";

        try (Connection connection = this.dataSource.getConnection()) {
            PreparedStatement prepared = connection.prepareStatement(sql);
            prepared.setString(1, path.getValue());
            ResultSet resultSet = prepared.executeQuery();
            if (resultSet.next()) {
                return resultSet.getString("value");
            }
        } catch (SQLException exception) {
            throw new RuntimeException("cannot read " + path + " from " + collection, exception);
        }

        return null;
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
                : this.readByPropertyJsonExtract(collection, property, propertyValue);
    }

    private Stream<PersistenceEntity<String>> readByPropertyIndexed(PersistenceCollection collection, IndexProperty indexProperty, Object propertyValue) {

        this.checkCollectionRegistered(collection);
        String table = this.table(collection);
        String indexTable = this.indexTable(collection);

        String sql = "select indexer.`key`, `value` from `" + table + "`" +
                " join `" + indexTable + "` indexer on `" + table + "`.`key` = indexer.`key`" +
                " where indexer.`property` = ? and indexer.`identity` = ?"; //  and json_extract(value, ?) = ? // double checking ???

        try (Connection connection = this.dataSource.getConnection()) {

            PreparedStatement prepared = connection.prepareStatement(sql);
            prepared.setString(1, indexProperty.getValue());
            prepared.setObject(2, propertyValue);
//            prepared.setObject(3, indexProperty.toSqlJsonPath());
//            prepared.setObject(4, propertyValue);
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

    private Stream<PersistenceEntity<String>> readByPropertyJsonExtract(PersistenceCollection collection, PersistencePath property, Object propertyValue) {

        this.checkCollectionRegistered(collection);
        String sql = "select `key`, `value` from `" + this.table(collection) + "` where json_extract(value, ?) = ?";

        try (Connection connection = this.dataSource.getConnection()) {

            PreparedStatement prepared = connection.prepareStatement(sql);
            prepared.setString(1, property.toSqlJsonPath());
            prepared.setObject(2, propertyValue);
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

        this.checkCollectionRegistered(collection);
        String sql = "insert into `" + this.table(collection) + "` (`key`, `value`) values (?, ?) on duplicate key update `value` = ?";

        try (Connection connection = this.dataSource.getConnection()) {
            PreparedStatement prepared = connection.prepareStatement(sql);
            prepared.setString(1, path.getValue());
            prepared.setString(2, raw);
            prepared.setString(3, raw);
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

        Set<IndexProperty> collectionIndexes = this.getKnownIndexes().get(collection);
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
    public boolean deleteAll(PersistenceCollection collection) {

        this.checkCollectionRegistered(collection);
        String sql = "truncate table `" + this.table(collection) + "`";

        Set<IndexProperty> collectionIndexes = this.getKnownIndexes().get(collection);
        if (collectionIndexes != null) {
            collectionIndexes.forEach(index -> this.dropIndex(collection, index));
        }

        try (Connection connection = this.dataSource.getConnection()) {
            PreparedStatement prepared = connection.prepareStatement(sql);
            return prepared.executeUpdate() > 0;
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

    private String table(PersistenceCollection collection) {
        return this.getBasePath().sub(collection).toSqlIdentifier();
    }

    private String indexTable(PersistenceCollection collection) {
        return this.getBasePath().sub(collection).sub("index").toSqlIdentifier();
    }
}
