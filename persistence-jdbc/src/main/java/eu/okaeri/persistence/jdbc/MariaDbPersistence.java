package eu.okaeri.persistence.jdbc;

import com.zaxxer.hikari.HikariConfig;
import eu.okaeri.persistence.PersistenceCollection;
import eu.okaeri.persistence.PersistenceEntity;
import eu.okaeri.persistence.PersistencePath;
import eu.okaeri.persistence.index.IndexProperty;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class MariaDbPersistence extends JdbcPersistence {

    public MariaDbPersistence(PersistencePath basePath, HikariConfig hikariConfig) {
        super(basePath, hikariConfig);
    }

    @Override
    public boolean updateIndex(PersistenceCollection collection, IndexProperty property, PersistencePath path, String identity) {

        this.checkCollectionRegistered(collection);
        String indexTable = this.indexTable(collection);
        String sql = "insert into `" + indexTable + "` (`key`, `property`, `identity`) values (?, ?, ?) on duplicate key update `identity` = ?";
        String key = path.getValue();

        try (Connection connection = this.getDataSource().getConnection()) {
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
    public void registerCollection(PersistenceCollection collection) {

        String sql = "create table if not exists `" + this.table(collection) + "` (" +
                "`key` varchar(" + collection.getKeyLength() + ") primary key not null," +
                "`value` json not null)" +
                "engine = InnoDB character set = utf8mb4;";

        try (Connection connection = this.getDataSource().getConnection()) {
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

        try (Connection connection = this.getDataSource().getConnection()) {
            connection.createStatement().execute(sql);
        } catch (SQLException exception) {
            throw new RuntimeException("cannot register collection", exception);
        }
    }

    @Override
    public Stream<PersistenceEntity<String>> readByProperty(PersistenceCollection collection, PersistencePath property, Object propertyValue) {
        return this.isIndexed(collection, property)
                ? this.readByPropertyIndexed(collection, IndexProperty.of(property.getValue()), propertyValue)
                : this.readByPropertyJsonExtract(collection, property, propertyValue);
    }

    private Stream<PersistenceEntity<String>> readByPropertyJsonExtract(PersistenceCollection collection, PersistencePath property, Object propertyValue) {

        this.checkCollectionRegistered(collection);
        String sql = "select `key`, `value` from `" + this.table(collection) + "` where json_extract(`value`, ?) = ?";

        try (Connection connection = this.getDataSource().getConnection()) {

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
    public boolean write(PersistenceCollection collection, PersistencePath path, String raw) {

        this.checkCollectionRegistered(collection);
        String sql = "insert into `" + this.table(collection) + "` (`key`, `value`) values (?, ?) on duplicate key update `value` = ?";

        try (Connection connection = this.getDataSource().getConnection()) {
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
    public long write(PersistenceCollection collection, Map<PersistencePath, String> entities) {

        this.checkCollectionRegistered(collection);
        String sql = "insert into `" + this.table(collection) + "` (`key`, `value`) values (?, ?) on duplicate key update `value` = ?";

        try (Connection connection = this.getDataSource().getConnection()) {
            PreparedStatement prepared = connection.prepareStatement(sql);
            connection.setAutoCommit(false);
            for (Map.Entry<PersistencePath, String> entry : entities.entrySet()) {
                prepared.setString(1, entry.getKey().getValue());
                prepared.setString(2, entry.getValue());
                prepared.setString(3, entry.getValue());
                prepared.addBatch();
            }
            int changes = prepared.executeUpdate();
            connection.commit();
            return changes;
        } catch (SQLException exception) {
            throw new RuntimeException("cannot write " + entities + " to " + collection, exception);
        }
    }
}
