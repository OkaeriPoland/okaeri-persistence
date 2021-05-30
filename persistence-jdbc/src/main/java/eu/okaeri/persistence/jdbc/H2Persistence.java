package eu.okaeri.persistence.jdbc;

import com.zaxxer.hikari.HikariConfig;
import eu.okaeri.persistence.PersistenceCollection;
import eu.okaeri.persistence.PersistenceEntity;
import eu.okaeri.persistence.PersistencePath;
import eu.okaeri.persistence.document.index.IndexProperty;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class H2Persistence extends JdbcPersistence {

    public H2Persistence(PersistencePath basePath, HikariConfig hikariConfig) {
        super(basePath, hikariConfig);
    }

    @Override
    public boolean updateIndex(PersistenceCollection collection, PersistencePath path, IndexProperty property, String identity) {

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
            throw new RuntimeException("cannot update index " + indexTable + "[" + property.getValue() + "] -> " + key + " = " + identity, exception);
        }
    }

    @Override
    public Stream<PersistenceEntity<String>> readByProperty(PersistenceCollection collection, PersistencePath property, Object propertyValue) {
        return this.isIndexed(collection, property)
                ? this.readByPropertyIndexed(collection, IndexProperty.of(property.getValue()), propertyValue)
                : this.readByPropertyInstr(collection, property, propertyValue);
    }

    private Stream<PersistenceEntity<String>> readByPropertyInstr(PersistenceCollection collection, PersistencePath property, Object propertyValue) {

        if (!this.canUseToString(propertyValue)) {
            return this.streamAll(collection);
        }

        this.checkCollectionRegistered(collection);
        String sql = "select `key`, `value` from `" + this.table(collection) + "` where instr(`value`, ?)";

        try (Connection connection = this.getDataSource().getConnection()) {

            PreparedStatement prepared = connection.prepareStatement(sql);
            prepared.setObject(1, String.valueOf(propertyValue));
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
