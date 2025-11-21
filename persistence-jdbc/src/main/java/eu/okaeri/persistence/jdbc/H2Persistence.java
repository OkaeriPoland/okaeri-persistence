package eu.okaeri.persistence.jdbc;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import eu.okaeri.persistence.PersistenceCollection;
import eu.okaeri.persistence.PersistenceEntity;
import eu.okaeri.persistence.PersistencePath;
import eu.okaeri.persistence.document.index.IndexProperty;
import eu.okaeri.persistence.filter.DeleteFilter;
import eu.okaeri.persistence.filter.FindFilter;
import eu.okaeri.persistence.filter.renderer.FilterRenderer;
import eu.okaeri.persistence.jdbc.filter.H2FilterRenderer;
import eu.okaeri.persistence.jdbc.filter.SqlStringRenderer;
import eu.okaeri.persistence.raw.PersistenceIndexMode;
import eu.okaeri.persistence.raw.PersistencePropertyMode;
import lombok.NonNull;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class H2Persistence extends JdbcPersistence {

    private static final FilterRenderer FILTER_RENDERER = new H2FilterRenderer(new SqlStringRenderer());

    public H2Persistence(@NonNull PersistencePath basePath, @NonNull HikariConfig hikariConfig) {
        super(basePath, hikariConfig, PersistencePropertyMode.NATIVE, PersistenceIndexMode.EMULATED);
    }

    public H2Persistence(@NonNull PersistencePath basePath, @NonNull HikariDataSource dataSource) {
        super(basePath, dataSource, PersistencePropertyMode.NATIVE, PersistenceIndexMode.EMULATED);
    }

    @Override
    public void registerCollection(@NonNull PersistenceCollection collection) {
        // First register using parent implementation (creates table if not exists)
        super.registerCollection(collection);

        // Migrate TEXT column to JSON type for H2 field reference support
        String tableName = this.table(collection);
        try (Connection connection = this.getDataSource().getConnection()) {
            // Check if value column is TEXT type (legacy)
            DatabaseMetaData metaData = connection.getMetaData();
            ResultSet columns = metaData.getColumns(null, null, tableName.toUpperCase(), "VALUE");

            if (columns.next()) {
                String columnType = columns.getString("TYPE_NAME");
                // If it's TEXT/VARCHAR/CLOB, migrate to JSON
                if (columnType.equalsIgnoreCase("VARCHAR") ||
                    columnType.equalsIgnoreCase("TEXT") ||
                    columnType.equalsIgnoreCase("CLOB") ||
                    columnType.equalsIgnoreCase("CHARACTER LARGE OBJECT") ||
                    columnType.equalsIgnoreCase("CHARACTER VARYING")) {

                    String migrateSql = "alter table `" + tableName + "` alter column `value` JSON";
                    connection.createStatement().execute(migrateSql);
                }
            }
            columns.close();
        } catch (SQLException exception) {
            throw new RuntimeException("cannot migrate collection " + tableName + " to JSON type", exception);
        }
    }


    @Override
    public boolean updateIndex(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull IndexProperty property, String identity) {

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
    public Stream<PersistenceEntity<String>> readByProperty(@NonNull PersistenceCollection collection, @NonNull PersistencePath property, @NonNull Object propertyValue) {
        return this.isIndexed(collection, property)
            ? this.readByPropertyIndexed(collection, IndexProperty.of(property.getValue()), propertyValue)
            : this.readByPropertyInstr(collection, property, propertyValue);
    }

    private Stream<PersistenceEntity<String>> readByPropertyInstr(@NonNull PersistenceCollection collection, @NonNull PersistencePath property, @NonNull Object propertyValue) {

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

            return results.stream();
        } catch (SQLException exception) {
            throw new RuntimeException("cannot ready by property from " + collection, exception);
        }
    }

    @Override
    public boolean write(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull String raw) {

        this.checkCollectionRegistered(collection);
        String sql = "insert into `" + this.table(collection) + "` (`key`, `value`) values (?, ? format json) on duplicate key update `value` = ? format json";

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
    public long write(@NonNull PersistenceCollection collection, @NonNull Map<PersistencePath, String> entities) {

        if (entities.isEmpty()) {
            return 0;
        }

        this.checkCollectionRegistered(collection);
        String sql = "insert into `" + this.table(collection) + "` (`key`, `value`) values (?, ? format json) on duplicate key update `value` = ? format json";

        try (Connection connection = this.getDataSource().getConnection()) {
            PreparedStatement prepared = connection.prepareStatement(sql);
            connection.setAutoCommit(false);
            for (Map.Entry<PersistencePath, String> entry : entities.entrySet()) {
                prepared.setString(1, entry.getKey().getValue());
                prepared.setString(2, entry.getValue());
                prepared.setString(3, entry.getValue());
                prepared.addBatch();
            }
            int[] results = prepared.executeBatch();
            connection.commit();
            return results.length;
        } catch (SQLException exception) {
            throw new RuntimeException("cannot write " + entities + " to " + collection, exception);
        }
    }

    @Override
    public Stream<PersistenceEntity<String>> readByFilter(@NonNull PersistenceCollection collection, @NonNull FindFilter filter) {

        this.checkCollectionRegistered(collection);
        String sql = "select `key`, `value` from `" + this.table(collection) + "` where " + FILTER_RENDERER.renderCondition(filter.getWhere());

        if (filter.hasOrderBy()) {
            sql += " order by " + FILTER_RENDERER.renderOrderBy(filter.getOrderBy());
        }

        if (filter.hasLimit()) {
            sql += " limit " + filter.getLimit();
        }

        if (filter.hasSkip()) {
            sql += " offset " + filter.getSkip();
        }

        try (Connection connection = this.getDataSource().getConnection()) {

            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            List<PersistenceEntity<String>> results = new ArrayList<>();

            while (resultSet.next()) {
                String key = resultSet.getString("key");
                String value = resultSet.getString("value");
                results.add(new PersistenceEntity<>(PersistencePath.of(key), value));
            }

            return results.stream();
        } catch (SQLException exception) {
            throw new RuntimeException("cannot read by filter from " + collection, exception);
        }
    }

    @Override
    public long deleteByFilter(@NonNull PersistenceCollection collection, @NonNull DeleteFilter filter) {

        this.checkCollectionRegistered(collection);
        String sql = "delete from `" + this.table(collection) + "` where " + FILTER_RENDERER.renderCondition(filter.getWhere());

        try (Connection connection = this.getDataSource().getConnection()) {
            Statement statement = connection.createStatement();
            return statement.executeUpdate(sql);
        } catch (SQLException exception) {
            throw new RuntimeException("cannot delete from " + collection + " with " + filter, exception);
        }
    }
}
