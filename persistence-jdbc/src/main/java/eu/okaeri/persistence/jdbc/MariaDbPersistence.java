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
import eu.okaeri.persistence.jdbc.filter.MariaDbFilterRenderer;
import eu.okaeri.persistence.jdbc.filter.SqlStringRenderer;
import eu.okaeri.persistence.raw.PersistenceIndexMode;
import eu.okaeri.persistence.raw.PersistencePropertyMode;
import lombok.NonNull;

import java.sql.*;
import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class MariaDbPersistence extends JdbcPersistence {

    private static final FilterRenderer FILTER_RENDERER = new MariaDbFilterRenderer(new SqlStringRenderer());

    public MariaDbPersistence(@NonNull PersistencePath basePath, @NonNull HikariConfig hikariConfig) {
        super(basePath, hikariConfig, PersistencePropertyMode.NATIVE, PersistenceIndexMode.EMULATED);
    }

    public MariaDbPersistence(@NonNull PersistencePath basePath, @NonNull HikariDataSource dataSource) {
        super(basePath, dataSource, PersistencePropertyMode.NATIVE, PersistenceIndexMode.EMULATED);
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
            throw new RuntimeException("cannot update index " + indexTable + " -> " + key + " = " + identity, exception);
        }
    }

    @Override
    public void registerCollection(@NonNull PersistenceCollection collection) {

        String collectionTable = this.table(collection);
        int keyLength = collection.getKeyLength();

        String sql = "create table if not exists `" + collectionTable + "` (" +
            "`key` varchar(" + keyLength + ") primary key not null," +
            "`value` json not null)" +
            "engine = InnoDB character set = utf8mb4;";
        String alterKeySql = "alter table `" + collectionTable + "` MODIFY COLUMN `key` varchar(" + keyLength + ") not null";

        try (Connection connection = this.getDataSource().getConnection()) {
            connection.createStatement().execute(sql);
            connection.createStatement().execute(alterKeySql);
        } catch (SQLException exception) {
            throw new RuntimeException("cannot register collection", exception);
        }

        Set<IndexProperty> indexes = collection.getIndexes();
        int identityLength = collection.getMaxIndexIdentityLength();
        int propertyLength = collection.getMaxIndexPropertyLength();
        indexes.forEach(index -> this.registerIndex(collection, index, identityLength, propertyLength));

        super.registerCollection(collection);
    }

    private void registerIndex(@NonNull PersistenceCollection collection, @NonNull IndexProperty property, int identityLength, int propertyLength) {

        int keyLength = collection.getKeyLength();
        String indexTable = this.indexTable(collection);

        String tableSql = "create table if not exists `" + indexTable + "` (" +
            "`key` varchar(" + keyLength + ") not null," +
            "`property` varchar(" + propertyLength + ") not null," +
            "`identity` varchar(" + identityLength + ") not null," +
            "primary key(`key`, `property`)," +
            "index (`identity`)," +
            "index (`property`, `identity`))" +
            "engine = InnoDB character set = utf8mb4;";

        String alterKeySql = "alter table `" + indexTable + "` MODIFY COLUMN `key` varchar(" + keyLength + ") not null";
        String alterPropertySql = "alter table `" + indexTable + "` MODIFY COLUMN `property` varchar(" + propertyLength + ") not null";
        String alterIdentitySql = "alter table `" + indexTable + "` MODIFY COLUMN `identity` varchar(" + identityLength + ") not null";

        try (Connection connection = this.getDataSource().getConnection()) {
            connection.createStatement().execute(tableSql);
            connection.createStatement().execute(alterKeySql);
            connection.createStatement().execute(alterPropertySql);
            connection.createStatement().execute(alterIdentitySql);
        } catch (SQLException exception) {
            throw new RuntimeException("cannot register collection", exception);
        }
    }

    @Override
    public Stream<PersistenceEntity<String>> readByProperty(@NonNull PersistenceCollection collection, @NonNull PersistencePath property, Object propertyValue) {
        return this.isIndexed(collection, property)
            ? this.readByPropertyIndexed(collection, IndexProperty.of(property.getValue()), propertyValue)
            : this.readByPropertyJsonExtract(collection, property, propertyValue);
    }

    private Stream<PersistenceEntity<String>> readByPropertyJsonExtract(@NonNull PersistenceCollection collection, @NonNull PersistencePath property, Object propertyValue) {

        this.checkCollectionRegistered(collection);
        String sql = "select `key`, `value` from `" + this.table(collection) + "` where json_extract(`value`, ?) = ?";

        try (Connection connection = this.getDataSource().getConnection()) {

            PreparedStatement prepared = connection.prepareStatement(sql);
            prepared.setString(1, property.toMariaDbJsonPath());
            prepared.setObject(2, propertyValue);
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
    public long write(@NonNull PersistenceCollection collection, @NonNull Map<PersistencePath, String> entities) {

        if (entities.isEmpty()) {
            return 0;
        }

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
            ResultSet resultSet = statement.executeQuery(this.debugQuery(sql));
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
    public Stream<PersistenceEntity<String>> stream(@NonNull PersistenceCollection collection, int batchSize) {

        this.checkCollectionRegistered(collection);
        String baseQuery = "select `key`, `value` from `" + this.table(collection) + "`";

        // Custom iterator that fetches batches lazily (Java 8 compatible)
        Iterator<PersistenceEntity<String>> iterator = new Iterator<PersistenceEntity<String>>() {
            private int offset = 0;
            private Iterator<PersistenceEntity<String>> currentBatch = null;
            private boolean hasMore = true;

            private void fetchNextBatch() {
                if (!this.hasMore) return;

                String sql = baseQuery + " limit " + batchSize + " offset " + this.offset;
                try (Connection connection = MariaDbPersistence.this.getDataSource().getConnection()) {
                    Statement statement = connection.createStatement();
                    ResultSet resultSet = statement.executeQuery(MariaDbPersistence.this.debugQuery(sql));
                    List<PersistenceEntity<String>> batch = new ArrayList<>();

                    while (resultSet.next()) {
                        String key = resultSet.getString("key");
                        String value = resultSet.getString("value");
                        batch.add(new PersistenceEntity<>(PersistencePath.of(key), value));
                    }

                    if (batch.isEmpty()) {
                        this.hasMore = false;
                        this.currentBatch = null;
                        return;
                    }

                    this.currentBatch = batch.iterator();
                    this.offset += batchSize;

                    // If we got fewer results than requested, this is the last batch
                    if (batch.size() < batchSize) {
                        this.hasMore = false;
                    }
                } catch (SQLException e) {
                    throw new RuntimeException("cannot stream from " + collection, e);
                }
            }

            @Override
            public boolean hasNext() {
                if ((this.currentBatch == null) || !this.currentBatch.hasNext()) {
                    if (!this.hasMore) return false;
                    this.fetchNextBatch();
                }
                return (this.currentBatch != null) && this.currentBatch.hasNext();
            }

            @Override
            public PersistenceEntity<String> next() {
                if (!this.hasNext()) throw new NoSuchElementException();
                return this.currentBatch.next();
            }
        };

        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false);
    }

    @Override
    public long deleteByFilter(@NonNull PersistenceCollection collection, @NonNull DeleteFilter filter) {

        this.checkCollectionRegistered(collection);
        String sql = "delete from `" + this.table(collection) + "` where " + FILTER_RENDERER.renderCondition(filter.getWhere());

        try (Connection connection = this.getDataSource().getConnection()) {
            Statement statement = connection.createStatement();
            return statement.executeUpdate(this.debugQuery(sql));
        } catch (SQLException exception) {
            throw new RuntimeException("cannot delete from " + collection + " with " + filter, exception);
        }
    }
}
