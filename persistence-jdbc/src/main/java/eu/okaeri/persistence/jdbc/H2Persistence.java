package eu.okaeri.persistence.jdbc;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import eu.okaeri.persistence.PersistenceCollection;
import eu.okaeri.persistence.PersistenceEntity;
import eu.okaeri.persistence.PersistencePath;
import eu.okaeri.persistence.filter.DeleteFilter;
import eu.okaeri.persistence.filter.FindFilter;
import eu.okaeri.persistence.jdbc.filter.H2FilterRenderer;
import eu.okaeri.persistence.jdbc.filter.SqlStringRenderer;
import eu.okaeri.persistence.raw.PersistenceIndexMode;
import eu.okaeri.persistence.raw.PersistencePropertyMode;
import lombok.NonNull;

import java.sql.*;
import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class H2Persistence extends JdbcPersistence {

    private final H2FilterRenderer filterRenderer;

    public H2Persistence(@NonNull PersistencePath basePath, @NonNull HikariConfig hikariConfig) {
        super(basePath, hikariConfig, PersistencePropertyMode.NATIVE, PersistenceIndexMode.NONE);
        this.filterRenderer = new H2FilterRenderer(new SqlStringRenderer());
    }

    public H2Persistence(@NonNull PersistencePath basePath, @NonNull HikariDataSource dataSource) {
        super(basePath, dataSource, PersistencePropertyMode.NATIVE, PersistenceIndexMode.NONE);
        this.filterRenderer = new H2FilterRenderer(new SqlStringRenderer());
    }

    @Override
    public void registerCollection(@NonNull PersistenceCollection collection) {

        String tableName = this.table(collection);
        int keyLength = collection.getKeyLength();

        // Create table if not exists (don't call super - we handle everything here)
        String createSql = "create table if not exists `" + tableName + "` (" +
            "`key` varchar(" + keyLength + ") primary key not null," +
            "`value` JSON not null)";

        try (Connection connection = this.getDataSource().getConnection()) {
            connection.createStatement().execute(createSql);
        } catch (SQLException exception) {
            throw new RuntimeException("cannot create collection table " + tableName, exception);
        }

        // Migrate TEXT column to JSON type for H2 field reference support (legacy migration)
        this.migrateValueColumnToJson(tableName);

        // Drop legacy emulated index table if exists
        this.dropLegacyIndexTable(collection);

        // Register in known collections (from RawPersistence)
        // Note: H2 doesn't benefit from indexes on virtual generated columns, so we skip index creation
        this.getKnownCollections().put(collection.getValue(), collection);
    }

    private void migrateValueColumnToJson(@NonNull String tableName) {
        try (Connection connection = this.getDataSource().getConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            ResultSet columns = metaData.getColumns(null, null, tableName.toUpperCase(), "VALUE");

            if (columns.next()) {
                String columnType = columns.getString("TYPE_NAME");
                if ("VARCHAR".equalsIgnoreCase(columnType) ||
                    "TEXT".equalsIgnoreCase(columnType) ||
                    "CLOB".equalsIgnoreCase(columnType) ||
                    "CHARACTER LARGE OBJECT".equalsIgnoreCase(columnType) ||
                    "CHARACTER VARYING".equalsIgnoreCase(columnType)) {

                    String migrateSql = "alter table `" + tableName + "` alter column `value` JSON";
                    connection.createStatement().execute(migrateSql);
                }
            }
            columns.close();
        } catch (SQLException exception) {
            throw new RuntimeException("cannot migrate collection " + tableName + " to JSON type", exception);
        }
    }

    private void dropLegacyIndexTable(@NonNull PersistenceCollection collection) {
        String indexTable = this.indexTable(collection);
        try (Connection connection = this.getDataSource().getConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            ResultSet tables = metaData.getTables(null, null, indexTable.toUpperCase(), new String[]{"TABLE"});
            if (tables.next()) {
                String dropSql = "drop table `" + indexTable + "`";
                connection.createStatement().execute(dropSql);
            }
            tables.close();
        } catch (SQLException exception) {
            throw new RuntimeException("cannot drop legacy index table " + indexTable, exception);
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

        String sql = "select `key`, `value` from `" + this.table(collection) + "`";

        if (filter.getWhere() != null) {
            sql += " where " + this.filterRenderer.renderCondition(filter.getWhere());
        }

        if (filter.hasOrderBy()) {
            sql += " order by " + this.filterRenderer.renderOrderBy(filter.getOrderBy());
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
                try (Connection connection = H2Persistence.this.getDataSource().getConnection()) {
                    Statement statement = connection.createStatement();
                    ResultSet resultSet = statement.executeQuery(H2Persistence.this.debugQuery(sql));
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
                if ((currentBatch == null) || !this.currentBatch.hasNext()) {
                    if (!this.hasMore) return false;
                    this.fetchNextBatch();
                }
                return (currentBatch != null) && this.currentBatch.hasNext();
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

        if (filter.getWhere() == null) {
            throw new IllegalArgumentException("deleteByFilter requires a WHERE condition - use deleteAll() to clear collection");
        }

        String sql = "delete from `" + this.table(collection) + "` where " + this.filterRenderer.renderCondition(filter.getWhere());

        try (Connection connection = this.getDataSource().getConnection()) {
            Statement statement = connection.createStatement();
            return statement.executeUpdate(this.debugQuery(sql));
        } catch (SQLException exception) {
            throw new RuntimeException("cannot delete from " + collection + " with " + filter, exception);
        }
    }
}
