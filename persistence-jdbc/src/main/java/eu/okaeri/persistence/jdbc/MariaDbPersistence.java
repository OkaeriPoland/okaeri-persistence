package eu.okaeri.persistence.jdbc;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import eu.okaeri.persistence.PersistenceCollection;
import eu.okaeri.persistence.PersistenceEntity;
import eu.okaeri.persistence.PersistencePath;
import eu.okaeri.persistence.document.index.IndexProperty;
import eu.okaeri.persistence.filter.DeleteFilter;
import eu.okaeri.persistence.filter.FindFilter;
import eu.okaeri.persistence.filter.UpdateFilter;
import eu.okaeri.persistence.filter.operation.UpdateOperation;
import eu.okaeri.persistence.jdbc.filter.MariaDbFilterRenderer;
import eu.okaeri.persistence.jdbc.filter.MariaDbStringRenderer;
import eu.okaeri.persistence.jdbc.filter.MariaDbUpdateRenderer;
import eu.okaeri.persistence.raw.PersistenceIndexMode;
import eu.okaeri.persistence.raw.PersistencePropertyMode;
import lombok.NonNull;

import java.sql.*;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class MariaDbPersistence extends JdbcPersistence {

    private static final Logger LOGGER = Logger.getLogger(MariaDbPersistence.class.getSimpleName());
    private static final String INDEX_COLUMN_PREFIX = "_idx_";
    private static final MariaDbStringRenderer STRING_RENDERER = new MariaDbStringRenderer();
    private static final MariaDbUpdateRenderer UPDATE_RENDERER = new MariaDbUpdateRenderer(STRING_RENDERER);
    private final MariaDbFilterRenderer filterRenderer;

    public MariaDbPersistence(@NonNull PersistencePath basePath, @NonNull HikariConfig hikariConfig) {
        super(basePath, hikariConfig, PersistencePropertyMode.NATIVE, PersistenceIndexMode.NATIVE);
        this.filterRenderer = new MariaDbFilterRenderer(STRING_RENDERER);
    }

    public MariaDbPersistence(@NonNull PersistencePath basePath, @NonNull HikariDataSource dataSource) {
        super(basePath, dataSource, PersistencePropertyMode.NATIVE, PersistenceIndexMode.NATIVE);
        this.filterRenderer = new MariaDbFilterRenderer(STRING_RENDERER);
    }

    /**
     * Get the generated column name for an index property.
     */
    public static String getIndexColumnName(@NonNull IndexProperty index) {
        return INDEX_COLUMN_PREFIX + index.toSqlIdentifier();
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

        // Drop legacy emulated index table if exists
        this.dropLegacyIndexTable(collection);

        // Manage generated columns for native indexing (like H2)
        this.manageGeneratedColumns(collection);

        // Register in known collections (from RawPersistence)
        this.getKnownCollections().put(collection.getValue(), collection);
        this.getKnownIndexes().put(collection.getValue(), collection.getIndexes());
    }

    private void dropLegacyIndexTable(@NonNull PersistenceCollection collection) {
        String indexTable = this.indexTable(collection);
        try (Connection connection = this.getDataSource().getConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            ResultSet tables = metaData.getTables(null, null, indexTable, new String[]{"TABLE"});
            if (tables.next()) {
                String dropSql = "drop table `" + indexTable + "`";
                connection.createStatement().execute(dropSql);
            }
            tables.close();
        } catch (SQLException exception) {
            throw new RuntimeException("cannot drop legacy index table " + indexTable, exception);
        }
    }

    private void manageGeneratedColumns(@NonNull PersistenceCollection collection) {
        String tableName = this.table(collection);
        Set<IndexProperty> desiredIndexes = collection.getIndexes();

        try (Connection connection = this.getDataSource().getConnection()) {
            // Get existing index columns
            Set<String> existingIndexColumns = this.getExistingIndexColumns(connection, tableName);

            // Get desired column names
            Set<String> desiredColumnNames = new HashSet<>();
            for (IndexProperty index : desiredIndexes) {
                desiredColumnNames.add(getIndexColumnName(index));
            }

            // Remove columns that are no longer needed
            for (String existingCol : existingIndexColumns) {
                if (!desiredColumnNames.contains(existingCol)) {
                    this.dropGeneratedColumn(connection, tableName, existingCol);
                }
            }

            // Add columns for desired indexes and ensure indexes exist
            for (IndexProperty index : desiredIndexes) {
                String columnName = getIndexColumnName(index);
                if (!existingIndexColumns.contains(columnName)) {
                    this.createGeneratedColumn(connection, tableName, index, columnName);
                } else {
                    // Column exists - ensure index exists too
                    this.ensureIndexExists(connection, tableName, columnName);
                }
            }
        } catch (SQLException exception) {
            throw new RuntimeException("cannot manage generated columns for " + tableName, exception);
        }
    }

    private Set<String> getExistingIndexColumns(@NonNull Connection connection, @NonNull String tableName) throws SQLException {
        Set<String> columns = new HashSet<>();
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet rs = metaData.getColumns(null, null, tableName, null);
        while (rs.next()) {
            String columnName = rs.getString("COLUMN_NAME");
            if (columnName.startsWith(INDEX_COLUMN_PREFIX)) {
                columns.add(columnName);
            }
        }
        rs.close();
        return columns;
    }

    private void createGeneratedColumn(@NonNull Connection connection, @NonNull String tableName,
                                       @NonNull IndexProperty index, @NonNull String columnName) throws SQLException {
        String expression = this.buildGeneratedColumnExpression(index);
        String columnType = this.getColumnType(index);

        // MariaDB requires STORED (not VIRTUAL) for indexed generated columns
        String addColumnSql = "alter table `" + tableName + "` add column `" + columnName + "` " +
            columnType + " as (" + expression + ") stored";

        try {
            connection.createStatement().execute(this.debugQuery(addColumnSql));

            // Create index on the generated column
            String indexName = tableName + "_" + columnName + "_idx";
            String createIndexSql = "create index `" + indexName + "` on `" + tableName + "` (`" + columnName + "`)";
            connection.createStatement().execute(this.debugQuery(createIndexSql));

        } catch (SQLException e) {
            LOGGER.warning("Could not create generated column " + columnName + ": " + e.getMessage());
        }
    }

    private void dropGeneratedColumn(@NonNull Connection connection, @NonNull String tableName,
                                     @NonNull String columnName) throws SQLException {
        // Drop index first
        String indexName = tableName + "_" + columnName + "_idx";
        String dropIndexSql = "drop index if exists `" + indexName + "` on `" + tableName + "`";
        try {
            connection.createStatement().execute(dropIndexSql);
        } catch (SQLException e) {
            // Index might not exist - that's okay
        }

        // Drop column
        String dropColumnSql = "alter table `" + tableName + "` drop column if exists `" + columnName + "`";
        connection.createStatement().execute(dropColumnSql);
    }

    private void ensureIndexExists(@NonNull Connection connection, @NonNull String tableName,
                                   @NonNull String columnName) throws SQLException {
        String indexName = tableName + "_" + columnName + "_idx";

        // Check if index exists
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet rs = metaData.getIndexInfo(null, null, tableName, false, false);
        boolean indexExists = false;
        while (rs.next()) {
            String existingIndexName = rs.getString("INDEX_NAME");
            if ((existingIndexName != null) && existingIndexName.equalsIgnoreCase(indexName)) {
                indexExists = true;
                break;
            }
        }
        rs.close();

        if (!indexExists) {
            String createIndexSql = "create index `" + indexName + "` on `" + tableName + "` (`" + columnName + "`)";
            try {
                connection.createStatement().execute(this.debugQuery(createIndexSql));
            } catch (SQLException e) {
                LOGGER.warning("Could not create index " + indexName + ": " + e.getMessage());
            }
        }
    }

    /**
     * Build expression that matches what MariaDbFilterRenderer generates.
     * This ensures the query optimizer can use the index.
     */
    private String buildGeneratedColumnExpression(@NonNull IndexProperty index) {
        String jsonPath = index.toMariaDbJsonPath();

        if (index.isFloatingPoint()) {
            // Match: cast(json_extract(`value`, '$.field') as decimal(20,10))
            return "cast(json_extract(`value`, '" + jsonPath + "') as decimal(20,10))";
        } else if (index.isNumeric()) {
            // Match: cast(json_extract(`value`, '$.field') as signed)
            return "cast(json_extract(`value`, '" + jsonPath + "') as signed)";
        } else if (index.isBoolean()) {
            // Boolean in MariaDB JSON: true/false literals
            // json_unquote returns 'true'/'false' strings for boolean JSON values
            // Match the string comparison approach used by filter renderer
            return "json_unquote(json_extract(`value`, '" + jsonPath + "'))";
        } else {
            // Match: json_unquote(json_extract(`value`, '$.field'))
            return "json_unquote(json_extract(`value`, '" + jsonPath + "'))";
        }
    }

    private String getColumnType(@NonNull IndexProperty index) {
        if (index.isFloatingPoint()) {
            return "decimal(20,10)";
        } else if (index.isNumeric()) {
            return "bigint";
        } else if (index.isBoolean()) {
            // Booleans stored as 'true'/'false' strings via json_unquote
            return "varchar(5)";
        } else {
            return "varchar(" + index.getMaxLength() + ")";
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

        // Set indexed properties context for the renderer to use generated columns
        Set<IndexProperty> indexes = this.getKnownIndexes().get(collection.getValue());
        this.filterRenderer.setIndexedProperties(indexes);

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

        if (filter.getWhere() == null) {
            throw new IllegalArgumentException("deleteByFilter requires a WHERE condition - use deleteAll() to clear collection");
        }

        // Set indexed properties context for the renderer to use generated columns
        Set<IndexProperty> indexes = this.getKnownIndexes().get(collection.getValue());
        this.filterRenderer.setIndexedProperties(indexes);

        String sql = "delete from `" + this.table(collection) + "` where " + this.filterRenderer.renderCondition(filter.getWhere());

        try (Connection connection = this.getDataSource().getConnection()) {
            Statement statement = connection.createStatement();
            return statement.executeUpdate(this.debugQuery(sql));
        } catch (SQLException exception) {
            throw new RuntimeException("cannot delete from " + collection + " with " + filter, exception);
        }
    }

    @Override
    public boolean updateOne(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull List<UpdateOperation> operations) {
        this.checkCollectionRegistered(collection);

        String updateExpr = UPDATE_RENDERER.render(operations);
        String sql = "update `" + this.table(collection) + "` set `value` = " + updateExpr + " where `key` = ?";

        try (Connection connection = this.getDataSource().getConnection()) {
            PreparedStatement prepared = connection.prepareStatement(this.debugQuery(sql));
            prepared.setString(1, path.getValue());
            int rowsAffected = prepared.executeUpdate();
            return rowsAffected > 0;
        } catch (SQLException exception) {
            throw new RuntimeException("cannot update " + path + " in " + collection, exception);
        }
    }

    @Override
    public Optional<String> updateOneAndGet(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull List<UpdateOperation> operations) {
        this.checkCollectionRegistered(collection);

        String lockSql = "select `value` from `" + this.table(collection) + "` where `key` = ? for update";
        String updateExpr = UPDATE_RENDERER.render(operations);
        String updateSql = "update `" + this.table(collection) + "` set `value` = " + updateExpr + " where `key` = ?";
        String selectSql = "select `value` from `" + this.table(collection) + "` where `key` = ?";

        try (Connection connection = this.getDataSource().getConnection()) {
            connection.setAutoCommit(false);

            try {
                // Lock the row first to ensure atomicity
                PreparedStatement lockStmt = connection.prepareStatement(lockSql);
                lockStmt.setString(1, path.getValue());
                ResultSet lockResult = lockStmt.executeQuery();

                if (!lockResult.next()) {
                    connection.rollback();
                    return Optional.empty();
                }
                lockStmt.close();

                // Perform the update on the locked row
                PreparedStatement updateStmt = connection.prepareStatement(this.debugQuery(updateSql));
                updateStmt.setString(1, path.getValue());
                updateStmt.executeUpdate();
                updateStmt.close();

                // Read the updated value
                PreparedStatement selectStmt = connection.prepareStatement(selectSql);
                selectStmt.setString(1, path.getValue());
                ResultSet resultSet = selectStmt.executeQuery();

                if (resultSet.next()) {
                    String value = resultSet.getString("value");
                    selectStmt.close();
                    connection.commit();
                    return Optional.ofNullable(value);
                }

                selectStmt.close();
                connection.commit();
                return Optional.empty();
            } catch (SQLException e) {
                connection.rollback();
                throw e;
            }
        } catch (SQLException exception) {
            throw new RuntimeException("cannot update and get " + path + " in " + collection, exception);
        }
    }

    @Override
    public Optional<String> getAndUpdateOne(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull List<UpdateOperation> operations) {
        this.checkCollectionRegistered(collection);

        String selectSql = "select `value` from `" + this.table(collection) + "` where `key` = ? for update";
        String updateExpr = UPDATE_RENDERER.render(operations);
        String updateSql = "update `" + this.table(collection) + "` set `value` = " + updateExpr + " where `key` = ?";

        try (Connection connection = this.getDataSource().getConnection()) {
            connection.setAutoCommit(false);

            try {
                // Lock and read the current value
                PreparedStatement selectStmt = connection.prepareStatement(selectSql);
                selectStmt.setString(1, path.getValue());
                ResultSet resultSet = selectStmt.executeQuery();

                if (!resultSet.next()) {
                    connection.rollback();
                    return Optional.empty();
                }

                String currentValue = resultSet.getString("value");
                selectStmt.close();

                // Perform the update
                PreparedStatement updateStmt = connection.prepareStatement(this.debugQuery(updateSql));
                updateStmt.setString(1, path.getValue());
                updateStmt.executeUpdate();
                updateStmt.close();

                connection.commit();
                return Optional.of(currentValue);
            } catch (SQLException e) {
                connection.rollback();
                throw e;
            }
        } catch (SQLException exception) {
            throw new RuntimeException("cannot get and update " + path + " in " + collection, exception);
        }
    }

    @Override
    public long update(@NonNull PersistenceCollection collection, @NonNull UpdateFilter filter) {
        this.checkCollectionRegistered(collection);

        if (filter.getWhere() == null) {
            throw new IllegalArgumentException("update requires a WHERE condition - use updateOne() for single document updates");
        }

        // Set indexed properties context for the renderer to use generated columns
        Set<IndexProperty> indexes = this.getKnownIndexes().get(collection.getValue());
        this.filterRenderer.setIndexedProperties(indexes);

        String updateExpr = UPDATE_RENDERER.render(filter.getOperations());
        String sql = "update `" + this.table(collection) + "` set `value` = " + updateExpr +
            " where " + this.filterRenderer.renderCondition(filter.getWhere());

        try (Connection connection = this.getDataSource().getConnection()) {
            Statement statement = connection.createStatement();
            return statement.executeUpdate(this.debugQuery(sql));
        } catch (SQLException exception) {
            throw new RuntimeException("cannot update " + collection + " with " + filter, exception);
        }
    }
}
