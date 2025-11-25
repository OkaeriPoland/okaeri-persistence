package eu.okaeri.persistence.jdbc;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import eu.okaeri.persistence.PersistenceCollection;
import eu.okaeri.persistence.PersistenceEntity;
import eu.okaeri.persistence.PersistencePath;
import eu.okaeri.persistence.document.index.IndexProperty;
import eu.okaeri.persistence.filter.DeleteFilter;
import eu.okaeri.persistence.filter.FindFilter;
import eu.okaeri.persistence.jdbc.filter.H2FilterRenderer;
import eu.okaeri.persistence.jdbc.filter.SqlStringRenderer;
import eu.okaeri.persistence.raw.PersistenceIndexMode;
import eu.okaeri.persistence.raw.PersistencePropertyMode;
import lombok.NonNull;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class H2Persistence extends JdbcPersistence {

    private static final String INDEX_COLUMN_PREFIX = "_idx_";
    private final H2FilterRenderer filterRenderer;

    public H2Persistence(@NonNull PersistencePath basePath, @NonNull HikariConfig hikariConfig) {
        super(basePath, hikariConfig, PersistencePropertyMode.NATIVE, PersistenceIndexMode.NATIVE);
        this.filterRenderer = new H2FilterRenderer(new SqlStringRenderer());
    }

    public H2Persistence(@NonNull PersistencePath basePath, @NonNull HikariDataSource dataSource) {
        super(basePath, dataSource, PersistencePropertyMode.NATIVE, PersistenceIndexMode.NATIVE);
        this.filterRenderer = new H2FilterRenderer(new SqlStringRenderer());
    }

    /**
     * Get the generated column name for an index property.
     */
    public static String getIndexColumnName(@NonNull IndexProperty index) {
        return INDEX_COLUMN_PREFIX + index.toSqlIdentifier();
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

        // Manage generated columns for native indexing
        this.manageGeneratedColumns(collection);

        // Register in known collections (from RawPersistence)
        this.getKnownCollections().put(collection.getValue(), collection);
        this.getKnownIndexes().put(collection.getValue(), collection.getIndexes());
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

    private void manageGeneratedColumns(@NonNull PersistenceCollection collection) {
        String tableName = this.table(collection);
        Set<IndexProperty> desiredIndexes = collection.getIndexes();

        try (Connection connection = this.getDataSource().getConnection()) {
            // Get existing index columns
            Set<String> existingIndexColumns = this.getExistingIndexColumns(connection, tableName);

            // Get desired column names
            Set<String> desiredColumnNames = desiredIndexes.stream()
                .map(H2Persistence::getIndexColumnName)
                .collect(Collectors.toSet());

            // Remove columns that are no longer needed
            for (String existingCol : existingIndexColumns) {
                if (!desiredColumnNames.contains(existingCol)) {
                    this.dropGeneratedColumn(connection, tableName, existingCol);
                }
            }

            // Add or update columns for desired indexes
            for (IndexProperty index : desiredIndexes) {
                String columnName = getIndexColumnName(index);
                if (existingIndexColumns.contains(columnName)) {
                    // Column exists - check if it needs updating (e.g., length change)
                    this.updateGeneratedColumnIfNeeded(connection, tableName, index, columnName);
                } else {
                    // Column doesn't exist - create it
                    this.createGeneratedColumn(connection, tableName, index, columnName);
                }
            }
        } catch (SQLException exception) {
            throw new RuntimeException("cannot manage generated columns for " + tableName, exception);
        }
    }

    private Set<String> getExistingIndexColumns(@NonNull Connection connection, @NonNull String tableName) throws SQLException {
        Set<String> columns = new HashSet<>();
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet rs = metaData.getColumns(null, null, tableName.toUpperCase(), null);
        while (rs.next()) {
            String columnName = rs.getString("COLUMN_NAME");
            if (columnName.toLowerCase().startsWith(INDEX_COLUMN_PREFIX)) {
                columns.add(columnName.toLowerCase());
            }
        }
        rs.close();
        return columns;
    }

    private void createGeneratedColumn(@NonNull Connection connection, @NonNull String tableName,
                                       @NonNull IndexProperty index, @NonNull String columnName) throws SQLException {
        // Use the index path directly (e.g., "category" -> "category", "nested.field" -> "nested"."field")
        String fieldRef = index.toH2FieldReference();
        String expression = this.buildGeneratedColumnExpression(index, fieldRef);
        String columnType = this.getColumnType(index);

        // Add generated column
        String addColumnSql = "alter table `" + tableName + "` add column `" + columnName + "` " +
            columnType + " generated always as (" + expression + ")";
        connection.createStatement().execute(this.debugQuery(addColumnSql));

        // Create index on the generated column
        String indexName = tableName + "_" + columnName + "_idx";
        String createIndexSql = "create index `" + indexName + "` on `" + tableName + "` (`" + columnName + "`)";
        connection.createStatement().execute(this.debugQuery(createIndexSql));

    }

    private void updateGeneratedColumnIfNeeded(@NonNull Connection connection, @NonNull String tableName,
                                               @NonNull IndexProperty index, @NonNull String columnName) throws SQLException {
        // For string columns, check if maxLength changed
        if (!index.isNumeric() && !index.isBoolean()) {
            DatabaseMetaData metaData = connection.getMetaData();
            ResultSet rs = metaData.getColumns(null, null, tableName.toUpperCase(), columnName.toUpperCase());
            if (rs.next()) {
                int currentLength = rs.getInt("COLUMN_SIZE");
                if (currentLength != index.getMaxLength()) {
                    // Need to recreate the column with new length
                    this.dropGeneratedColumn(connection, tableName, columnName);
                    this.createGeneratedColumn(connection, tableName, index, columnName);
                }
            }
            rs.close();
        }
    }

    private void dropGeneratedColumn(@NonNull Connection connection, @NonNull String tableName,
                                     @NonNull String columnName) throws SQLException {
        // Drop index first
        String indexName = tableName + "_" + columnName + "_idx";
        String dropIndexSql = "drop index if exists `" + indexName + "`";
        connection.createStatement().execute(dropIndexSql);

        // Drop column
        String dropColumnSql = "alter table `" + tableName + "` drop column if exists `" + columnName + "`";
        connection.createStatement().execute(dropColumnSql);
    }

    private String buildGeneratedColumnExpression(@NonNull IndexProperty index, @NonNull String fieldRef) {
        // Build expression based on field type
        // H2 field reference: (`value`)."field" returns JSON type
        String jsonField = "(`value`)." + fieldRef;

        if (index.isFloatingPoint()) {
            // Cast to decimal for floating-point types: JSON -> VARCHAR -> DECIMAL
            return "cast(cast(" + jsonField + " as varchar) as decimal(20,10))";
        } else if (index.isNumeric()) {
            // Cast to bigint for integer types: JSON -> VARCHAR -> BIGINT
            return "cast(cast(" + jsonField + " as varchar) as bigint)";
        } else if (index.isBoolean()) {
            // Boolean values in JSON are true/false without quotes
            // Cast JSON -> VARCHAR -> BOOLEAN (H2 can't cast JSON directly to BOOLEAN)
            return "cast(cast(" + jsonField + " as varchar) as boolean)";
        } else {
            // String: extract and unescape JSON string
            // 1. Cast to varchar: "hello \"world\"" (with quotes and escapes)
            // 2. Use SUBSTRING to remove exactly first and last character (the outer quotes)
            // 3. Unescape \\ to \ and \" to " using REPLACE
            // Order matters: unescape \\ first, then \" (so \\\" becomes \" not \")
            String castField = "cast(" + jsonField + " as varchar)";
            return "replace(replace(substring(" + castField + ", 2, length(" + castField + ") - 2), '\\\\', '\\'), '\\\"', '\"')";
        }
    }

    private String getColumnType(@NonNull IndexProperty index) {
        if (index.isFloatingPoint()) {
            return "decimal(20,10)";
        } else if (index.isNumeric()) {
            return "bigint";
        } else if (index.isBoolean()) {
            return "boolean";
        } else {
            return "varchar(" + index.getMaxLength() + ")";
        }
    }

    /**
     * Check if an index column exists for the given property.
     * Used by H2FilterRenderer to decide whether to use the generated column or JSON expression.
     */
    public boolean hasIndexColumn(@NonNull PersistenceCollection collection, @NonNull String propertyPath) {
        Set<IndexProperty> indexes = this.getKnownIndexes().get(collection.getValue());
        if (indexes == null) return false;

        for (IndexProperty index : indexes) {
            if (index.getValue().equals(propertyPath)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Get the IndexProperty for a given property path if it's indexed.
     */
    public IndexProperty getIndexProperty(@NonNull PersistenceCollection collection, @NonNull String propertyPath) {
        Set<IndexProperty> indexes = this.getKnownIndexes().get(collection.getValue());
        if (indexes == null) return null;

        for (IndexProperty index : indexes) {
            if (index.getValue().equals(propertyPath)) {
                return index;
            }
        }
        return null;
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
                if (!hasMore) return;

                String sql = baseQuery + " limit " + batchSize + " offset " + offset;
                try (Connection connection = getDataSource().getConnection()) {
                    Statement statement = connection.createStatement();
                    ResultSet resultSet = statement.executeQuery(debugQuery(sql));
                    List<PersistenceEntity<String>> batch = new ArrayList<>();

                    while (resultSet.next()) {
                        String key = resultSet.getString("key");
                        String value = resultSet.getString("value");
                        batch.add(new PersistenceEntity<>(PersistencePath.of(key), value));
                    }

                    if (batch.isEmpty()) {
                        hasMore = false;
                        currentBatch = null;
                        return;
                    }

                    currentBatch = batch.iterator();
                    offset += batchSize;

                    // If we got fewer results than requested, this is the last batch
                    if (batch.size() < batchSize) {
                        hasMore = false;
                    }
                } catch (SQLException e) {
                    throw new RuntimeException("cannot stream from " + collection, e);
                }
            }

            @Override
            public boolean hasNext() {
                if (currentBatch == null || !currentBatch.hasNext()) {
                    if (!hasMore) return false;
                    fetchNextBatch();
                }
                return currentBatch != null && currentBatch.hasNext();
            }

            @Override
            public PersistenceEntity<String> next() {
                if (!hasNext()) throw new NoSuchElementException();
                return currentBatch.next();
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
}
