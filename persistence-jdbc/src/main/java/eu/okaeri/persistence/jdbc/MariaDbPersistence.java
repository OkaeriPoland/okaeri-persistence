package eu.okaeri.persistence.jdbc;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import eu.okaeri.configs.serdes.OkaeriSerdes;
import eu.okaeri.persistence.*;
import eu.okaeri.persistence.document.ConfigurerProvider;
import eu.okaeri.persistence.document.Document;
import eu.okaeri.persistence.document.DocumentSerializer;
import eu.okaeri.persistence.document.index.IndexProperty;
import eu.okaeri.persistence.filter.DeleteFilter;
import eu.okaeri.persistence.filter.FindFilter;
import eu.okaeri.persistence.filter.UpdateFilter;
import eu.okaeri.persistence.filter.operation.UpdateOperation;
import eu.okaeri.persistence.jdbc.filter.MariaDbFilterRenderer;
import eu.okaeri.persistence.jdbc.filter.MariaDbStringRenderer;
import eu.okaeri.persistence.jdbc.filter.MariaDbUpdateRenderer;
import eu.okaeri.persistence.util.ConnectionRetry;
import lombok.Getter;
import lombok.NonNull;

import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * MariaDB persistence backend with native JSON filtering, indexing, and updates.
 * Uses generated columns for efficient JSON field indexing.
 */
public class MariaDbPersistence implements Persistence, FilterablePersistence, StreamablePersistence, UpdatablePersistence {

    private static final boolean DEBUG = Boolean.parseBoolean(System.getProperty("okaeri.platform.debug", "false"));
    private static final Logger LOGGER = Logger.getLogger(MariaDbPersistence.class.getSimpleName());
    private static final String INDEX_COLUMN_PREFIX = "_f_";

    private static final MariaDbStringRenderer STRING_RENDERER = new MariaDbStringRenderer();
    private static final MariaDbUpdateRenderer UPDATE_RENDERER = new MariaDbUpdateRenderer(STRING_RENDERER);

    private final @Getter PersistencePath basePath;
    private @Getter HikariDataSource dataSource;

    private final @Getter DocumentSerializer serializer;
    private final MariaDbFilterRenderer filterRenderer;
    private final Map<String, PersistenceCollection> knownCollections = new ConcurrentHashMap<>();

    public MariaDbPersistence(@NonNull PersistencePath basePath, @NonNull HikariConfig hikariConfig,
                              @NonNull ConfigurerProvider configurerProvider, @NonNull OkaeriSerdes... serdes) {
        this.basePath = basePath;
        this.serializer = new DocumentSerializer(configurerProvider, serdes);
        this.filterRenderer = new MariaDbFilterRenderer(STRING_RENDERER);
        this.connect(hikariConfig);
    }

    public MariaDbPersistence(@NonNull PersistencePath basePath, @NonNull HikariDataSource dataSource,
                              @NonNull ConfigurerProvider configurerProvider, @NonNull OkaeriSerdes... serdes) {
        this.basePath = basePath;
        this.dataSource = dataSource;
        this.serializer = new DocumentSerializer(configurerProvider, serdes);
        this.filterRenderer = new MariaDbFilterRenderer(STRING_RENDERER);
    }

    public MariaDbPersistence(@NonNull HikariConfig hikariConfig,
                              @NonNull ConfigurerProvider configurerProvider, @NonNull OkaeriSerdes... serdes) {
        this(PersistencePath.of(""), hikariConfig, configurerProvider, serdes);
    }

    public MariaDbPersistence(@NonNull HikariDataSource dataSource,
                              @NonNull ConfigurerProvider configurerProvider, @NonNull OkaeriSerdes... serdes) {
        this(PersistencePath.of(""), dataSource, configurerProvider, serdes);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private PersistencePath basePath;
        private HikariConfig hikariConfig;
        private HikariDataSource dataSource;
        private ConfigurerProvider configurerProvider;
        private OkaeriSerdes[] serdes = new OkaeriSerdes[0];

        public Builder basePath(@NonNull String basePath) {
            this.basePath = PersistencePath.of(basePath);
            return this;
        }

        public Builder basePath(@NonNull PersistencePath basePath) {
            this.basePath = basePath;
            return this;
        }

        public Builder hikariConfig(@NonNull HikariConfig hikariConfig) {
            this.hikariConfig = hikariConfig;
            return this;
        }

        public Builder dataSource(@NonNull HikariDataSource dataSource) {
            this.dataSource = dataSource;
            return this;
        }

        public Builder configurer(@NonNull ConfigurerProvider configurerProvider) {
            this.configurerProvider = configurerProvider;
            return this;
        }

        public Builder serdes(@NonNull OkaeriSerdes... packs) {
            this.serdes = packs;
            return this;
        }

        public MariaDbPersistence build() {
            if ((this.hikariConfig == null) && (this.dataSource == null)) {
                throw new IllegalStateException("hikariConfig or dataSource is required");
            }
            if ((this.hikariConfig != null) && (this.dataSource != null)) {
                throw new IllegalStateException("hikariConfig and dataSource are mutually exclusive");
            }
            if (this.configurerProvider == null) {
                throw new IllegalStateException("configurer is required");
            }
            PersistencePath path = (this.basePath != null) ? this.basePath : PersistencePath.of("");
            if (this.dataSource != null) {
                return new MariaDbPersistence(path, this.dataSource, this.configurerProvider, this.serdes);
            }
            return new MariaDbPersistence(path, this.hikariConfig, this.configurerProvider, this.serdes);
        }
    }

    private void connect(@NonNull HikariConfig hikariConfig) {
        this.dataSource = ConnectionRetry.of(this.basePath.getValue())
            .connector(() -> new HikariDataSource(hikariConfig))
            .connect();
    }

    /**
     * Get the generated column name for an index property.
     */
    public static String getIndexColumnName(@NonNull IndexProperty index) {
        return INDEX_COLUMN_PREFIX + index.toSqlIdentifier();
    }

    // ==================== COLLECTION MANAGEMENT ====================

    @Override
    public void registerCollection(@NonNull PersistenceCollection collection) {
        String tableName = this.table(collection);
        int keyLength = collection.getKeyLength();

        String createSql = "create table if not exists `" + tableName + "` (" +
            "`key` varchar(" + keyLength + ") primary key not null, " +
            "`value` json not null) " +
            "engine = InnoDB character set = utf8mb4 collate = utf8mb4_bin;";
        String alterKeySql = "alter table `" + tableName + "` MODIFY COLUMN `key` varchar(" + keyLength + ") not null";

        try (Connection connection = this.dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute(this.debugQuery(createSql));
            statement.execute(this.debugQuery(alterKeySql));
        } catch (SQLException exception) {
            throw new RuntimeException("cannot register collection", exception);
        }

        // Drop legacy emulated index table if exists
        this.dropLegacyIndexTable(collection);

        // Manage generated columns for native indexing
        this.manageGeneratedColumns(collection);

        // Track collection
        this.knownCollections.put(collection.getValue(), collection);
    }

    private void dropLegacyIndexTable(@NonNull PersistenceCollection collection) {
        String indexTable = this.basePath.sub(collection).sub("index").toSqlIdentifier();
        try (Connection connection = this.dataSource.getConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            try (ResultSet tables = metaData.getTables(null, null, indexTable, new String[]{"TABLE"})) {
                if (tables.next()) {
                    String dropSql = "drop table `" + indexTable + "`";
                    try (Statement statement = connection.createStatement()) {
                        statement.execute(this.debugQuery(dropSql));
                    }
                }
            }
        } catch (SQLException exception) {
            throw new RuntimeException("cannot drop legacy index table " + indexTable, exception);
        }
    }

    private void manageGeneratedColumns(@NonNull PersistenceCollection collection) {
        String tableName = this.table(collection);
        Set<IndexProperty> desiredIndexes = collection.getIndexes();

        try (Connection connection = this.dataSource.getConnection()) {
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
        try (ResultSet rs = metaData.getColumns(null, null, tableName, null)) {
            while (rs.next()) {
                String columnName = rs.getString("COLUMN_NAME");
                if (columnName.startsWith(INDEX_COLUMN_PREFIX)) {
                    columns.add(columnName);
                }
            }
        }
        return columns;
    }

    private void createGeneratedColumn(@NonNull Connection connection, @NonNull String tableName,
                                       @NonNull IndexProperty index, @NonNull String columnName) throws SQLException {
        String expression = this.buildGeneratedColumnExpression(index);
        String columnType = this.getColumnType(index);

        String addColumnSql = "alter table `" + tableName + "` add column `" + columnName + "` " +
            columnType + " as (" + expression + ") stored";

        try (Statement statement = connection.createStatement()) {
            statement.execute(this.debugQuery(addColumnSql));

            String indexName = tableName + "_" + columnName + "_idx";
            String createIndexSql = "create index `" + indexName + "` on `" + tableName + "` (`" + columnName + "`)";
            statement.execute(this.debugQuery(createIndexSql));
        } catch (SQLException e) {
            LOGGER.warning("Could not create generated column " + columnName + ": " + e.getMessage());
        }
    }

    private void dropGeneratedColumn(@NonNull Connection connection, @NonNull String tableName,
                                     @NonNull String columnName) throws SQLException {
        String indexName = tableName + "_" + columnName + "_idx";
        String dropIndexSql = "drop index if exists `" + indexName + "` on `" + tableName + "`";
        try (Statement statement = connection.createStatement()) {
            statement.execute(this.debugQuery(dropIndexSql));
        } catch (SQLException e) {
            // Index might not exist - that's okay
        }

        String dropColumnSql = "alter table `" + tableName + "` drop column if exists `" + columnName + "`";
        try (Statement statement = connection.createStatement()) {
            statement.execute(this.debugQuery(dropColumnSql));
        }
    }

    private void ensureIndexExists(@NonNull Connection connection, @NonNull String tableName,
                                   @NonNull String columnName) throws SQLException {
        String indexName = tableName + "_" + columnName + "_idx";

        DatabaseMetaData metaData = connection.getMetaData();
        boolean indexExists = false;
        try (ResultSet rs = metaData.getIndexInfo(null, null, tableName, false, false)) {
            while (rs.next()) {
                String existingIndexName = rs.getString("INDEX_NAME");
                if ((existingIndexName != null) && existingIndexName.equalsIgnoreCase(indexName)) {
                    indexExists = true;
                    break;
                }
            }
        }

        if (!indexExists) {
            String createIndexSql = "create index `" + indexName + "` on `" + tableName + "` (`" + columnName + "`)";
            try (Statement statement = connection.createStatement()) {
                statement.execute(this.debugQuery(createIndexSql));
            } catch (SQLException e) {
                LOGGER.warning("Could not create index " + indexName + ": " + e.getMessage());
            }
        }
    }

    private String buildGeneratedColumnExpression(@NonNull IndexProperty index) {
        String jsonPath = index.toMariaDbJsonPath();

        if (index.isFloatingPoint()) {
            return "cast(json_extract(`value`, '" + jsonPath + "') as decimal(20,10))";
        } else if (index.isNumeric()) {
            // Use floor() to handle decimal representations of integers (e.g., 100.0 -> 100)
            // JSON serializers may output integers as decimals during read-modify-write cycles
            return "cast(floor(json_extract(`value`, '" + jsonPath + "')) as signed)";
        } else if (index.isBoolean()) {
            return "json_unquote(json_extract(`value`, '" + jsonPath + "'))";
        } else {
            return "json_unquote(json_extract(`value`, '" + jsonPath + "'))";
        }
    }

    private String getColumnType(@NonNull IndexProperty index) {
        if (index.isFloatingPoint()) {
            return "decimal(20,10)";
        } else if (index.isNumeric()) {
            return "bigint";
        } else if (index.isBoolean()) {
            return "varchar(5)";
        } else {
            return "varchar(" + index.getMaxLength() + ")";
        }
    }

    private void checkCollectionRegistered(@NonNull PersistenceCollection collection) {
        if (!this.knownCollections.containsKey(collection.getValue())) {
            throw new IllegalArgumentException("Collection not registered: " + collection.getValue());
        }
    }

    private String table(@NonNull PersistenceCollection collection) {
        return this.basePath.sub(collection).toSqlIdentifier();
    }

    private String debugQuery(@NonNull String sql) {
        if (DEBUG) {
            System.out.println("[MariaDB] " + sql);
        }
        return sql;
    }

    // ==================== READ OPERATIONS ====================

    @Override
    public boolean exists(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {
        this.checkCollectionRegistered(collection);
        String sql = "select 1 from `" + this.table(collection) + "` where `key` = ? limit 1";

        try (Connection connection = this.dataSource.getConnection();
             PreparedStatement prepared = connection.prepareStatement(this.debugQuery(sql))) {
            prepared.setString(1, path.getValue());
            try (ResultSet resultSet = prepared.executeQuery()) {
                return resultSet.next();
            }
        } catch (SQLException exception) {
            throw new RuntimeException("cannot check if " + path + " exists in " + collection, exception);
        }
    }

    @Override
    public long count(@NonNull PersistenceCollection collection) {
        this.checkCollectionRegistered(collection);
        String sql = "select count(1) from `" + this.table(collection) + "`";

        try (Connection connection = this.dataSource.getConnection();
             PreparedStatement prepared = connection.prepareStatement(this.debugQuery(sql));
             ResultSet resultSet = prepared.executeQuery()) {
            if (resultSet.next()) {
                return resultSet.getLong(1);
            }
        } catch (SQLException exception) {
            throw new RuntimeException("cannot count " + collection, exception);
        }

        return 0;
    }

    @Override
    public Optional<Document> read(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {
        this.checkCollectionRegistered(collection);
        String sql = "select `value` from `" + this.table(collection) + "` where `key` = ? limit 1";

        try (Connection connection = this.dataSource.getConnection();
             PreparedStatement prepared = connection.prepareStatement(this.debugQuery(sql))) {
            prepared.setString(1, path.getValue());
            try (ResultSet resultSet = prepared.executeQuery()) {
                if (resultSet.next()) {
                    String json = resultSet.getString("value");
                    return Optional.of(this.serializer.deserialize(collection, path, json));
                }
            }
        } catch (SQLException exception) {
            throw new RuntimeException("cannot read " + path + " from " + collection, exception);
        }

        return Optional.empty();
    }

    private static final int BATCH_SIZE = 1000;

    @Override
    public Map<PersistencePath, Document> read(@NonNull PersistenceCollection collection, @NonNull Collection<PersistencePath> paths) {
        this.checkCollectionRegistered(collection);
        if (paths.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<PersistencePath, Document> map = new LinkedHashMap<>();
        List<PersistencePath> pathList = new ArrayList<>(paths);

        for (int i = 0; i < pathList.size(); i += BATCH_SIZE) {
            List<PersistencePath> batch = pathList.subList(i, Math.min(i + BATCH_SIZE, pathList.size()));
            String placeholders = batch.stream().map(p -> "?").collect(Collectors.joining(", "));
            String sql = "select `key`, `value` from `" + this.table(collection) + "` where `key` in (" + placeholders + ")";

            try (Connection connection = this.dataSource.getConnection();
                 PreparedStatement prepared = connection.prepareStatement(this.debugQuery(sql))) {
                int currentIndex = 1;
                for (PersistencePath path : batch) {
                    prepared.setString(currentIndex++, path.getValue());
                }
                try (ResultSet resultSet = prepared.executeQuery()) {
                    while (resultSet.next()) {
                        String key = resultSet.getString("key");
                        String json = resultSet.getString("value");
                        PersistencePath path = PersistencePath.of(key);
                        map.put(path, this.serializer.deserialize(collection, path, json));
                    }
                }
            } catch (SQLException exception) {
                throw new RuntimeException("cannot read batch from " + collection, exception);
            }
        }

        return map;
    }

    @Override
    public Map<PersistencePath, Document> readAll(@NonNull PersistenceCollection collection) {
        return this.streamAll(collection).collect(Collectors.toMap(
            PersistenceEntity::getPath,
            PersistenceEntity::getValue
        ));
    }

    // ==================== STREAMING ====================

    @Override
    public Stream<PersistenceEntity<Document>> streamAll(@NonNull PersistenceCollection collection) {
        this.checkCollectionRegistered(collection);
        String sql = "select `key`, `value` from `" + this.table(collection) + "`";

        try (Connection connection = this.dataSource.getConnection();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(this.debugQuery(sql))) {
            List<PersistenceEntity<Document>> results = new ArrayList<>();

            while (resultSet.next()) {
                String key = resultSet.getString("key");
                String json = resultSet.getString("value");
                PersistencePath path = PersistencePath.of(key);
                Document doc = this.serializer.deserialize(collection, path, json);
                results.add(new PersistenceEntity<>(path, doc));
            }

            return results.stream();
        } catch (SQLException exception) {
            throw new RuntimeException("cannot stream all from " + collection, exception);
        }
    }

    @Override
    public Stream<PersistenceEntity<Document>> stream(@NonNull PersistenceCollection collection, int batchSize) {
        this.checkCollectionRegistered(collection);
        String baseQuery = "select `key`, `value` from `" + this.table(collection) + "`";

        Iterator<PersistenceEntity<Document>> iterator = new Iterator<PersistenceEntity<Document>>() {
            private int offset = 0;
            private Iterator<PersistenceEntity<Document>> currentBatch = null;
            private boolean hasMore = true;

            private void fetchNextBatch() {
                if (!this.hasMore) return;

                String sql = baseQuery + " limit " + batchSize + " offset " + this.offset;
                try (Connection connection = MariaDbPersistence.this.dataSource.getConnection();
                     Statement statement = connection.createStatement();
                     ResultSet resultSet = statement.executeQuery(MariaDbPersistence.this.debugQuery(sql))) {
                    List<PersistenceEntity<Document>> batch = new ArrayList<>();

                    while (resultSet.next()) {
                        String key = resultSet.getString("key");
                        String json = resultSet.getString("value");
                        PersistencePath path = PersistencePath.of(key);
                        Document doc = MariaDbPersistence.this.serializer.deserialize(collection, path, json);
                        batch.add(new PersistenceEntity<>(path, doc));
                    }

                    if (batch.isEmpty()) {
                        this.hasMore = false;
                        this.currentBatch = null;
                        return;
                    }

                    this.currentBatch = batch.iterator();
                    this.offset += batchSize;

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
            public PersistenceEntity<Document> next() {
                if (!this.hasNext()) throw new NoSuchElementException();
                return this.currentBatch.next();
            }
        };

        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false);
    }

    // ==================== FILTERING ====================

    @Override
    public Stream<PersistenceEntity<Document>> find(@NonNull PersistenceCollection collection, @NonNull FindFilter filter) {
        this.checkCollectionRegistered(collection);

        // Set indexed properties context for the renderer to use generated columns
        Set<IndexProperty> indexes = this.knownCollections.get(collection.getValue()).getIndexes();
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

        try (Connection connection = this.dataSource.getConnection();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(this.debugQuery(sql))) {
            List<PersistenceEntity<Document>> results = new ArrayList<>();

            while (resultSet.next()) {
                String key = resultSet.getString("key");
                String json = resultSet.getString("value");
                PersistencePath path = PersistencePath.of(key);
                Document doc = this.serializer.deserialize(collection, path, json);
                results.add(new PersistenceEntity<>(path, doc));
            }

            return results.stream();
        } catch (SQLException exception) {
            throw new RuntimeException("cannot read by filter from " + collection, exception);
        }
    }

    @Override
    public long delete(@NonNull PersistenceCollection collection, @NonNull DeleteFilter filter) {
        this.checkCollectionRegistered(collection);

        if (filter.getWhere() == null) {
            throw new IllegalArgumentException("DeleteFilter requires WHERE condition - use deleteAll() instead");
        }

        Set<IndexProperty> indexes = this.knownCollections.get(collection.getValue()).getIndexes();
        this.filterRenderer.setIndexedProperties(indexes);

        String sql = "delete from `" + this.table(collection) + "` where " +
            this.filterRenderer.renderCondition(filter.getWhere());

        try (Connection connection = this.dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            return statement.executeUpdate(this.debugQuery(sql));
        } catch (SQLException exception) {
            throw new RuntimeException("cannot delete from " + collection + " with " + filter, exception);
        }
    }

    // ==================== UPDATES ====================

    @Override
    public boolean updateOne(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull List<UpdateOperation> operations) {
        this.checkCollectionRegistered(collection);

        String updateExpr = UPDATE_RENDERER.render(operations);
        String sql = "update `" + this.table(collection) + "` set `value` = " + updateExpr + " where `key` = ?";

        try (Connection connection = this.dataSource.getConnection();
             PreparedStatement prepared = connection.prepareStatement(this.debugQuery(sql))) {
            prepared.setString(1, path.getValue());
            return prepared.executeUpdate() > 0;
        } catch (SQLException exception) {
            throw new RuntimeException("cannot update " + path + " in " + collection, exception);
        }
    }

    @Override
    public Optional<Document> updateOneAndGet(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull List<UpdateOperation> operations) {
        this.checkCollectionRegistered(collection);

        String lockSql = "select `value` from `" + this.table(collection) + "` where `key` = ? for update";
        String updateExpr = UPDATE_RENDERER.render(operations);
        String updateSql = "update `" + this.table(collection) + "` set `value` = " + updateExpr + " where `key` = ?";
        String selectSql = "select `value` from `" + this.table(collection) + "` where `key` = ?";

        try (Connection connection = this.dataSource.getConnection()) {
            connection.setAutoCommit(false);

            try {
                try (PreparedStatement lockStmt = connection.prepareStatement(this.debugQuery(lockSql))) {
                    lockStmt.setString(1, path.getValue());
                    try (ResultSet lockResult = lockStmt.executeQuery()) {
                        if (!lockResult.next()) {
                            connection.rollback();
                            return Optional.empty();
                        }
                    }
                }

                try (PreparedStatement updateStmt = connection.prepareStatement(this.debugQuery(updateSql))) {
                    updateStmt.setString(1, path.getValue());
                    updateStmt.executeUpdate();
                }

                try (PreparedStatement selectStmt = connection.prepareStatement(this.debugQuery(selectSql))) {
                    selectStmt.setString(1, path.getValue());
                    try (ResultSet resultSet = selectStmt.executeQuery()) {
                        if (resultSet.next()) {
                            String json = resultSet.getString("value");
                            connection.commit();
                            return Optional.of(this.serializer.deserialize(collection, path, json));
                        }
                    }
                }

                connection.commit();
                return Optional.empty();
            } catch (SQLException e) {
                connection.rollback();
                throw e;
            } finally {
                connection.setAutoCommit(true);
            }
        } catch (SQLException exception) {
            throw new RuntimeException("cannot update and get " + path + " in " + collection, exception);
        }
    }

    @Override
    public Optional<Document> getAndUpdateOne(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull List<UpdateOperation> operations) {
        this.checkCollectionRegistered(collection);

        String selectSql = "select `value` from `" + this.table(collection) + "` where `key` = ? for update";
        String updateExpr = UPDATE_RENDERER.render(operations);
        String updateSql = "update `" + this.table(collection) + "` set `value` = " + updateExpr + " where `key` = ?";

        try (Connection connection = this.dataSource.getConnection()) {
            connection.setAutoCommit(false);

            try {
                String json;
                try (PreparedStatement selectStmt = connection.prepareStatement(this.debugQuery(selectSql))) {
                    selectStmt.setString(1, path.getValue());
                    try (ResultSet resultSet = selectStmt.executeQuery()) {
                        if (!resultSet.next()) {
                            connection.rollback();
                            return Optional.empty();
                        }
                        json = resultSet.getString("value");
                    }
                }

                try (PreparedStatement updateStmt = connection.prepareStatement(this.debugQuery(updateSql))) {
                    updateStmt.setString(1, path.getValue());
                    updateStmt.executeUpdate();
                }

                connection.commit();
                return Optional.of(this.serializer.deserialize(collection, path, json));
            } catch (SQLException e) {
                connection.rollback();
                throw e;
            } finally {
                connection.setAutoCommit(true);
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

        Set<IndexProperty> indexes = this.knownCollections.get(collection.getValue()).getIndexes();
        this.filterRenderer.setIndexedProperties(indexes);

        String updateExpr = UPDATE_RENDERER.render(filter.getOperations());
        String sql = "update `" + this.table(collection) + "` set `value` = " + updateExpr +
            " where " + this.filterRenderer.renderCondition(filter.getWhere());

        try (Connection connection = this.dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            return statement.executeUpdate(this.debugQuery(sql));
        } catch (SQLException exception) {
            throw new RuntimeException("cannot update " + collection + " with " + filter, exception);
        }
    }

    // ==================== WRITE OPERATIONS ====================

    @Override
    public boolean write(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull Document document) {
        this.checkCollectionRegistered(collection);
        this.serializer.setupDocument(document, collection, path);

        String json = this.serializer.serialize(document);
        String sql = "insert into `" + this.table(collection) + "` (`key`, `value`) values (?, ?) " +
            "on duplicate key update `value` = ?";

        try (Connection connection = this.dataSource.getConnection();
             PreparedStatement prepared = connection.prepareStatement(this.debugQuery(sql))) {
            prepared.setString(1, path.getValue());
            prepared.setString(2, json);
            prepared.setString(3, json);
            return prepared.executeUpdate() > 0;
        } catch (SQLException exception) {
            throw new RuntimeException("cannot write " + path + " to " + collection, exception);
        }
    }

    @Override
    public long write(@NonNull PersistenceCollection collection, @NonNull Map<PersistencePath, Document> documents) {
        if (documents.isEmpty()) {
            return 0;
        }
        this.checkCollectionRegistered(collection);

        String sql = "insert into `" + this.table(collection) + "` (`key`, `value`) values (?, ?) " +
            "on duplicate key update `value` = ?";

        try (Connection connection = this.dataSource.getConnection();
             PreparedStatement prepared = connection.prepareStatement(this.debugQuery(sql))) {
            connection.setAutoCommit(false);

            try {
                for (Map.Entry<PersistencePath, Document> entry : documents.entrySet()) {
                    PersistencePath path = entry.getKey();
                    Document document = entry.getValue();
                    this.serializer.setupDocument(document, collection, path);

                    String json = this.serializer.serialize(document);
                    prepared.setString(1, path.getValue());
                    prepared.setString(2, json);
                    prepared.setString(3, json);
                    prepared.addBatch();
                }

                int[] results = prepared.executeBatch();
                connection.commit();
                return results.length;
            } finally {
                connection.setAutoCommit(true);
            }
        } catch (SQLException exception) {
            throw new RuntimeException("cannot write batch to " + collection, exception);
        }
    }

    // ==================== DELETE OPERATIONS ====================

    @Override
    public boolean delete(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {
        this.checkCollectionRegistered(collection);
        String sql = "delete from `" + this.table(collection) + "` where `key` = ?";

        try (Connection connection = this.dataSource.getConnection();
             PreparedStatement prepared = connection.prepareStatement(this.debugQuery(sql))) {
            prepared.setString(1, path.getValue());
            return prepared.executeUpdate() > 0;
        } catch (SQLException exception) {
            throw new RuntimeException("cannot delete " + path + " from " + collection, exception);
        }
    }

    @Override
    public long delete(@NonNull PersistenceCollection collection, @NonNull Collection<PersistencePath> paths) {
        this.checkCollectionRegistered(collection);
        if (paths.isEmpty()) {
            return 0;
        }

        long totalDeleted = 0;
        List<PersistencePath> pathList = new ArrayList<>(paths);

        for (int i = 0; i < pathList.size(); i += BATCH_SIZE) {
            List<PersistencePath> batch = pathList.subList(i, Math.min(i + BATCH_SIZE, pathList.size()));
            String placeholders = batch.stream().map(p -> "?").collect(Collectors.joining(", "));
            String sql = "delete from `" + this.table(collection) + "` where `key` in (" + placeholders + ")";

            try (Connection connection = this.dataSource.getConnection();
                 PreparedStatement prepared = connection.prepareStatement(this.debugQuery(sql))) {
                int currentIndex = 1;
                for (PersistencePath path : batch) {
                    prepared.setString(currentIndex++, path.getValue());
                }
                totalDeleted += prepared.executeUpdate();
            } catch (SQLException exception) {
                throw new RuntimeException("cannot delete batch from " + collection, exception);
            }
        }

        return totalDeleted;
    }

    @Override
    public boolean deleteAll(@NonNull PersistenceCollection collection) {
        this.checkCollectionRegistered(collection);
        String sql = "truncate table `" + this.table(collection) + "`";

        try (Connection connection = this.dataSource.getConnection();
             PreparedStatement prepared = connection.prepareStatement(this.debugQuery(sql))) {
            prepared.executeUpdate();
            return true;
        } catch (SQLException exception) {
            throw new RuntimeException("cannot truncate " + collection, exception);
        }
    }

    @Override
    public long deleteAll() {
        return this.knownCollections.values().stream()
            .map(this::deleteAll)
            .filter(Predicate.isEqual(true))
            .count();
    }

    @Override
    public void close() throws IOException {
        this.dataSource.close();
    }
}
