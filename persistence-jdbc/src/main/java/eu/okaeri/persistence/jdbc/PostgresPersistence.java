package eu.okaeri.persistence.jdbc;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import eu.okaeri.configs.configurer.Configurer;
import eu.okaeri.configs.serdes.OkaeriSerdes;
import eu.okaeri.persistence.*;
import eu.okaeri.persistence.document.Document;
import eu.okaeri.persistence.document.DocumentSerializer;
import eu.okaeri.persistence.document.DocumentSerializerConfig;
import eu.okaeri.persistence.document.PersistenceBuilder;
import eu.okaeri.persistence.filter.DeleteFilter;
import eu.okaeri.persistence.filter.FindFilter;
import eu.okaeri.persistence.filter.UpdateFilter;
import eu.okaeri.persistence.filter.operation.UpdateOperation;
import eu.okaeri.persistence.filter.renderer.FilterRenderer;
import eu.okaeri.persistence.jdbc.filter.PostgresFilterRenderer;
import eu.okaeri.persistence.jdbc.filter.PostgresUpdateRenderer;
import eu.okaeri.persistence.jdbc.filter.SqlStringRenderer;
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
 * PostgreSQL persistence backend with native jsonb filtering, indexing, and updates.
 */
public class PostgresPersistence implements Persistence, FilterablePersistence, StreamablePersistence, UpdatablePersistence {

    private static final boolean DEBUG = Boolean.parseBoolean(System.getProperty("okaeri.platform.debug", "false"));
    private static final Logger LOGGER = Logger.getLogger(PostgresPersistence.class.getSimpleName());

    private static final SqlStringRenderer STRING_RENDERER = new SqlStringRenderer();
    private static final FilterRenderer FILTER_RENDERER = new PostgresFilterRenderer(STRING_RENDERER);
    private static final PostgresUpdateRenderer UPDATE_RENDERER = new PostgresUpdateRenderer(STRING_RENDERER);

    private final @Getter PersistencePath basePath;
    private @Getter HikariDataSource dataSource;

    private final @Getter DocumentSerializer serializer;
    private final Map<String, PersistenceCollection> knownCollections = new ConcurrentHashMap<>();

    public PostgresPersistence(@NonNull PersistencePath basePath, @NonNull HikariConfig hikariConfig,
                               @NonNull Configurer configurer, @NonNull OkaeriSerdes... serdes) {
        this.basePath = basePath;
        this.serializer = new DocumentSerializer(configurer, serdes);
        this.connect(hikariConfig);
    }

    public PostgresPersistence(@NonNull HikariConfig hikariConfig,
                               @NonNull Configurer configurer, @NonNull OkaeriSerdes... serdes) {
        this(PersistencePath.of(""), hikariConfig, configurer, serdes);
    }

    public PostgresPersistence(@NonNull PersistencePath basePath, @NonNull HikariDataSource dataSource,
                               @NonNull Configurer configurer, @NonNull OkaeriSerdes... serdes) {
        this.basePath = basePath;
        this.dataSource = dataSource;
        this.serializer = new DocumentSerializer(configurer, serdes);
    }

    public PostgresPersistence(@NonNull HikariDataSource dataSource,
                               @NonNull Configurer configurer, @NonNull OkaeriSerdes... serdes) {
        this(PersistencePath.of(""), dataSource, configurer, serdes);
    }

    public PostgresPersistence(@NonNull PersistencePath basePath, @NonNull HikariConfig hikariConfig,
                               @NonNull DocumentSerializerConfig serializerConfig) {
        this.basePath = basePath;
        this.serializer = new DocumentSerializer(serializerConfig);
        this.connect(hikariConfig);
    }

    public PostgresPersistence(@NonNull PersistencePath basePath, @NonNull HikariDataSource dataSource,
                               @NonNull DocumentSerializerConfig serializerConfig) {
        this.basePath = basePath;
        this.dataSource = dataSource;
        this.serializer = new DocumentSerializer(serializerConfig);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends PersistenceBuilder<Builder, PostgresPersistence> {
        private HikariConfig hikariConfig;
        private HikariDataSource dataSource;

        public Builder hikariConfig(@NonNull HikariConfig hikariConfig) {
            this.hikariConfig = hikariConfig;
            return this;
        }

        public Builder dataSource(@NonNull HikariDataSource dataSource) {
            this.dataSource = dataSource;
            return this;
        }

        @Override
        public PostgresPersistence build() {
            if ((this.hikariConfig == null) && (this.dataSource == null)) {
                throw new IllegalStateException("hikariConfig or dataSource is required");
            }
            if ((this.hikariConfig != null) && (this.dataSource != null)) {
                throw new IllegalStateException("hikariConfig and dataSource are mutually exclusive");
            }

            DocumentSerializerConfig serializerConfig = this.buildSerializerConfig();
            PersistencePath path = this.resolveBasePath();

            if (this.dataSource != null) {
                return new PostgresPersistence(path, this.dataSource, serializerConfig);
            }
            return new PostgresPersistence(path, this.hikariConfig, serializerConfig);
        }
    }

    private void connect(@NonNull HikariConfig hikariConfig) {
        this.dataSource = ConnectionRetry.of(this.basePath.getValue())
            .connector(() -> new HikariDataSource(hikariConfig))
            .connect();
    }

    // ==================== COLLECTION MANAGEMENT ====================

    @Override
    public void registerCollection(@NonNull PersistenceCollection collection) {
        String tableName = this.table(collection);
        int keyLength = collection.getKeyLength();

        String createSql = "create table if not exists \"" + tableName + "\" (" +
            "key varchar(" + keyLength + ") primary key not null," +
            "value jsonb not null)";
        String alterKeySql = "alter table " + tableName + " alter column key type varchar(" + keyLength + ")";

        try (Connection connection = this.dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute(this.debugQuery(createSql));
            statement.execute(this.debugQuery(alterKeySql));
        } catch (SQLException exception) {
            throw new RuntimeException("cannot register collection", exception);
        }

        // Create native jsonb indexes
        collection.getIndexes().forEach(index -> {
            String indexName = this.basePath.sub(collection).sub(index).sub("idx").toSqlIdentifier();
            PersistencePath indexPath = PersistencePath.of("value").sub(index);

            // Apply type cast based on field type for proper index usage
            String indexExpression;
            if (index.isNumeric()) {
                indexExpression = "((" + indexPath.toPostgresJsonPath() + ")::numeric)";
            } else if (index.isBoolean()) {
                indexExpression = "((" + indexPath.toPostgresJsonPath() + ")::boolean)";
            } else {
                indexExpression = "(" + indexPath.toPostgresJsonPath(true) + ")";
            }

            try (Connection connection = this.dataSource.getConnection()) {
                // Check if index exists and if it needs migration
                String checkSql = "select indexdef from pg_indexes where indexname = ?";
                boolean needsCreate = true;

                try (PreparedStatement checkStmt = connection.prepareStatement(this.debugQuery(checkSql))) {
                    checkStmt.setString(1, indexName);
                    try (ResultSet rs = checkStmt.executeQuery()) {
                        if (rs.next()) {
                            String existingDef = rs.getString("indexdef");
                            if ((existingDef != null) && existingDef.contains(indexExpression)) {
                                needsCreate = false;
                            } else {
                                String dropSql = "drop index if exists " + indexName;
                                try (Statement stmt = connection.createStatement()) {
                                    stmt.execute(this.debugQuery(dropSql));
                                }
                                LOGGER.info("Migrating index " + indexName + " to new expression: " + indexExpression);
                            }
                        }
                    }
                }

                if (needsCreate) {
                    String indexSql = "create index " + indexName + " on " + tableName + " (" + indexExpression + ")";
                    try (Statement stmt = connection.createStatement()) {
                        stmt.execute(this.debugQuery(indexSql));
                    }
                }
            } catch (SQLException exception) {
                throw new RuntimeException("cannot register collection index " + indexName, exception);
            }
        });

        // Track collection
        this.knownCollections.put(collection.getValue(), collection);
    }

    /**
     * Update table statistics for query planner optimization.
     * Call after bulk data loads to ensure indexes are used effectively.
     */
    public void analyze(@NonNull PersistenceCollection collection) {
        this.checkCollectionRegistered(collection);
        String sql = "analyze \"" + this.table(collection) + "\"";
        try (Connection connection = this.dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute(this.debugQuery(sql));
        } catch (SQLException exception) {
            throw new RuntimeException("cannot analyze " + collection, exception);
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
            System.out.println("[PostgreSQL] " + sql);
        }
        return sql;
    }

    // ==================== READ OPERATIONS ====================

    @Override
    public boolean exists(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {
        this.checkCollectionRegistered(collection);
        String sql = "select 1 from \"" + this.table(collection) + "\" where key = ? limit 1";

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
        String sql = "select count(1) from \"" + this.table(collection) + "\"";

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
        String sql = "select value from \"" + this.table(collection) + "\" where key = ? limit 1";

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

    @Override
    public Map<PersistencePath, Document> read(@NonNull PersistenceCollection collection, @NonNull Collection<PersistencePath> paths) {
        this.checkCollectionRegistered(collection);
        if (paths.isEmpty()) {
            return Collections.emptyMap();
        }

        String keys = paths.stream().map(k -> "?").collect(Collectors.joining(", "));
        String sql = "select key, value from \"" + this.table(collection) + "\" where key in (" + keys + ")";
        Map<PersistencePath, Document> map = new LinkedHashMap<>();

        try (Connection connection = this.dataSource.getConnection();
             PreparedStatement prepared = connection.prepareStatement(this.debugQuery(sql))) {
            int currentIndex = 1;
            for (PersistencePath path : paths) {
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
            throw new RuntimeException("cannot read " + paths + " from " + collection, exception);
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
        String sql = "select key, value from \"" + this.table(collection) + "\"";

        try (Connection connection = this.dataSource.getConnection();
             PreparedStatement prepared = connection.prepareStatement(this.debugQuery(sql));
             ResultSet resultSet = prepared.executeQuery()) {
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
        String sql = "select key, value from \"" + this.table(collection) + "\"";

        try {
            Connection connection = this.dataSource.getConnection();
            connection.setAutoCommit(false); // Required for cursor-based streaming in PostgreSQL

            PreparedStatement prepared = connection.prepareStatement(this.debugQuery(sql));
            prepared.setFetchSize(batchSize);
            ResultSet resultSet = prepared.executeQuery();

            Iterator<PersistenceEntity<Document>> iterator = new Iterator<PersistenceEntity<Document>>() {
                private boolean hasNextCached = false;

                @Override
                public boolean hasNext() {
                    if (this.hasNextCached) {
                        return true;
                    }
                    try {
                        this.hasNextCached = resultSet.next();
                        return this.hasNextCached;
                    } catch (SQLException e) {
                        throw new RuntimeException("error during streaming", e);
                    }
                }

                @Override
                public PersistenceEntity<Document> next() {
                    if (!this.hasNext()) {
                        throw new NoSuchElementException();
                    }
                    try {
                        String key = resultSet.getString("key");
                        String json = resultSet.getString("value");
                        PersistencePath path = PersistencePath.of(key);
                        Document doc = PostgresPersistence.this.serializer.deserialize(collection, path, json);
                        this.hasNextCached = false;
                        return new PersistenceEntity<>(path, doc);
                    } catch (SQLException e) {
                        throw new RuntimeException("error reading result", e);
                    }
                }
            };

            return StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED),
                false
            ).onClose(() -> {
                try {
                    resultSet.close();
                    prepared.close();
                    connection.close();
                } catch (SQLException ignored) {
                }
            });

        } catch (SQLException exception) {
            throw new RuntimeException("cannot stream from " + collection, exception);
        }
    }

    // ==================== FILTERING ====================

    @Override
    public Stream<PersistenceEntity<Document>> find(@NonNull PersistenceCollection collection, @NonNull FindFilter filter) {
        this.checkCollectionRegistered(collection);
        String sql = "select key, value from \"" + this.table(collection) + "\"";

        if (filter.getWhere() != null) {
            sql += " where " + FILTER_RENDERER.renderCondition(filter.getWhere());
        }

        if (filter.hasOrderBy()) {
            sql += " order by " + FILTER_RENDERER.renderOrderBy(filter.getOrderBy());
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

        String sql = "delete from \"" + this.table(collection) + "\" where " +
            FILTER_RENDERER.renderCondition(filter.getWhere());

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
        String sql = "update \"" + this.table(collection) + "\" set value = " + updateExpr + " where key = ?";

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

        String updateExpr = UPDATE_RENDERER.render(operations);
        String sql = "update \"" + this.table(collection) + "\" set value = " + updateExpr + " where key = ? returning value";

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
            throw new RuntimeException("cannot update and get " + path + " in " + collection, exception);
        }

        return Optional.empty();
    }

    @Override
    public Optional<Document> getAndUpdateOne(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull List<UpdateOperation> operations) {
        this.checkCollectionRegistered(collection);

        String selectSql = "select value from \"" + this.table(collection) + "\" where key = ? for update";
        String updateExpr = UPDATE_RENDERER.render(operations);
        String updateSql = "update \"" + this.table(collection) + "\" set value = " + updateExpr + " where key = ?";

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

        String updateExpr = UPDATE_RENDERER.render(filter.getOperations());
        String sql = "update \"" + this.table(collection) + "\" set value = " + updateExpr +
            " where " + FILTER_RENDERER.renderCondition(filter.getWhere());

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
        String sql = "insert into \"" + this.table(collection) + "\" (key, value) values (?, ?::jsonb) " +
            "on conflict(key) do update set value = EXCLUDED.value";

        try (Connection connection = this.dataSource.getConnection();
             PreparedStatement prepared = connection.prepareStatement(this.debugQuery(sql))) {
            prepared.setString(1, path.getValue());
            prepared.setString(2, json);
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

        String sql = "insert into \"" + this.table(collection) + "\" (key, value) values (?, ?::jsonb) " +
            "on conflict(key) do update set value = EXCLUDED.value";

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
        String sql = "delete from \"" + this.table(collection) + "\" where key = ?";

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

        String keys = paths.stream().map(k -> "?").collect(Collectors.joining(", "));
        String sql = "delete from \"" + this.table(collection) + "\" where key in (" + keys + ")";

        try (Connection connection = this.dataSource.getConnection();
             PreparedStatement prepared = connection.prepareStatement(this.debugQuery(sql))) {
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
    public boolean deleteAll(@NonNull PersistenceCollection collection) {
        this.checkCollectionRegistered(collection);
        String sql = "truncate table \"" + this.table(collection) + "\"";

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
