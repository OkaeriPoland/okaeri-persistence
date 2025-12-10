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
import eu.okaeri.persistence.jdbc.filter.H2FilterRenderer;
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
 * H2 database persistence backend with native JSON filtering.
 * Uses H2's built-in JSON functions for all queries - no in-memory indexing needed.
 */
public class H2Persistence implements Persistence, FilterablePersistence, StreamablePersistence {

    private static final boolean DEBUG = Boolean.parseBoolean(System.getProperty("okaeri.platform.debug", "false"));
    private static final Logger LOGGER = Logger.getLogger(H2Persistence.class.getSimpleName());
    private static final int BATCH_SIZE = 1000;

    private final @Getter PersistencePath basePath;
    private @Getter HikariDataSource dataSource;
    private final @Getter DocumentSerializer serializer;

    private final H2FilterRenderer filterRenderer;
    private final Map<String, PersistenceCollection> knownCollections = new ConcurrentHashMap<>();

    public H2Persistence(@NonNull PersistencePath basePath, @NonNull HikariConfig hikariConfig,
                         @NonNull Configurer configurer, @NonNull OkaeriSerdes... serdes) {
        this.basePath = basePath;
        this.serializer = new DocumentSerializer(configurer, serdes);
        this.filterRenderer = new H2FilterRenderer(new SqlStringRenderer());
        this.connect(hikariConfig);
    }

    public H2Persistence(@NonNull PersistencePath basePath, @NonNull HikariDataSource dataSource,
                         @NonNull Configurer configurer, @NonNull OkaeriSerdes... serdes) {
        this.basePath = basePath;
        this.dataSource = dataSource;
        this.serializer = new DocumentSerializer(configurer, serdes);
        this.filterRenderer = new H2FilterRenderer(new SqlStringRenderer());
    }

    public H2Persistence(@NonNull HikariConfig hikariConfig,
                         @NonNull Configurer configurer, @NonNull OkaeriSerdes... serdes) {
        this(PersistencePath.of(""), hikariConfig, configurer, serdes);
    }

    public H2Persistence(@NonNull HikariDataSource dataSource,
                         @NonNull Configurer configurer, @NonNull OkaeriSerdes... serdes) {
        this(PersistencePath.of(""), dataSource, configurer, serdes);
    }

    public H2Persistence(@NonNull PersistencePath basePath, @NonNull HikariConfig hikariConfig,
                         @NonNull DocumentSerializerConfig serializerConfig) {
        this.basePath = basePath;
        this.serializer = new DocumentSerializer(serializerConfig);
        this.filterRenderer = new H2FilterRenderer(new SqlStringRenderer());
        this.connect(hikariConfig);
    }

    public H2Persistence(@NonNull PersistencePath basePath, @NonNull HikariDataSource dataSource,
                         @NonNull DocumentSerializerConfig serializerConfig) {
        this.basePath = basePath;
        this.dataSource = dataSource;
        this.serializer = new DocumentSerializer(serializerConfig);
        this.filterRenderer = new H2FilterRenderer(new SqlStringRenderer());
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends PersistenceBuilder<Builder, H2Persistence> {
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
        public H2Persistence build() {
            if ((this.hikariConfig == null) && (this.dataSource == null)) {
                throw new IllegalStateException("hikariConfig or dataSource is required");
            }
            if ((this.hikariConfig != null) && (this.dataSource != null)) {
                throw new IllegalStateException("hikariConfig and dataSource are mutually exclusive");
            }

            DocumentSerializerConfig serializerConfig = this.buildSerializerConfig();
            PersistencePath path = this.resolveBasePath();

            if (this.dataSource != null) {
                return new H2Persistence(path, this.dataSource, serializerConfig);
            }
            return new H2Persistence(path, this.hikariConfig, serializerConfig);
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

        // Create table with JSON column type
        String createSql = "create table if not exists `" + tableName + "` (" +
            "`key` varchar(" + keyLength + ") primary key not null," +
            "`value` JSON not null)";

        try (Connection connection = this.dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute(this.debugQuery(createSql));
        } catch (SQLException exception) {
            throw new RuntimeException("cannot create collection table " + tableName, exception);
        }

        // Migrate TEXT column to JSON type if needed (legacy migration)
        this.migrateValueColumnToJson(tableName);

        // Drop legacy emulated index table if exists
        this.dropLegacyIndexTable(collection);

        // Track collection
        this.knownCollections.put(collection.getValue(), collection);
    }

    private void migrateValueColumnToJson(@NonNull String tableName) {
        try (Connection connection = this.dataSource.getConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            try (ResultSet columns = metaData.getColumns(null, null, tableName.toUpperCase(), "VALUE")) {
                if (columns.next()) {
                    String columnType = columns.getString("TYPE_NAME");
                    if ("VARCHAR".equalsIgnoreCase(columnType) ||
                        "TEXT".equalsIgnoreCase(columnType) ||
                        "CLOB".equalsIgnoreCase(columnType) ||
                        "CHARACTER LARGE OBJECT".equalsIgnoreCase(columnType) ||
                        "CHARACTER VARYING".equalsIgnoreCase(columnType)) {

                        String migrateSql = "alter table `" + tableName + "` alter column `value` JSON";
                        try (Statement statement = connection.createStatement()) {
                            statement.execute(this.debugQuery(migrateSql));
                        }
                    }
                }
            }
        } catch (SQLException exception) {
            throw new RuntimeException("cannot migrate collection " + tableName + " to JSON type", exception);
        }
    }

    private void dropLegacyIndexTable(@NonNull PersistenceCollection collection) {
        String indexTable = this.basePath.sub(collection).sub("index").toSqlIdentifier();
        try (Connection connection = this.dataSource.getConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            try (ResultSet tables = metaData.getTables(null, null, indexTable.toUpperCase(), new String[]{"TABLE"})) {
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
            System.out.println("[H2] " + sql);
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
                String sql = baseQuery + " limit " + batchSize + " offset " + this.offset;
                this.offset += batchSize;

                try (Connection connection = H2Persistence.this.dataSource.getConnection();
                     Statement statement = connection.createStatement();
                     ResultSet resultSet = statement.executeQuery(H2Persistence.this.debugQuery(sql))) {
                    List<PersistenceEntity<Document>> batch = new ArrayList<>();

                    while (resultSet.next()) {
                        String key = resultSet.getString("key");
                        String json = resultSet.getString("value");
                        PersistencePath path = PersistencePath.of(key);
                        Document doc = H2Persistence.this.serializer.deserialize(collection, path, json);
                        batch.add(new PersistenceEntity<>(path, doc));
                    }

                    this.currentBatch = batch.iterator();
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
            throw new RuntimeException("cannot find in " + collection, exception);
        }
    }

    @Override
    public long delete(@NonNull PersistenceCollection collection, @NonNull DeleteFilter filter) {
        this.checkCollectionRegistered(collection);

        if (filter.getWhere() == null) {
            throw new IllegalArgumentException("DeleteFilter requires WHERE condition - use deleteAll() instead");
        }

        String sql = "delete from `" + this.table(collection) + "` where " +
            this.filterRenderer.renderCondition(filter.getWhere());

        try (Connection connection = this.dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            return statement.executeUpdate(this.debugQuery(sql));
        } catch (SQLException exception) {
            throw new RuntimeException("cannot delete from " + collection + " with " + filter, exception);
        }
    }

    // ==================== WRITE OPERATIONS ====================

    @Override
    public boolean write(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull Document document) {
        this.checkCollectionRegistered(collection);
        this.serializer.setupDocument(document, collection, path);

        String json = this.serializer.serialize(document);
        String sql = "insert into `" + this.table(collection) + "` (`key`, `value`) values (?, ? format json) " +
            "on duplicate key update `value` = ? format json";

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

        String sql = "insert into `" + this.table(collection) + "` (`key`, `value`) values (?, ? format json) " +
            "on duplicate key update `value` = ? format json";

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
