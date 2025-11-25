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
import eu.okaeri.persistence.filter.renderer.FilterRenderer;
import eu.okaeri.persistence.jdbc.filter.PostgresFilterRenderer;
import eu.okaeri.persistence.jdbc.filter.PostgresUpdateRenderer;
import eu.okaeri.persistence.jdbc.filter.SqlStringRenderer;
import eu.okaeri.persistence.raw.NativeRawPersistence;
import lombok.Getter;
import lombok.NonNull;
import lombok.SneakyThrows;

import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.function.Predicate;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class PostgresPersistence extends NativeRawPersistence {

    private static final Logger LOGGER = Logger.getLogger(PostgresPersistence.class.getSimpleName());
    private static final SqlStringRenderer STRING_RENDERER = new SqlStringRenderer();
    private static final FilterRenderer FILTER_RENDERER = new PostgresFilterRenderer(STRING_RENDERER); // TODO: allow customization
    private static final PostgresUpdateRenderer UPDATE_RENDERER = new PostgresUpdateRenderer(STRING_RENDERER);

    @Getter protected HikariDataSource dataSource;

    public PostgresPersistence(@NonNull PersistencePath basePath, @NonNull HikariConfig hikariConfig) {
        super(basePath);
        this.connect(hikariConfig);
    }

    public PostgresPersistence(@NonNull HikariConfig hikariConfig) {
        this(PersistencePath.of(""), hikariConfig);
    }

    public PostgresPersistence(@NonNull PersistencePath basePath, @NonNull HikariDataSource dataSource) {
        super(basePath);
        this.dataSource = dataSource;
    }

    public PostgresPersistence(@NonNull HikariDataSource dataSource) {
        this(PersistencePath.of(""), dataSource);
    }

    @SneakyThrows
    protected void connect(@NonNull HikariConfig hikariConfig) {
        do {
            try {
                this.dataSource = new HikariDataSource(hikariConfig);
            } catch (Exception exception) {
                if (exception.getCause() != null) {
                    LOGGER.severe("[" + this.getBasePath().getValue() + "] Cannot connect with database (waiting 30s): " + exception.getMessage() + " caused by " + exception.getCause().getMessage());
                } else {
                    LOGGER.severe("[" + this.getBasePath().getValue() + "] Cannot connect with database (waiting 30s): " + exception.getMessage());
                }
                Thread.sleep(30_000);
            }
        } while (this.dataSource == null);
    }

    @Override
    public void registerCollection(@NonNull PersistenceCollection collection) {

        String collectionTable = this.table(collection);
        int keyLength = collection.getKeyLength();

        String sql = "create table if not exists \"" + collectionTable + "\" (" +
            "key varchar(" + keyLength + ") primary key not null," +
            "value jsonb not null)";
        String alterKeySql = "alter table " + collectionTable + " alter column key type varchar(" + keyLength + ")";

        try (Connection connection = this.getDataSource().getConnection()) {
            connection.createStatement().execute(this.debugQuery(sql));
            connection.createStatement().execute(this.debugQuery(alterKeySql));
        } catch (SQLException exception) {
            throw new RuntimeException("cannot register collection", exception);
        }

        collection.getIndexes().forEach(index -> {

            String indexName = this.getBasePath().sub(collection).sub(index).sub("idx").toSqlIdentifier();
            PersistencePath indexPath = PersistencePath.of("value").sub(index);

            // Apply type cast based on field type for proper index usage
            // Must match expression used in queries (see PostgresFilterRenderer)
            String indexExpression;
            if (index.isNumeric()) {
                indexExpression = "((" + indexPath.toPostgresJsonPath() + ")::numeric)";
            } else if (index.isBoolean()) {
                indexExpression = "((" + indexPath.toPostgresJsonPath() + ")::boolean)";
            } else {
                // String fields: use ->> (text extraction) to match query expressions
                indexExpression = "(" + indexPath.toPostgresJsonPath(true) + ")";
            }

            try (Connection connection = this.getDataSource().getConnection()) {
                // Check if index exists and if it needs migration
                String checkSql = "select indexdef from pg_indexes where indexname = ?";
                PreparedStatement checkStmt = connection.prepareStatement(checkSql);
                checkStmt.setString(1, indexName);
                ResultSet rs = checkStmt.executeQuery();

                boolean needsCreate = true;
                if (rs.next()) {
                    String existingDef = rs.getString("indexdef");
                    // Check if existing index has the correct expression
                    // Normalize for comparison (PostgreSQL may format differently)
                    if (existingDef != null && existingDef.contains(indexExpression)) {
                        needsCreate = false; // Index already has correct expression
                    } else {
                        // Index exists but with different expression - drop it for migration
                        String dropSql = "drop index if exists " + indexName;
                        connection.createStatement().execute(this.debugQuery(dropSql));
                        LOGGER.info("Migrating index " + indexName + " to new expression: " + indexExpression);
                    }
                }
                rs.close();
                checkStmt.close();

                if (needsCreate) {
                    String indexSql = "create index " + indexName + " on " + collectionTable + " (" + indexExpression + ")";
                    connection.createStatement().execute(this.debugQuery(indexSql));
                }
            } catch (SQLException exception) {
                throw new RuntimeException("cannot register collection index " + indexName, exception);
            }
        });

        super.registerCollection(collection);
    }

    @Override
    public Stream<PersistenceEntity<String>> readByFilter(@NonNull PersistenceCollection collection, @NonNull FindFilter filter) {

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
            throw new RuntimeException("cannot ready by property from " + collection, exception);
        }
    }

    @Override
    public Optional<String> read(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {

        this.checkCollectionRegistered(collection);
        String sql = "select value from \"" + this.table(collection) + "\" where key = ? limit 1";

        try (Connection connection = this.getDataSource().getConnection()) {
            PreparedStatement prepared = connection.prepareStatement(this.debugQuery(sql));
            prepared.setString(1, path.getValue());
            ResultSet resultSet = prepared.executeQuery();
            if (resultSet.next()) {
                return Optional.ofNullable(resultSet.getString("value"));
            }
        } catch (SQLException exception) {
            throw new RuntimeException("cannot read " + path + " from " + collection, exception);
        }

        return Optional.empty();
    }

    @Override
    public Map<PersistencePath, String> read(@NonNull PersistenceCollection collection, @NonNull Collection<PersistencePath> paths) {

        this.checkCollectionRegistered(collection);
        String keys = paths.stream().map(key -> "?").collect(Collectors.joining(", "));
        String sql = "select key, value from \"" + this.table(collection) + "\" where key in (" + keys + ")";
        Map<PersistencePath, String> map = new LinkedHashMap<>();

        try (Connection connection = this.getDataSource().getConnection()) {
            PreparedStatement prepared = connection.prepareStatement(this.debugQuery(sql));
            int currentIndex = 1;
            for (PersistencePath path : paths) {
                prepared.setString(currentIndex++, path.getValue());
            }
            ResultSet resultSet = prepared.executeQuery();
            while (resultSet.next()) {
                String key = resultSet.getString("key");
                String value = resultSet.getString("value");
                map.put(PersistencePath.of(key), value);
            }
        } catch (SQLException exception) {
            throw new RuntimeException("cannot read " + paths + " from " + collection, exception);
        }

        return map;
    }

    @Override
    public Map<PersistencePath, String> readAll(@NonNull PersistenceCollection collection) {
        return this.streamAll(collection).collect(Collectors.toMap(
            PersistenceEntity::getPath,
            PersistenceEntity::getValue
        ));
    }

    @Override
    public Stream<PersistenceEntity<String>> streamAll(@NonNull PersistenceCollection collection) {

        this.checkCollectionRegistered(collection);
        String sql = "select key, value from \"" + this.table(collection) + "\"";

        try (Connection connection = this.getDataSource().getConnection()) {

            PreparedStatement prepared = connection.prepareStatement(this.debugQuery(sql));
            ResultSet resultSet = prepared.executeQuery();
            List<PersistenceEntity<String>> results = new ArrayList<>();

            while (resultSet.next()) {
                String key = resultSet.getString("key");
                String value = resultSet.getString("value");
                results.add(new PersistenceEntity<>(PersistencePath.of(key), value));
            }

            return results.stream();
        } catch (SQLException exception) {
            throw new RuntimeException("cannot stream all from " + collection, exception);
        }
    }

    @Override
    public Stream<PersistenceEntity<String>> stream(@NonNull PersistenceCollection collection, int batchSize) {

        this.checkCollectionRegistered(collection);
        String sql = "select key, value from \"" + this.table(collection) + "\"";

        try {
            Connection connection = this.getDataSource().getConnection();
            connection.setAutoCommit(false); // Required for cursor-based streaming in PostgreSQL

            PreparedStatement prepared = connection.prepareStatement(this.debugQuery(sql));
            prepared.setFetchSize(batchSize); // Hint for batch size - PostgreSQL will fetch in batches
            ResultSet resultSet = prepared.executeQuery();

            // Create iterator that fetches batches lazily
            Iterator<PersistenceEntity<String>> iterator = new Iterator<PersistenceEntity<String>>() {
                private boolean hasNextCached = false;
                private boolean nextCalled = false;

                @Override
                public boolean hasNext() {
                    if (this.nextCalled) {
                        return false;
                    }
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
                public PersistenceEntity<String> next() {
                    if (!this.hasNext()) {
                        throw new NoSuchElementException();
                    }
                    try {
                        String key = resultSet.getString("key");
                        String value = resultSet.getString("value");
                        this.hasNextCached = false;
                        return new PersistenceEntity<>(PersistencePath.of(key), value);
                    } catch (SQLException e) {
                        throw new RuntimeException("error reading result", e);
                    }
                }
            };

            return StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED),
                false
            ).onClose(() -> {
                // Clean up resources when stream is closed
                try {
                    resultSet.close();
                    prepared.close();
                    connection.close();
                } catch (SQLException ignored) {
                    // Log error but don't throw - we're in cleanup
                }
            });

        } catch (SQLException exception) {
            throw new RuntimeException("cannot stream from " + collection, exception);
        }
    }

    @Override
    public long count(@NonNull PersistenceCollection collection) {

        this.checkCollectionRegistered(collection);
        String sql = "select count(1) from \"" + this.table(collection) + "\"";

        try (Connection connection = this.getDataSource().getConnection()) {
            PreparedStatement prepared = connection.prepareStatement(this.debugQuery(sql));
            ResultSet resultSet = prepared.executeQuery();
            if (resultSet.next()) {
                return resultSet.getLong(1);
            }
        } catch (SQLException exception) {
            throw new RuntimeException("cannot count " + collection, exception);
        }

        return 0;
    }

    @Override
    public boolean exists(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {

        this.checkCollectionRegistered(collection);
        String sql = "select 1 from \"" + this.table(collection) + "\" where key = ? limit 1";

        try (Connection connection = this.getDataSource().getConnection()) {
            PreparedStatement prepared = connection.prepareStatement(this.debugQuery(sql));
            prepared.setString(1, path.getValue());
            ResultSet resultSet = prepared.executeQuery();
            return resultSet.next();
        } catch (SQLException exception) {
            throw new RuntimeException("cannot check if " + path + " exists in " + collection, exception);
        }
    }

    @Override
    public boolean write(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull String raw) {

        this.checkCollectionRegistered(collection);
        String sql = "insert into \"" + this.table(collection) + "\" (key, value) values (?, ?::json) on conflict(key) do update set value = EXCLUDED.value";

        try (Connection connection = this.getDataSource().getConnection()) {
            PreparedStatement prepared = connection.prepareStatement(this.debugQuery(sql));
            prepared.setString(1, path.getValue());
            prepared.setString(2, raw);
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
        String sql = "insert into \"" + this.table(collection) + "\" (key, value) values (?, ?::jsonb) on conflict(key) do update set value = EXCLUDED.value";

        try (Connection connection = this.getDataSource().getConnection()) {
            PreparedStatement prepared = connection.prepareStatement(this.debugQuery(sql));
            connection.setAutoCommit(false);
            for (Map.Entry<PersistencePath, String> entry : entities.entrySet()) {
                prepared.setString(1, entry.getKey().getValue());
                prepared.setString(2, entry.getValue());
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
    public boolean delete(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {

        this.checkCollectionRegistered(collection);
        String sql = "delete from \"" + this.table(collection) + "\" where key = ?";
        String key = path.getValue();

        Set<IndexProperty> collectionIndexes = this.getKnownIndexes().get(collection.getValue());
        if (collectionIndexes != null) {
            collectionIndexes.forEach(index -> this.dropIndex(collection, path));
        }

        try (Connection connection = this.getDataSource().getConnection()) {
            PreparedStatement prepared = connection.prepareStatement(this.debugQuery(sql));
            prepared.setString(1, key);
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

        String keys = paths.stream().map(key -> "?").collect(Collectors.joining(", "));
        String deleteSql = "delete from \"" + this.table(collection) + "\" where key in (" + keys + ")";

        try (Connection connection = this.getDataSource().getConnection()) {
            PreparedStatement prepared = connection.prepareStatement(this.debugQuery(deleteSql));
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

        try (Connection connection = this.getDataSource().getConnection()) {
            PreparedStatement prepared = connection.prepareStatement(this.debugQuery(sql));
            prepared.executeUpdate();
            return true;
        } catch (SQLException exception) {
            throw new RuntimeException("cannot truncate " + collection, exception);
        }
    }

    @Override
    public long deleteAll() {
        return this.getKnownCollections().values().stream()
        .map(this::deleteAll)
        .filter(Predicate.isEqual(true))
        .count();
    }

    @Override
    public long deleteByFilter(@NonNull PersistenceCollection collection, @NonNull DeleteFilter filter) {

        this.checkCollectionRegistered(collection);

        if (filter.getWhere() == null) {
            throw new IllegalArgumentException("deleteByFilter requires a WHERE condition - use deleteAll() to clear collection");
        }

        String sql = "delete from \"" + this.table(collection) + "\" where " + FILTER_RENDERER.renderCondition(filter.getWhere());

        try (Connection connection = this.getDataSource().getConnection()) {
            Statement statement = connection.createStatement();
            return statement.executeUpdate(this.debugQuery(sql));
        } catch (SQLException exception) {
            throw new RuntimeException("cannot delete from " + collection + " with " + filter, exception);
        }
    }

    // ===== UPDATE OPERATIONS =====

    @Override
    public boolean updateOne(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull List<UpdateOperation> operations) {
        this.checkCollectionRegistered(collection);

        String updateExpr = UPDATE_RENDERER.render(operations);
        String sql = "update \"" + this.table(collection) + "\" set value = " + updateExpr + " where key = ?";

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

        String updateExpr = UPDATE_RENDERER.render(operations);
        String sql = "update \"" + this.table(collection) + "\" set value = " + updateExpr + " where key = ? returning value";

        try (Connection connection = this.getDataSource().getConnection()) {
            PreparedStatement prepared = connection.prepareStatement(this.debugQuery(sql));
            prepared.setString(1, path.getValue());
            ResultSet resultSet = prepared.executeQuery();
            if (resultSet.next()) {
                return Optional.ofNullable(resultSet.getString("value"));
            }
        } catch (SQLException exception) {
            throw new RuntimeException("cannot update and get " + path + " in " + collection, exception);
        }

        return Optional.empty();
    }

    @Override
    public Optional<String> getAndUpdateOne(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull List<UpdateOperation> operations) {
        this.checkCollectionRegistered(collection);

        String selectSql = "select value from \"" + this.table(collection) + "\" where key = ? for update";
        String updateExpr = UPDATE_RENDERER.render(operations);
        String updateSql = "update \"" + this.table(collection) + "\" set value = " + updateExpr + " where key = ?";

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

        String updateExpr = UPDATE_RENDERER.render(filter.getOperations());
        String sql = "update \"" + this.table(collection) + "\" set value = " + updateExpr +
                     " where " + FILTER_RENDERER.renderCondition(filter.getWhere());

        try (Connection connection = this.getDataSource().getConnection()) {
            Statement statement = connection.createStatement();
            return statement.executeUpdate(this.debugQuery(sql));
        } catch (SQLException exception) {
            throw new RuntimeException("cannot update " + collection + " with " + filter, exception);
        }
    }

    @Override
    public void close() throws IOException {
        this.getDataSource().close();
    }

    protected String table(PersistenceCollection collection) {
        return this.getBasePath().sub(collection).toSqlIdentifier();
    }
}
