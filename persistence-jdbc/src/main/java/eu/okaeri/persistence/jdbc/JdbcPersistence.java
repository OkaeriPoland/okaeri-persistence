package eu.okaeri.persistence.jdbc;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import eu.okaeri.persistence.PersistenceCollection;
import eu.okaeri.persistence.PersistenceEntity;
import eu.okaeri.persistence.PersistencePath;
import eu.okaeri.persistence.document.index.IndexProperty;
import eu.okaeri.persistence.raw.RawPersistence;
import eu.okaeri.persistence.raw.PersistenceIndexMode;
import eu.okaeri.persistence.raw.PersistencePropertyMode;
import lombok.Getter;
import lombok.NonNull;
import lombok.SneakyThrows;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Predicate;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class JdbcPersistence extends RawPersistence {

    private static final Logger LOGGER = Logger.getLogger(JdbcPersistence.class.getSimpleName());
    @Getter protected HikariDataSource dataSource;

    protected JdbcPersistence(@NonNull PersistencePath basePath, @NonNull HikariConfig hikariConfig, @NonNull PersistencePropertyMode propertyMode, @NonNull PersistenceIndexMode indexMode) {
        super(basePath, propertyMode, indexMode);
        this.connect(hikariConfig);
    }

    protected JdbcPersistence(@NonNull PersistencePath basePath, @NonNull HikariDataSource dataSource, @NonNull PersistencePropertyMode propertyMode, @NonNull PersistenceIndexMode indexMode) {
        super(basePath, propertyMode, indexMode);
        this.dataSource = dataSource;
    }

    public JdbcPersistence(@NonNull PersistencePath basePath, @NonNull HikariConfig hikariConfig) {
        this(basePath, hikariConfig, PersistencePropertyMode.TOSTRING, PersistenceIndexMode.EMULATED);
    }

    public JdbcPersistence(@NonNull PersistencePath basePath, @NonNull HikariDataSource dataSource) {
        this(basePath, dataSource, PersistencePropertyMode.TOSTRING, PersistenceIndexMode.EMULATED);
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

        String sql = "create table if not exists `" + collectionTable + "` (" +
            "`key` varchar(" + keyLength + ") primary key not null," +
            "`value` text not null)";
        String alterKeySql = "alter table `" + collectionTable + "` MODIFY COLUMN `key` varchar(" + keyLength + ") not null";

        try (Connection connection = this.getDataSource().getConnection()) {
            connection.createStatement().execute(sql);
            connection.createStatement().execute(alterKeySql);
        } catch (SQLException exception) {
            throw new RuntimeException("cannot register collection", exception);
        }

        super.registerCollection(collection);
    }

    @Override
    public Set<PersistencePath> findMissingIndexes(@NonNull PersistenceCollection collection, @NonNull Set<IndexProperty> indexProperties) {
        // No-op: native indexing handled by subclasses
        return Collections.emptySet();
    }

    @Override
    public boolean updateIndex(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull IndexProperty property, String identity) {
        // No-op: native indexing handled by subclasses (generated columns auto-update)
        return true;
    }

    @Override
    public boolean dropIndex(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull IndexProperty property) {
        // No-op: native indexing handled by subclasses
        return true;
    }

    @Override
    public boolean dropIndex(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {
        // No-op: native indexing handled by subclasses
        return true;
    }

    @Override
    public boolean dropIndex(@NonNull PersistenceCollection collection, @NonNull IndexProperty property) {
        // No-op: native indexing handled by subclasses
        return true;
    }

    @Override
    public Optional<String> read(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {

        this.checkCollectionRegistered(collection);
        String sql = "select `value` from `" + this.table(collection) + "` where `key` = ? limit 1";

        try (Connection connection = this.getDataSource().getConnection()) {
            PreparedStatement prepared = connection.prepareStatement(sql);
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
        String keys = paths.stream().map(path -> "`key` = ?").collect(Collectors.joining(" or "));
        String sql = "select `key`, `value` from `" + this.table(collection) + "` where " + keys;
        Map<PersistencePath, String> map = new LinkedHashMap<>();

        try (Connection connection = this.getDataSource().getConnection()) {
            PreparedStatement prepared = connection.prepareStatement(sql);
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

        this.checkCollectionRegistered(collection);
        String sql = "select `key`, `value` from `" + this.table(collection) + "`";
        Map<PersistencePath, String> map = new LinkedHashMap<>();

        try (Connection connection = this.getDataSource().getConnection()) {

            PreparedStatement prepared = connection.prepareStatement(sql);
            ResultSet resultSet = prepared.executeQuery();

            while (resultSet.next()) {
                String key = resultSet.getString("key");
                String value = resultSet.getString("value");
                map.put(PersistencePath.of(key), value);
            }
        } catch (SQLException exception) {
            throw new RuntimeException("cannot read all from " + collection, exception);
        }

        return map;
    }

    @Override
    public Stream<PersistenceEntity<String>> streamAll(@NonNull PersistenceCollection collection) {

        this.checkCollectionRegistered(collection);
        String sql = "select `key`, `value` from `" + this.table(collection) + "`";

        try (Connection connection = this.getDataSource().getConnection()) {

            PreparedStatement prepared = connection.prepareStatement(sql);
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
    public long count(@NonNull PersistenceCollection collection) {

        this.checkCollectionRegistered(collection);
        String sql = "select count(1) from `" + this.table(collection) + "`";

        try (Connection connection = this.getDataSource().getConnection()) {
            PreparedStatement prepared = connection.prepareStatement(sql);
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
        String sql = "select 1 from `" + this.table(collection) + "` where `key` = ? limit 1";

        try (Connection connection = this.getDataSource().getConnection()) {
            PreparedStatement prepared = connection.prepareStatement(sql);
            prepared.setString(1, path.getValue());
            ResultSet resultSet = prepared.executeQuery();
            return resultSet.next();
        } catch (SQLException exception) {
            throw new RuntimeException("cannot check if " + path + " exists in " + collection, exception);
        }
    }

    @Override
    public boolean write(@NonNull PersistenceCollection collection, @NonNull PersistencePath path, @NonNull String raw) {

        if (this.read(collection, path).isPresent()) {
            String sql = "update `" + this.table(collection) + "` set `value` = ? where `key` = ?";
            try (Connection connection = this.getDataSource().getConnection()) {
                PreparedStatement prepared = connection.prepareStatement(sql);
                prepared.setString(1, raw);
                prepared.setString(2, path.getValue());
                return prepared.executeUpdate() > 0;
            } catch (SQLException exception) {
                throw new RuntimeException("cannot write " + path + " to " + collection, exception);
            }
        }

        String sql = "insert into `" + this.table(collection) + "` (`key`, `value`) values (?, ?)";
        try (Connection connection = this.getDataSource().getConnection()) {
            PreparedStatement prepared = connection.prepareStatement(sql);
            prepared.setString(1, path.getValue());
            prepared.setString(2, raw);
            return prepared.executeUpdate() > 0;
        } catch (SQLException exception) {
            throw new RuntimeException("cannot write " + path + " to " + collection, exception);
        }
    }

    @Override
    public boolean delete(@NonNull PersistenceCollection collection, @NonNull PersistencePath path) {

        this.checkCollectionRegistered(collection);
        String sql = "delete from `" + this.table(collection) + "` where `key` = ?";

        try (Connection connection = this.getDataSource().getConnection()) {
            PreparedStatement prepared = connection.prepareStatement(sql);
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

        String keys = paths.stream().map(path -> "`key` = ?").collect(Collectors.joining(" or "));
        String deleteSql = "delete from `" + this.table(collection) + "` where " + keys;

        try (Connection connection = this.getDataSource().getConnection()) {
            PreparedStatement prepared = connection.prepareStatement(deleteSql);
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
        String sql = "truncate table `" + this.table(collection) + "`";

        try (Connection connection = this.getDataSource().getConnection()) {
            PreparedStatement prepared = connection.prepareStatement(sql);
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
    public void close() throws IOException {
        this.getDataSource().close();
    }

    protected String table(PersistenceCollection collection) {
        return this.getBasePath().sub(collection).toSqlIdentifier();
    }

    /**
     * @deprecated Legacy emulated index table - kept for migration purposes only
     */
    @Deprecated
    protected String indexTable(PersistenceCollection collection) {
        return this.getBasePath().sub(collection).sub("index").toSqlIdentifier();
    }
}
