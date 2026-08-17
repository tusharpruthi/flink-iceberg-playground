package com.hevo.icebergplayground.cdc.postgres;

import com.hevo.icebergplayground.cdc.ChangeDataSource;
import com.hevo.icebergplayground.cdc.ChangeRecord;
import com.hevo.icebergplayground.config.SourceDatabaseConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Reads accumulated changes off a Postgres logical replication slot using the {@code wal2json}
 * output plugin, via plain JDBC calls. Fetching uses {@code pg_logical_slot_peek_changes}, which
 * does not consume anything — the slot's position is only advanced by
 * {@link #confirmChangesProcessed()}, once the caller has durably written the fetched changes
 * downstream. Consuming eagerly on fetch (via {@code pg_logical_slot_get_changes}) would drop
 * changes for good if the downstream write then failed, since there would be no way to ask
 * Postgres for them again.
 */
public class PostgresWalChangeDataSource implements ChangeDataSource {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresWalChangeDataSource.class);
    private static final String WAL2JSON_PLUGIN = "wal2json";

    private final SourceDatabaseConfig config;
    private final Set<String> trackedTableNames;
    private final WalJsonChangeRecordParser parser;
    private final Connection connection;
    private String lastPeekedLsn;

    public PostgresWalChangeDataSource(SourceDatabaseConfig config) {
        this.config = config;
        this.trackedTableNames = config.tableNames().stream().collect(Collectors.toSet());
        this.parser = new WalJsonChangeRecordParser(trackedTableNames);
        this.connection = openConnection(config);
        ensureReplicationSlotExists(connection, config.getReplicationSlotName());
    }

    @Override
    public List<ChangeRecord> fetchChangesSinceLastRun() {
        List<ChangeRecord> changeRecords = new ArrayList<>();
        lastPeekedLsn = null;
        String sql = "SELECT lsn, data FROM pg_logical_slot_peek_changes(?, NULL, NULL)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, config.getReplicationSlotName());
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    lastPeekedLsn = resultSet.getString("lsn");
                    changeRecords.addAll(parser.parse(resultSet.getString("data")));
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException(
                    "Failed to read changes from replication slot '" + config.getReplicationSlotName() + "'", e);
        }
        LOG.info("Fetched {} tracked-table change(s) from slot '{}'", changeRecords.size(), config.getReplicationSlotName());
        return changeRecords;
    }

    @Override
    public void confirmChangesProcessed() {
        if (lastPeekedLsn == null) {
            return;
        }
        String sql = "SELECT pg_replication_slot_advance(?, ?::pg_lsn)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, config.getReplicationSlotName());
            statement.setString(2, lastPeekedLsn);
            statement.execute();
            LOG.info("Advanced slot '{}' to {}", config.getReplicationSlotName(), lastPeekedLsn);
            lastPeekedLsn = null;
        } catch (SQLException e) {
            throw new IllegalStateException(
                    "Failed to advance replication slot '" + config.getReplicationSlotName() + "' to " + lastPeekedLsn, e);
        }
    }

    private static Connection openConnection(SourceDatabaseConfig config) {
        try {
            Properties properties = new Properties();
            properties.setProperty("user", config.getUsername());
            properties.setProperty("password", config.getPassword());
            return DriverManager.getConnection(config.jdbcUrl(), properties);
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to connect to source Postgres database at " + config.jdbcUrl(), e);
        }
    }

    private static void ensureReplicationSlotExists(Connection connection, String slotName) {
        String existsSql = "SELECT 1 FROM pg_replication_slots WHERE slot_name = ?";
        try (PreparedStatement statement = connection.prepareStatement(existsSql)) {
            statement.setString(1, slotName);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    return;
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to check for existing replication slot '" + slotName + "'", e);
        }

        String createSql = "SELECT pg_create_logical_replication_slot(?, ?)";
        try (PreparedStatement statement = connection.prepareStatement(createSql)) {
            statement.setString(1, slotName);
            statement.setString(2, WAL2JSON_PLUGIN);
            statement.execute();
            LOG.info("Created logical replication slot '{}' using the {} plugin", slotName, WAL2JSON_PLUGIN);
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to create replication slot '" + slotName + "'", e);
        }
    }

    @Override
    public void close() {
        try {
            connection.close();
        } catch (SQLException e) {
            LOG.warn("Failed to close Postgres connection cleanly", e);
        }
    }
}
