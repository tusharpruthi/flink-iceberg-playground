package com.hevo.icebergplayground.job;

import com.hevo.icebergplayground.catalog.IcebergCatalogProvider;
import com.hevo.icebergplayground.catalog.IcebergCatalogProviderFactory;
import com.hevo.icebergplayground.catalog.IcebergTableProvisioner;
import com.hevo.icebergplayground.cdc.ChangeDataSource;
import com.hevo.icebergplayground.cdc.ChangeDataSourceFactory;
import com.hevo.icebergplayground.cdc.ChangeRecord;
import com.hevo.icebergplayground.config.AppConfig;
import com.hevo.icebergplayground.config.ConfigLoader;
import com.hevo.icebergplayground.config.SourceDatabaseConfig;
import com.hevo.icebergplayground.schema.TableSchemaResolver;
import com.hevo.icebergplayground.sink.ChangelogToIcebergWriter;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.iceberg.catalog.Catalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Entrypoint for one batch run: read whatever WAL changes have accumulated on the configured
 * source tables since the last run, and write them into their Iceberg tables. Designed to be
 * triggered every 5 minutes by an external scheduler (cron, Airflow, ...) — the job process
 * starts, does one pass over all configured tables, and exits.
 */
public final class IcebergWalSyncJob {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergWalSyncJob.class);

    private IcebergWalSyncJob() {
    }

    public static void main(String[] args) {
        String environment = args.length > 0 ? args[0] : System.getProperty("app.environment", "local");
        LOG.info("Starting Iceberg WAL sync job for environment '{}'", environment);
        AppConfig config = ConfigLoader.load(environment);
        run(config);
        LOG.info("Iceberg WAL sync job run complete");
    }

    static void run(AppConfig config) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        IcebergCatalogProvider catalogProvider = IcebergCatalogProviderFactory.create(config.getCatalog());
        Catalog icebergCatalog = catalogProvider.catalog();

        TableSyncTask task = new TableSyncTask(icebergCatalog, catalogProvider.catalogLoader(), env,
                new TableSchemaResolver(), new IcebergTableProvisioner(), new ChangelogToIcebergWriter());

        SourceDatabaseConfig sourceConfig = config.getSource();
        try (ChangeDataSource changeDataSource = ChangeDataSourceFactory.create(sourceConfig);
             Connection metadataConnection = openMetadataConnection(sourceConfig)) {

            List<ChangeRecord> capturedChanges = changeDataSource.fetchChangesSinceLastRun();
            var changesByTable = capturedChanges.stream()
                    .collect(Collectors.groupingBy(ChangeRecord::getTableName));

            for (String tableName : sourceConfig.tableNames()) {
                List<ChangeRecord> changesForTable = changesByTable.getOrDefault(tableName, List.of());
                task.sync(metadataConnection, sourceConfig.getSchema(), tableName, changesForTable);
            }

            // Only advance the source's position once every table's changes are durably in
            // Iceberg — if any sync above threw, the source keeps returning this same batch on
            // the next run rather than silently dropping it.
            changeDataSource.confirmChangesProcessed();
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to open metadata connection to source database", e);
        }
    }

    private static Connection openMetadataConnection(SourceDatabaseConfig sourceConfig) throws SQLException {
        Properties properties = new Properties();
        properties.setProperty("user", sourceConfig.getUsername());
        properties.setProperty("password", sourceConfig.getPassword());
        return DriverManager.getConnection(sourceConfig.jdbcUrl(), properties);
    }
}
