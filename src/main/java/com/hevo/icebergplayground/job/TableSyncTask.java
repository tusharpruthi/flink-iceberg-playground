package com.hevo.icebergplayground.job;

import com.hevo.icebergplayground.cdc.ChangeRecord;
import com.hevo.icebergplayground.catalog.IcebergTableProvisioner;
import com.hevo.icebergplayground.schema.ResolvedTableSchema;
import com.hevo.icebergplayground.schema.TableSchemaResolver;
import com.hevo.icebergplayground.sink.ChangelogToIcebergWriter;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.flink.CatalogLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.List;

/**
 * Orchestrates a single source table's sync for one job run: resolve its current schema,
 * make sure the Iceberg table exists (or evolve it) to match, then write whatever changes were
 * captured for it. Kept deliberately thin — each step is delegated to a single-purpose
 * collaborator so this class only sequences them.
 */
public class TableSyncTask {

    private static final Logger LOG = LoggerFactory.getLogger(TableSyncTask.class);

    private final Catalog icebergCatalog;
    private final CatalogLoader catalogLoader;
    private final StreamExecutionEnvironment env;
    private final TableSchemaResolver schemaResolver;
    private final IcebergTableProvisioner tableProvisioner;
    private final ChangelogToIcebergWriter changelogWriter;

    public TableSyncTask(Catalog icebergCatalog, CatalogLoader catalogLoader, StreamExecutionEnvironment env,
                          TableSchemaResolver schemaResolver, IcebergTableProvisioner tableProvisioner,
                          ChangelogToIcebergWriter changelogWriter) {
        this.icebergCatalog = icebergCatalog;
        this.catalogLoader = catalogLoader;
        this.env = env;
        this.schemaResolver = schemaResolver;
        this.tableProvisioner = tableProvisioner;
        this.changelogWriter = changelogWriter;
    }

    public void sync(Connection metadataConnection, String schemaName, String tableName, List<ChangeRecord> changeRecords) {
        LOG.info("Syncing {}.{}: {} change(s) captured", schemaName, tableName, changeRecords.size());
        ResolvedTableSchema resolvedSchema = schemaResolver.resolve(metadataConnection, schemaName, tableName);
        tableProvisioner.ensureTableExists(icebergCatalog, schemaName, tableName, resolvedSchema);
        changelogWriter.write(env, catalogLoader, schemaName, tableName, resolvedSchema, changeRecords);
    }
}
