package com.hevo.icebergplayground.sink;

import com.hevo.icebergplayground.cdc.ChangeRecord;
import com.hevo.icebergplayground.schema.ResolvedTableSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Writes a batch of {@link ChangeRecord}s for one table into its Iceberg table via Iceberg's
 * {@code FlinkSink} DataStream API — a bounded {@code DataStream<RowData>} carrying the right
 * {@link org.apache.flink.types.RowKind} per record, written directly through the equality-delete
 * / upsert writer. This deliberately bypasses the Table API's {@code fromChangelogStream} +
 * {@code INSERT INTO} path: that path runs changelog input through a stateful normalization
 * operator that expects to have seen an INSERT for a key before it accepts a DELETE/UPDATE_AFTER
 * for it. Since every run is a fresh job with no memory of earlier runs, a batch containing only
 * deletes (the row was inserted in a *previous* run) would silently produce no write at all.
 * {@code FlinkSink}'s writer has no such expectation — it applies each row's RowKind directly.
 */
public class ChangelogToIcebergWriter {

    private static final Logger LOG = LoggerFactory.getLogger(ChangelogToIcebergWriter.class);

    private final ChangeRecordRowConverter rowConverter = new ChangeRecordRowConverter();

    public void write(StreamExecutionEnvironment env, CatalogLoader catalogLoader, String namespace, String tableName,
                       ResolvedTableSchema resolvedSchema, List<ChangeRecord> changeRecords) {
        if (changeRecords.isEmpty()) {
            LOG.info("No changes to write for {}.{}", namespace, tableName);
            return;
        }

        RowType rowType = FlinkSchemaUtil.convert(resolvedSchema.getIcebergSchema());
        List<RowData> rows = changeRecords.stream()
                .map(record -> (RowData) rowConverter.toRowData(record, resolvedSchema))
                .collect(Collectors.toList());
        DataStream<RowData> changelogStream = env.fromCollection(rows, InternalTypeInfo.of(rowType));

        TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, TableIdentifier.of(namespace, tableName));

        LOG.info("Writing {} change(s) into {}.{}.{}", rows.size(), catalogLoader, namespace, tableName);
        FlinkSink.forRowData(changelogStream)
                .tableLoader(tableLoader)
                .equalityFieldColumns(resolvedSchema.getPrimaryKeyColumnNames())
                .upsert(true)
                .append();

        try {
            env.execute("iceberg-wal-sync:" + namespace + "." + tableName);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to write changes to Iceberg table " + namespace + "." + tableName, e);
        }
    }
}
