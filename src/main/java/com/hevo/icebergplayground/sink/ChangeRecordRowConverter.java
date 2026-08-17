package com.hevo.icebergplayground.sink;

import com.hevo.icebergplayground.cdc.ChangeRecord;
import com.hevo.icebergplayground.schema.ResolvedTableSchema;
import com.hevo.icebergplayground.schema.TableSchemaResolver;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.types.RowKind;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

/**
 * Converts a {@link ChangeRecord} into a Flink {@link GenericRowData} carrying the
 * {@link RowKind} the Iceberg {@code FlinkSink} write path needs (INSERT / UPDATE_AFTER /
 * DELETE), with every field coerced to Flink's internal representation for the matching Iceberg
 * column type.
 */
public final class ChangeRecordRowConverter {

    public GenericRowData toRowData(ChangeRecord changeRecord, ResolvedTableSchema resolvedSchema) {
        List<String> columnNames = resolvedSchema.getOrderedColumnNames();
        Schema icebergSchema = resolvedSchema.getIcebergSchema();
        RowKind rowKind;
        Map<String, Object> values;

        switch (changeRecord.getOperation()) {
            case INSERT:
                rowKind = RowKind.INSERT;
                values = changeRecord.getAfterValues();
                break;
            case UPDATE:
                rowKind = RowKind.UPDATE_AFTER;
                values = changeRecord.getAfterValues();
                break;
            case DELETE:
                rowKind = RowKind.DELETE;
                values = changeRecord.getBeforeValues();
                break;
            default:
                throw new IllegalStateException("Unhandled operation: " + changeRecord.getOperation());
        }

        GenericRowData row = new GenericRowData(rowKind, columnNames.size());
        for (int i = 0; i < columnNames.size(); i++) {
            String columnName = columnNames.get(i);
            if (TableSchemaResolver.LAST_UPDATED_AT_COLUMN.equals(columnName)) {
                row.setField(i, TimestampData.fromLocalDateTime(LocalDateTime.now(ZoneOffset.UTC)));
                continue;
            }
            Type icebergType = icebergSchema.findField(columnName).type();
            row.setField(i, ChangeValueRowDataCoercion.coerce(values.get(columnName), icebergType));
        }
        return row;
    }
}
