package com.hevo.icebergplayground.schema;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Reads a source table's column and primary-key metadata over JDBC and translates it into a
 * {@link ResolvedTableSchema}. Column type translation is delegated to
 * {@link PostgresTypeToIcebergTypeMapper} so this class stays engine-agnostic in shape (a MySQL
 * variant would only need to swap the mapper).
 */
public class TableSchemaResolver {

    /**
     * Synthetic column appended to every synced table, stamped with the wall-clock time of the
     * write (not sourced from the CDC payload) so it's visible in Iceberg when a row was last
     * upserted. See {@link com.hevo.icebergplayground.sink.ChangeRecordRowConverter}, which
     * special-cases this column name when building row values.
     */
    public static final String LAST_UPDATED_AT_COLUMN = "last_updated_at";

    public ResolvedTableSchema resolve(Connection connection, String schemaName, String tableName) {
        try {
            Set<String> primaryKeyColumnNames = readPrimaryKeyColumnNames(connection, schemaName, tableName);
            List<Types.NestedField> fields = new ArrayList<>();
            List<String> orderedColumnNames = new ArrayList<>();
            List<Integer> identifierFieldIds = new ArrayList<>();

            DatabaseMetaData metaData = connection.getMetaData();
            int nextFieldId = 1;
            try (ResultSet columns = metaData.getColumns(null, schemaName, tableName, null)) {
                while (columns.next()) {
                    String columnName = columns.getString("COLUMN_NAME");
                    String typeName = columns.getString("TYPE_NAME");
                    int precision = columns.getInt("COLUMN_SIZE");
                    int scale = columns.getInt("DECIMAL_DIGITS");
                    boolean isPrimaryKey = primaryKeyColumnNames.contains(columnName);

                    Type icebergType = PostgresTypeToIcebergTypeMapper.map(typeName, precision, scale);
                    int fieldId = nextFieldId++;
                    // Non-key columns are always optional in Iceberg, regardless of the source's
                    // NOT NULL constraint: without REPLICA IDENTITY FULL, a WAL delete only ever
                    // carries the primary key, so a delete's changelog row has null in every other
                    // column. Mirroring the source's NOT NULL onto those columns would make such
                    // rows constraint-violations and silently drop the delete instead of applying it.
                    Types.NestedField field = isPrimaryKey
                            ? Types.NestedField.required(fieldId, columnName, icebergType)
                            : Types.NestedField.optional(fieldId, columnName, icebergType);
                    fields.add(field);
                    orderedColumnNames.add(columnName);
                    if (isPrimaryKey) {
                        identifierFieldIds.add(fieldId);
                    }
                }
            }

            fields.add(Types.NestedField.optional(nextFieldId++, LAST_UPDATED_AT_COLUMN, Types.TimestampType.withoutZone()));
            orderedColumnNames.add(LAST_UPDATED_AT_COLUMN);

            if (fields.isEmpty()) {
                throw new IllegalStateException(
                        "No columns found for table '" + schemaName + "." + tableName + "' — does it exist?");
            }
            if (identifierFieldIds.isEmpty()) {
                throw new IllegalStateException(
                        "Table '" + schemaName + "." + tableName + "' has no primary key; a primary key is required "
                                + "so CDC updates/deletes can be applied as Iceberg upserts");
            }

            Schema icebergSchema = new Schema(fields, Set.copyOf(identifierFieldIds));
            return new ResolvedTableSchema(icebergSchema, orderedColumnNames, List.copyOf(primaryKeyColumnNames));
        } catch (SQLException e) {
            throw new IllegalStateException(
                    "Failed to resolve schema for table '" + schemaName + "." + tableName + "'", e);
        }
    }

    private Set<String> readPrimaryKeyColumnNames(Connection connection, String schemaName, String tableName) throws SQLException {
        Set<String> primaryKeyColumnNames = new LinkedHashSet<>();
        try (ResultSet primaryKeys = connection.getMetaData().getPrimaryKeys(null, schemaName, tableName)) {
            while (primaryKeys.next()) {
                primaryKeyColumnNames.add(primaryKeys.getString("COLUMN_NAME"));
            }
        }
        return primaryKeyColumnNames;
    }
}
