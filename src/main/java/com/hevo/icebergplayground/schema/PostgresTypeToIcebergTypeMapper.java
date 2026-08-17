package com.hevo.icebergplayground.schema;

import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maps a Postgres JDBC column type name to the equivalent Iceberg {@link Type}. Isolated behind
 * its own class so a future {@code MySqlTypeToIcebergTypeMapper} sibling can be dropped in next
 * to it without touching {@link TableSchemaResolver}.
 */
public final class PostgresTypeToIcebergTypeMapper {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresTypeToIcebergTypeMapper.class);

    private PostgresTypeToIcebergTypeMapper() {
    }

    public static Type map(String postgresTypeName, int precision, int scale) {
        String typeName = postgresTypeName.toLowerCase().trim();
        switch (typeName) {
            case "int2":
            case "smallint":
            case "int4":
            case "integer":
            case "serial":
                return Types.IntegerType.get();
            case "int8":
            case "bigint":
            case "bigserial":
                return Types.LongType.get();
            case "numeric":
            case "decimal":
                int effectivePrecision = precision > 0 ? Math.min(precision, 38) : 38;
                int effectiveScale = Math.max(scale, 0);
                return Types.DecimalType.of(effectivePrecision, effectiveScale);
            case "float4":
            case "real":
                return Types.FloatType.get();
            case "float8":
            case "double precision":
                return Types.DoubleType.get();
            case "bool":
            case "boolean":
                return Types.BooleanType.get();
            case "date":
                return Types.DateType.get();
            case "time":
            case "timetz":
                return Types.TimeType.get();
            case "timestamp":
                return Types.TimestampType.withoutZone();
            case "timestamptz":
                return Types.TimestampType.withZone();
            case "bytea":
                return Types.BinaryType.get();
            case "uuid":
            case "varchar":
            case "character varying":
            case "char":
            case "bpchar":
            case "text":
            case "json":
            case "jsonb":
                return Types.StringType.get();
            default:
                LOG.warn("No explicit Iceberg mapping for Postgres type '{}', defaulting to string", postgresTypeName);
                return Types.StringType.get();
        }
    }
}
