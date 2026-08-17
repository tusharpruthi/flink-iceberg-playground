package com.hevo.icebergplayground.sink;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Base64;

/**
 * Converts a raw value decoded from wal2json into Flink's internal {@code RowData} field
 * representation for a given Iceberg {@link Type} (e.g. {@code String} -> {@link StringData},
 * decimal text/number -> {@link DecimalData}), as required by {@link ChangeRecordRowConverter}
 * when building rows for Iceberg's {@code FlinkSink} DataStream API.
 */
final class ChangeValueRowDataCoercion {

    private static final DateTimeFormatter TIMESTAMP_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss[.SSSSSS][.SSS]");
    private static final DateTimeFormatter TIMESTAMPTZ_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss[.SSSSSS][.SSS]XXX");

    private ChangeValueRowDataCoercion() {
    }

    static Object coerce(Object rawValue, Type icebergType) {
        if (rawValue == null) {
            return null;
        }
        switch (icebergType.typeId()) {
            case INTEGER:
                return toNumber(rawValue).intValue();
            case LONG:
                return toNumber(rawValue).longValue();
            case FLOAT:
                return toNumber(rawValue).floatValue();
            case DOUBLE:
                return toNumber(rawValue).doubleValue();
            case DECIMAL:
                Types.DecimalType decimalType = (Types.DecimalType) icebergType;
                BigDecimal decimal = rawValue instanceof BigDecimal ? (BigDecimal) rawValue : new BigDecimal(rawValue.toString());
                return DecimalData.fromBigDecimal(decimal, decimalType.precision(), decimalType.scale());
            case BOOLEAN:
                return rawValue instanceof Boolean ? rawValue : Boolean.parseBoolean(rawValue.toString());
            case DATE:
                return (int) LocalDate.parse(rawValue.toString()).toEpochDay();
            case TIME:
                return LocalTime.parse(rawValue.toString()).toSecondOfDay() * 1000;
            case TIMESTAMP:
                return ((Types.TimestampType) icebergType).shouldAdjustToUTC()
                        ? TimestampData.fromInstant(toOffsetDateTime(rawValue.toString()).toInstant())
                        : TimestampData.fromLocalDateTime(LocalDateTime.parse(rawValue.toString(), TIMESTAMP_FORMAT));
            case BINARY:
            case FIXED:
                return decodeBinary(rawValue.toString());
            case STRING:
            default:
                return StringData.fromString(rawValue.toString());
        }
    }

    private static Number toNumber(Object rawValue) {
        if (rawValue instanceof Number) {
            return (Number) rawValue;
        }
        return new BigDecimal(rawValue.toString());
    }

    private static OffsetDateTime toOffsetDateTime(String value) {
        try {
            return OffsetDateTime.parse(value, TIMESTAMPTZ_FORMAT);
        } catch (Exception e) {
            return LocalDateTime.parse(value, TIMESTAMP_FORMAT).atOffset(ZoneOffset.UTC);
        }
    }

    /** wal2json renders {@code bytea} as a Postgres hex literal, e.g. {@code \x0a1b}. */
    private static byte[] decodeBinary(String value) {
        String hex = value.startsWith("\\x") ? value.substring(2) : value;
        if (hex.matches("[0-9a-fA-F]*") && hex.length() % 2 == 0 && !hex.isEmpty()) {
            byte[] bytes = new byte[hex.length() / 2];
            for (int i = 0; i < bytes.length; i++) {
                bytes[i] = (byte) Integer.parseInt(hex.substring(i * 2, i * 2 + 2), 16);
            }
            return bytes;
        }
        return Base64.getDecoder().decode(value);
    }
}
