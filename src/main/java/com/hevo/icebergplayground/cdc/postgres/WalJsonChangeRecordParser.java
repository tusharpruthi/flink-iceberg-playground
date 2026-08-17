package com.hevo.icebergplayground.cdc.postgres;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hevo.icebergplayground.cdc.ChangeRecord;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Parses one {@code wal2json} payload as returned by {@code pg_logical_slot_get_changes} — one
 * JSON object per row, but that object wraps a {@code change} array with one entry per row-level
 * change in the transaction (wal2json batches by transaction, not by row) — into zero or more
 * {@link ChangeRecord}s. Each change entry looks like:
 * <pre>
 * {"kind":"insert","schema":"public","table":"orders",
 *  "columnnames":["id","amount"],"columntypes":["bigint","numeric(10,2)"],"columnvalues":[5,5.55]}
 * </pre>
 * with updates/deletes additionally carrying an {@code oldkeys} object
 * ({@code keynames}/{@code keyvalues}) identifying the row.
 */
public final class WalJsonChangeRecordParser {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Set<String> trackedTableNames;

    public WalJsonChangeRecordParser(Set<String> trackedTableNames) {
        this.trackedTableNames = trackedTableNames;
    }

    public List<ChangeRecord> parse(String walJsonPayload) {
        List<ChangeRecord> records = new ArrayList<>();
        JsonNode changes = readTree(walJsonPayload).path("change");
        for (JsonNode change : changes) {
            String tableName = change.path("table").asText();
            if (!trackedTableNames.contains(tableName)) {
                continue;
            }
            ChangeRecord record = toChangeRecord(change, tableName);
            if (record != null) {
                records.add(record);
            }
        }
        return records;
    }

    private ChangeRecord toChangeRecord(JsonNode change, String tableName) {
        String schemaName = change.path("schema").asText();
        String kind = change.path("kind").asText();
        switch (kind) {
            case "insert":
                return ChangeRecord.insert(schemaName, tableName, columnsToMap(change));
            case "update":
                return ChangeRecord.update(schemaName, tableName, oldKeysToMap(change), columnsToMap(change));
            case "delete":
                return ChangeRecord.delete(schemaName, tableName, oldKeysToMap(change));
            default:
                throw new IllegalArgumentException("Unrecognized wal2json change kind: " + kind);
        }
    }

    private Map<String, Object> columnsToMap(JsonNode change) {
        return arraysToMap(change.path("columnnames"), change.path("columnvalues"));
    }

    private Map<String, Object> oldKeysToMap(JsonNode change) {
        JsonNode oldKeys = change.path("oldkeys");
        return arraysToMap(oldKeys.path("keynames"), oldKeys.path("keyvalues"));
    }

    private Map<String, Object> arraysToMap(JsonNode namesArray, JsonNode valuesArray) {
        Map<String, Object> values = new LinkedHashMap<>();
        if (namesArray.isMissingNode() || valuesArray.isMissingNode()) {
            return values;
        }
        for (int i = 0; i < namesArray.size(); i++) {
            values.put(namesArray.get(i).asText(), toJavaValue(valuesArray.get(i)));
        }
        return values;
    }

    private JsonNode readTree(String payload) {
        try {
            return objectMapper.readTree(payload);
        } catch (Exception e) {
            throw new IllegalArgumentException("Malformed wal2json payload: " + payload, e);
        }
    }

    private Object toJavaValue(JsonNode valueNode) {
        if (valueNode == null || valueNode.isMissingNode() || valueNode.isNull()) {
            return null;
        }
        if (valueNode.isIntegralNumber()) {
            return valueNode.longValue();
        }
        if (valueNode.isFloatingPointNumber()) {
            // decimalValue(), not doubleValue(), to preserve exact precision for NUMERIC columns.
            return valueNode.decimalValue();
        }
        if (valueNode.isBoolean()) {
            return valueNode.booleanValue();
        }
        return valueNode.asText();
    }
}
