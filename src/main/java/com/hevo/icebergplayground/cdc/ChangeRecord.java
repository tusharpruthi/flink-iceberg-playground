package com.hevo.icebergplayground.cdc;

import java.util.Collections;
import java.util.Map;

/**
 * A single row-level change captured from a source database, independent of which engine
 * (Postgres WAL, MySQL binlog, ...) produced it. Downstream sink code only ever deals with this
 * type, never with engine-specific payloads.
 */
public final class ChangeRecord {

    private final String schemaName;
    private final String tableName;
    private final ChangeOperation operation;
    /** Column values identifying the row before the change; present for UPDATE and DELETE. */
    private final Map<String, Object> beforeValues;
    /** Column values of the row after the change; present for INSERT and UPDATE. */
    private final Map<String, Object> afterValues;

    private ChangeRecord(String schemaName, String tableName, ChangeOperation operation,
                          Map<String, Object> beforeValues, Map<String, Object> afterValues) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.operation = operation;
        this.beforeValues = beforeValues == null ? Collections.emptyMap() : beforeValues;
        this.afterValues = afterValues == null ? Collections.emptyMap() : afterValues;
    }

    public static ChangeRecord insert(String schemaName, String tableName, Map<String, Object> afterValues) {
        return new ChangeRecord(schemaName, tableName, ChangeOperation.INSERT, null, afterValues);
    }

    public static ChangeRecord update(String schemaName, String tableName,
                                       Map<String, Object> beforeValues, Map<String, Object> afterValues) {
        return new ChangeRecord(schemaName, tableName, ChangeOperation.UPDATE, beforeValues, afterValues);
    }

    public static ChangeRecord delete(String schemaName, String tableName, Map<String, Object> beforeValues) {
        return new ChangeRecord(schemaName, tableName, ChangeOperation.DELETE, beforeValues, null);
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public String qualifiedTableName() {
        return schemaName + "." + tableName;
    }

    public ChangeOperation getOperation() {
        return operation;
    }

    public Map<String, Object> getBeforeValues() {
        return beforeValues;
    }

    public Map<String, Object> getAfterValues() {
        return afterValues;
    }

    @Override
    public String toString() {
        return "ChangeRecord{" + operation + " " + qualifiedTableName() + " after=" + afterValues
                + " before=" + beforeValues + "}";
    }
}
