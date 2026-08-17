package com.hevo.icebergplayground.schema;

import org.apache.iceberg.Schema;

import java.util.List;

/**
 * A source table's schema translated into Iceberg terms: the {@link Schema} itself, the column
 * order as declared on the source table, and which columns form the primary key (used both as
 * the Iceberg identifier fields and as the changelog upsert key).
 */
public final class ResolvedTableSchema {

    private final Schema icebergSchema;
    private final List<String> orderedColumnNames;
    private final List<String> primaryKeyColumnNames;

    public ResolvedTableSchema(Schema icebergSchema, List<String> orderedColumnNames, List<String> primaryKeyColumnNames) {
        this.icebergSchema = icebergSchema;
        this.orderedColumnNames = orderedColumnNames;
        this.primaryKeyColumnNames = primaryKeyColumnNames;
    }

    public Schema getIcebergSchema() {
        return icebergSchema;
    }

    public List<String> getOrderedColumnNames() {
        return orderedColumnNames;
    }

    public List<String> getPrimaryKeyColumnNames() {
        return primaryKeyColumnNames;
    }
}
