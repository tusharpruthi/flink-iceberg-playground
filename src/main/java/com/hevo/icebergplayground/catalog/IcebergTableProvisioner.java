package com.hevo.icebergplayground.catalog;

import com.hevo.icebergplayground.schema.ResolvedTableSchema;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Ensures the Iceberg table backing a source table exists before it's written to: creates it
 * (with the primary key as identifier/equality-delete columns and upserts enabled) on first sync,
 * or additively evolves it — new nullable columns only — on later syncs when the source schema
 * has grown. Never drops or narrows columns; a shrinking source schema is left to the operator.
 */
public class IcebergTableProvisioner {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergTableProvisioner.class);

    public Table ensureTableExists(Catalog catalog, String namespaceName, String tableName, ResolvedTableSchema resolvedSchema) {
        Namespace namespace = Namespace.of(namespaceName);
        if (catalog instanceof SupportsNamespaces) {
            SupportsNamespaces namespaceCatalog = (SupportsNamespaces) catalog;
            if (!namespaceCatalog.namespaceExists(namespace)) {
                namespaceCatalog.createNamespace(namespace);
            }
        }

        TableIdentifier tableIdentifier = TableIdentifier.of(namespace, tableName);
        if (catalog.tableExists(tableIdentifier)) {
            Table table = catalog.loadTable(tableIdentifier);
            evolveSchemaIfNeeded(table, resolvedSchema.getIcebergSchema());
            return table;
        }

        LOG.info("Creating Iceberg table {} with {} column(s)", tableIdentifier,
                resolvedSchema.getIcebergSchema().columns().size());
        Map<String, String> tableProperties = new HashMap<>();
        tableProperties.put(TableProperties.FORMAT_VERSION, "2");
        tableProperties.put(TableProperties.UPSERT_ENABLED, "true");

        return catalog.createTable(tableIdentifier, resolvedSchema.getIcebergSchema(), PartitionSpec.unpartitioned(), tableProperties);
    }

    private void evolveSchemaIfNeeded(Table table, Schema desiredSchema) {
        Schema currentSchema = table.schema();
        Set<String> existingColumnNames = currentSchema.columns().stream()
                .map(Types.NestedField::name)
                .collect(Collectors.toSet());

        UpdateSchema updateSchema = table.updateSchema();
        boolean hasChanges = false;
        for (Types.NestedField desiredField : desiredSchema.columns()) {
            if (!existingColumnNames.contains(desiredField.name())) {
                LOG.info("Evolving table {}: adding column '{}' ({})", table.name(), desiredField.name(), desiredField.type());
                updateSchema.addColumn(desiredField.name(), desiredField.type());
                hasChanges = true;
            }
        }
        if (hasChanges) {
            updateSchema.commit();
        }
    }
}
