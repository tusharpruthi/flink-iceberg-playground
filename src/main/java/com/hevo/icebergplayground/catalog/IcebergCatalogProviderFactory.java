package com.hevo.icebergplayground.catalog;

import com.hevo.icebergplayground.config.IcebergCatalogConfig;

/**
 * Selects the {@link IcebergCatalogProvider} implementation for the configured catalog type.
 * Mirrors {@link com.hevo.icebergplayground.cdc.ChangeDataSourceFactory} on the source side —
 * the same factory-per-pluggable-concern shape used throughout this app.
 */
public final class IcebergCatalogProviderFactory {

    private IcebergCatalogProviderFactory() {
    }

    public static IcebergCatalogProvider create(IcebergCatalogConfig config) {
        String catalogType = config.getType().toLowerCase().trim();
        switch (catalogType) {
            case "rest":
                return new RestIcebergCatalogProvider(config);
            case "glue":
                return new GlueCatalogProvider(config);
            default:
                throw new IllegalArgumentException("Unsupported Iceberg catalog type: " + config.getType());
        }
    }
}
