package com.hevo.icebergplayground.config;

/**
 * Root configuration for a single job run: which source database to read WAL changes from,
 * and which Iceberg catalog to write them to.
 */
public class AppConfig {

    private SourceDatabaseConfig source;
    private IcebergCatalogConfig catalog;

    public SourceDatabaseConfig getSource() {
        return source;
    }

    public void setSource(SourceDatabaseConfig source) {
        this.source = source;
    }

    public IcebergCatalogConfig getCatalog() {
        return catalog;
    }

    public void setCatalog(IcebergCatalogConfig catalog) {
        this.catalog = catalog;
    }
}
