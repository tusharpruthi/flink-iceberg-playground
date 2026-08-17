package com.hevo.icebergplayground.catalog;

import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.flink.CatalogLoader;

/**
 * Produces a ready-to-use Iceberg {@link Catalog} for the environment this job is running in,
 * plus a matching serializable {@link CatalogLoader} for Flink's {@code FlinkSink} DataStream
 * write path (which needs to reload the catalog/table on the JobManager side, so it can't just
 * hold a live {@link Catalog} instance). Everything downstream (schema resolution, table
 * provisioning, writes) talks to this interface only, so switching from a local REST catalog to
 * AWS Glue in production is a config change
 * ({@link com.hevo.icebergplayground.config.IcebergCatalogConfig#getType()}), not a code change.
 */
public interface IcebergCatalogProvider {

    Catalog catalog();

    CatalogLoader catalogLoader();
}
