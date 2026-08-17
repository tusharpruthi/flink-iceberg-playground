package com.hevo.icebergplayground.catalog;

import com.hevo.icebergplayground.config.IcebergCatalogConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.flink.CatalogLoader;

import java.util.HashMap;
import java.util.Map;

/**
 * Production catalog: AWS Glue Data Catalog backed by real S3, using the AWS SDK's default
 * credential provider chain (IAM role, env vars, etc. — never hardcoded credentials).
 */
public class GlueCatalogProvider implements IcebergCatalogProvider {

    private static final String GLUE_CATALOG_IMPL = "org.apache.iceberg.aws.glue.GlueCatalog";

    private final IcebergCatalogConfig config;

    public GlueCatalogProvider(IcebergCatalogConfig config) {
        this.config = config;
    }

    @Override
    public Catalog catalog() {
        return CatalogUtil.loadCatalog(GLUE_CATALOG_IMPL, config.getCatalogName(), properties(), new Configuration());
    }

    @Override
    public CatalogLoader catalogLoader() {
        return CatalogLoader.custom(config.getCatalogName(), properties(), new Configuration(), GLUE_CATALOG_IMPL);
    }

    private Map<String, String> properties() {
        Map<String, String> properties = new HashMap<>();
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, config.getWarehousePath());
        properties.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO");
        properties.put(AwsClientProperties.CLIENT_REGION, config.getRegion());
        properties.putAll(config.getAdditionalProperties());
        return properties;
    }
}
