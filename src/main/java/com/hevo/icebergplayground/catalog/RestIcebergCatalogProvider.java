package com.hevo.icebergplayground.catalog;

import com.hevo.icebergplayground.config.IcebergCatalogConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.flink.CatalogLoader;

import java.util.HashMap;
import java.util.Map;

/**
 * Any Iceberg REST-protocol catalog (Polaris, Tabular's reference server, etc.) backed by S3 or
 * an S3-compatible store (MinIO locally). {@code warehouse} here identifies whichever
 * warehouse/catalog was already provisioned server-side against that storage — it is not
 * necessarily an S3 path on the client side, unlike {@link GlueCatalogProvider}. OAuth2
 * credentials are optional: some local REST catalog servers run without auth, so
 * {@code credential}/{@code scope} are only sent when a credential is actually configured.
 */
public class
RestIcebergCatalogProvider implements IcebergCatalogProvider {

    private static final String REST_CATALOG_IMPL = "org.apache.iceberg.rest.RESTCatalog";

    private final IcebergCatalogConfig config;

    public RestIcebergCatalogProvider(IcebergCatalogConfig config) {
        this.config = config;
    }

    @Override
    public Catalog catalog() {
        return CatalogUtil.loadCatalog(REST_CATALOG_IMPL, config.getCatalogName(), properties(), new Configuration());
    }

    @Override
    public CatalogLoader catalogLoader() {
        return CatalogLoader.custom(config.getCatalogName(), properties(), new Configuration(), REST_CATALOG_IMPL);
    }

    private Map<String, String> properties() {
        Map<String, String> properties = new HashMap<>();
        properties.put(CatalogProperties.URI, config.getRestUri());
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, config.getWarehousePath());
        properties.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO");
        if (config.getRestCredential() != null && !config.getRestCredential().isEmpty()) {
            properties.put("credential", config.getRestCredential());
            properties.put("scope", "PRINCIPAL_ROLE:ALL");
        }

        properties.put(S3FileIOProperties.ENDPOINT, config.getS3Endpoint());
        properties.put(S3FileIOProperties.ACCESS_KEY_ID, config.getS3AccessKeyId());
        properties.put(S3FileIOProperties.SECRET_ACCESS_KEY, config.getS3SecretAccessKey());
        properties.put(S3FileIOProperties.PATH_STYLE_ACCESS, String.valueOf(config.isPathStyleAccess()));
        properties.put(AwsClientProperties.CLIENT_REGION, config.getRegion());

        properties.putAll(config.getAdditionalProperties());
        return properties;
    }
}
