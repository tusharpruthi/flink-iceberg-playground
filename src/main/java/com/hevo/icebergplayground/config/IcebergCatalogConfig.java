package com.hevo.icebergplayground.config;

import java.util.HashMap;
import java.util.Map;

/**
 * Catalog + storage details for the Iceberg sink side. {@code type} selects which
 * {@link com.hevo.icebergplayground.catalog.IcebergCatalogProvider} implementation is used
 * (REST/Polaris locally, Glue in production), so the sink code never branches on environment.
 */
public class IcebergCatalogConfig {

    private String type;
    private String catalogName;
    private String warehousePath;

    /** REST catalog (Polaris) settings. */
    private String restUri;
    private String restCredential;

    /** S3 / MinIO settings. */
    private String s3Endpoint;
    private String s3AccessKeyId;
    private String s3SecretAccessKey;
    private String region;
    private boolean pathStyleAccess;

    private Map<String, String> additionalProperties = new HashMap<>();

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public void setCatalogName(String catalogName) {
        this.catalogName = catalogName;
    }

    public String getWarehousePath() {
        return warehousePath;
    }

    public void setWarehousePath(String warehousePath) {
        this.warehousePath = warehousePath;
    }

    public String getRestUri() {
        return restUri;
    }

    public void setRestUri(String restUri) {
        this.restUri = restUri;
    }

    public String getRestCredential() {
        return restCredential;
    }

    public void setRestCredential(String restCredential) {
        this.restCredential = restCredential;
    }

    public String getS3Endpoint() {
        return s3Endpoint;
    }

    public void setS3Endpoint(String s3Endpoint) {
        this.s3Endpoint = s3Endpoint;
    }

    public String getS3AccessKeyId() {
        return s3AccessKeyId;
    }

    public void setS3AccessKeyId(String s3AccessKeyId) {
        this.s3AccessKeyId = s3AccessKeyId;
    }

    public String getS3SecretAccessKey() {
        return s3SecretAccessKey;
    }

    public void setS3SecretAccessKey(String s3SecretAccessKey) {
        this.s3SecretAccessKey = s3SecretAccessKey;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public boolean isPathStyleAccess() {
        return pathStyleAccess;
    }

    public void setPathStyleAccess(boolean pathStyleAccess) {
        this.pathStyleAccess = pathStyleAccess;
    }

    public Map<String, String> getAdditionalProperties() {
        return additionalProperties;
    }

    public void setAdditionalProperties(Map<String, String> additionalProperties) {
        this.additionalProperties = additionalProperties;
    }
}
