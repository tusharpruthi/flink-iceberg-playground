package com.hevo.icebergplayground.config;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Connection details for the source OLTP database this job captures changes from.
 * {@code type} selects which {@link com.hevo.icebergplayground.cdc.ChangeDataSource}
 * implementation is instantiated at runtime, so adding MySQL later is a config value, not
 * a code change.
 */
public class SourceDatabaseConfig {

    private String type;
    private String host;
    private int port;
    private String database;
    private String schema;
    private String username;
    private String password;
    private String replicationSlotName;
    private String publicationName;

    /** Comma-separated {@code schema.table} or {@code table} names, e.g. "orders,customers". */
    private String tables;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getReplicationSlotName() {
        return replicationSlotName;
    }

    public void setReplicationSlotName(String replicationSlotName) {
        this.replicationSlotName = replicationSlotName;
    }

    public String getPublicationName() {
        return publicationName;
    }

    public void setPublicationName(String publicationName) {
        this.publicationName = publicationName;
    }

    public String getTables() {
        return tables;
    }

    public void setTables(String tables) {
        this.tables = tables;
    }

    /** Splits the comma-separated {@link #tables} value into individual, trimmed table names. */
    public List<String> tableNames() {
        return Arrays.stream(tables.split(","))
                .map(String::trim)
                .filter(name -> !name.isEmpty())
                .collect(Collectors.toList());
    }

    public String jdbcUrl() {
        return String.format("jdbc:postgresql://%s:%d/%s", host, port, database);
    }
}
