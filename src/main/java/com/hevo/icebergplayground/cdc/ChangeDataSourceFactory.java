package com.hevo.icebergplayground.cdc;

import com.hevo.icebergplayground.cdc.postgres.PostgresWalChangeDataSource;
import com.hevo.icebergplayground.config.SourceDatabaseConfig;

/**
 * Selects the {@link ChangeDataSource} implementation for a configured source database engine.
 * Adding MySQL support later means adding a {@code MySqlBinlogChangeDataSource} and a branch
 * here — nothing else in the job depends on which engine produced a {@link ChangeRecord}.
 */
public final class ChangeDataSourceFactory {

    private ChangeDataSourceFactory() {
    }

    public static ChangeDataSource create(SourceDatabaseConfig config) {
        String dbType = config.getType().toLowerCase().trim();
        switch (dbType) {
            case "postgres":
            case "postgresql":
                return new PostgresWalChangeDataSource(config);
            default:
                throw new IllegalArgumentException("Unsupported source database type: " + config.getType());
        }
    }
}
