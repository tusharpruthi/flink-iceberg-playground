package com.hevo.icebergplayground.cdc;

import java.util.List;

/**
 * Reads whatever row-level changes have accumulated on the source database since this source
 * was last polled, for one bounded batch run. Implementations own the mechanics of talking to a
 * specific database engine's change feed (Postgres logical decoding, MySQL binlog, ...); callers
 * only see {@link ChangeRecord}s.
 * <p>
 * Fetching is non-destructive: {@link #fetchChangesSinceLastRun()} can be called repeatedly and
 * keeps returning the same changes until {@link #confirmChangesProcessed()} is called. Callers
 * must only confirm after the fetched changes have been durably written downstream — otherwise a
 * write failure would silently drop changes with no way to reprocess them. Iceberg upserts on the
 * write side make re-processing the same batch after a retry safe (INSERT/UPDATE_AFTER re-upsert
 * the same row, DELETE is a no-op the second time).
 */
public interface ChangeDataSource extends AutoCloseable {

    /**
     * Returns all changes accumulated since the last confirmed position (or since the change
     * source was provisioned, on the very first call), for the tables this source was configured
     * to track. Safe to call more than once without confirming; it keeps returning the same set.
     */
    List<ChangeRecord> fetchChangesSinceLastRun();

    /**
     * Advances the source's position past everything returned by the most recent
     * {@link #fetchChangesSinceLastRun()} call. Call only once those changes are confirmed
     * written to their destination.
     */
    void confirmChangesProcessed();

    @Override
    void close();
}
