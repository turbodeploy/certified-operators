package com.vmturbo.history.db.bulk;

import java.util.Collection;

import org.jooq.Record;

/**
 * Simple interface for most clients of a {@link BulkInserter}, exposing only methods that
 * allow only record insertion.
 * @param <R> type of inserted records
 */
public interface BulkLoader<R extends Record> {

    /**
     * Insert a single record.
     * @param record record to be inserted
     * @throws InterruptedException if interrupted
     */
    void insert(R record) throws InterruptedException;

    /**
     * Insert a collection of records.
     *
     * <p>The records may be split into multiple batches.</p>
     * @param records records to be inserted
     * @throws InterruptedException if interrupted
     */
    default void insertAll(Collection<R> records) throws InterruptedException {
        for (R record : records) {
            insert(record);
        }
    }
}
