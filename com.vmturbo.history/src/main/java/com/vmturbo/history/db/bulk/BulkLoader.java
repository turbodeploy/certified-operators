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

    /**
     * Flush this loader.
     *
     * <p>If there are any records that have been sent to this loader for insertion but have not yet been collected
     * into a batch scheduled for execution, that is now done.</p>
     *
     * <p>If <code>awaitExecution</code> is set, this method then blocks until all this loader's scheduled batches
     * have in fact completed execution. Otherwise hte method may return while batches remain unexecuted or in
     * process.</p>
     *
     * @param awaitExecution true to wait for all batches to complete execution
     * @throws InterruptedException if interrupted
     */
    void flush(boolean awaitExecution) throws InterruptedException;

    /**
     * Wait for all active and pending executions for this loader to complete.
     *
     * @throws InterruptedException if interrupted
     */
    void quiesce() throws InterruptedException;
}
