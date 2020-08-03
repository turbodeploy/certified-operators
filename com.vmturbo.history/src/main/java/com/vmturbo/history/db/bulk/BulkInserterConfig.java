package com.vmturbo.history.db.bulk;

import org.immutables.value.Value;
import org.immutables.value.Value.Default;

/**
 * This class collects various configuration parameters used by {@link BulkInserter}
 * class. It's easy to list these out of order in constructors, so passing this
 * instead is more fool-proof. (Yes, that was a bug for a while.)
 */
@Value.Immutable
public interface BulkInserterConfig {
    /**
     * Get max nubmer of records to be written in a single batch.
     *
     * @return batch size
     */
    int batchSize();

    /**
     * Get max number of retries after a batch insertion fails.
     *
     * @return max retries
     */
    int maxBatchRetries();

    /**
     * Return maximum milliseconds to wait before attempting final retry.
     *
     * <p>Prior retries use back-off times that successively double up to this value.</p>
     *
     * @return max backoff time in msec
     */
    int maxRetryBackoffMsec();

    /**
     * Get the maximum number of batches that a {@link BulkInserter} instance can have in flight
     * simultaneously.
     *
     * <p>When the limit is reached, new the inserter blocks when there's a new batch to submit
     * for execution, until an in-flight batch completes.</p>
     *
     * @return max number of pending batches
     */
    int maxPendingBatches();

    /**
     * Should this table be dropped when the inserter is closed?
     *
     * @return true to drop the table
     */
    @Default()
    default boolean dropOnClose() {
        return false;
    }
}
