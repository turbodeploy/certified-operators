package com.vmturbo.history.db.bulk;

import static com.vmturbo.sql.utils.JooqQueryTrimmer.trimJooqErrorMessage;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import javax.annotation.Nonnull;

import com.google.common.base.Stopwatch;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Record;
import org.jooq.Table;
import org.jooq.exception.DataAccessException;

import com.vmturbo.history.SharedMetrics;
import com.vmturbo.history.SharedMetrics.BatchInsertDisposition;
import com.vmturbo.history.db.BasedbIO;
import com.vmturbo.history.db.RecordTransformer;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.db.bulk.DbInserters.DbInserter;

/**
 * Class to insert records into a database table using batched insert operations.
 *
 * <p>Records are accumulated until a designated flush threshold is attained, at which point,
 * accumulated records are written to the database. When all records have been written, the inserter
 * should be closed, which will cause remaining records to be flushed, if any.</p>
 *
 * <p>Batches are written by tasks submitted to an {@link ExecutorService}, but task
 * submission is essentially throttled by the fact that each inserter is restricted as to the number
 * of "in-flight" batches it can queue up at any one time. Parallelism results both from setting
 * that limit > 1, and from having multiple inserters operating concurrently, on different tables.</p>
 *
 * <p>Each executing task has a built-in retry loop that is employed whenever an error is
 * encountered when attempting an operation. Retries are performed using newly acquired
 * connections, and after increasingly long backoff wait times. After all the scheduled retries
 * have occurred and failed, the overall batch operation fails.</p>
 *
 * <p>An inserter can be configured with a {@link RecordTransformer}, which will be applied to
 * each record before it is saved the database. The transformer may create a record of a different
 * type, which is why this class has two different type parameters - one to which the transformer
 * is applied, and one for its output.</p>
 *
 * <p>An inserter is also configured with a {@link DbInserter}, which is responsible for actually
 * invoking appropriate database operations to save the batch of records as efficiently as
 * possible to the database. A few common db inserters are available in the {@link DbInserters}
 * class.</p>
 *
 * <p>This class is thread-safe; multiple clients with references to the same bulk loader instance can
 * safely insert records and know that, barring insertion failures, those records will make it
 * into the database. Records from mutliple clients will be intermingled unpredictably.</p>
 *
 * @param <InT> type of records presented to this inserter instance
 * @param <OutT> type of records saved to the database by this inserter instance
 */
public class BulkInserter<InT extends Record, OutT extends Record> implements BulkLoader<InT> {

    private static final Logger logger = LogManager.getLogger(BulkInserter.class);

    // computed sequence of wait times between retries of a failing batch
    private final int[] batchRetryBackoffsMsec;
    // sum of retry backoffs
    private final int sumOfRetryBackoffsMsec;

    // maximum number of batchs each inserter is allowed to have submitted to the executor (and
    // not yet resolved) at any time.
    private int maxPendingBatches;

    // records received but not yet submitted for insertion.
    private final List<OutT> pendingRecords;

    // max number of records to be written in a batch. When pending records grows to this size,
    // it is flushed.
    private final int batchSize;

    // transformer that will be applied to incoming records
    private final RecordTransformer<InT, OutT> recordTransformer;

    // db inserter that will perform batch inserts
    private final DbInserter<OutT> dbInserter;

    // statistics pertaining to this inserter
    private final BulkInserterStats inserterStats;

    // sequential number of next batch to submit for execution
    private int batchNo = 1;

    // Futures representing batches that have been submitted for executions but whose results
    // have not yet been retrieved by this inserter.
    private final ArrayDeque<Future<BatchStats>> pendingExecutions = new ArrayDeque<>();

    // true once this inserter has been closed; inserts are then disallowed
    private AtomicBoolean closed = new AtomicBoolean(false);

    private final Table<InT> inTable;
    private final Table<OutT> outTable;
    private final BasedbIO basedbIO;
    private final ExecutorService executor;

    /**
     * create a new inserter instance.
     *
     * @param basedbIO          basic database methods, including connection creation
     * @param key               object to use as a key for this inserter's stats (typically the
     *                          output table)
     * @param inTable           table for records of RI type, that will sent to this instance
     * @param outTable          table for records of RO type, which will be stored to the database
     * @param config            config parameters
     * @param recordTransformer function to transform input records to output records
     * @param dbInserter        function to perform batch insertions
     * @param executor          executor service that will execute our batch operations
     */
    public BulkInserter(@Nonnull BasedbIO basedbIO,
                        @Nonnull Object key,
                        @Nonnull Table<InT> inTable,
                        @Nonnull Table<OutT> outTable,
                        @Nonnull BulkInserterConfig config,
                        @Nonnull RecordTransformer<InT, OutT> recordTransformer,
                        @Nonnull DbInserter<OutT> dbInserter,
                        @Nonnull ExecutorService executor) {
        this.basedbIO = basedbIO;
        this.inTable = inTable;
        this.outTable = outTable;
        this.recordTransformer = recordTransformer;
        this.dbInserter = dbInserter;
        this.batchSize = config.batchSize();
        this.batchRetryBackoffsMsec = computeBatchRetryBackoffs(
            config.maxBatchRetries(), config.maxRetryBackoffMsec());
        this.sumOfRetryBackoffsMsec = IntStream.of(batchRetryBackoffsMsec).sum();
        this.maxPendingBatches = config.maxPendingBatches();
        this.executor = executor;
        this.inserterStats = new BulkInserterStats(key, inTable, outTable);
        this.pendingRecords = new ArrayList<>(batchSize);
    }

    /**
     * Add a record to the pending records queue, and flush pending records if the batch size
     * is attained.
     *
     * @param record record to be inserted
     * @throws InterruptedException if interrupted
     */
    public void insert(@Nonnull InT record) throws InterruptedException {
        checkClosed();
        synchronized (pendingRecords) {
            // is there room for more?
            if (pendingRecords.size() >= batchSize) {
                // nope, make some room
                flush();
            }
            // now add our record
            Optional<OutT> out = recordTransformer.transform(record, inTable, outTable);
            out.ifPresent(pendingRecords::add);
        }
    }

    /**
     * Flush the records currently on the pending records list by submiting a task to write them
     * to the database.
     *
     * <p>Normally this occurs automatically when the flush threshold is attained, or when the
     * inserter is closed. However, the operation may be explicitly invoked at any time.</p>
     *
     * <p>A flush operation may block if previous tasks submitted by this inserter have not yet
     * harvested.</p>
     *
     * @throws InterruptedException if interrupted
     */
    void flush() throws InterruptedException {
        int thisBatchNo;
        List<OutT> batch;
        // capture pending records and remove them from pending list
        synchronized (pendingRecords) {
            if (pendingRecords.isEmpty()) {
                // nothing to flush
                return;
            }
            batch = new ArrayList<>(pendingRecords.size());
            batch.addAll(pendingRecords);
            pendingRecords.clear();
            thisBatchNo = batchNo++;
        }
        // no longer locking the pending insertions queue... submit new batch
        synchronized (pendingExecutions) {
            while (pendingExecutions.size() >= maxPendingBatches) {
                // we've hit our pending batch limit; need to make headroom
                handleOnePendingExecution();
            }
            // OK, queue this batch up for execution
            pendingExecutions.addLast(executor.submit(
                    new InsertExecutor(dbInserter, thisBatchNo, batch)));
        }
    }

    /**
     * Attempt to harvest the future representing execution of batch we least recently
     * submitted. This may causes blocking.
     *
     * @return true if a pending execution was harvested
     * @throws InterruptedException if interrupted
     */
    private boolean handleOnePendingExecution() throws InterruptedException {
        synchronized (pendingExecutions) {
            if (pendingExecutions.isEmpty()) {
                return false;
            }
            // this will only be set non-null when we harvest a successful execution
            BatchStats batchStats = null;
            final Future<BatchStats> future = pendingExecutions.removeFirst();
            try {
                // wait for a prior task to complete and yield its stats. Max wait is computed
                // from retry backoff times, with additional time to account for scheduling
                // delays, as well as some errors that can take time to materialize (e.g. lock
                // wait timeouts)
                batchStats = future.get(sumOfRetryBackoffsMsec * 5, TimeUnit.MILLISECONDS);
            } catch (ExecutionException | RuntimeException e) {
                logger.error("Batch insertion failed", e);
            } catch (TimeoutException e) {
                // aggressively try to free up the pool thread
                future.cancel(true);
                logger.error("Batch insertion timed out", e);
            }
            if (batchStats != null) {
                // record the results of the resolved execution in our overall write stats
                inserterStats.updateForBatch(batchStats);
            } else {
                // here when the execution was canceled or threw an exception, so count it as failed
                inserterStats.failedBatch();
            }
            // either way, we harvested a pending execution
            return true;
        }
    }

    /**
     * Drain the pending exeuctions queue until it's empty.
     *
     * @throws InterruptedException if interrupted
     */
    void quiesce() throws InterruptedException {
        while (handleOnePendingExecution()) {
        }
    }

    /**
     * A class that performs the database operations on a batch of records once the submitted
     * task becomes active.
     */
    private class InsertExecutor implements Callable<BatchStats> {

        private final DbInserter<OutT> dbInserter;
        private final List<OutT> records;
        private final int batchNo;

        /**
         * Create a new instance.
         *
         * @param dbInserter a {@link DbInserter} that will perform needed database operations
         * @param batchNo    the batch number represented by this task
         * @param records    the records to be inserted
         */
        InsertExecutor(@Nonnull DbInserter<OutT> dbInserter, int batchNo, @Nonnull List<OutT> records) {
            this.dbInserter = dbInserter;
            this.records = records;
            this.batchNo = batchNo;
        }

        @Override
        @Nonnull
        public BatchStats call() throws InterruptedException {
            // time spent either doing work that failed, or backing off before a retry
            long lostTimeNanos = 0L;
            // perform retries until our retry limit is exhausted
            for (int i = 0; i <= batchRetryBackoffsMsec.length; i++) {
                Stopwatch operationTimer = Stopwatch.createStarted();
                try (Connection conn = basedbIO.transConnection()) {
                    try {
                        // use our dbInserter to actually perform the operation
                        dbInserter.insert(records, conn);
                    } catch (Exception e) {
                        // rollback the transaction since closing will only release back to
                        // connection pool
                        conn.rollback();
                        throw e;
                    }
                    // commit the results, and record the operation in log and in metrics
                    conn.commit();
                    operationTimer.stop();
                    logger.debug("Wrote batch #{} of {} records to table {} in {}", batchNo,
                        records.size(), inTable.getName(), operationTimer);
                    SharedMetrics.RECORDS_WRITTEN_BY_TABLE.labels(inTable.getName())
                        .increment(((double)records.size()));
                    SharedMetrics.BATCHED_INSERTS
                        .labels(inTable.getName(), BatchInsertDisposition.success.name())
                        .increment();
                    // no more retries required
                    // ultimately successful execution... send back stats
                    long workTimeNanos = operationTimer.elapsed().toNanos();
                    return BatchStats.goodBatch(records.size(), workTimeNanos, lostTimeNanos);
                } catch (DataAccessException | VmtDbException | SQLException e) {
                    // something when wrong
                    operationTimer.stop();
                    lostTimeNanos += operationTimer.elapsed().toNanos();
                    if (i < batchRetryBackoffsMsec.length) {
                        // attempt a retry if we have any left to try
                        logger.warn("Table {}: Failed insertion, batch #{} try #{}; retrying: {}",
                            inTable.getName(), batchNo, i + 1, trimJooqErrorMessage(e));
                        SharedMetrics.BATCHED_INSERTS
                            .labels(inTable.getName(), BatchInsertDisposition.retry.name())
                            .increment();
                        // sleep through the backoff interval for this retry attempt
                        Stopwatch sleepTimer = Stopwatch.createStarted();
                        Thread.sleep(batchRetryBackoffsMsec[i]);
                        lostTimeNanos += sleepTimer.elapsed().toNanos();
                    } else {
                        // out of retry attempts - this operation fails
                        logger.error("Failed to save {} records in batch #{} " +
                                "to table {} after {} tries: {}",
                            records.size(),
                            batchNo,
                            inTable.getName(),
                            batchRetryBackoffsMsec.length,
                            trimJooqErrorMessage(e));
                        SharedMetrics.BATCHED_INSERTS
                            .labels(inTable.getName(), BatchInsertDisposition.failure.name())
                            .increment();
                        // this will cause the task to be counted as a failed execution
                        return BatchStats.failedBatch(lostTimeNanos);
                    }
                }
            }
            // following is dead code, but compiler doesn't figure that out and thinks it needs
            // a return
            throw new IllegalStateException("exec loop in task neither succeeded nor failed");
        }

        /**
         * Return records to be inserted by this instance.
         *
         * <p>This method is used by tests.</p>
         *
         * @return records bound to this instance
         */
        List<OutT> getRecords() {
            return records;
        }

        /**
         * Get the batch number for this instance.
         *
         * <p>This method is used by tests.</p>
         *
         * @return batch number
         */
        int getBatchNo() {
            return batchNo;
        }
    }

    /**
     * Close the inserter, after flushing its pending records (if any), and awaiting
     * completion of any still pending batches (possibly including a batch arising from
     * the flush operation).
     *
     * <p>Final statistics for the loader may be logged.</p>
     *
     * @param logger Logger where stats are reported; null means do not log
     * @throws InterruptedException if interrupted
     */
    public void close(Logger logger) throws InterruptedException {
        // only one closer actually does the work of closing; subsequent closers can't
        // return until that work is finished
        synchronized (this) {
            if (!closed.getAndSet(true)) {
                // only the first closer will ever get hre
                flush();
                quiesce();
                if (logger != null) {
                    inserterStats.logStats(logger);
                }
            }
        }
    }

    /**
     * Return the overall stats object for this inserter instance.
     *
     * <p>This is probably most useful after the inserter has been closed, but it is the live
     * instance, not a copy, so a caller could use it to monitor progress if desired.</p>
     *
     * @return stats object
     */
    public BulkInserterStats getStats() {
        return inserterStats;
    }

    /**
     * Throw an exception if this inserter is closed.
     */
    private void checkClosed() {
        if (closed.get()) {
            throw new IllegalStateException("BulkInserter cannot be used after it is closed");
        }
    }

    /*
     * Compute a schedule of backoff times between retry attempts for a failing batch.
     *
     * <p>The times will increase in roughly exponential fashion up to the maximum confiugred
     * by {@link #maxRetryBackoffMsec} config option, and the overall schedule length will be as
     * configured by {@link #maxBatchRetries}.</p>
     *
     * @return backoff schedule
     */
    private int[] computeBatchRetryBackoffs(int maxBatchRetries, int maxBatchRetryBackoffMsec) {
        int[] backoffs = new int[maxBatchRetries];

        // prior lengths are progressively halfed
        int i = backoffs.length - 1;
        backoffs[i] = maxBatchRetryBackoffMsec;

        // prior lengths are progressively halfed
        while (--i > 0) {
            backoffs[i] = backoffs[i + 1] / 2;
        }

        // first retry occurs immediately - no backoff - unless we only allow a single retry,
        // in which case the max retry setting already in place is left alone
        if (maxBatchRetries > 1) {
            backoffs[0] = 0;
        }
        return backoffs;
    }

    /**
     * POJO to represent statistics relating to the execution of a single batch.
     */
    static class BatchStats {

        private final boolean failed;
        private final int records;
        private final long workTimeNanos;
        private final long lostTimeNanos;

        BatchStats(boolean failed, int records, long workTimeNanos, long lostTimeNanos) {
            this.failed = failed;
            this.records = records;
            this.workTimeNanos = workTimeNanos;
            this.lostTimeNanos = lostTimeNanos;
        }

        /**
         * Record a successful batch execution.
         *
         * @param records       number of records saved
         * @param workTimeNanos amount of time actively working on the batch
         * @param lostTimeNanos amount of time lost to retry backoff waits
         * @return this object, for chained calls
         */
        static BatchStats goodBatch(int records, long workTimeNanos, long lostTimeNanos) {
            return new BatchStats(false, records, workTimeNanos, lostTimeNanos);
        }

        /**
         * Record an unsuccessful batch execution.
         *
         * @param lostTimeNanos amount of time lost to retry backoff waits
         * @return this object, for chained calls
         */
        static BatchStats failedBatch(long lostTimeNanos) {
            return new BatchStats(true, 0, 0L, lostTimeNanos);
        }

        /**
         * Determine whether this is a failed batch.
         *
         * @return true of this represents a failed batch
         */
        public boolean isFailed() {
            return failed;
        }

        /**
         * Get number of records written by this batch.
         *
         * @return number of records written by this batch
         */
        public long getRecords() {
            return records;
        }

        /**
         * Get nanonseconds of work time executing this batch.
         *
         * @return amount of work spent executing this batch
         */
        public long getWorkTimeNanos() {
            return workTimeNanos;
        }

        /**
         * Get nanoseconds spent waiting to retry failed executions for this batch.
         *
         * @return amount of time spent waiting to retry failed executions for this batch
         */
        public long getLostTimeNanos() {
            return lostTimeNanos;
        }
    }
}
