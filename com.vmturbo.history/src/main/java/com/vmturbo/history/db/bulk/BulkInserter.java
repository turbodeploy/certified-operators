package com.vmturbo.history.db.bulk;

import static com.vmturbo.sql.utils.JooqQueryTrimmer.trimJooqErrorMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import javax.annotation.Nonnull;

import com.google.common.base.Stopwatch;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Table;
import org.jooq.exception.DataAccessException;

import com.vmturbo.history.SharedMetrics;
import com.vmturbo.history.SharedMetrics.BatchInsertDisposition;
import com.vmturbo.history.db.RecordTransformer;
import com.vmturbo.history.db.bulk.DbInserters.DbInserter;

/**
 * Class to insert records into a database table using batched insert operations.
 *
 * <p>Records are accumulated until a designated flush threshold is attained, at which point,
 * accumulated records are written to the database. When all records have been written, the inserter
 * should be closed, which will cause remaining records to be flushed, if any.</p>
 *
 * <p>Batches are written by tasks submitted to an {@link ExecutorService}, by way of an instance
 * of {@link ThrottlingCompletingExecutor}. This acts as an {@link Executor}, and submitted tasks
 * are queued for execution. However, the throttling executor has two features that are critical for
 * our use. (1) It maintains a cap on how many tasks can be in-flight or queued at any given point
 * in time. Attempts to submit new tasks when that cap is met become blocking operations, awaiting
 * completion of an existing task. (2) Tasks are reaped immediately upon completion, using a {@link
 * CompletionService}. Results from completed tasks are delivered to the submitter via callbacks.
 * These features are important because we could otherwise consume great amounts of memory due to
 * the {@link Record} objects wrapped up in waiting tasks.</p>
 *
 * <p>Each executing task has a built-in retry loop that is employed whenever an error is
 * encountered when attempting an operation. Retries are performed using newly acquired connections,
 * and after increasingly long backoff wait times. After all the scheduled retries have occurred and
 * failed, the overall batch operation fails.</p>
 *
 * <p>An inserter can be configured with a {@link RecordTransformer}, which will be applied to
 * each record before it is saved the database. The transformer may create a record of a different
 * type, which is why this class has two different type parameters - one to which the transformer is
 * applied, and one for its output.</p>
 *
 * <p>An inserter is also configured with a {@link DbInserter}, which is responsible for actually
 * invoking appropriate database operations to save the batch of records as efficiently as possible
 * to the database. A few common db inserters are available in the {@link DbInserters} class.</p>
 *
 * <p>This class is thread-safe; multiple clients with references to the same bulk loader instance
 * can safely insert records and know that, barring insertion failures, those records will make it
 * into the database. Records from multiple callers will be intermingled unpredictably.</p>
 *
 * @param <InT>  type of records presented to this inserter instance
 * @param <OutT> type of records saved to the database by this inserter instance
 */
public class BulkInserter<InT extends Record, OutT extends Record> implements BulkLoader<InT> {

    private static final Logger logger = LogManager.getLogger(BulkInserter.class);

    // computed sequence of wait times between retries of a failing batch
    private final int[] batchRetryBackoffsMsec;

    // whether to drop out-table when closing the inserter (used for transient loaders)
    private final boolean dropOnClose;

    // records received but not yet submitted for insertion.
    private final List<OutT> pendingRecords;

    // stashes all scheduled batches as tasks.
    private final Set<Future<BatchStats>> executingBatches;

    // max number of records to be written in a batch. When pending records grows to this size,
    // it is flushed.
    private final int batchSize;

    // transformer that will be applied to incoming records
    private final RecordTransformer<InT, OutT> recordTransformer;

    // db inserter that will perform batch inserts
    private final DbInserter<OutT> dbInserter;

    // statistics pertaining to this inserter
    private final BulkInserterStats inserterStats;
    private final long flushTimeoutSecs;

    // sequential number of next batch to submit for execution
    private int batchNo = 1;

    // true once this inserter has been closed; inserts are then disallowed
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /*
     * True once this inserter has been disabled; inserts are then ignored.
     *
     * The inserter is disabled if the pre-insertion hook returns false, which can happen in case
     * the target table is corrupted. In this case, any records will get discarded to avoid raising
     * an exception all the way up to the writer, affecting other non-corrupted tables as well.
     */
    private final AtomicBoolean disabled = new AtomicBoolean(false);

    // Number of executions that have been submitted for execution but have not yet completed.
    // This is mostly used in close processing so we can complain if,
    // after allowing the throttling executor to quiesce, we still have outstanding tasks.
    private final AtomicInteger pendingExecutionCount = new AtomicInteger(0);

    private Function<Record, Boolean> preInsertionHook;
    private boolean firstInsertionAttempt = true;

    private final Table<InT> inTable;
    private final Table<OutT> outTable;
    private final DSLContext dsl;
    private final ThrottlingCompletingExecutor<BatchStats> batchCompletionService;

    /**
     * create a new inserter instance.
     *
     * @param dsl                    basic database methods, including connection creation
     * @param key                    object to use as a key for this inserter's stats (typically the
     *                               output table)
     * @param inTable                table for records of RI type, that will be sent to this instance
     * @param outTable               table for records of RO type, which will be stored to the
     *                               database
     * @param config                 config parameters
     * @param recordTransformer      function to transform input records to output records
     * @param dbInserter             function to perform batch insertions
     * @param batchCompletionService completion service for executing batches
     */
    public BulkInserter(@Nonnull DSLContext dsl,
            @Nonnull Object key,
            @Nonnull Table<InT> inTable,
            @Nonnull Table<OutT> outTable,
            @Nonnull BulkInserterConfig config,
            @Nonnull RecordTransformer<InT, OutT> recordTransformer,
            @Nonnull DbInserter<OutT> dbInserter,
            @Nonnull ThrottlingCompletingExecutor<BatchStats> batchCompletionService) {
        this.dsl = dsl;
        this.inTable = inTable;
        this.outTable = outTable;
        this.recordTransformer = recordTransformer;
        this.dbInserter = dbInserter;
        this.batchSize = config.batchSize();
        this.batchRetryBackoffsMsec = computeBatchRetryBackoffs(
                config.maxBatchRetries(), config.maxRetryBackoffMsec());
        this.dropOnClose = config.dropOnClose();
        this.batchCompletionService = batchCompletionService;
        this.inserterStats = new BulkInserterStats(key, inTable, outTable);
        this.pendingRecords = new ArrayList<>(batchSize);
        this.executingBatches = ConcurrentHashMap.newKeySet();
        this.flushTimeoutSecs = config.flushTimeoutSecs();
    }

    public Table<InT> getInTable() {
        return inTable;
    }

    public Table<OutT> getOutTable() {
        return outTable;
    }

    /**
     * Add a record to the pending records queue, and flush pending records if the batch size is
     * attained.
     *
     * @param record record to be inserted
     */
    public void insert(@Nonnull InT record) {
        checkClosed();
        synchronized (pendingRecords) {
            if (firstInsertionAttempt && preInsertionHook != null) {
                firstInsertionAttempt = false;
                if (!preInsertionHook.apply(record)) {
                    if (!disabled.get()) {
                        logger.error("Pre-insertion hook failed; insertions disallowed");
                        disabled.set(true);
                    }
                }
            }
            if (disabled.get()) {
                inserterStats.updateDiscardedRecords();
                return;
            }
        }
        synchronized (pendingRecords) {
            // is there room for more?
            if (pendingRecords.size() >= batchSize) {
                // nope, make some room
                sendBatch();
                // and then add our record
            }
            Optional<OutT> out = recordTransformer.transform(record, inTable, outTable);
            out.ifPresent(pendingRecords::add);
        }
    }

    /**
     * Provide a hook to be invoked the first time a record is presented to this inserter for
     * insertion. The record is passed to the hook, and if the hook fails, the inserter closes
     * immediately without inserting any records.
     *
     * @param preInsertionHook hook function
     */
    public void setPreInsertionHook(Function<Record, Boolean> preInsertionHook) {
        synchronized (pendingRecords) {
            if (this.preInsertionHook == null) {
                this.preInsertionHook = preInsertionHook;
            }
        }
    }

    private void sendBatch() {
        int thisBatchNo = -1;
        List<OutT> batch;
        // capture pending records and remove them from pending list
        synchronized (pendingRecords) {
            if (!pendingRecords.isEmpty()) {
                batch = new ArrayList<>(pendingRecords);
                pendingRecords.clear();
                thisBatchNo = batchNo++;
            } else {
                return;
            }
        }
        // no longer locking the pending insertions queue... submit new batch
        try {
            final InsertTask task = new InsertTask(dbInserter, thisBatchNo, batch);
            synchronized (batchCompletionService) {
                batchCompletionService.submit(task, executingBatches::add,
                        this::handleBatchCompletion);
                this.pendingExecutionCount.incrementAndGet();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (TimeoutException e) {
            logger.error("Failed to submit insertion batch #{} for execution", thisBatchNo);
            inserterStats.setMayBePartial();
        }
    }

    @Override
    public void flush(boolean awaitCompletion) {
        synchronized (pendingRecords) {
            if (!pendingRecords.isEmpty()) {
                sendBatch();
            }
        }
        if (!awaitCompletion) {
            return;
        }
        // wait for all tasks to be handled, but cap total accumulated wait
        long timeoutBarrier = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(
                flushTimeoutSecs);
        try {
            synchronized (executingBatches) {
                while (true) {
                    if (pendingExecutionCount.get() == 0) {
                        return;
                    }
                    long remainingTime = timeoutBarrier - System.currentTimeMillis();
                    if (remainingTime > 0) {
                        executingBatches.wait(remainingTime);
                    } else {
                        logger.warn("Timed out waiting to flush pending inserts for out table {}",
                                outTable.getName());
                        return;
                    }
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Harvest the future representing execution of batch previously submitted.
     *
     * @param future result of batch execution
     */
    private void handleBatchCompletion(Future<BatchStats> future) {
        if (future.isDone()) {
            try {
                final BatchStats batchStats = future.get();
                inserterStats.updateForBatch(batchStats);
            } catch (ExecutionException | RuntimeException e) {
                inserterStats.failedBatch();
                logger.error("Batch insertion failed", e);
            } catch (InterruptedException e) {
                inserterStats.failedBatch();
                Thread.currentThread().interrupt();
            }
        } else {
            logger.error("Batch completion sent before task was complete; canceling: {}", future);
            future.cancel(true);
            inserterStats.failedBatch();
        }
        synchronized (executingBatches) {
            executingBatches.remove(future);
            pendingExecutionCount.decrementAndGet();
            // close processing does a timed wait loop for `executingBatches` to drain, so we
            // need to notify in case we're in that state, to allow the close to progress
            executingBatches.notifyAll();
        }
    }

    /**
     * A class that performs the database operations on a batch of records once the submitted task
     * becomes active.
     */
    private class InsertTask implements Callable<BatchStats> {

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
        InsertTask(@Nonnull DbInserter<OutT> dbInserter, int batchNo,
                @Nonnull List<OutT> records) {
            this.dbInserter = dbInserter;
            this.records = records;
            this.batchNo = batchNo;
        }

        @Override
        @Nonnull
        public BatchStats call() throws InterruptedException {
            // time spent either doing work that failed, or backing off before a retry
            long lostTimeNanos = 0L;
            final long finalLostTimeNanos = lostTimeNanos;
            // perform retries until our retry limit is exhausted
            for (int i = 0; i <= batchRetryBackoffsMsec.length; i++) {
                Stopwatch operationTimer = Stopwatch.createStarted();
                try {
                    return dsl.transactionResult(trans -> {
                        // use our dbInserter to actually perform the operation
                        dbInserter.insert(outTable, records, trans.dsl());
                        // commit the results, and record the operation in log and in metrics
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
                        return BatchStats.goodBatch(records.size(), workTimeNanos,
                                finalLostTimeNanos);
                    });
                } catch (DataAccessException e) {
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
                        logger.error("Failed to save {} records in batch #{} "
                                        + "to table {} after {} tries: {}",
                                records.size(),
                                batchNo,
                                inTable.getName(),
                                batchRetryBackoffsMsec.length,
                                trimJooqErrorMessage(e));
                        SharedMetrics.BATCHED_INSERTS
                                .labels(inTable.getName(), BatchInsertDisposition.failure.name())
                                .increment();
                        // this will cause the task to be counted as a failed execution
                        return BatchStats.failedBatch(finalLostTimeNanos);
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
     * Close the inserter, after flushing its pending records (if any) and awaiting completion of
     * any still pending batches (possibly including a batch arising from the flush operation).
     *
     * <p>Final statistics for the loader may be logged.</p>
     *
     * @param statsLogger Logger where stats are reported; null means do not log
     * @throws InterruptedException if interrupted
     */
    public void close(Logger statsLogger) throws InterruptedException {
        // only one closer actually does the work of closing; subsequent closers can't
        // return until that work is finished
        if (!closed.getAndSet(true)) {
            flush(true);
            if (pendingExecutionCount.get() > 0) {
                logger.warn("Some batch executions still pending at close for out table {}",
                        outTable.getName());
                inserterStats.setMayBePartial();
            }
            if (statsLogger != null) {
                inserterStats.logStats(statsLogger);
            }
        }
        // close our out table if it's transient
        if (dropOnClose) {
            try {
                dsl.dropTable(outTable).execute();
            } catch (DataAccessException e) {
                // create our own logger for this if the caller didn't provide one
                final Logger log = statsLogger != null ? statsLogger : LogManager.getLogger();
                log.error(
                        "Failed to drop transient bulk inserter table {} when inserter was closed",
                        outTable.getName(), e);
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
     * <p>The times will increase in roughly exponential fashion up to the maximum configured
     * by {@link #maxRetryBackoffMsec} config option, and the overall schedule length will be as
     * configured by {@link #maxBatchRetries}.</p>
     *
     * @return backoff schedule
     */
    private int[] computeBatchRetryBackoffs(int maxBatchRetries, int maxBatchRetryBackoffMsec) {
        int[] backoffs = new int[maxBatchRetries];

        // prior lengths are progressively halved
        int i = backoffs.length - 1;
        backoffs[i] = maxBatchRetryBackoffMsec;

        // prior lengths are progressively halved
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
    public static class BatchStats {

        private final boolean failed;
        private final int records;
        private final long workTimeNanos;
        private final long lostTimeNanos;

        /**
         * Create a new instance.
         * @param failed true if the batch failed in its execution
         * @param records nubmer of records inserted
         * @param workTimeNanos amount of time spent performing insertion
         * @param lostTimeNanos time spent waiting for retry or exeucting a failed insertion
         */
        public BatchStats(boolean failed, int records, long workTimeNanos, long lostTimeNanos) {
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
         * Get nanoseconds of work time executing this batch.
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
