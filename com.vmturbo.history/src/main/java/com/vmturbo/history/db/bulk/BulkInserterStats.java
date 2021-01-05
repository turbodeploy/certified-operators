package com.vmturbo.history.db.bulk;

import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.logging.log4j.Logger;
import org.jooq.Table;

import com.vmturbo.history.db.bulk.BulkInserter.BatchStats;

/**
 * Class to accumulate statistics for a {@link BulkInserter} instance.
 */
@ThreadSafe
public class BulkInserterStats {

    private final Object key;
    private final Table<?> inTable;
    private final Table<?> outTable;

    // total number of records successfully written
    private long written = 0L;

    // total number of successful batches
    private int batches = 0;
    // total number of failed batches
    private int failedBatches = 0;

    // total nanoseconds spent working on database operations for this writer, including failed
    // attempts
    private long workTimeNanos = 0L;
    // total nanoseconds spent
    private long lostTimeNanos;

    /**
     * Create a new instance.
     *
     * @param key      the inserter's key value
     * @param inTable  the inserter's inTable
     * @param outTable the inseter's outTable
     */
    public BulkInserterStats(Object key, Table<?> inTable, Table<?> outTable) {
        this.key = key;
        this.inTable = inTable;
        this.outTable = outTable;
    }

    public Object getKey() {
        return key;
    }

    public Table<?> getInTable() {
        return inTable;
    }

    public Table<?> getOutTable() {
        return outTable;
    }

    /**
     * Get number of records successfully written by this writer.
     *
     * @return number of records written
     */
    public synchronized long getWritten() {
        return written;
    }

    /**
     * Get number of batches successfully completed by this writer.
     *
     * @return number of batches
     */
    public synchronized int getBatches() {
        return batches;
    }

    /**
     * Get number of batches that ultimately failed for this writer.
     *
     * @return number of failed batches
     */
    public synchronized int getFailedBatches() {
        return failedBatches;
    }

    /**
     * Get total time spent in database operations by this writer.
     *
     * @return work time, in nanoseconds
     */
    public synchronized long getWorkTimeNanos() {
        return workTimeNanos;
    }

    /**
     * Get total time spent in backoff waits prior to retries.
     *
     * @return lost time, in nanoseconds
     */
    public synchronized long getLostTimeNanos() {
        return lostTimeNanos;
    }

    /**
     * Register stats for a batch execution.
     *
     * @param batchStats {@link BulkInserter.BatchStats} object
     */
    synchronized void updateForBatch(BatchStats batchStats) {
        if (batchStats.isFailed()) {
            failedBatches += 1;
        } else {
            batches += 1;
        }
        this.written += batchStats.getRecords();
        this.workTimeNanos += batchStats.getWorkTimeNanos();
        this.lostTimeNanos += batchStats.getLostTimeNanos();
    }

    /**
     * Incorporate new data into this stats object.
     *
     * @param written addition to written records count
     * @param batches addition to batch count
     * @param failedBatches addition to failed batch count
     * @param workTimeNanos addition to work time
     * @param lostTimeNanos addition to lost time
     */
    public synchronized void update(final long written,
                       final int batches,
                       final int failedBatches,
                       final long workTimeNanos,
                       final long lostTimeNanos) {
        this.written += written;
        this.batches += batches;
        this.failedBatches += failedBatches;
        this.workTimeNanos += workTimeNanos;
        this.lostTimeNanos += lostTimeNanos;
    }

    /**
     * Register a failed batch.
     */
    synchronized void failedBatch() {
        this.failedBatches += 1;
    }

    /**
     * Report overall statistics for the associated writer.
     *
     * @param logger a logger to use in making the report
     */
    public synchronized void logStats(Logger logger) {
        double workSecs = TimeUnit.NANOSECONDS.toSeconds(workTimeNanos);
        final String ratePerSec = String.format("%.1f",
                // don't go with written / secs cuz latter is often rounded to zero
                (double)written * TimeUnit.SECONDS.toNanos(1) / workTimeNanos);
        String workTimeString = formatNanos(workTimeNanos);
        String lostTimeString = formatNanos(lostTimeNanos);
        logger.info("Table {}: wrote {} recs ({}/sec) in {} batches in {}; "
                        + "{} lost in retries; {} failed batches",
                inTable != null ? inTable.getName() : "(unknown)", written, ratePerSec, batches,
                workTimeString, lostTimeString, failedBatches);
    }

    private @Nonnull
    String formatNanos(long nanos) {
        long millis = TimeUnit.NANOSECONDS.toMillis(nanos);
        return DurationFormatUtils.formatDuration(millis, "H:ss.SSS");
    }

}
