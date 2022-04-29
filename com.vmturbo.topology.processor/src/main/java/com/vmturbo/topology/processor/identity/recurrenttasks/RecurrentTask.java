package com.vmturbo.topology.processor.identity.recurrenttasks;

import static com.vmturbo.topology.processor.db.Tables.RECURRENT_TASKS;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;

import com.vmturbo.proactivesupport.DataMetricHistogram;
import com.vmturbo.topology.processor.db.tables.records.RecurrentTasksRecord;

/**
 * Abstract class that takes care of common operations across tasks. This includes measuring the time, handling
 * errors and writing to the {@link com.vmturbo.topology.processor.db.tables.pojos.RecurrentTasks} table.
 */
public abstract class RecurrentTask {
    /**
     * Batch size for oids updates.
     */
    @VisibleForTesting
    public static final int BATCH_SIZE = 100;
    static final int MAX_ERROR_LENGTH = 150;
    static final Logger logger = LogManager.getLogger();

    final long currentTimeMs;
    final Clock clock;
    final DSLContext context;

    /**
     * Track the time taken to perform and oid expiration task. This will be broken into two steps:
     *   "update_timestamps" -- the amount of time it takes to update the last seen timestamps of the oids
     *   "expire_records" -- the amount of time it takes to expire the oids.
     */
    static final DataMetricHistogram OID_EXPIRATION_EXECUTION_TIME = DataMetricHistogram.builder()
            .withName("turbo_oid_expiration_execution_seconds")
            .withHelp("Time (in seconds) spent to perform an oid expiration task.")
            .withLabelNames("step")
            .withBuckets(1, 10, 100)
            .build()
            .register();

    /**
     * Supported types of Recurrent tasks.
     */
    public enum RecurrentTasksEnum {
        /**
         * Task that updates the last seen of assigned identity records.
         */
        OID_TIMESTAMP_UPDATE,
        /**
         * Task that set the expiration of assigned identity records and drops them from the memory cache.
         */
        OID_EXPIRATION,
        /**
         * Task that deletes expired records from the database.
         */
        OID_DELETION;
    }

    RecurrentTask(final long taskStartingTime, @Nonnull final Clock clock, @Nonnull final DSLContext context) {
        this.currentTimeMs = taskStartingTime;
        this.context = context;
        this.clock = clock;
    }

    /**
     * Run a {@link RecurrentTask}.
     * @return the amount of affected records
     */
    public int run() {
        final RecurrentTaskRecord recurrentTask = new RecurrentTaskRecord(getRecurrentTaskType(), LocalDateTime.ofInstant(Instant.ofEpochMilli(currentTimeMs), clock.getZone()));
        int affectedRows = 0;
        final Stopwatch stopwatch = Stopwatch.createStarted();
        logger.info("Starting the {} task", getRecurrentTaskType());
        final String summary = "Task took %s";
        try {
            affectedRows = performTask();
            recurrentTask.setSuccessfulTaskAndStore(affectedRows, String.format(summary, stopwatch), context);
            logger.info("Finished {} task successfully. Updated {} records", getRecurrentTaskType(), affectedRows);
        } catch (Exception e) {
            logger.error("Error with the {} task", getRecurrentTaskType().toString(), e);
            recurrentTask.setFailedTaskAndStore(affectedRows, String.format(summary, stopwatch), e.getMessage(), context);
        }
        return affectedRows;
    }

    abstract int performTask();

    abstract RecurrentTasksEnum getRecurrentTaskType();

    /**
     * Class to represent an {@link RecurrentTaskRecord record} to insert in the database.
     */
    @VisibleForTesting
    public static class RecurrentTaskRecord {
        private final RecurrentTasksEnum recurrentTask;
        private final LocalDateTime executionTime;
        private boolean successful = true;
        private int affectedRows = 0;
        private String summary = "";
        private String errors = "";

        RecurrentTaskRecord(@Nonnull final RecurrentTasksEnum recurrentTask, @Nonnull final LocalDateTime executionTime) {
            this.executionTime = executionTime;
            this.recurrentTask = recurrentTask;
        }

        /**
         * Build an instance of a {@link RecurrentTaskRecord}.
         * @param recurrentTaskRecord used to initialize the instance
         */
        public RecurrentTaskRecord(@Nonnull RecurrentTasksRecord recurrentTaskRecord) {
            this.recurrentTask = RecurrentTasksEnum.valueOf(recurrentTaskRecord.getOperationName());
            this.executionTime = recurrentTaskRecord.getExecutionTime();
            this.successful = recurrentTaskRecord.getSuccessful();
            this.affectedRows = recurrentTaskRecord.getAffectedRows();
            this.summary = recurrentTaskRecord.getSummary();
            this.errors = recurrentTaskRecord.getErrors();
        }

        void setSuccessfulTaskAndStore(final int affectedRows, final String summary, @Nonnull final DSLContext context) {
            this.successful = true;
            this.affectedRows = affectedRows;
            this.summary = summary;
            store(context);
        }

        void setFailedTaskAndStore(final int affectedRows, final String summary, final String errors, final DSLContext context) {
            this.successful = false;
            this.summary = summary;
            this.affectedRows = affectedRows;
            this.errors = errors.substring(0, Math.min(errors.length(), MAX_ERROR_LENGTH));
            store(context);
        }

        void store(@Nonnull final DSLContext context) {
            context.insertInto(RECURRENT_TASKS, RECURRENT_TASKS.EXECUTION_TIME, RECURRENT_TASKS.OPERATION_NAME,
                            RECURRENT_TASKS.AFFECTED_ROWS, RECURRENT_TASKS.SUCCESSFUL, RECURRENT_TASKS.SUMMARY, RECURRENT_TASKS.ERRORS)
                    .values(executionTime, recurrentTask.toString(), affectedRows, successful, summary, errors).execute();
        }

        /**
         * Serialize the object into a csv format for diags.
         * @return the csv
         */
        public String toCsvLine() {
            return String.format("%s, %s, %s, %d, %s, %s", this.executionTime, recurrentTask,
                    this.successful, this.affectedRows, this.summary, this.errors);
        }
    }
}