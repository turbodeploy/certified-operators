package com.vmturbo.topology.processor.identity;

import static com.vmturbo.topology.processor.db.Tables.RECURRENT_OPERATIONS;
import static com.vmturbo.topology.processor.db.Tables.RECURRENT_TASKS;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;

import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.topology.processor.identity.recurrenttasks.OidDeletionTask;
import com.vmturbo.topology.processor.identity.recurrenttasks.OidExpirationTask;
import com.vmturbo.topology.processor.identity.recurrenttasks.OidTimestampUpdateTask;
import com.vmturbo.topology.processor.identity.recurrenttasks.RecurrentTask;

/**
 * Class that handles the expiration of the oids. This class performs a periodic
 * {@link OidManagement} every {@link OidManagementParameters#validationFrequencyMs} that sets the
 * last_seen value for all the oids that exist in the entity store. It then gets the
 * oids that haven't been seen for more than {@link OidManagementParameters#entityExpirationTimeMs},
 * set their expired value to true. See https://vmturbo.atlassian.net/wiki/spaces/XD/pages/2372600988/Stale+OID+Management
 * for more information about this feature.
 */
public class StaleOidManagerImpl implements StaleOidManager {

    /**
     * Header for the diags.
     */
    public static final String DIAGS_HEADER = "Execution time, Task name, Successful Update, Successful Expiration, Updated Records, Expired Records, Errors";

    /**
     * Maximum number of operations that we want to copy in the diags.
     */
    public static final int N_OPERATIONS_IN_DIAGS = 500;

    /**
     * Name of the file that will appear in the diags.
     */
    public static final String DIAGS_FILE_NAME = "RecurrentOperations.csv";

    private static final Logger logger = LogManager.getLogger();
    private static final int RECURRENT_OPERATIONS_RETENTION_YEARS = 1;

    private final ScheduledExecutorService executorService;
    private final long initialExpirationDelayMs;
    private final DSLContext context;
    private final OidManagementParameters oidManagementParameters;
    private Supplier<Set<Long>> getCurrentOids;
    private Consumer<Set<Long>> notifyExpiredOids;

    /**
     * Creates an instance of a {@link StaleOidManagerImpl}.
     * @param initialExpirationDelayMs initial delay for the {@link OidManagement}
     */
    public StaleOidManagerImpl(long initialExpirationDelayMs, @Nonnull final ScheduledExecutorService oidsExpirationScheduledExecutor,
                               @Nonnull OidManagementParameters oidManagementParameters) {
        this.initialExpirationDelayMs = initialExpirationDelayMs;
        this.executorService = oidsExpirationScheduledExecutor;
        this.oidManagementParameters = oidManagementParameters;
        this.context = oidManagementParameters.getContext();
    }

    @Nonnull
    @Override
    public String getFileName() {
        return DIAGS_FILE_NAME;
    }

    @Override
    public void collectDiags(@Nonnull DiagnosticsAppender appender) throws DiagnosticsException {
        appender.appendString(DIAGS_HEADER);
        List<RecurrentTask.RecurrentTaskRecord> taskRecords = getLatestRecurrentTasks(N_OPERATIONS_IN_DIAGS);
        for (RecurrentTask.RecurrentTaskRecord record : taskRecords) {
            appender.appendString(record.toCsvLine());
        }
    }

    /**
     * Initializes the {@link StaleOidManagerImpl}. The initialization consists in scheduling a {@link OidManagement}
     * to run periodically, every {@link OidManagementParameters#validationFrequencyMs} and with an initial
     * delay of {@link StaleOidManagerImpl#initialExpirationDelayMs}.
     *
     * @param getCurrentOids function to get the oids present in the entity store.
     * @param notifyExpiredOids to notify when oids are marked stale.
     * @return a ScheduledFuture with the task currently running
     */
    public ScheduledFuture<?> initialize(@Nonnull final Supplier<Set<Long>> getCurrentOids,
            @Nonnull Consumer<Set<Long>> notifyExpiredOids) {
        this.getCurrentOids = getCurrentOids;
        this.notifyExpiredOids = notifyExpiredOids;
        logger.info("Initializing StaleOidManager with expiration set to {}. Next task will happen in {} hours."
                        + " After that there will be a task running every {} hours", oidManagementParameters.isExpireOids(),
                TimeUnit.MILLISECONDS.toHours(initialExpirationDelayMs), TimeUnit.MILLISECONDS.toHours(
                        oidManagementParameters.getValidationFrequencyMs()));
        if (!oidManagementParameters.getExpirationDaysPerEntity().keySet().isEmpty()) {
            logger.info("StaleOidManager settings: {}", oidManagementParameters.getExpirationDaysPerEntity());
        }
        deleteLatestRecurrentOperations();
        return this.executorService.scheduleWithFixedDelay(new OidManagement(oidManagementParameters, getCurrentOids, notifyExpiredOids), initialExpirationDelayMs,
                oidManagementParameters.getValidationFrequencyMs(), TimeUnit.MILLISECONDS);
    }

    /**
     * Runs an {@link OidManagement} asynchronously. It waits for the thread to be done.
     * Should only be used for triggering this process from the api.
     *
     * @return the number of expired oids
     * @throws ExecutionException if the computation threw an
     * exception
     * @throws InterruptedException if the expiration oid thread was interrupted
     * @throws TimeoutException if the wait timed out
     * while waiting
     */
    public int expireOidsImmediatly() throws InterruptedException, ExecutionException, TimeoutException {
        logger.info("Running the OidExpirationTask asynchronously");
        final OidManagementParameters oidManagementParameters =
                new StaleOidManagerImpl.OidManagementParameters.Builder(this.oidManagementParameters.getEntityExpirationTimeMs(),
                        context, this.oidManagementParameters.isExpireOids(), this.oidManagementParameters.getClock(),
                        this.oidManagementParameters.getValidationFrequencyMs(), true,
                        this.oidManagementParameters.expiredRecordsRetentionDays)
                        .setForceExpiration(true).build();
        OidManagement task = new OidManagement(oidManagementParameters, this.getCurrentOids, this.notifyExpiredOids);
        Future<?> future = this.executorService.submit(task);
        future.get(20, TimeUnit.MINUTES);
        return task.getNumberOfExpiredOids();
    }

    private List<RecurrentTask.RecurrentTaskRecord> getLatestRecurrentTasks(final int nOperations) {
        return context.selectFrom(RECURRENT_TASKS)
                .orderBy(RECURRENT_TASKS.EXECUTION_TIME.desc()).limit(nOperations).fetch().stream().map(
                        RecurrentTask.RecurrentTaskRecord::new).collect(Collectors.toList());
    }

    /**
     * Delete the recurrent operations that are older than {@link StaleOidManagerImpl#RECURRENT_OPERATIONS_RETENTION_YEARS}.
     * This should be done only once after every restart of the component.
     * @return the number of deleted operations
     */
    private int deleteLatestRecurrentOperations() {
        int deletedOperations = context.deleteFrom(RECURRENT_OPERATIONS)
                .where(RECURRENT_OPERATIONS.EXECUTION_TIME.lessThan(LocalDateTime.now().minusYears(RECURRENT_OPERATIONS_RETENTION_YEARS)))
                .execute();
        logger.info("Deleted {} operations from the recurrent operations table", deletedOperations);
        return deletedOperations;
    }

    /**
     * Class used to facilitate the usage of input parameters for the {@link StaleOidManagerImpl}. It guarantees immutability
     */
    static class OidManagementParameters {
        private final DSLContext context;
        private final long entityExpirationTimeMs;

        public DSLContext getContext() {
            return context;
        }

        public long getEntityExpirationTimeMs() {
            return entityExpirationTimeMs;
        }

        public boolean isExpireOids() {
            return expireOids;
        }

        public Clock getClock() {
            return clock;
        }

        public long getValidationFrequencyMs() {
            return validationFrequencyMs;
        }

        public boolean isShouldDeleteExpiredRecords() {
            return shouldDeleteExpiredRecords;
        }

        public boolean isForceExpiration() {
            return forceExpiration;
        }

        public Map<Integer, Long> getExpirationDaysPerEntity() {
            return expirationDaysPerEntity;
        }

        public int getExpiredRecordsRetentionDays() {
            return expiredRecordsRetentionDays;
        }

        private final boolean expireOids;
        private final Clock clock;
        private final long validationFrequencyMs;
        private final boolean shouldDeleteExpiredRecords;
        private final boolean forceExpiration;
        private final Map<Integer, Long> expirationDaysPerEntity;
        private final int expiredRecordsRetentionDays;

        /**
         * Instance of a builder.
         */
        static class Builder {
            // Required parameters
            private final DSLContext context;
            private final long entityExpirationTimeMs;
            private final boolean expireOids;
            private final Clock clock;
            private final long validationFrequencyMs;
            private final boolean shouldDeleteExpiredRecords;
            private final int expiredRecordsRetentionDays;

            // Optional parameters
            private boolean forceExpiration = false;
            private Map<Integer, Long> expirationDaysPerEntity = Collections.emptyMap();

            /**
             * Gets an instance of the Builder.
             * @param entityExpirationTimeMs the entity expiration time
             * @param context to access the database
             * @param expireOids the expiration flag
             * @param clock clock shared among the tasks
             * @param validationFrequencyMs the frequency for the tasks
             * @param shouldDeleteExpiredRecords the deletion flag
             * @param expiredRecordsRetentionDays the amount of days before deleting an expired record
             */
            Builder(final long entityExpirationTimeMs, @Nonnull final DSLContext context, final boolean expireOids,
                    @Nonnull final Clock clock, long validationFrequencyMs, boolean shouldDeleteExpiredRecords, int expiredRecordsRetentionDays) {
                this.entityExpirationTimeMs = entityExpirationTimeMs;
                this.context = context;
                this.expireOids = expireOids;
                this.clock = clock;
                this.validationFrequencyMs = validationFrequencyMs;
                this.shouldDeleteExpiredRecords = shouldDeleteExpiredRecords;
                this.expiredRecordsRetentionDays = expiredRecordsRetentionDays;
            }

            Builder setForceExpiration(boolean forceExpiration) {
                this.forceExpiration = forceExpiration;
                return this;
            }

            Builder setExpirationDaysPerEntity(final Map<String, String> expirationDaysPerEntity) {
                this.expirationDaysPerEntity = parseExpirationTimesPerEntityType(expirationDaysPerEntity);
                return this;
            }

            private HashMap<Integer, Long> parseExpirationTimesPerEntityType(Map<String, String> expirationDaysPerEntity) {
                HashMap<Integer, Long> expirationTimePerEntity = new HashMap<>();
                for (Map.Entry<String, String> entry : expirationDaysPerEntity.entrySet()) {
                    try {
                        expirationTimePerEntity.put(CommonDTO.EntityDTO.EntityType.valueOf(entry.getKey()).getNumber(),
                                Math.max(0, TimeUnit.DAYS.toMillis(Integer.parseInt(entry.getValue()))));
                    } catch (IllegalArgumentException e) {
                        logger.error("Could not convert yaml parameter {} into an EntityType, will "
                                + "skip this setting", entry.getKey(), e);
                    }
                }
                return expirationTimePerEntity;
            }

            OidManagementParameters build() {
                return new OidManagementParameters(this);
            }
        }

        private OidManagementParameters(@Nonnull Builder builder) {
            context = builder.context;
            entityExpirationTimeMs = builder.entityExpirationTimeMs;
            expireOids = builder.expireOids;
            clock = builder.clock;
            validationFrequencyMs = builder.validationFrequencyMs;
            shouldDeleteExpiredRecords = builder.shouldDeleteExpiredRecords;
            forceExpiration = builder.forceExpiration;
            expirationDaysPerEntity = builder.expirationDaysPerEntity;
            expiredRecordsRetentionDays = builder.expiredRecordsRetentionDays;
        }

    }

    /**
     * Runnable that performs all the tasks related to oid management. At the moment it runs a {@link OidTimestampUpdateTask},
     * a {@link OidExpirationTask} and a {@link OidDeletionTask}.
     */
    private static class OidManagement implements Runnable {
        private final OidManagementParameters oidManagementParameters;
        private final Supplier<Set<Long>> getCurrentOids;
        private final Consumer<Set<Long>> expireOidsFromCache;
        private int numberOfExpiredOids;

        OidManagement(@Nonnull OidManagementParameters oidManagementParameters, final Supplier<Set<Long>> getCurrentOids,
                      @Nonnull final Consumer<Set<Long>> expireOidsFromCache) {
            this.oidManagementParameters = oidManagementParameters;
            this.getCurrentOids = getCurrentOids;
            this.expireOidsFromCache = expireOidsFromCache;
        }

        /**
         * Run the task.
         */
        @Override
        public synchronized void run() {
            try {
                final Clock clock = oidManagementParameters.getClock();
                final DSLContext context = oidManagementParameters.getContext();
                long currentTimeMs = clock.millis();
                final boolean shouldExpireOids = shouldExpireOids(oidManagementParameters);
                new OidTimestampUpdateTask(currentTimeMs, clock, context.dsl(), getCurrentOids).run();
                numberOfExpiredOids = new OidExpirationTask(oidManagementParameters.getEntityExpirationTimeMs(), shouldExpireOids,
                        expireOidsFromCache, oidManagementParameters.getExpirationDaysPerEntity(), currentTimeMs, clock, context.dsl()).run();
                new OidDeletionTask(currentTimeMs, clock, context.dsl(),  oidManagementParameters.isShouldDeleteExpiredRecords(),
                        oidManagementParameters.getExpiredRecordsRetentionDays()).run();
            } catch (Throwable t) {
                // We need to catch all the exceptions to make sure the scheduled runnable will keep going even
                // if one task fails
                logger.error("Error in performing the expiration and dropping of the oids ", t);
            }
        }

        public int getNumberOfExpiredOids() {
            return numberOfExpiredOids;
        }

        /**
         * Determines whether an {@link OidManagement} should also perform an expiration or
         * just update the last_seen flag. We should not expire in the case the expireOids is set to false
         * OR if in the past entityExpirationTime we haven't successfully run at least 50% of the tasks we were
         * supposed to run. The number of runs that are supposed to run in a entityExpirationTime time range is given by
         * entityExpirationTime / validationFrequency
         * @return whether oids should be expired
         */
        private boolean shouldExpireOids(@Nonnull OidManagementParameters oidManagementParameters) {
            // This should only be true when the task is manually triggered by the api
            if (oidManagementParameters.isForceExpiration()) {
                return true;
            }
            if (!oidManagementParameters.isExpireOids()) {
                return false;
            }
            final long entityExpirationTimeMs = oidManagementParameters.getEntityExpirationTimeMs();
            long entityExpirationTimeDays = TimeUnit.MILLISECONDS.toDays(entityExpirationTimeMs);
            int successfulUpdatesCount = oidManagementParameters.getContext().selectCount()
                    .from(RECURRENT_TASKS)
                    .where(RECURRENT_TASKS.EXECUTION_TIME
                            .greaterThan(LocalDateTime.now().minusDays(entityExpirationTimeDays))
                            .and(RECURRENT_TASKS.OPERATION_NAME.eq(RecurrentTask.RecurrentTasksEnum.OID_TIMESTAMP_UPDATE.toString()))
                            .and(RECURRENT_TASKS.SUCCESSFUL.isTrue())).fetchOne(0, int.class);
            int expectedNumberOfSuccessfulTasks = (int)(entityExpirationTimeMs / oidManagementParameters.getValidationFrequencyMs()) / 2;
            if (successfulUpdatesCount >= expectedNumberOfSuccessfulTasks) {
                return true;
            }

            logger.info("Not enough successful tasks performed in the past {} days. Number of tasks executed: {},"
                    + "Expected number of tasks to be executed to run expiration: {}", entityExpirationTimeDays, successfulUpdatesCount, expectedNumberOfSuccessfulTasks);

            return false;
        }


    }
}
