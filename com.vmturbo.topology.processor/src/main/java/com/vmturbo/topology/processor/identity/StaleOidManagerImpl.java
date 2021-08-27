package com.vmturbo.topology.processor.identity;

import static com.vmturbo.topology.processor.db.Tables.RECURRENT_OPERATIONS;
import static com.vmturbo.topology.processor.db.tables.AssignedIdentity.ASSIGNED_IDENTITY;

import java.sql.Timestamp;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Supplier;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Record1;
import org.jooq.Result;
import org.jooq.SelectConditionStep;
import org.jooq.SelectQuery;
import org.jooq.UpdateConditionStep;
import org.jooq.exception.DataAccessException;

import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.topology.processor.db.tables.records.AssignedIdentityRecord;

/**
 * Class that handles the expiration of the oids. This class performs a periodic
 * {@link OidExpirationTask} every {@link StaleOidManagerImpl#validationFrequencyMs} that sets the
 * last_seen value for all the oids that exist in the entity store. It then gets the
 * oids that haven't been seen for more than {@link StaleOidManagerImpl#entityExpirationTimeMs},
 * set their expired value to true. See https://vmturbo.atlassian.net/wiki/spaces/XD/pages/2372600988/Stale+OID+Management
 * for more information about this feature.
 */
public class StaleOidManagerImpl implements StaleOidManager {

    /**
     * Name of the task run by the StaleOidManager.
     */
    public static final String EXPIRATION_TASK_NAME = "OID_EXPIRATION_TASK";
    private static final Logger logger = LogManager.getLogger();

    private final long entityExpirationTimeMs;
    private final long validationFrequencyMs;
    private final DSLContext context;
    private final Clock clock;
    private final boolean expireOids;
    private final ScheduledExecutorService executorService;
    private final Map<Integer, Long> expirationDaysPerEntity;
    private final long initialExpirationDelayMs;
    private Consumer<Set<Long>> listener;
    private Supplier<Set<Long>> getCurrentOids;


    /**
     * Creates an instance of a {@link StaleOidManagerImpl}.
     * @param entityExpirationTimeMs amount of time before an entity gets expired if not present in
     *                            the entity store
     * @param validationFrequencyMs how often we perform the {@link OidExpirationTask}
     * @param initialExpirationDelayMs initial delay for the {@link OidExpirationTask}
     * @param context used to interact with the database.
     * @param clock used to get a consistent current time
     * @param expireOids if enabled oids are expired, otherwise we only set the last_seen timestamp
     * @param executorService executor service to schedule a recurring task
     * @param expirationDaysPerEntity expiration days per entity type
     */
    public StaleOidManagerImpl(final long entityExpirationTimeMs,
                               final long validationFrequencyMs,
                               final long initialExpirationDelayMs,
                               @Nonnull DSLContext context, final Clock clock,
                               final boolean expireOids,
                               @Nonnull final ScheduledExecutorService executorService,
                               @Nonnull final Map<String, String> expirationDaysPerEntity) {
        this.entityExpirationTimeMs = entityExpirationTimeMs;
        this.validationFrequencyMs = validationFrequencyMs;
        this.context = context;
        this.clock = clock;
        this.expireOids = expireOids;
        this.executorService = executorService;
        this.expirationDaysPerEntity = parseExpirationTimesPerEntityType(expirationDaysPerEntity);
        this.initialExpirationDelayMs = initialExpirationDelayMs;
    }

    /**
     * Initializes the {@link StaleOidManagerImpl}. It checks when was the last time an
     * {@link OidExpirationTask} has been run, if any. Then it calculates when the next
     * {@link OidExpirationTask} will be run, and schedule it with a constant delay of
     * {@link StaleOidManagerImpl#validationFrequencyMs}. If no task has been run in the past, the
     * initial delay will be equal to {@link StaleOidManagerImpl#validationFrequencyMs}. If a task has
     * been run in the past, the initial delay will be equal to the validation frequency minus
     * the difference between the current time, and the time of the latest operation.
     *
     * @param getCurrentOids function to get the oids present in the entity store.
     * @param listener to notify when oids are marked stale.
     * @return a ScheduledFuture with the task currently running
     */
    public ScheduledFuture<?> initialize(@Nonnull final Supplier<Set<Long>> getCurrentOids,
            @Nonnull Consumer<Set<Long>> listener) {
        this.getCurrentOids = getCurrentOids;
        this.listener = listener;
        logger.info("Initializing StaleOidManager with expiration set to {}. Next task will happen in {} hours."
                        + " After that there will be a task running every {} hours", this.expireOids,
                TimeUnit.MILLISECONDS.toHours(initialExpirationDelayMs), TimeUnit.MILLISECONDS.toHours(
                        validationFrequencyMs));
        if (!expirationDaysPerEntity.keySet().isEmpty()) {
            logger.info("StaleOidManager settings: {}", this.expirationDaysPerEntity);
        }
        return this.executorService.scheduleWithFixedDelay(new OidExpirationTask(getCurrentOids,
                        this::notifyListener, entityExpirationTimeMs, context, expireOids, clock,
                        this.expirationDaysPerEntity, validationFrequencyMs, false), initialExpirationDelayMs,
                validationFrequencyMs, TimeUnit.MILLISECONDS);
    }

    private void notifyListener(@Nonnull Set<Long> removedOids) {
        logger.debug("Notifying listener of {} oids removed", removedOids.size());
        if (listener != null) {
            listener.accept(removedOids);
        } else {
            logger.error("No listener provided. Stale OIDs will not be cleared from cache.");
        }
    }

    /**
     * Runs an {@link OidExpirationTask} asynchronously. It waits for the thread to be done.
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
        OidExpirationTask task = new OidExpirationTask(this.getCurrentOids, this::notifyListener,
                entityExpirationTimeMs, context, expireOids, clock, this.expirationDaysPerEntity,
                validationFrequencyMs,
                true);
        Future<?> future = this.executorService.submit(task);
        future.get(5, TimeUnit.MINUTES);
        return task.getNumberOfExpiredOids();
    }

    private HashMap<Integer, Long> parseExpirationTimesPerEntityType(Map<String, String> expirationDaysPerEntity) {
        HashMap<Integer, Long> expirationTimePerEntity = new HashMap<>();
        for (Entry<String, String> entry : expirationDaysPerEntity.entrySet()) {
            try {
                expirationTimePerEntity.put(CommonDTO.EntityDTO.EntityType.valueOf(entry.getKey()).getNumber(),
                    Math.max(0, TimeUnit.DAYS.toMillis(Integer.parseInt(entry.getValue()))));
            } catch (IllegalArgumentException e) {
                logger.error("Could not convert yaml parameter {} into an EntityType, will "
                    + "skip this setting", entry.getKey());
            }
        }
        return expirationTimePerEntity;
    }

    /**
     * Task that expires the oids. These are the operations performed by this task:
     * 1) Set the last_seen timestamp of the oids retrieved from {@link OidExpirationTask#getCurrentOids}
     * 2) Get all the oids that haven't been seen for {@link OidExpirationTask#entityExpirationTimeMs}
     * 3) Set all those oids as expired in the {@link com.vmturbo.topology.processor.db.tables.AssignedIdentity} table
     * 4) Sets the result of the operation in the {@link com.vmturbo.topology.processor.db.tables.RecurrentOperations} table
     */
    private static class OidExpirationTask implements Runnable {

        private final DSLContext context;
        private final long entityExpirationTimeMs;
        private final Supplier<Set<Long>> getCurrentOids;
        private final Consumer<Set<Long>> notifyExpiredOids;
        private final boolean expireOids;
        private final Clock clock;
        private final Map<Integer, Long> expirationDaysPerEntity;
        private final long validationFrequencyMs;
        private final boolean forceExpiration;
        private long currentTimeMs;
        private int numberOfExpiredOids;
        private OidExpirationResultRecord oidExpirationResultRecord;

        OidExpirationTask(@Nonnull final Supplier<Set<Long>> getCurrentOids, @Nonnull final Consumer<Set<Long>> notifyExpiredOids,
                final long entityExpirationTimeMs, final DSLContext context, final boolean expireOids,
                final Clock clock, final Map<Integer, Long> expirationTimePerEntity,
                long validationFrequencyMs, boolean forceExpiration) {
            this.getCurrentOids = getCurrentOids;
            this.notifyExpiredOids = notifyExpiredOids;
            this.entityExpirationTimeMs = entityExpirationTimeMs;
            this.context = context;
            this.expireOids = expireOids;
            this.clock = clock;
            this.expirationDaysPerEntity = expirationTimePerEntity;
            this.validationFrequencyMs = validationFrequencyMs;
            this.forceExpiration = forceExpiration;
        }

        /**
         * Run the task.
         */
        @Override
        public synchronized void run() {
            try {
                this.oidExpirationResultRecord = new OidExpirationResultRecord(Instant.ofEpochMilli(clock.millis()));
                this.currentTimeMs = clock.millis();
                final int updatedRecords = setLastSeenTimeStamps(getCurrentOids.get());
                oidExpirationResultRecord.setUpdatedRecords(updatedRecords);
                if (shouldExpireOids()) {
                    Set<Long> expiredOids = getExpiredRecords(entityExpirationTimeMs, expirationDaysPerEntity);
                    setExpiredRecords(expirationDaysPerEntity);
                    oidExpirationResultRecord.setExpiredRecords(expiredOids.size());
                    notifyExpiredOids.accept(expiredOids);
                    numberOfExpiredOids = expiredOids.size();
                    logger.info("OidExpirationTask finished in {} seconds. Number of expired oids: {}",
                            TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - currentTimeMs),
                            numberOfExpiredOids);
                }
            // We need to catch all the exceptions to make sure the scheduled tasks will keep going even
            // if one task fails
            } catch (Exception e) {
                logger.error("OidExpirationTask failed due to ", e);
                oidExpirationResultRecord.setErrors(e.getMessage());
            } finally {
                try {
                    storeOidExpirationResultRecord(oidExpirationResultRecord);
                } catch (Exception additionalException) {
                    logger.error("Could not write failure of OidExpirationTask due to ",
                            additionalException);
                }
            }
        }

        /**
         * Determines whether or not an {@link OidExpirationTask} should also perform an expiration or
         * just update the last_seen flag. We should not expire in the case the expireOids is set to false
         * OR if in the past entityExpirationTime we haven't successfully run at least 50% of the tasks we were
         * supposed to run. The number of runs that are supposed to run in a entityExpirationTime time range is given by
         * entityExpirationTime / validationFrequency
         * @return whether or not oids should be expired
         */
        private boolean shouldExpireOids() {
            // This should only be true when the task is manually triggered by the api
            if (forceExpiration) {
                return true;
            }
            if (!expireOids) {
                return false;
            }
            long entityExpirationTimeDays = TimeUnit.MILLISECONDS.toDays(entityExpirationTimeMs);
            int successfulUpdatesCount = context.selectCount()
                    .from(RECURRENT_OPERATIONS)
                    .where(RECURRENT_OPERATIONS.EXECUTION_TIME
                            .greaterThan(LocalDateTime.now().minusDays(entityExpirationTimeDays))
                            .and(RECURRENT_OPERATIONS.LAST_SEEN_UPDATE_SUCCESSFUL.isTrue())).fetchOne(0, int.class);
            int expectedNumberOfSuccessfulTasks = (int)(entityExpirationTimeMs / validationFrequencyMs) / 2;
            if (successfulUpdatesCount >= expectedNumberOfSuccessfulTasks) {
                return true;
            }

            logger.info("Not enough successful tasks performed in the past {} days. Number of tasks executed: {},"
                    + "Expected number of tasks to be executed to run expiration: {}", entityExpirationTimeDays, successfulUpdatesCount, expectedNumberOfSuccessfulTasks);

            return false;
        }

        private int setLastSeenTimeStamps(Set<Long> currentOids) throws DataAccessException {
            final Timestamp currentTimeStamp = new Timestamp(currentTimeMs);
            final int updatedOids =  context.update(ASSIGNED_IDENTITY)
                .set(ASSIGNED_IDENTITY.LAST_SEEN, currentTimeStamp)
                .where(ASSIGNED_IDENTITY.ID.in(currentOids)).execute();
            logger.info("OidExpirationTask updated the last_seen column to {} for {} oids", currentTimeStamp, updatedOids);
            return updatedOids;
        }

        private Set<Long> getExpiredRecords(long entityExpirationTime, Map<Integer, Long> expirationDaysPerEntity) throws DataAccessException {
            Timestamp expirationDateMs =
                Timestamp.from(Instant.ofEpochMilli(currentTimeMs - entityExpirationTime));
            final Result<Record1<Long>> updatedRecords;
            SelectConditionStep<Record1<Long>> defaultExpirationQuery =
                context.select(ASSIGNED_IDENTITY.ID)
                .from(ASSIGNED_IDENTITY)
                .where(ASSIGNED_IDENTITY.LAST_SEEN
                    .lessThan(expirationDateMs)).and(ASSIGNED_IDENTITY.EXPIRED.isFalse());

            if (!expirationDaysPerEntity.keySet().isEmpty()) {
                updatedRecords = defaultExpirationQuery.andNot(ASSIGNED_IDENTITY.ENTITY_TYPE.in(expirationDaysPerEntity.keySet())).fetch();
                for (Entry<Integer, Long> entry: expirationDaysPerEntity.entrySet()) {
                    SelectQuery<Record1<Long>> queryPerEntity =
                        getQueryForSelectingExpiredRecordsByEntity(entry.getKey(), entry.getValue());
                    updatedRecords.addAll(queryPerEntity.fetch());
                }
            } else {
                updatedRecords = defaultExpirationQuery.fetch();
            }
            return updatedRecords.intoSet(ASSIGNED_IDENTITY.ID);
        }

        private int setExpiredRecords(Map<Integer, Long> expirationDaysPerEntity) throws DataAccessException {
            int updatedRecords = 0;
            Timestamp expirationDateMs =
                Timestamp.from(Instant.ofEpochMilli(currentTimeMs - entityExpirationTimeMs));
             UpdateConditionStep<AssignedIdentityRecord> defaultUpdateQuery = context.update(ASSIGNED_IDENTITY)
                .set(ASSIGNED_IDENTITY.EXPIRED, true)
                .where(ASSIGNED_IDENTITY.LAST_SEEN
                    .lessThan(expirationDateMs)).and(ASSIGNED_IDENTITY.EXPIRED.isFalse());
            if (!expirationDaysPerEntity.keySet().isEmpty()) {
                updatedRecords += defaultUpdateQuery.andNot(ASSIGNED_IDENTITY.ENTITY_TYPE.in(expirationDaysPerEntity.keySet())).execute();
                for (Entry<Integer, Long> entry: expirationDaysPerEntity.entrySet()) {
                    UpdateConditionStep<AssignedIdentityRecord> queryPerEntity =
                        getQueryForSettingExpiredRecordsByEntity(entry.getKey(), entry.getValue());
                    updatedRecords += queryPerEntity.execute();
                }
            } else {
                updatedRecords = defaultUpdateQuery.execute();
            }
            return updatedRecords;
        }

        private void storeOidExpirationResultRecord(
                @Nonnull final OidExpirationResultRecord oidExpirationResultRecord) throws DataAccessException {
            context.insertInto(RECURRENT_OPERATIONS,
                RECURRENT_OPERATIONS.EXECUTION_TIME,
                RECURRENT_OPERATIONS.OPERATION_NAME,
                RECURRENT_OPERATIONS.EXPIRATION_SUCCESSFUL,
                RECURRENT_OPERATIONS.LAST_SEEN_UPDATE_SUCCESSFUL,
                RECURRENT_OPERATIONS.EXPIRED_RECORDS,
                RECURRENT_OPERATIONS.UPDATED_RECORDS,
                RECURRENT_OPERATIONS.ERRORS)
                .values(LocalDateTime.ofInstant(oidExpirationResultRecord.getTimeStamp(), clock.getZone()),
                        EXPIRATION_TASK_NAME, oidExpirationResultRecord.isSuccessfulExpiration(), oidExpirationResultRecord.isSuccessfulUpdate(),
                        oidExpirationResultRecord.getExpiredRecords(), oidExpirationResultRecord.getUpdatedRecords(),
                        oidExpirationResultRecord.getErrors()).execute();
        }

        public int getNumberOfExpiredOids() {
            return numberOfExpiredOids;
        }

        private SelectQuery<Record1<Long>> getQueryForSelectingExpiredRecordsByEntity(int entityType,
                                                                                      long entityExpirationTimeMs) {
            Timestamp entityExpirationDateMs =
                Timestamp.from(Instant.ofEpochMilli(currentTimeMs - entityExpirationTimeMs));
            return context.select(ASSIGNED_IDENTITY.ID)
                .from(ASSIGNED_IDENTITY)
                .where(ASSIGNED_IDENTITY.LAST_SEEN
                    .lessThan(entityExpirationDateMs)).and(ASSIGNED_IDENTITY.EXPIRED.isFalse())
                .and(ASSIGNED_IDENTITY.ENTITY_TYPE.eq(entityType)).getQuery();
        }

        private UpdateConditionStep<AssignedIdentityRecord> getQueryForSettingExpiredRecordsByEntity(int entityType,
                                                                                                     long entityExpirationTimeMs) {
            Timestamp entityExpirationDateMs =
                Timestamp.from(Instant.ofEpochMilli(currentTimeMs - entityExpirationTimeMs));
            return context.update(ASSIGNED_IDENTITY)
                .set(ASSIGNED_IDENTITY.EXPIRED, true)
                .where(ASSIGNED_IDENTITY.LAST_SEEN
                    .lessThan(entityExpirationDateMs)).and(ASSIGNED_IDENTITY.EXPIRED.isFalse())
                .and(ASSIGNED_IDENTITY.ENTITY_TYPE.eq(entityType));
        }

        /**
         * Class to represent an {@link OidExpirationTask record} to insert in the database.
         */
        private static class OidExpirationResultRecord {
            private final Instant timeStamp;
            private boolean successfulUpdate;
            private boolean successfulExpiration;
            private int updatedRecords;
            private int expiredRecords;
            private String errors;

            OidExpirationResultRecord(Instant timeStamp) {
                this.timeStamp = timeStamp;
                this.successfulExpiration = false;
                this.successfulUpdate = false;
                this.updatedRecords = 0;
                this.expiredRecords = 0;
            }

            public void setUpdatedRecords(int updatedRecords) {
                this.successfulUpdate = true;
                this.updatedRecords = updatedRecords;
            }

            public void setExpiredRecords(int expiredRecords) {
                this.successfulExpiration = true;
                this.expiredRecords = expiredRecords;
            }

            public void setErrors(String errors) {
                this.errors = errors;
            }

            public Instant getTimeStamp() {
                return timeStamp;
            }

            public int getUpdatedRecords() {
                return updatedRecords;
            }

            public int getExpiredRecords() {
                return expiredRecords;
            }

            public String getErrors() {
                return errors;
            }

            public boolean isSuccessfulUpdate() {
                return successfulUpdate;
            }

            public boolean isSuccessfulExpiration() {
                return successfulExpiration;
            }
        }
    }
}
