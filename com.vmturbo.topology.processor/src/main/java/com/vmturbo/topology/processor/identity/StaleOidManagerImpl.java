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
import java.util.Optional;
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
 * {@link OidExpirationTask} every {@link StaleOidManagerImpl#validationFrequency} that sets the
 * last_seen value for all the oids that exist in the entity store. It then gets the
 * oids that haven't been seen for more than {@link StaleOidManagerImpl#entityExpirationTime}, send
 * them to kafka and set their expired value to true.
 */
public class StaleOidManagerImpl implements StaleOidManager {

    /**
     * Name of the task run by the StaleOidManager.
     */
    public static final String EXPIRATION_TASK_NAME = "OID_EXPIRATION_TASK";
    private static final Logger logger = LogManager.getLogger();

    private final long entityExpirationTime;
    private final long validationFrequency;
    private final DSLContext context;
    private final Clock clock;
    private Supplier<Set<Long>> getCurrentOids;
    private boolean expireOids;
    private final ScheduledExecutorService executorService;
    private final Map<Integer, Long> expirationDaysPerEntity;
    private Consumer<Set<Long>> listener;

    /**
     * Creates an instance of a {@link StaleOidManagerImpl}.
     * @param entityExpirationTime amount of time before an entity gets expired if not present in
     *                            the entity store
     * @param validationFrequency how often we perform the {@link OidExpirationTask}
     * @param context used to interact with the database.
     * @param clock used to get a consistent current time
     * @param expireOids if enabled oids are expired, otherwise we only set the last_seen timestamp
     * @param executorService executor service to schedule a recurring task
     * @param expirationDaysPerEntity expiration days per entity type
     */
    public StaleOidManagerImpl(final long entityExpirationTime,
                               final long validationFrequency,
                               @Nonnull DSLContext context, final Clock clock,
                               final boolean expireOids,
                               @Nonnull final ScheduledExecutorService executorService,
                               @Nonnull final Map<String, String> expirationDaysPerEntity) {
        this.entityExpirationTime = entityExpirationTime;
        this.validationFrequency = validationFrequency;
        this.context = context;
        this.clock = clock;
        this.expireOids = expireOids;
        this.executorService = executorService;
        this.expirationDaysPerEntity = parseExpirationTimesPerEntityType(expirationDaysPerEntity);
    }

    /**
     * Initializes the {@link StaleOidManagerImpl}. It checks when was the last time an
     * {@link OidExpirationTask} has been run, if any. Then it calculates when the next
     * {@link OidExpirationTask} will be run, and schedule it with a constant delay of
     * {@link StaleOidManagerImpl#validationFrequency}. If no task has been run in the past, the
     * initial delay will be equal to {@link StaleOidManagerImpl#validationFrequency}. If a task has
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
        long currentTimeMs = clock.millis();
        Optional<LocalDateTime> optionalLastValidationTime = getLatestOidExpirationExecutionTime();
        long latestValidationTime = optionalLastValidationTime
            .map(localDateTime -> Timestamp.valueOf(localDateTime).getTime()).orElse(currentTimeMs);
        long timeToNextExpirationTask =
            Math.max(0, validationFrequency - (currentTimeMs - latestValidationTime));
        logger.info("Initializing StaleOidManager with expiration set to {}. Next task will happen in {} hours."
                        + " After that there will be a task running every {} hours", this.expireOids,
                TimeUnit.MILLISECONDS.toHours(timeToNextExpirationTask), TimeUnit.MILLISECONDS.toHours(validationFrequency));
        if (!expirationDaysPerEntity.keySet().isEmpty()) {
            logger.info("StaleOidManager settings: {}", this.expirationDaysPerEntity);
        }
        return this.executorService.scheduleWithFixedDelay(new OidExpirationTask(getCurrentOids,
                        this::notifyListener, entityExpirationTime, context, expireOids, clock,
                        this.expirationDaysPerEntity), timeToNextExpirationTask,
                validationFrequency, TimeUnit.MILLISECONDS);
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
                entityExpirationTime, context, expireOids, clock, this.expirationDaysPerEntity);
        Future<?> future = this.executorService.submit(task);
        future.get(5, TimeUnit.MINUTES);
        return task.getNumberOfExpiredOids();
    }

    private Optional<LocalDateTime> getLatestOidExpirationExecutionTime() throws DataAccessException {
        Record1<LocalDateTime> latestExecutionTime =
            context.select(RECURRENT_OPERATIONS.EXECUTION_TIME).from(RECURRENT_OPERATIONS)
            .where(RECURRENT_OPERATIONS.OPERATION_NAME.eq(EXPIRATION_TASK_NAME))
            .orderBy(RECURRENT_OPERATIONS.EXECUTION_TIME.desc())
            .limit(1).fetchOne();
        return latestExecutionTime != null
            ? Optional.of(latestExecutionTime.into(LocalDateTime.class)) : Optional.empty();
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
     * 2) Get all the oids that haven't been seen for {@link OidExpirationTask#entityExpirationTime}
     * 3) Send all those oids to a kafka topic (NOT IMPLEMENTED YET TODO: OM-68900)
     * 4) Set all those oids as expired in the
     * {@link com.vmturbo.topology.processor.db.tables.AssignedIdentity} table
     * 5) Sets the result of the operation in the
     * {@link com.vmturbo.topology.processor.db.tables.RecurrentOperations} table
     */
    private static class OidExpirationTask implements Runnable {

        private final DSLContext context;
        private final long entityExpirationTime;
        private final Supplier<Set<Long>> getCurrentOids;
        private final Consumer<Set<Long>> notifyExpiredOids;
        private long currentTimeMs;
        private int numberOfExpiredOids;
        private final boolean expireOids;
        private final Clock clock;
        private final Map<Integer, Long> expirationDaysPerEntity;

        OidExpirationTask(@Nonnull final Supplier<Set<Long>> getCurrentOids,
                          @Nonnull final Consumer<Set<Long>> notifyExpiredOids,
                          final long entityExpirationTime,
                          final DSLContext context, final boolean expireOids, final Clock clock,
                          final Map<Integer, Long> expirationTimePerEntity) {
            this.getCurrentOids = getCurrentOids;
            this.notifyExpiredOids = notifyExpiredOids;
            this.entityExpirationTime = entityExpirationTime;
            this.context = context;
            this.expireOids = expireOids;
            this.clock = clock;
            this.expirationDaysPerEntity = expirationTimePerEntity;
        }

        /**
         * Run the task.
         */
        @Override
        public synchronized void run() {
            this.currentTimeMs = clock.millis();
            setLastSeenTimeStamps(getCurrentOids.get());
            if (expireOids) {
                try {
                    Set<Long> expiredOids = getExpiredRecords(entityExpirationTime, expirationDaysPerEntity);
                    sendExpiredOids(expiredOids);
                    setExpiredRecords(expirationDaysPerEntity);
                    notifyExpiredOids.accept(expiredOids);
                    setSuccesfulOidExpirationTask(expiredOids.size());
                    numberOfExpiredOids = expiredOids.size();
                    logger.info("OidExpirationTask finished in {} seconds. Number of expired oids: {}",
                        TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - currentTimeMs),
                        numberOfExpiredOids);
                } catch (DataAccessException e) {
                    logger.error("OidExpirationTask failed due to ", e);
                    setFailedOidExpirationTask(e.getMessage());
                }
            }
        }

        private int setLastSeenTimeStamps(Set<Long> currentOids) throws DataAccessException {
            return context.update(ASSIGNED_IDENTITY)
                .set(ASSIGNED_IDENTITY.LAST_SEEN, new Timestamp(currentTimeMs))
                .where(ASSIGNED_IDENTITY.ID.in(currentOids)).execute();
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
                Timestamp.from(Instant.ofEpochMilli(currentTimeMs - entityExpirationTime));
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

        private void setSuccesfulOidExpirationTask(int nExpiredOids) throws DataAccessException {
            final String summary = String.format("Expired Oids : %d", nExpiredOids);
            context.insertInto(RECURRENT_OPERATIONS,
                RECURRENT_OPERATIONS.EXECUTION_TIME,
                RECURRENT_OPERATIONS.OPERATION_NAME,
                RECURRENT_OPERATIONS.SUMMARY)
                .values(LocalDateTime.ofInstant(Instant.ofEpochMilli(currentTimeMs), clock.getZone()),
                EXPIRATION_TASK_NAME, summary).execute();
        }

        private void setFailedOidExpirationTask(String error) throws DataAccessException {
            final String summary = String.format("Failed due to : %s", error);
            context.insertInto(RECURRENT_OPERATIONS,
                RECURRENT_OPERATIONS.EXECUTION_TIME,
                RECURRENT_OPERATIONS.OPERATION_NAME,
                RECURRENT_OPERATIONS.SUMMARY)
                .values(LocalDateTime.ofInstant(Instant.ofEpochMilli(currentTimeMs), clock.getZone()),
                    EXPIRATION_TASK_NAME, summary).execute();
        }

        // TODO: this method will be implemented as part of OM-68900
        private void sendExpiredOids(Set<Long> expiredOids) {

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
    }
}
