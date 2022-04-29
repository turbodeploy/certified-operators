package com.vmturbo.topology.processor.identity.recurrenttasks;

import static com.vmturbo.topology.processor.db.Tables.RECURRENT_OPERATIONS;
import static com.vmturbo.topology.processor.db.tables.AssignedIdentity.ASSIGNED_IDENTITY;

import java.sql.Timestamp;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import javax.annotation.Nonnull;

import com.google.common.base.Stopwatch;

import org.jooq.DSLContext;
import org.jooq.Record1;
import org.jooq.Result;
import org.jooq.SelectConditionStep;
import org.jooq.SelectQuery;
import org.jooq.UpdateConditionStep;
import org.jooq.exception.DataAccessException;

import com.vmturbo.proactivesupport.DataMetricCounter;
import com.vmturbo.topology.processor.db.tables.records.AssignedIdentityRecord;
import com.vmturbo.topology.processor.identity.StaleOidManagerImpl;

/**
 * Implementation of a {@link RecurrentTask} for setting the expiration for the oids.
 */
public class OidExpirationTask extends RecurrentTask {
    /**
     * Name of the task run by the StaleOidManager.
     */
    public static final String EXPIRATION_TASK_NAME = "OID_EXPIRATION_TASK";

    /**
     * A counter metric that represents the total number of expired oids.
     */
    private static final DataMetricCounter EXPIRED_ENTITIES_COUNT = DataMetricCounter.builder()
            .withName("turbo_expired_entities_total")
            .withHelp("Total number of entity OIDs expired by an oid expiration task since topology processor started.")
            .build()
            .register();

    private static final String EXPIRE_RECORDS = "expire_records";

    private final boolean shouldExpireOids;
    private final Consumer<Set<Long>> notifyExpiredOids;
    private final Map<Integer, Long> expirationDaysPerEntity;
    final StaleOidManagerImpl.OidExpirationResultRecord oidExpirationResultRecord;
    final long entityExpirationTimeMs;

    /**
     * Builds an instance of a {@link OidExpirationTask}.
     * @param entityExpirationTimeMs the expiration time
     * @param shouldExpireOids whether the task should expire oids or not
     * @param notifyExpiredOids function that drops the oids from the cache
     * @param expirationDaysPerEntity expiration times broken down by entity type
     * @param oidExpirationResultRecord record to store in the recurrent operations
     * @param taskStartingTime the starting time of the task
     * @param clock clock shared among other tasks
     * @param context the dsl context
     */
    public OidExpirationTask(final long entityExpirationTimeMs, final boolean shouldExpireOids, @Nonnull final Consumer<Set<Long>> notifyExpiredOids,
                             @Nonnull Map<Integer, Long> expirationDaysPerEntity,
                             @Nonnull final StaleOidManagerImpl.OidExpirationResultRecord oidExpirationResultRecord,
                             final long taskStartingTime, @Nonnull final Clock clock, @Nonnull final DSLContext context) {
        super(taskStartingTime, clock, context);
        this.shouldExpireOids = shouldExpireOids;
        this.notifyExpiredOids = notifyExpiredOids;
        this.expirationDaysPerEntity = expirationDaysPerEntity;
        this.oidExpirationResultRecord = oidExpirationResultRecord;
        this.entityExpirationTimeMs = entityExpirationTimeMs;
    }

    @Override
    public int performTask() {
        final Stopwatch stopwatch = Stopwatch.createStarted();
        int numberOfExpiredOids = 0;
        try {
            if (shouldExpireOids) {
                Set<Long> expiredOids = getRecordsToExpire(entityExpirationTimeMs, expirationDaysPerEntity);
                numberOfExpiredOids = expireRecords(expirationDaysPerEntity, oidExpirationResultRecord);
                notifyExpiredOids.accept(expiredOids);
                OID_EXPIRATION_EXECUTION_TIME.labels(EXPIRE_RECORDS).observe((double)stopwatch.elapsed(TimeUnit.SECONDS));
            }
        } catch (Exception e) {
            logger.error("OID_EXPIRATION task failed due to ", e);
            oidExpirationResultRecord.setErrors(e.getMessage().substring(0, Math.min(e.getMessage().length(), MAX_ERROR_LENGTH)));
            throw e;
        } finally {
            storeOidExpirationResultRecord(oidExpirationResultRecord);
        }
        return numberOfExpiredOids;
    }

    @Override
    public RecurrentTasksEnum getRecurrentTaskType() {
        return RecurrentTasksEnum.OID_EXPIRATION;
    }

    private Set<Long> getRecordsToExpire(long entityExpirationTime, @Nonnull final Map<Integer, Long> expirationDaysPerEntity) throws DataAccessException {
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
            for (Map.Entry<Integer, Long> entry: expirationDaysPerEntity.entrySet()) {
                SelectQuery<Record1<Long>> queryPerEntity =
                        getQueryForSelectingExpiredRecordsByEntity(entry.getKey(), entry.getValue());
                updatedRecords.addAll(queryPerEntity.fetch());
            }
        } else {
            updatedRecords = defaultExpirationQuery.fetch();
        }
        return updatedRecords.intoSet(ASSIGNED_IDENTITY.ID);
    }

    private int expireRecords(@Nonnull final Map<Integer, Long> expirationDaysPerEntity, StaleOidManagerImpl.OidExpirationResultRecord oidExpirationResultRecord) throws DataAccessException {
        int updatedRecords = 0;
        Timestamp expirationDateMs =
                Timestamp.from(Instant.ofEpochMilli(currentTimeMs - entityExpirationTimeMs));
        UpdateConditionStep<AssignedIdentityRecord> defaultUpdateQuery = context.update(ASSIGNED_IDENTITY)
                .set(ASSIGNED_IDENTITY.EXPIRED, true)
                .where(ASSIGNED_IDENTITY.LAST_SEEN
                        .lessThan(expirationDateMs)).and(ASSIGNED_IDENTITY.EXPIRED.isFalse());
        if (!expirationDaysPerEntity.keySet().isEmpty()) {
            updatedRecords += defaultUpdateQuery.andNot(ASSIGNED_IDENTITY.ENTITY_TYPE.in(expirationDaysPerEntity.keySet())).execute();
            for (Map.Entry<Integer, Long> entry: expirationDaysPerEntity.entrySet()) {
                UpdateConditionStep<AssignedIdentityRecord> queryPerEntity =
                        getQueryForSettingExpiredRecordsByEntity(entry.getKey(), entry.getValue());
                updatedRecords += queryPerEntity.execute();
            }
        } else {
            updatedRecords = defaultUpdateQuery.execute();
        }
        if (updatedRecords > 0) {
            EXPIRED_ENTITIES_COUNT.increment((double)updatedRecords);
        }
        oidExpirationResultRecord.setExpiredRecords(updatedRecords);
        return updatedRecords;
    }

    private void storeOidExpirationResultRecord(
            @Nonnull final StaleOidManagerImpl.OidExpirationResultRecord oidExpirationResultRecord) throws DataAccessException {
        context.insertInto(RECURRENT_OPERATIONS,
                        RECURRENT_OPERATIONS.EXECUTION_TIME,
                        RECURRENT_OPERATIONS.OPERATION_NAME,
                        RECURRENT_OPERATIONS.EXPIRATION_SUCCESSFUL,
                        RECURRENT_OPERATIONS.LAST_SEEN_UPDATE_SUCCESSFUL,
                        RECURRENT_OPERATIONS.EXPIRED_RECORDS,
                        RECURRENT_OPERATIONS.UPDATED_RECORDS,
                        RECURRENT_OPERATIONS.ERRORS)
                .values(LocalDateTime.ofInstant(Instant.ofEpochMilli(currentTimeMs), clock.getZone()),
                        EXPIRATION_TASK_NAME, oidExpirationResultRecord.isSuccessfulExpiration(), oidExpirationResultRecord.isSuccessfulUpdate(),
                        oidExpirationResultRecord.getExpiredRecords(), oidExpirationResultRecord.getUpdatedRecords(),
                        oidExpirationResultRecord.getErrors()).execute();
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
