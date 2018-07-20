package com.vmturbo.topology.processor.controllable;

import static com.vmturbo.topology.processor.db.Tables.ENTITY_ACTION;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.topology.processor.db.enums.EntityActionStatus;
import com.vmturbo.topology.processor.db.tables.records.EntityActionRecord;

public class EntityActionDaoImp implements EntityActionDao {
    private final Logger logger = LogManager.getLogger();

    private final DSLContext dsl;

    // For "in progress" or 'queued' entity action records. if their last update time is older than this
    // time threshold, they will be deleted from entity action tables. This try to handle the case that
    // if Probe is down and can not send back action progress notification, they will be considered
    // as time out actions and be cleaned up.
    private final int controllableSucceedRecordExpiredSeconds;

    // For "succeed" entity action records, if their last update time is older than this time threshold,
    // they will be deleted from entity action table. This makes succeed entities will not participate
    // Market analysis immediately, it will have some default cool down time.
    private final int controllableInProgressExpiredSeconds;

    public EntityActionDaoImp(@Nonnull final DSLContext dsl,
                              final int controllableSucceedRecordExpiredSeconds,
                              final int controllableInProgressExpiredSeconds) {
        this.dsl = Objects.requireNonNull(dsl);
        this.controllableSucceedRecordExpiredSeconds = controllableSucceedRecordExpiredSeconds;
        this.controllableInProgressExpiredSeconds = controllableInProgressExpiredSeconds;
    }

    @Override
    public void insertAction(final long actionId, @Nonnull final Set<Long> entityIds) {
        if (entityIds.isEmpty()) {
            return;
        }
        dsl.transaction(configuration -> {
            final LocalDateTime now = LocalDateTime.now(ZoneOffset.UTC);
            final DSLContext transactionDsl = DSL.using(configuration);
            for (final long entityId : entityIds) {
                transactionDsl.insertInto(ENTITY_ACTION)
                        .set(ENTITY_ACTION.ACTION_ID, actionId)
                        .set(ENTITY_ACTION.ENTITY_ID, entityId)
                        .set(ENTITY_ACTION.STATUS, EntityActionStatus.queued)
                        .set(ENTITY_ACTION.UPDATE_TIME, now)
                        .execute();
            }
        });

    }

    @Override
    public void updateActionState(final long actionId, @Nonnull final ActionState newState)
            throws ControllableRecordNotFoundException {
        try {
            dsl.transaction(configuration -> {
                final DSLContext transactionDsl = DSL.using(configuration);
                final LocalDateTime now = LocalDateTime.now(ZoneOffset.UTC);
                List<EntityActionRecord> records = getInProgressRecordsByActionId(transactionDsl, actionId);
                // for actionId rows, update their status to new state and time to current.
                for (EntityActionRecord record : records) {
                    record.setStatus(getActionState(newState));
                    record.setUpdateTime(now);
                }
                transactionDsl.batchUpdate(records).execute();
            });
        } catch (DataAccessException e) {
            if (e.getCause() instanceof ControllableRecordNotFoundException) {
                throw (ControllableRecordNotFoundException) e.getCause();
            } else {
                throw e;
            }
        }
    }

    /**
     * First it will delete all expired records from controllable tables. For 'failed' status records,
     * it will delete all of them. For 'queued' and 'in progress' status records, it has a
     * configured {@link #controllableInProgressExpiredSeconds} which stores the timeout threshold
     * about when they should be considered as expired. For 'succeed' status records, it has a
     * configured {@link #controllableSucceedRecordExpiredSeconds} which has a different timeout
     * threshold to determine when to delete. After delete expired records, it will get all entity
     * ids which status is 'in progress' and 'succeed', those entity ids are the all not controllable
     * entities.
     */
    @Override
    public Set<Long> getNonControllableEntityIds() {
        return dsl.transactionResult(configuration -> {
            LocalDateTime now = LocalDateTime.now(ZoneOffset.UTC);
            final DSLContext transactionDsl = DSL.using(configuration);
            final LocalDateTime expiredInProgressThresholdTime =
                    now.minusSeconds(controllableInProgressExpiredSeconds);
            final LocalDateTime expiredSucceedThresholdTime =
                    now.minusSeconds(controllableSucceedRecordExpiredSeconds);
            final long deletedQueuedOrInProgressControllableCount = transactionDsl.deleteFrom(ENTITY_ACTION)
                    .where((ENTITY_ACTION.STATUS.eq(EntityActionStatus.in_progress)
                                .or(ENTITY_ACTION.STATUS.eq(EntityActionStatus.queued)))
                            .and(ENTITY_ACTION.UPDATE_TIME.lessOrEqual(expiredInProgressThresholdTime)))
                    .execute();
            if (deletedQueuedOrInProgressControllableCount > 0) {
                logger.warn("Deleted {} rows out of date in progress controllable records which update " +
                                "time is less than {}",
                        deletedQueuedOrInProgressControllableCount, expiredInProgressThresholdTime);
            }
            // delete all failed entity action records.
            transactionDsl.deleteFrom(ENTITY_ACTION)
                    .where(ENTITY_ACTION.STATUS.eq(EntityActionStatus.failed))
                    .execute();
            // delete all expired succeed records.
            transactionDsl.deleteFrom(ENTITY_ACTION)
                    .where(ENTITY_ACTION.STATUS.eq(EntityActionStatus.succeed)
                            .and(ENTITY_ACTION.UPDATE_TIME.lessOrEqual(expiredSucceedThresholdTime)))
                    .execute();
            // after deleted expired records, the rest of 'succeed' or 'in progress' status records
            // are the entities not controllable. And for 'queued' records, it means its action is not
            // be executed by probes yet, so it is still controllable.
            return transactionDsl.selectFrom(ENTITY_ACTION)
                    .where(ENTITY_ACTION.STATUS.eq(EntityActionStatus.in_progress)
                            .or(ENTITY_ACTION.STATUS.eq(EntityActionStatus.succeed)))
                    .fetchSet(ENTITY_ACTION.ENTITY_ID);
        });
    }

    private List<EntityActionRecord> getInProgressRecordsByActionId(final DSLContext transactionDsl,
                                                                    final long actionId)
            throws ControllableRecordNotFoundException {
        List<EntityActionRecord> records = transactionDsl.selectFrom(ENTITY_ACTION)
                .where(ENTITY_ACTION.ACTION_ID.eq(actionId)
                        .and(ENTITY_ACTION.STATUS.eq(EntityActionStatus.in_progress)
                                .or(ENTITY_ACTION.STATUS.eq(EntityActionStatus.queued))))
                .fetch();
        if (records.isEmpty()) {
            throw new ControllableRecordNotFoundException(actionId);
        }
        return records;
    }

    private EntityActionStatus getActionState(@Nonnull final ActionState actionState)
        throws NotSupportedActionStateException {
        switch (actionState) {
            case IN_PROGRESS:
                return EntityActionStatus.in_progress;
            case SUCCEEDED:
                return EntityActionStatus.succeed;
            case FAILED:
                return EntityActionStatus.failed;
            default:
                logger.error("Not supported action state: {}", actionState);
                throw new NotSupportedActionStateException(actionState);
        }
    }

    public static class ControllableRecordNotFoundException extends Exception {
        public ControllableRecordNotFoundException(final long actionId) {
            super("Controllable record with actionId: " + actionId + " not found in existing database.");
        }
    }

    public static class NotSupportedActionStateException extends Exception {
        public NotSupportedActionStateException(@Nonnull final ActionState actionState) {
            super("Not supported action state: " + actionState);
        }
    }
}