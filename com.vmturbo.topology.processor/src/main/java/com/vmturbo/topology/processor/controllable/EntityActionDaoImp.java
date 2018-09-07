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
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.topology.processor.db.enums.EntityActionStatus;
import com.vmturbo.topology.processor.db.enums.EntityActionActionType;
import com.vmturbo.topology.processor.db.tables.records.EntityActionRecord;

public class EntityActionDaoImp implements EntityActionDao {
    private final Logger logger = LogManager.getLogger();

    private final DSLContext dsl;

    // For "in progress" or 'queued' move action records. if their last update time is older than this
    // time threshold, they will be deleted from entity action tables. This try to handle the case that
    // if Probe is down and can not send back action progress notification, they will be considered
    // as time out actions and be cleaned up.
    private final int moveSucceedRecordExpiredSeconds;

    // For "succeed" move action records, if their last update time is older than this time threshold,
    // they will be deleted from entity action table. This makes succeed entities will not participate
    // Market analysis immediately, it will have some default cool down time.
    private final int activateOrMoveInProgressExpiredSeconds;

    // For "succeed" activate action records, if their last update time is older than this time threshold,
    // they will be deleted from entity action table. This immediately allow succeed entities suspendable.
    private final int activateSucceedExpiredSeconds;

    public EntityActionDaoImp(@Nonnull final DSLContext dsl,
                              final int moveSucceedRecordExpiredSeconds,
                              final int activateOrMoveInProgressExpiredSeconds,
                              final int activateSucceedExpiredSeconds) {
        this.dsl = Objects.requireNonNull(dsl);
        this.moveSucceedRecordExpiredSeconds = moveSucceedRecordExpiredSeconds;
        this.activateOrMoveInProgressExpiredSeconds = activateOrMoveInProgressExpiredSeconds;
        this.activateSucceedExpiredSeconds = activateSucceedExpiredSeconds;
    }

    @Override
    public void insertAction(final long actionId, final ActionType actionType, @Nonnull final Set<Long> entityIds) 
            throws IllegalArgumentException {
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
                        .set(ENTITY_ACTION.ACTION_TYPE, getActionType(actionType))
                        .set(ENTITY_ACTION.STATUS, EntityActionStatus.queued)
                        .set(ENTITY_ACTION.UPDATE_TIME, now)
                        .execute();
            }
        });
        logger.info("Queued action {} into controllable table", actionId);
    }

    /**
     * Get EntityActionActionType from action DTO's action type
     * @param actionType action type from action DTO
     * @return EntityActionActionType
     */
    private EntityActionActionType getActionType(final ActionType actionType) {
        if (actionType == ActionType.ACTIVATE) {
            return EntityActionActionType.activate;
        } else if (actionType == ActionType.MOVE) {
            return EntityActionActionType.move;
        } else {
            throw new IllegalArgumentException("Inserting an action with type " + actionType);
        }
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
                logger.info("Update action {} status to {} in controllable table", actionId, newState.name());
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
     * It will delete all expired MOVE action records from controllable tables. For 'failed' status records,
     * it will delete all of them. For 'queued' and 'in progress' status records, it has a
     * configured {@link #activateOrMoveInProgressExpiredSeconds} which stores the timeout threshold
     * about when they should be considered as expired. For 'succeed' status records, it has a
     * configured {@link #moveSucceedRecordExpiredSeconds} which has a different timeout
     * threshold to determine when to delete. After delete expired MOVE action records, it will get
     * from MOVE action records all entity ids which status is 'in progress' and 'succeed', those entity
     * ids are the not controllable entities.
     */
    @Override
    public Set<Long> getNonControllableEntityIds() {
        return dsl.transactionResult(configuration -> {
            LocalDateTime now = LocalDateTime.now(ZoneOffset.UTC);
            final DSLContext transactionDsl = DSL.using(configuration);
            final LocalDateTime expiredInProgressThresholdTime =
                    now.minusSeconds(activateOrMoveInProgressExpiredSeconds);
            final LocalDateTime expiredSucceedThresholdTime =
                    now.minusSeconds(moveSucceedRecordExpiredSeconds);
            final long deletedQueuedOrInProgressControllableCount = transactionDsl.deleteFrom(ENTITY_ACTION)
                    .where((ENTITY_ACTION.STATUS.eq(EntityActionStatus.in_progress)
                                .or(ENTITY_ACTION.STATUS.eq(EntityActionStatus.queued)))
                            .and(ENTITY_ACTION.ACTION_TYPE.eq(EntityActionActionType.move))
                            .and(ENTITY_ACTION.UPDATE_TIME.lessOrEqual(expiredInProgressThresholdTime)))
                    .execute();
            if (deletedQueuedOrInProgressControllableCount > 0) {
                logger.warn("Deleted {} rows out of date in progress move action records which update " +
                                "time is less than {}",
                        deletedQueuedOrInProgressControllableCount, expiredInProgressThresholdTime);
            }
            // delete all failed move entity action records.
            transactionDsl.deleteFrom(ENTITY_ACTION)
                    .where(ENTITY_ACTION.STATUS.eq(EntityActionStatus.failed)
                            .and(ENTITY_ACTION.ACTION_TYPE.eq(EntityActionActionType.move)))
                    .execute();
            // delete all expired succeed records.
            transactionDsl.deleteFrom(ENTITY_ACTION)
                    .where(ENTITY_ACTION.STATUS.eq(EntityActionStatus.succeed)
                            .and(ENTITY_ACTION.UPDATE_TIME.lessOrEqual(expiredSucceedThresholdTime))
                            .and(ENTITY_ACTION.ACTION_TYPE.eq(EntityActionActionType.move)))
                    .execute();
            // after deleted expired records, the rest of 'succeed' or 'in progress' or 'queued' status
            // records are the entities not controllable.
            return transactionDsl.selectFrom(ENTITY_ACTION)
                    .where((ENTITY_ACTION.STATUS.eq(EntityActionStatus.in_progress)
                            // 'queued' status is also consider as non-controllable.
                            .or(ENTITY_ACTION.STATUS.eq(EntityActionStatus.queued))
                            .or(ENTITY_ACTION.STATUS.eq(EntityActionStatus.succeed)))
                           .and(ENTITY_ACTION.ACTION_TYPE.eq(EntityActionActionType.move)))
                    .fetchSet(ENTITY_ACTION.ENTITY_ID);
        });
    }

    @Override
    public Set<Long> getNonSuspendableEntityIds() {
        return dsl.transactionResult(configuration -> {
            LocalDateTime now = LocalDateTime.now(ZoneOffset.UTC);
            final DSLContext transactionDsl = DSL.using(configuration);
            final LocalDateTime expiredInProgressThresholdTime =
                    now.minusSeconds(activateOrMoveInProgressExpiredSeconds);
            final LocalDateTime expiredSucceedThresholdTime =
                    now.minusSeconds(activateSucceedExpiredSeconds);
            final long deletedQueuedOrInProgressActionCount = transactionDsl.deleteFrom(ENTITY_ACTION)
                    .where((ENTITY_ACTION.STATUS.eq(EntityActionStatus.in_progress)
                                .or(ENTITY_ACTION.STATUS.eq(EntityActionStatus.queued)))
                            .and(ENTITY_ACTION.ACTION_TYPE.eq(EntityActionActionType.activate))
                            .and(ENTITY_ACTION.UPDATE_TIME.lessOrEqual(expiredInProgressThresholdTime)))
                    .execute();
            if (deletedQueuedOrInProgressActionCount > 0) {
                logger.warn("Deleted {} rows out of date in progress activate action records which update " +
                                "time is less than {}",
                        deletedQueuedOrInProgressActionCount, expiredInProgressThresholdTime);
            }
            // delete all failed activate entity action records.
            transactionDsl.deleteFrom(ENTITY_ACTION)
                    .where(ENTITY_ACTION.STATUS.eq(EntityActionStatus.failed)
                            .and(ENTITY_ACTION.ACTION_TYPE.eq(EntityActionActionType.activate)))
                    .execute();
            // delete all expired successful records.
            transactionDsl.deleteFrom(ENTITY_ACTION)
                    .where(ENTITY_ACTION.STATUS.eq(EntityActionStatus.succeed)
                            .and(ENTITY_ACTION.ACTION_TYPE.eq(EntityActionActionType.activate))
                            .and(ENTITY_ACTION.UPDATE_TIME.lessOrEqual(expiredSucceedThresholdTime)))
                    .execute();
            // after deleted expired records, the rest of 'succeed', 'queued' or 'in progress' status records
            // are the entities not suspendable. It is important to notice that for entity with activate action
            // in 'queued' or 'in progress' state, it remain as inactive so it will not be considered for
            // suspension. For those entities, either suspendable true or false does not make a difference.
            // For simplicity, we make those entities suspendable false.
            return transactionDsl.selectFrom(ENTITY_ACTION)
                    .where((ENTITY_ACTION.STATUS.eq(EntityActionStatus.in_progress)
                            .or(ENTITY_ACTION.STATUS.eq(EntityActionStatus.succeed))
                            .or(ENTITY_ACTION.STATUS.eq(EntityActionStatus.queued)))
                           .and(ENTITY_ACTION.ACTION_TYPE.eq(EntityActionActionType.activate)))
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