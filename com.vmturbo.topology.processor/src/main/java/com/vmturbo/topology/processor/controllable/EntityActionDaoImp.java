package com.vmturbo.topology.processor.controllable;

import static com.vmturbo.topology.processor.db.Tables.ENTITY_ACTION;
import static org.jooq.impl.DSL.inline;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO;
import com.vmturbo.topology.processor.db.enums.EntityActionActionType;
import com.vmturbo.topology.processor.db.enums.EntityActionStatus;
import com.vmturbo.topology.processor.db.tables.records.EntityActionRecord;

/**
Note: For any database transactions (those that aquire locks), the order
      in which locks are acquired is important and should follow this order:

      0: ACTION_ID
      1: ENTITY_ID
      2: STATUS
      3: UPDATE_TIME
      4. ACTION_TYPE

    This is to prevent deadlocks: https://stackoverflow.com/a/2423921.
 */
public class EntityActionDaoImp implements EntityActionDao {
    private final Logger logger = LogManager.getLogger();

    private final DSLContext dsl;

    // For "succeed" move action records, if their last update time is older than this time threshold,
    // they will be deleted from entity action table. This makes succeed entities will not participate
    // Market analysis immediately, it will have some default cool down time.
    final int moveSucceedRecordExpiredSeconds;

    // For "in progress" or 'queued' action records. if their last update time is older than this
    // time threshold, they will be deleted from entity action tables. This try to handle the case that
    // if Probe is down and can not send back action progress notification, they will be considered
    // as time out actions and be cleaned up.
    final int inProgressActionExpiredSeconds;

    // For "succeed" activate action records, if their last update time is older than this time threshold,
    // they will be deleted from entity action table. This immediately allow succeed entities suspendable.
    final int activateSucceedExpiredSeconds;

    // For "succeed" scale (on cloud) action records, if their last update time is older than this time threshold,
    // they will be deleted from entity action table.
    final int scaleSucceedRecordExpiredSeconds;

    // For "succeed" resize (on prem) action records, if their last update time is older than this time threshold,
    // they will be deleted from entity action table.
    final int resizeSucceedRecordExpiredSeconds;

    public EntityActionDaoImp(@Nonnull final DSLContext dsl,
                              final int moveSucceedRecordExpiredSeconds,
                              final int activateOrMoveInProgressExpiredSeconds,
                              final int activateSucceedExpiredSeconds,
                              final int scaleSucceedRecordExpiredSeconds,
                              final int resizeSucceedRecordExpiredSeconds) {

        this.dsl = Objects.requireNonNull(dsl);
        this.moveSucceedRecordExpiredSeconds = moveSucceedRecordExpiredSeconds;
        this.inProgressActionExpiredSeconds = activateOrMoveInProgressExpiredSeconds;
        this.activateSucceedExpiredSeconds = activateSucceedExpiredSeconds;
        this.scaleSucceedRecordExpiredSeconds = scaleSucceedRecordExpiredSeconds;
        this.resizeSucceedRecordExpiredSeconds = resizeSucceedRecordExpiredSeconds;
    }

    @Override
    public void insertAction(final long actionId,
                             @Nonnull final ActionItemDTO.ActionType actionType,
                             @Nonnull final Set<Long> entityIds)
            throws IllegalArgumentException {
        logger.info("Start transaction to insert action into entity actions table. actionId: {}," +
                        " actionType: {}, numEntities: {}", actionId, actionType, entityIds.size());
        dsl.transaction(configuration -> {
            final LocalDateTime now = LocalDateTime.now(ZoneOffset.UTC);
            final DSLContext transactionDsl = DSL.using(configuration);
            for (final long entityId : entityIds) {
                // Initial state is "queued"
                final EntityActionStatus initialEntityStatus = EntityActionStatus.queued;
                // If there is already a row for this action/entity combination, update instead of
                // insert. This is possible when using PREP/POST workflows, for example.
                Optional<EntityActionRecord> existingRecord =
                    getRecord(transactionDsl, actionId, entityId);
                if (existingRecord.isPresent()) {
                    logger.info("Action already existing in entity actions table. actionId: {}, entityId: {}",
                                    actionId, entityId);
                    EntityActionRecord actionRecord = existingRecord.get();
                    actionRecord.setStatus(initialEntityStatus);
                    actionRecord.setUpdateTime(now);
                    transactionDsl.executeUpdate(actionRecord);
                } else {
                    // Insert a new row for this aciton/entity combination.
                    logger.info("Insert new action in entity actions table. actionId: {}, entityId: {}",
                                    actionId, entityId);
                    // use DSL.inline so it works with both mysql and postgres enum
                    transactionDsl.insertInto(ENTITY_ACTION)
                        .set(ENTITY_ACTION.ACTION_ID, actionId)
                        .set(ENTITY_ACTION.ENTITY_ID, entityId)
                        .set(ENTITY_ACTION.STATUS, inline(initialEntityStatus))
                        .set(ENTITY_ACTION.UPDATE_TIME, now)
                        .set(ENTITY_ACTION.ACTION_TYPE, inline(getActionType(actionType)))
                        .execute();
                }
            }
        });
        logger.info("Queued action {} into entity actions table", actionId);
    }

    /**
     * Get EntityActionActionType from actionItemDTO's action type
     *
     * @param actionType action type from actionItemDTO
     * @return EntityActionActionType
     */
    private EntityActionActionType getActionType(final ActionItemDTO.ActionType actionType) {
        switch (actionType) {
            case START:
            case PROVISION:
                return EntityActionActionType.activate;
            case MOVE:
            case MOVE_TOGETHER:
            case CROSS_TARGET_MOVE:
            case CHANGE:
                return EntityActionActionType.move;
            // Resize actions on prem are represented as 'RIGHT_SIZE'
            case RIGHT_SIZE:
            case RESIZE:
                return EntityActionActionType.resize;
            // Resize actions on cloud are represented as 'SCALE'
            case SCALE:
                return EntityActionActionType.scale;
            default:
                logger.error("Failure in extracting action with type : " + actionType);
                throw new IllegalArgumentException("Inserting an action with type " + actionType);
        }
    }

    @Override
    public void updateActionState(final long actionId, @Nonnull final ActionState newState)
            throws ActionRecordNotFoundException {
        try {
            dsl.transaction(configuration -> {
                final DSLContext transactionDsl = DSL.using(configuration);
                final LocalDateTime now = LocalDateTime.now(ZoneOffset.UTC);
                final Condition inProgressRecordsCondition = ENTITY_ACTION.ACTION_ID.eq(actionId)
                        .and(ENTITY_ACTION.STATUS.eq(inline(EntityActionStatus.in_progress))
                                .or(ENTITY_ACTION.STATUS.eq(inline(EntityActionStatus.queued))));
                if (transactionDsl.fetchCount(ENTITY_ACTION, inProgressRecordsCondition) == 0) {
                    throw new ActionRecordNotFoundException(actionId);
                }

                // for actionId rows, update their status to new state and time to current.
                transactionDsl.update(ENTITY_ACTION)
                        .set(ENTITY_ACTION.STATUS, inline(getActionState(newState)))
                        .set(ENTITY_ACTION.UPDATE_TIME, now)
                        .where(inProgressRecordsCondition)
                        .execute();
                logger.info("Update action {} status to {} in entity actions table", actionId, newState.name());
            });
        } catch (DataAccessException e) {
            if (e.getCause() instanceof ActionRecordNotFoundException) {
                throw (ActionRecordNotFoundException) e.getCause();
            } else {
                throw e;
            }
        }
    }

    @Override
    public void deleteMoveActions(final @NonNull List<@NonNull Long> entityOids) {
        dsl.deleteFrom(ENTITY_ACTION)
            .where(ENTITY_ACTION.ENTITY_ID.in(entityOids)
                .and(ENTITY_ACTION.ACTION_TYPE.eq(inline(EntityActionActionType.move))))
            .execute();
    }

    /**
     * Get entities non-controllable on-prem.
     * These are entityIds of : source, destination and target for which move action was executed recently.
     * @return set of entities that satisfy the criteria
     */
    @Override
    public Set<Long> getNonControllableEntityIds() {
        return getRelevantEntityIds(EntityActionActionType.move,
                moveSucceedRecordExpiredSeconds,
                inProgressActionExpiredSeconds);

    }

    /**
     * Get entities non-suspendable on-prem.
     * These are entityIds of : target for which activate action was executed recently.
     * @return set of entities that satisfy the criteria
     */
    @Override
    public Set<Long> getNonSuspendableEntityIds() {
        return getRelevantEntityIds(EntityActionActionType.activate,
                activateSucceedExpiredSeconds,
                inProgressActionExpiredSeconds);
    }

    /**
     * Get the record representing the intersection of the provided action and entity IDs.
     *
     * @param transactionDsl the transaction context in which to execute this query
     * @param actionId the id of the action whose record to retrieve
     * @param entityId the id of the entity whose record to retrieve
     * @return an EntityActionRecord if the row exists, or Optional.empty if not
     */
    private Optional<EntityActionRecord> getRecord(final @Nonnull DSLContext transactionDsl,
                                                   final long actionId,
                                                   final long entityId) {
        List<EntityActionRecord> records = transactionDsl.selectFrom(ENTITY_ACTION)
            .where(ENTITY_ACTION.ACTION_ID.eq(actionId)
                .and(ENTITY_ACTION.ENTITY_ID.eq(entityId)))
            .fetch();
        if (records.size() > 1) {
            // This shouldn't happen, but if it does the functionality should still work
            logger.warn("Duplicate entity_action records detected for action {} and entity {}!",
                actionId, entityId);
        }
        return records.stream().findFirst();

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

    /**
     * Get entities ineligible for resize on cloud.
     * These are entityIds of : target for which 'scale' action was executed recently.
     * @return set of entities that satisfy the criteria
     */
    @Override
    public Set<Long> ineligibleForScaleEntityIds() {
        // Resize actions on cloud are represented as 'SCALE'.
        return getRelevantEntityIds(EntityActionActionType.scale, scaleSucceedRecordExpiredSeconds,
                inProgressActionExpiredSeconds);
    }

    /**
     * Get entities ineligible for resize on-prem.
     * These are entityIds of : target for which resize action was executed recently.
     * @return set of entities that satisfy the criteria
     */
    @Override
    public Set<Long> ineligibleForResizeDownEntityIds() {
        // Resize actions on-prem are represented as 'resize'.
        return getRelevantEntityIds(EntityActionActionType.resize,
                resizeSucceedRecordExpiredSeconds,
                inProgressActionExpiredSeconds);
    }

    @Override
    public Set<Long> ineligibleForReconfigureEntityIds() {
        // Unimplemented for the old logic using the entity_action table since the current
        // implementation of getRelevantEntityIds deletes entries from the table to grab the
        // results. This becomes problematic when we need multiple thresholds on the same type of
        // action. i.e. the min(list(thresholds)) controls how long the entry stays in the table.
        return new HashSet<>();
    }

    /**
     * It will delete all expired @actionType records from entity action table. For 'failed' status records,
     * it will delete all of them. For 'queued' and 'in progress' status records, it has a
     * configured {@link #inProgressActionExpiredSeconds} which stores the timeout threshold
     * about when they should be considered as expired. For 'succeed' status records, it has a
     * configured {@link #activateSucceedExpiredSeconds} which has a different timeout
     * threshold to determine when to delete. After delete expired action records, it will get
     * from action records all entity ids which status is 'in progress' and 'succeed', those entity
     * ids are the relevant entities.
     * @return set of entity Oids.
     */
    private Set<Long> getRelevantEntityIds(EntityActionActionType actionType,
            int actionSucceedThresholdTime,
            int inProgressActionExpiredSeconds) {
        return dsl.transactionResult(configuration -> {
            LocalDateTime now = LocalDateTime.now(ZoneOffset.UTC);
            final DSLContext transactionDsl = DSL.using(configuration);
            final LocalDateTime expiredInProgressThresholdTime =
                    now.minusSeconds(inProgressActionExpiredSeconds);
            final LocalDateTime expiredSucceedThresholdTime =
                    now.minusSeconds(actionSucceedThresholdTime);

            final Condition queuedOrInProgressCondition =
                (ENTITY_ACTION.STATUS.eq(inline(EntityActionStatus.in_progress))
                    .or(ENTITY_ACTION.STATUS.eq(inline(EntityActionStatus.queued))))
                    .and(ENTITY_ACTION.UPDATE_TIME.lessOrEqual(expiredInProgressThresholdTime));
            final Condition failedCondition = ENTITY_ACTION.STATUS.eq(inline(EntityActionStatus.failed));
            final Condition succeedCondition = ENTITY_ACTION.STATUS.eq(inline(EntityActionStatus.succeed))
                .and(ENTITY_ACTION.UPDATE_TIME.lessOrEqual(expiredSucceedThresholdTime));

            transactionDsl.deleteFrom(ENTITY_ACTION)
                .where(failedCondition.or(queuedOrInProgressCondition).or(succeedCondition))
                    .and(ENTITY_ACTION.ACTION_TYPE.eq(inline(actionType)))
                .execute();

            // after deleted expired records, the rest of 'succeed' or 'in progress' or 'queued' status
            // records are the entities which are relevant..
            return transactionDsl.selectFrom(ENTITY_ACTION)
                    .where((ENTITY_ACTION.STATUS.eq(inline(EntityActionStatus.in_progress))
                            .or(ENTITY_ACTION.STATUS.eq(inline(EntityActionStatus.queued)))
                            .or(ENTITY_ACTION.STATUS.eq(inline(EntityActionStatus.succeed))))
                            .and(ENTITY_ACTION.ACTION_TYPE.eq(inline(actionType))))
                    .fetchSet(ENTITY_ACTION.ENTITY_ID);
        });
    }

    /**
     * Exception thrown when action record was not found in database.
     */
    public static class ActionRecordNotFoundException extends Exception {
        public ActionRecordNotFoundException(final long actionId) {
            super("Action record with actionId: " + actionId + " not found in existing database.");
        }
    }

    /**
     * Exception thrown action record's state is not supported.
     */
    public static class NotSupportedActionStateException extends Exception {
        public NotSupportedActionStateException(@Nonnull final ActionState actionState) {
            super("Not supported action state: " + actionState);
        }
    }
}
