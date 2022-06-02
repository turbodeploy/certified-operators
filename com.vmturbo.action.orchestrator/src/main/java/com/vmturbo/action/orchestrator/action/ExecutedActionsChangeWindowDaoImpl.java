package com.vmturbo.action.orchestrator.action;

import static com.vmturbo.action.orchestrator.db.tables.ExecutedActionsChangeWindow.EXECUTED_ACTIONS_CHANGE_WINDOW;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import com.vmturbo.action.orchestrator.db.tables.records.ExecutedActionsChangeWindowRecord;
import com.vmturbo.action.orchestrator.exception.ActionStoreOperationException;
import com.vmturbo.common.protobuf.action.ActionDTO.ExecutedActionsChangeWindow;
import com.vmturbo.common.protobuf.action.ActionDTO.ExecutedActionsChangeWindow.LivenessState;
import com.vmturbo.common.protobuf.action.ActionDTO.UpdateActionChangeWindowRequest.ActionLivenessInfo;
import com.vmturbo.components.common.utils.TimeUtil;

/**
 * DAO backed by RDBMS to hold executed actions' change window.
 */
public class ExecutedActionsChangeWindowDaoImpl implements ExecutedActionsChangeWindowDao {
    /**
     * Database access context.
     */
    private final DSLContext dsl;

    private final Clock clock;

    private final int chunkSize;

    /**
     * Constructs executed actions change window DAO.
     * @param dsl database access context.
     * @param clock System clock.
     * @param chunkSize Chunk sizes when applicable for DB read/write.
     */
    public ExecutedActionsChangeWindowDaoImpl(@Nonnull final DSLContext dsl, Clock clock,
            int chunkSize) {
        this.dsl = Objects.requireNonNull(dsl);
        this.clock = clock;
        this.chunkSize = chunkSize;
    }

    @Override
    public void saveExecutedAction(final long actionId, final long entityId)
            throws ActionStoreOperationException {
        try {
            dsl.insertInto(EXECUTED_ACTIONS_CHANGE_WINDOW)
                    .set(EXECUTED_ACTIONS_CHANGE_WINDOW.ENTITY_OID, entityId)
                    .set(EXECUTED_ACTIONS_CHANGE_WINDOW.ACTION_OID, actionId)
                    .set(EXECUTED_ACTIONS_CHANGE_WINDOW.LIVENESS_STATE, LivenessState.NEW_VALUE)
                    .onDuplicateKeyIgnore()
                    .execute();
        } catch (DataAccessException dae) {
            throw new ActionStoreOperationException("Could not save executed action "
                    + actionId + " for entity " + entityId, dae);
        }
    }

    @Override
    public void updateActionLivenessInfo(@Nonnull Set<ActionLivenessInfo> actionLivenessInfo)
            throws ActionStoreOperationException {
        try {
            dsl.batchUpdate(actionLivenessInfo
                            .stream()
                            .map(alInfo -> {
                                final ExecutedActionsChangeWindowRecord record =
                                        new ExecutedActionsChangeWindowRecord();
                                record.setLivenessState(alInfo.getLivenessState().getNumber());

                                final LocalDateTime updateTime = TimeUtil.millisToLocalDateTime(
                                        alInfo.getTimestamp(), clock);
                                // If it is a live action, then we update the start time.
                                if (alInfo.getLivenessState() == LivenessState.LIVE) {
                                    record.setStartTime(updateTime);
                                } else {
                                    record.setEndTime(updateTime);
                                }
                                record.setActionOid(alInfo.getActionOid());
                                record.changed(EXECUTED_ACTIONS_CHANGE_WINDOW.ACTION_OID, false);
                                return record;
                            })
                            .collect(Collectors.toList()))
                    .execute();
        } catch (DataAccessException dae) {
            throw new ActionStoreOperationException("Could not make " + actionLivenessInfo.size()
                    + " action liveness info updates.", dae);
        }
    }

    @Override
    public Optional<Long> getActionsByLivenessState(@Nonnull final Set<LivenessState> livenessStates,
            @Nonnull final Set<Long> actionIds, long currentCursor,
            @Nonnull Consumer<ExecutedActionsChangeWindow> consumer)
            throws ActionStoreOperationException {
        AtomicLong nextCursor = new AtomicLong();
        try {
            final Set<Integer> livenessCodes = livenessStates.stream()
                    .map(LivenessState::getNumber)
                    .collect(Collectors.toSet());
            final Condition stateCondition = EXECUTED_ACTIONS_CHANGE_WINDOW.LIVENESS_STATE
                    .in(livenessCodes);
            final Condition idCondition = actionIds.isEmpty() ? stateCondition
                    : stateCondition.and(EXECUTED_ACTIONS_CHANGE_WINDOW.ACTION_OID.in(actionIds));
            final Condition seekCondition = idCondition.and(
                    EXECUTED_ACTIONS_CHANGE_WINDOW.ACTION_OID.greaterThan(currentCursor));
            final List<ExecutedActionsChangeWindow> results = new ArrayList<>();
            dsl.connection(conn -> {
                conn.setAutoCommit(false);
                try (Stream<ExecutedActionsChangeWindow> savingsEventStream =
                             DSL.using(conn, dsl.settings())
                                     .selectFrom(EXECUTED_ACTIONS_CHANGE_WINDOW)
                                     .where(seekCondition)
                                     .orderBy(EXECUTED_ACTIONS_CHANGE_WINDOW.ACTION_OID)
                                     .limit(chunkSize)
                                     .fetchSize(chunkSize)
                                     .fetchLazy()
                                     .stream()
                                     .map(this::toProtobuf)) {
                    savingsEventStream.forEach(results::add);
                    if (results.size() == chunkSize) {
                        // Set the next cursor for client only if we got all the results we
                        // requested. Otherwise, (if results are less than the chunk requested),
                        // this is the last page of results, so don't set the next cursor.
                        // We are getting the last action oid here, one with the largest value.
                        nextCursor.set(results.get(results.size() - 1).getActionOid());
                    }
                    results.forEach(consumer);
                }
            });
        } catch (DataAccessException dae) {
            throw new ActionStoreOperationException("Could not fetch actions in " + livenessStates
                    + " states.", dae);
        }
        return nextCursor.get() == 0 ? Optional.empty() : Optional.of(nextCursor.get());
    }

    @Nonnull
    @Override
    public Map<Long, List<ExecutedActionsChangeWindow>> getActionsByEntityOid(List<Long> entityOids) {
        // Turn result into a map that maps entity OIDs to lists of ExecutedActionsChangeWindow protobuf objects.
        List<ExecutedActionsChangeWindowRecord> records =
                dsl.selectFrom(EXECUTED_ACTIONS_CHANGE_WINDOW)
                        .where(EXECUTED_ACTIONS_CHANGE_WINDOW.ENTITY_OID.in(entityOids))
                        .orderBy(EXECUTED_ACTIONS_CHANGE_WINDOW.START_TIME)
                        .fetch();
        final Map<Long, List<com.vmturbo.common.protobuf.action.ActionDTO.ExecutedActionsChangeWindow>> result = new HashMap<>();
        records.forEach(record ->
                result.computeIfAbsent(record.getEntityOid(), r -> new ArrayList<>()).add(
                        toProtobuf(record)));
        return result;
    }

    /**
     * Converts from DB record to protobuf. Sets the ActionSpec optionally if entry is available.
     *
     * @param record DB record for change window.
     * @return Change window protobuf object.
     */
    private ExecutedActionsChangeWindow toProtobuf(ExecutedActionsChangeWindowRecord record) {
        final ExecutedActionsChangeWindow.Builder builder = ExecutedActionsChangeWindow.newBuilder();
        builder.setActionOid(record.getActionOid())
                .setEntityOid(record.getEntityOid())
                .setLivenessState(LivenessState.forNumber(record.getLivenessState()));
        if (record.getStartTime() != null) {
            builder.setStartTime(TimeUtil.localTimeToMillis(record.getStartTime(), clock));
        }
        if (record.getEndTime() != null) {
            builder.setEndTime(TimeUtil.localTimeToMillis(record.getEndTime(), clock));
        }
        return builder.build();
    }
}