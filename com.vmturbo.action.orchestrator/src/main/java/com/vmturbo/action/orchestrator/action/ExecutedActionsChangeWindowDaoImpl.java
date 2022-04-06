package com.vmturbo.action.orchestrator.action;

import static com.vmturbo.action.orchestrator.db.tables.ExecutedActionsChangeWindow.EXECUTED_ACTIONS_CHANGE_WINDOW;

import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;

import com.vmturbo.action.orchestrator.db.tables.records.ExecutedActionsChangeWindowRecord;
import com.vmturbo.common.protobuf.action.ActionDTO.ExecutedActionsChangeWindow;
import com.vmturbo.components.common.utils.TimeUtil;

/**
 * DAO backed by RDBMS to hold executed actions' change window.
 */
public class ExecutedActionsChangeWindowDaoImpl implements ExecutedActionsChangeWindowDao {

    private final Logger logger = LogManager.getLogger();

    /**
     * Database access context.
     */
    private final DSLContext dsl;

    private final Clock clock;

    /**
     * Constructs executed actions change window DAO.
     * @param dsl database access context.
     */
    public ExecutedActionsChangeWindowDaoImpl(@Nonnull final DSLContext dsl, Clock clock) {
        this.dsl = Objects.requireNonNull(dsl);
        this.clock = clock;
    }

    /**
     * Persist an executed action change window record, based on Action {@link Action} and execution details.
     *
     * <p>It's intended to persist action change window details of Succeeded actions. It should be added to, and not updated.
     * @param actionId the action id.
     * @param entityId the entityId.
     * @param completionTime the change window's satrt time is the time that the action completed successfully.
     */
    @Override
    public void persistExecutedActionsChangeWindow(final long actionId, final long entityId,
                                                   final long completionTime) {
        final com.vmturbo.action.orchestrator.db.tables.pojos.ExecutedActionsChangeWindow
                executedActionsChangeWindow = new com.vmturbo.action.orchestrator.db.tables.pojos.ExecutedActionsChangeWindow(actionId,
                entityId,
                TimeUtil.millisToLocalDateTime(completionTime, clock),
                null, null);
        dsl.newRecord(EXECUTED_ACTIONS_CHANGE_WINDOW, executedActionsChangeWindow).store();
    }

    @Nonnull
    @Override
    public Map<Long, List<ExecutedActionsChangeWindow>> getExecutedActionsChangeWindowMap(List<Long> entityOids) {
        // Turn result into a map that maps entity OIDs to lists of ExecutedActionsChangeWindow protobuf objects.
        List<ExecutedActionsChangeWindowRecord> records =
                dsl.selectFrom(EXECUTED_ACTIONS_CHANGE_WINDOW)
                        .where(EXECUTED_ACTIONS_CHANGE_WINDOW.ENTITY_OID.in(entityOids))
                        .orderBy(EXECUTED_ACTIONS_CHANGE_WINDOW.START_TIME)
                        .fetch();
        final Map<Long, List<com.vmturbo.common.protobuf.action.ActionDTO.ExecutedActionsChangeWindow>> result = new HashMap<>();
        records.forEach(record ->
                result.computeIfAbsent(record.getEntityOid(), r -> new ArrayList<>()).add(toProtobuf(record)));
        return result;
    }

    private ExecutedActionsChangeWindow toProtobuf(ExecutedActionsChangeWindowRecord record) {
        return ExecutedActionsChangeWindow.newBuilder()
                .setActionOid(record.getActionOid())
                .setEntityOid(record.getEntityOid())
                .setStartTime(TimeUtil.localTimeToMillis(record.getStartTime(), clock))
                .build();
    }
}