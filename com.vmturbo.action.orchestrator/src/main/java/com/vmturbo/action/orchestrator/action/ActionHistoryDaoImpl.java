package com.vmturbo.action.orchestrator.action;

import static com.vmturbo.action.orchestrator.db.tables.ActionHistory.ACTION_HISTORY;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vmturbo.action.orchestrator.action.Action.SerializationState;
import com.vmturbo.action.orchestrator.db.tables.pojos.ActionHistory;
import com.vmturbo.auth.api.authorization.jwt.SecurityConstant;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionDecision;
import com.vmturbo.common.protobuf.action.ActionDTO.ExecutionStep;

/**
 * DAO backed by RDBMS to hold action history.
 */
public class ActionHistoryDaoImpl implements ActionHistoryDao {

    private final Logger logger = LoggerFactory.getLogger(ActionHistoryDaoImpl.class);

    /**
     * Database access context.
     */
    private final DSLContext dsl;

    /**
     * Constructs action history DAO.
     *
     * @param dsl database access context
     */
    public ActionHistoryDaoImpl(@Nonnull final DSLContext dsl) {
        this.dsl = Objects.requireNonNull(dsl);
    }

    @Nonnull
    @Override
    public ActionHistory persistActionHistory(
            final long actionId,
            @Nonnull final ActionDTO.Action recommendation,
            final long realtimeTopologyContextId,
            @Nonnull final LocalDateTime recommedationTime,
            @Nonnull final ActionDecision decision,
            @Nonnull final ExecutionStep executionStep,
            @Nonnull final int currentState) {
        final LocalDateTime curTime = LocalDateTime.now();
        String userName = SecurityConstant.USER_ID_CTX_KEY.get();
        if (userName == null) {
            userName = "SYSTEM";
        }
        ActionHistory actionHistory = new
                ActionHistory(actionId,
                curTime, curTime,
                realtimeTopologyContextId,
                recommendation,
                recommedationTime,
                decision,
                executionStep,
                currentState,
                userName);
        dsl.newRecord(ACTION_HISTORY, actionHistory).store();
        return actionHistory;
    }

    @Nonnull
    @Override
    public List<Action> getAllActionHistory() {
        List<ActionHistory> actionHistoryList = dsl
                .selectFrom(ACTION_HISTORY)
                .fetch()
                .into(ActionHistory.class);

        return actionHistoryList.stream()
                .map(actionHistory -> new Action(
                        new SerializationState(
                                actionHistory.getId(),
                                actionHistory.getRecommendation(),
                                actionHistory.getRecommendationTime(),
                                actionHistory.getActionDecision(),
                                actionHistory.getExecutionStep(),
                                ActionDTO.ActionState.forNumber(actionHistory.getCurrentState()),
                                new ActionTranslation(actionHistory.getRecommendation()))))
                .collect(Collectors.toList());
    }
}
