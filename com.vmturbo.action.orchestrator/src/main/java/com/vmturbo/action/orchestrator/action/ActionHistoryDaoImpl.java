package com.vmturbo.action.orchestrator.action;

import static com.vmturbo.action.orchestrator.db.tables.ActionHistory.ACTION_HISTORY;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.jooq.DSLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vmturbo.action.orchestrator.action.Action.SerializationState;
import com.vmturbo.action.orchestrator.db.tables.pojos.ActionHistory;
import com.vmturbo.action.orchestrator.db.tables.records.ActionHistoryRecord;
import com.vmturbo.auth.api.authorization.jwt.SecurityConstant;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionDecision;
import com.vmturbo.common.protobuf.action.ActionDTO.ExecutionStep;
import com.vmturbo.proactivesupport.DataMetricGauge;

/**
 * DAO backed by RDBMS to hold action history.
 */
public class ActionHistoryDaoImpl implements ActionHistoryDao {

    private final Logger logger = LoggerFactory.getLogger(ActionHistoryDaoImpl.class);

    private static final DataMetricGauge ACTION_HISTORY_SIZE_GAUGE = DataMetricGauge.builder()
            .withName("ao_action_history_size")
            .withHelp("Number of actions in action history.")
            .build()
            .register();

    private static final DataMetricGauge ACTION_HISTORY_FETCH_TIME_SECS = DataMetricGauge.builder()
            .withName("ao_action_history_fetch_time_secs")
            .withHelp("Amount of time (in seconds) it takes to fetch the entire action_history table")
            .build()
            .register();


    /**
     * Database access context.
     */
    private final DSLContext dsl;

    private final ActionModeCalculator actionModeCalculator;

    /**
     * Constructs action history DAO.
     *
     * @param dsl database access context
     */
    public ActionHistoryDaoImpl(@Nonnull final DSLContext dsl, @Nonnull ActionModeCalculator actionModeCalculator) {
        this.dsl = Objects.requireNonNull(dsl);
        this.actionModeCalculator = Objects.requireNonNull(actionModeCalculator);
    }

    /**
     * Persist a action history, based on Action {@link Action}. It's intended to persist executed
     * action which has either SUCCEEDED or FAILED state. And it should be added and not updated.
     *
     * @return action history, if created
     */
    @Nonnull
    @Override
    public ActionHistory persistActionHistory(
            final long actionId,
            @Nonnull final ActionDTO.Action recommendation,
            final long realtimeTopologyContextId,
            @Nonnull final LocalDateTime recommendationTime,
            @Nonnull final ActionDecision decision,
            @Nonnull final ExecutionStep executionStep,
            @Nonnull final int currentState,
            @Nullable final byte[] actionDetailData,
            @Nullable final Long associatedAccountId,
            @Nullable final Long associatedResourceGroupId,
            long recommendationOid) {
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
                recommendationTime,
                decision,
                executionStep,
                currentState,
                userName,
                actionDetailData,
                associatedAccountId, associatedResourceGroupId,
                recommendationOid);
        dsl.newRecord(ACTION_HISTORY, actionHistory).store();
        return actionHistory;
    }

    /**
     * Returns all the existing action history between 'startDate' and 'endDate'.
     *
     * @param startDate the start date
     * @param endDate   the end date
     * @return List of {@link Action} within the startDate and endDate.
     */
    @Nonnull
    @Override
    public List<ActionView> getActionHistoryByDate(@Nonnull final LocalDateTime startDate,
                                               @Nonnull final LocalDateTime endDate) {
        try (Stream<ActionHistoryRecord> stream =  dsl
                .selectFrom(ACTION_HISTORY)
                .where(ACTION_HISTORY.CREATE_TIME.between(startDate, endDate))
                .fetchSize(Integer.MIN_VALUE) // use streaming fetch
                .stream()) {
            return stream.map(actionHistory -> mapDbActionHistoryToAction(actionHistory))
                    .collect(Collectors.toList());
        }
    }

    private ActionView mapDbActionHistoryToAction(final ActionHistoryRecord actionHistory) {
        return new Action(new SerializationState(
            actionHistory.getId(),
            actionHistory.getRecommendation(),
            actionHistory.getRecommendationTime(),
            actionHistory.getActionDecision(),
            actionHistory.getExecutionStep(),
            ActionDTO.ActionState.forNumber(actionHistory.getCurrentState()),
            new ActionTranslation(actionHistory.getRecommendation()),
            actionHistory.getAssociatedAccountId(),
            actionHistory.getAssociatedResourceGroupId(),
            actionHistory.getActionDetailData(),
            actionHistory.getRecommendationOid()), actionModeCalculator);
    }
}
