package com.vmturbo.action.orchestrator.action;

import static com.vmturbo.action.orchestrator.db.tables.ActionHistory.ACTION_HISTORY;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vmturbo.action.orchestrator.action.Action.SerializationState;
import com.vmturbo.action.orchestrator.db.tables.pojos.ActionHistory;
import com.vmturbo.action.orchestrator.db.tables.records.ActionHistoryRecord;
import com.vmturbo.auth.api.authorization.jwt.SecurityConstant;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionDecision;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.ExecutionStep;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
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
    private final Clock clock;
    private final int recordFetchBatchSize;

    /**
     * Constructs action history DAO.
     * @param dsl database access context
     * @param actionModeCalculator calculates action mode
     * @param clock clock to track the time
     * @param recordFetchBatchSize batch size for record fetch
     */
    public ActionHistoryDaoImpl(@Nonnull final DSLContext dsl,
            @Nonnull ActionModeCalculator actionModeCalculator, @Nonnull Clock clock,
            int recordFetchBatchSize) {
        this.dsl = Objects.requireNonNull(dsl);
        this.actionModeCalculator = Objects.requireNonNull(actionModeCalculator);
        this.clock = Objects.requireNonNull(clock);
        this.recordFetchBatchSize = recordFetchBatchSize;
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
        final LocalDateTime curTime = LocalDateTime.now(clock);
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
     * Returns all the existing action history filtered by action query filter parameters, like 'startDate' and
     * 'endDate', and scope filters.
     *
     * @param actionQueryFilter the query filter with filter parameters to fetch by.
     * @return List of {@link Action} matching the filters in the action query filter.
     */
    @Nonnull
    @Override
    public List<ActionView> getActionHistoryByFilter(@Nonnull final ActionQueryFilter actionQueryFilter) {
        final LocalDateTime startDate = ActionDTOUtil.getLocalDateTime(actionQueryFilter.getStartDate());
        final LocalDateTime endDate = ActionDTOUtil.getLocalDateTime(actionQueryFilter.getEndDate());
        final List<Condition> conditions = new ArrayList<>();
        conditions.add(ACTION_HISTORY.CREATE_TIME.between(startDate, endDate));
        List<Long> accountOids;
        if (actionQueryFilter.hasAccountFilter() && !(accountOids = actionQueryFilter.getAccountFilter()
                .getAccountIdList()).isEmpty()) {
            conditions.add(ACTION_HISTORY.ASSOCIATED_ACCOUNT_ID.in(accountOids));
        }
        final List<Long> rgOids;
        if (actionQueryFilter.hasResourceGroupFilter() && !(rgOids = actionQueryFilter.getResourceGroupFilter()
                .getResourceGroupOidList()).isEmpty()) {
            conditions.add(ACTION_HISTORY.ASSOCIATED_RESOURCE_GROUP_ID.in(rgOids));
        }

        final List<ActionView> actionList = new ArrayList<>();
        dsl.connection(conn -> {
            conn.setAutoCommit(false);
            try (Stream<ActionHistoryRecord> stream = DSL.using(conn, dsl.settings())
                    .selectFrom(ACTION_HISTORY)
                    .where(conditions)
                    .fetchSize(recordFetchBatchSize)
                    .stream()) {
                actionList.addAll(stream
                        .map(actionHistory -> mapDbActionHistoryToAction(actionHistory))
                        .collect(Collectors.toList()));
            }
        });
        return actionList;
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
