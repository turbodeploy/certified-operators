package com.vmturbo.action.orchestrator.market;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;

import javax.annotation.Nonnull;

import org.junit.Test;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo.MarketActionPlanInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;

public class ActionPlanAssessorTest {

    private final Clock clock = mock(Clock.class);

    private static final long LIVE_TOPOLOGY_CONTEXT_ID = 98765L;

    private static final long MAX_LIVE_ACTION_PLAN_AGE_SECONDS = 100L;

    private static final long ANALYSIS_COMPLETION_TIME_SECONDS = 5L;

    private final ActionPlanAssessor actionPlanAssessor = new ActionPlanAssessor(clock,
        MAX_LIVE_ACTION_PLAN_AGE_SECONDS);


    @Nonnull
    private ActionPlan actionPlan(final TopologyType topologyType) {
        return ActionPlan.newBuilder()
            .setId(1234L)
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyId(5678L)
                        .setTopologyContextId(123)
                        .setTopologyType(topologyType))))
            .setAnalysisStartTimestamp(Instant.ofEpochSecond(4L).toEpochMilli())
            .setAnalysisCompleteTimestamp(Instant.ofEpochSecond(ANALYSIS_COMPLETION_TIME_SECONDS).toEpochMilli())
            .build();
    }

    @Test
    public void testPlanActionPlanNotExpired() {
        final ActionPlan actionPlan = actionPlan(TopologyType.PLAN);
        when(clock.instant()).thenReturn(Instant.ofEpochSecond(
            MAX_LIVE_ACTION_PLAN_AGE_SECONDS + ANALYSIS_COMPLETION_TIME_SECONDS + 1));

        assertFalse(actionPlanAssessor.isActionPlanExpired(actionPlan));
    }

    @Test
    public void testYoungLiveActionPlanNotExpired() {
        final ActionPlan actionPlan = actionPlan(TopologyType.REALTIME);
        when(clock.instant()).thenReturn(Instant.ofEpochSecond(6L));

        assertFalse(actionPlanAssessor.isActionPlanExpired(actionPlan));
    }

    @Test
    public void testOldLiveActionPlanIsExpired() {
        final ActionPlan actionPlan = actionPlan(TopologyType.REALTIME);
        when(clock.instant()).thenReturn(Instant.ofEpochSecond(
            MAX_LIVE_ACTION_PLAN_AGE_SECONDS + ANALYSIS_COMPLETION_TIME_SECONDS + 1));

        assertTrue(actionPlanAssessor.isActionPlanExpired(actionPlan));
    }

    @Test
    public void testBoundaryActionPlanNotExpired() {
        final ActionPlan actionPlan = actionPlan(TopologyType.REALTIME);
        when(clock.instant()).thenReturn(Instant.ofEpochSecond(
            MAX_LIVE_ACTION_PLAN_AGE_SECONDS + ANALYSIS_COMPLETION_TIME_SECONDS));

        assertFalse(actionPlanAssessor.isActionPlanExpired(actionPlan));
    }
}