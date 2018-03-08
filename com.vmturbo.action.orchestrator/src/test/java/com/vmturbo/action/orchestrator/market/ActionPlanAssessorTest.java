package com.vmturbo.action.orchestrator.market;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;

import org.junit.Test;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;

public class ActionPlanAssessorTest {

    private final Clock clock = mock(Clock.class);

    private static final long LIVE_TOPOLOGY_CONTEXT_ID = 98765L;

    private static final long MAX_LIVE_ACTION_PLAN_AGE_SECONDS = 100L;

    private static final long ANALYSIS_COMPLETION_TIME_SECONDS = 5L;

    private final ActionPlanAssessor actionPlanAssessor = new ActionPlanAssessor(clock, LIVE_TOPOLOGY_CONTEXT_ID,
        MAX_LIVE_ACTION_PLAN_AGE_SECONDS);

    private final ActionPlan.Builder actionPlanBuilder = ActionPlan.newBuilder()
        .setId(1234L)
        .setTopologyId(5678L)
        .setAnalysisStartTimestamp(Instant.ofEpochSecond(4L).toEpochMilli())
        .setAnalysisCompleteTimestamp(Instant.ofEpochSecond(ANALYSIS_COMPLETION_TIME_SECONDS).toEpochMilli());

    @Test
    public void testPlanActionPlanNotExpired() {
        actionPlanBuilder.setTopologyContextId(LIVE_TOPOLOGY_CONTEXT_ID + 1);
        when(clock.instant()).thenReturn(Instant.ofEpochSecond(
            MAX_LIVE_ACTION_PLAN_AGE_SECONDS + ANALYSIS_COMPLETION_TIME_SECONDS + 1));

        assertFalse(actionPlanAssessor.isActionPlanExpired(actionPlanBuilder.build()));
    }

    @Test
    public void testYoungLiveActionPlanNotExpired() {
        actionPlanBuilder.setTopologyContextId(LIVE_TOPOLOGY_CONTEXT_ID);
        when(clock.instant()).thenReturn(Instant.ofEpochSecond(6L));

        assertFalse(actionPlanAssessor.isActionPlanExpired(actionPlanBuilder.build()));
    }

    @Test
    public void testOldLiveActionPlanIsExpired() {
        actionPlanBuilder.setTopologyContextId(LIVE_TOPOLOGY_CONTEXT_ID);
        when(clock.instant()).thenReturn(Instant.ofEpochSecond(
            MAX_LIVE_ACTION_PLAN_AGE_SECONDS + ANALYSIS_COMPLETION_TIME_SECONDS + 1));

        assertTrue(actionPlanAssessor.isActionPlanExpired(actionPlanBuilder.build()));
    }

    @Test
    public void testBoundaryActionPlanNotExpired() {
        actionPlanBuilder.setTopologyContextId(LIVE_TOPOLOGY_CONTEXT_ID);
        when(clock.instant()).thenReturn(Instant.ofEpochSecond(
            MAX_LIVE_ACTION_PLAN_AGE_SECONDS + ANALYSIS_COMPLETION_TIME_SECONDS));

        assertFalse(actionPlanAssessor.isActionPlanExpired(actionPlanBuilder.build()));
    }
}