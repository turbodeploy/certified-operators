package com.vmturbo.action.orchestrator.market;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.LocalDateTime;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.action.orchestrator.api.ActionOrchestratorNotificationSender;
import com.vmturbo.action.orchestrator.store.ActionStore;
import com.vmturbo.action.orchestrator.store.pipeline.ActionPipeline;
import com.vmturbo.action.orchestrator.store.pipeline.ActionProcessingInfo;
import com.vmturbo.action.orchestrator.store.pipeline.LiveActionPipelineFactory;
import com.vmturbo.action.orchestrator.store.pipeline.PlanActionPipelineFactory;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo.MarketActionPlanInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;

/**
 * Tests for {@link ActionOrchestrator}.
 */
public class ActionOrchestratorTest {
    private static final long REALTIME_TOPOLOGY_CONTEXT_ID = 1234;
    private final ActionOrchestratorNotificationSender notificationSender =
        mock(ActionOrchestratorNotificationSender.class);
    private final LiveActionPipelineFactory liveActionPipelineFactory = mock(LiveActionPipelineFactory.class);
    private final PlanActionPipelineFactory planActionPipelineFactory = mock(PlanActionPipelineFactory.class);
    private final ActionOrchestrator orchestrator = new ActionOrchestrator(liveActionPipelineFactory,
        planActionPipelineFactory, notificationSender, REALTIME_TOPOLOGY_CONTEXT_ID);

    private final ActionPlan liveActionPlan = ActionPlan.newBuilder()
        .setId(1)
        .setInfo(ActionPlanInfo.newBuilder()
            .setMarket(MarketActionPlanInfo.newBuilder()
                .setSourceTopologyInfo(TopologyInfo.newBuilder()
                    .setTopologyId(123)
                    .setTopologyContextId(REALTIME_TOPOLOGY_CONTEXT_ID))))
        .build();

    private final ActionPlan planActionPlan = ActionPlan.newBuilder()
        .setId(1)
        .setInfo(ActionPlanInfo.newBuilder()
            .setMarket(MarketActionPlanInfo.newBuilder()
                .setSourceTopologyInfo(TopologyInfo.newBuilder()
                    .setTopologyId(123)
                    .setTopologyContextId(REALTIME_TOPOLOGY_CONTEXT_ID + 1))))
        .build();

    private final LocalDateTime startTime = LocalDateTime.MIN;
    private final LocalDateTime endTime = LocalDateTime.MIN;

    /**
     * setup.
     * @throws Exception on exception.
     */
    @SuppressWarnings("unchecked")
    @Before
    public void setup() throws Exception {
        final ActionPipeline pipeline = mock(ActionPipeline.class);
        when(pipeline.run(any(ActionStore.class))).thenReturn(new ActionProcessingInfo(1));

        when(liveActionPipelineFactory.actionPipeline(any(ActionPlan.class))).thenReturn(pipeline);
        when(planActionPipelineFactory.actionPipeline(any(ActionPlan.class))).thenReturn(pipeline);
    }

    /**
     * testPlanActionsGetsPlanPipeline.
     *
     * @throws Exception on exception.
     */
    @Test
    public void testPlanActionsGetsPlanPipeline() throws Exception {
        orchestrator.processActions(planActionPlan, startTime, endTime);
        verify(planActionPipelineFactory).actionPipeline(eq(planActionPlan));
    }

    /**
     * testLiveActionsGetsLivePipeline.
     *
     * @throws Exception on exception.
     */
    @Test
    public void testLiveActionsGetsLivePipeline() throws Exception {
        orchestrator.processActions(liveActionPlan, startTime, endTime);
        verify(liveActionPipelineFactory).actionPipeline(eq(liveActionPlan));
    }

    /**
     * testNotifyActionsUpdated.
     *
     * @throws Exception on exception.
     */
    @Test
    public void testNotifyActionsUpdated() throws Exception {
        orchestrator.processActions(liveActionPlan, startTime, endTime);
        verify(notificationSender).notifyActionsUpdated(eq(liveActionPlan));
    }

    /**
     * testNotifyActionsUpdateFailure.
     *
     * @throws Exception on anything going wrong.
     */
    @Test
    public void testNotifyActionsUpdateFailure() throws Exception {
        when(liveActionPipelineFactory.actionPipeline(eq(liveActionPlan))).thenThrow(new RuntimeException());
        orchestrator.processActions(liveActionPlan, startTime, endTime);

        verify(notificationSender).notifyActionsUpdateFailure(eq(liveActionPlan));
    }
}
