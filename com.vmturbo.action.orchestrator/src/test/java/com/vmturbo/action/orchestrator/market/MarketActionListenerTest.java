package com.vmturbo.action.orchestrator.market;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;

import io.opentracing.SpanContext;

import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.action.orchestrator.execution.MockExecutorService;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo.MarketActionPlanInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ActionPlanSummary;
import com.vmturbo.common.protobuf.topology.TopologyDTO.AnalysisSummary;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;

/**
 * Test for the {@link MarketActionListener}.
 */
public class MarketActionListenerTest {

    private static final long realtimeTopologyContextId = 1234;
    private final ActionPlanAssessor actionPlanAssessor = mock(ActionPlanAssessor.class);
    private final ActionOrchestrator orchestrator = mock(ActionOrchestrator.class);
    private final MockExecutorService projectedActionPlanThreadPool = new MockExecutorService();
    private final MockExecutorService realtimeActionPlanThreadPool = new MockExecutorService();

    /**
     * testOnActionsReceivedProcessActions.
     */
    @Test
    public void testOnActionsReceivedProcessActions() {
        ActionPlan actionPlan = ActionPlan.newBuilder()
            .setId(1)
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyId(123)
                        .setTopologyContextId(realtimeTopologyContextId))))
            .build();
        when(actionPlanAssessor.isActionPlanExpired(eq(actionPlan))).thenReturn(false);

        MarketActionListener actionsListener = new MarketActionListener(orchestrator,
                actionPlanAssessor, projectedActionPlanThreadPool, realtimeActionPlanThreadPool,
                realtimeTopologyContextId);
        actionsListener.onActionsReceived(actionPlan, mock(SpanContext.class));

        executeAllSubmittedTasks();
        verify(orchestrator).processActions(eq(actionPlan), any(LocalDateTime.class), any(LocalDateTime.class));
    }

    @Test
    public void testDropExpiredActionPlan() {
        ActionPlan actionPlan = ActionPlan.newBuilder()
            .setId(1)
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyId(123)
                        .setTopologyContextId(realtimeTopologyContextId))))
            .build();
        when(actionPlanAssessor.isActionPlanExpired(eq(actionPlan))).thenReturn(true);

        MarketActionListener actionsListener =
            new MarketActionListener(orchestrator, actionPlanAssessor,
                    projectedActionPlanThreadPool, realtimeActionPlanThreadPool,
                    realtimeTopologyContextId);
        actionsListener.onActionsReceived(actionPlan, mock(SpanContext.class));

        executeAllSubmittedTasks();
        verify(orchestrator, Mockito.never()).processActions(any(ActionPlan.class),
            any(LocalDateTime.class), any(LocalDateTime.class));
    }

    @Test
    public void testDropLiveMarketActionPlanIfMoreRecentAvailable() {
        ActionPlan actionPlan = ActionPlan.newBuilder()
            .setId(1)
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyId(123)
                        .setTopologyContextId(realtimeTopologyContextId))))
            .build();
        // Not expired
        when(actionPlanAssessor.isActionPlanExpired(eq(actionPlan))).thenReturn(false);

        MarketActionListener actionsListener =
            new MarketActionListener(orchestrator, actionPlanAssessor,
                    projectedActionPlanThreadPool, realtimeActionPlanThreadPool,
                    realtimeTopologyContextId);

        actionsListener.onAnalysisSummary(AnalysisSummary.newBuilder()
            .setActionPlanSummary(ActionPlanSummary.newBuilder()
                .setActionPlanId(actionPlan.getId() + 1))
            .setSourceTopologyInfo(TopologyInfo.newBuilder()
                .setTopologyType(TopologyType.REALTIME))
            .build());

        actionsListener.onActionsReceived(actionPlan, mock(SpanContext.class));

        executeAllSubmittedTasks();
        verify(orchestrator, Mockito.never()).processActions(any(ActionPlan.class),
            any(LocalDateTime.class), any(LocalDateTime.class));
    }

    @Test
    public void testPlanAnalysisNotIgnored() {
        // P plan.
        ActionPlan actionPlan = ActionPlan.newBuilder()
            .setId(1)
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyId(123)
                        .setTopologyType(TopologyType.PLAN)
                        .setTopologyContextId(1231231))))
            .build();
        // Not expired
        when(actionPlanAssessor.isActionPlanExpired(eq(actionPlan))).thenReturn(false);

        MarketActionListener actionsListener =
            new MarketActionListener(orchestrator, actionPlanAssessor,
                    projectedActionPlanThreadPool, realtimeActionPlanThreadPool,
                    realtimeTopologyContextId);

        // Got a NEWER live action plan
        actionsListener.onAnalysisSummary(AnalysisSummary.newBuilder()
            .setActionPlanSummary(ActionPlanSummary.newBuilder()
                .setActionPlanId(actionPlan.getId() + 1))
            .setSourceTopologyInfo(TopologyInfo.newBuilder()
                .setTopologyType(TopologyType.REALTIME))
            .build());

        actionsListener.onActionsReceived(actionPlan, mock(SpanContext.class));

        executeAllSubmittedTasks();
        // We should still have saved the plan action plan.
        verify(orchestrator).processActions(eq(actionPlan),
            any(LocalDateTime.class), any(LocalDateTime.class));
    }

    @Test
    public void testPlanAnalysisSummaryNoAffectLiveActionPlan() {
        // Realtime plan.
        ActionPlan actionPlan = ActionPlan.newBuilder()
            .setId(1)
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyId(123)
                        .setTopologyType(TopologyType.REALTIME)
                        .setTopologyContextId(realtimeTopologyContextId))))
            .build();
        // Not expired
        when(actionPlanAssessor.isActionPlanExpired(eq(actionPlan))).thenReturn(false);

        MarketActionListener actionsListener =
            new MarketActionListener(orchestrator, actionPlanAssessor,
                    projectedActionPlanThreadPool, realtimeActionPlanThreadPool,
                    realtimeTopologyContextId);

        // Got a NEWER plan action plan
        actionsListener.onAnalysisSummary(AnalysisSummary.newBuilder()
            .setActionPlanSummary(ActionPlanSummary.newBuilder()
                .setActionPlanId(actionPlan.getId() + 1))
            .setSourceTopologyInfo(TopologyInfo.newBuilder()
                .setTopologyType(TopologyType.PLAN))
            .build());

        actionsListener.onActionsReceived(actionPlan, mock(SpanContext.class));

        executeAllSubmittedTasks();
        // We should still have saved the realtime plan.
        verify(orchestrator).processActions(eq(actionPlan),
            any(LocalDateTime.class), any(LocalDateTime.class));
    }

    private void executeAllSubmittedTasks() {
        projectedActionPlanThreadPool.executeTasks();
        realtimeActionPlanThreadPool.executeTasks();
    }

    /**
     * Test that realtime actions are processing in realtimeActionsExecutorService.
     */
    @Test
    public void testProcessingRealtimeActionsInRealtimeActionExecutorService() {
        // ARRANGE
        final ExecutorService planActionsExecutorService = Mockito.mock(ExecutorService.class);
        final ExecutorService realtimeActionsExecutorService = Mockito.mock(ExecutorService.class);

        final ActionPlan realTimeActionPlan = ActionPlan.newBuilder()
                .setId(1)
                .setInfo(ActionPlanInfo.newBuilder()
                        .setMarket(MarketActionPlanInfo.newBuilder()
                                .setSourceTopologyInfo(TopologyInfo.newBuilder()
                                        .setTopologyId(123)
                                        .setTopologyContextId(realtimeTopologyContextId))))
                .build();
        when(actionPlanAssessor.isActionPlanExpired(eq(realTimeActionPlan))).thenReturn(false);

        final MarketActionListener actionsListener = new MarketActionListener(orchestrator,
                actionPlanAssessor, planActionsExecutorService, realtimeActionsExecutorService,
                realtimeTopologyContextId);

        // ACT
        actionsListener.onActionsReceived(realTimeActionPlan, mock(SpanContext.class));

        // VERIFY
        verify(planActionsExecutorService, Mockito.never()).execute(any(Runnable.class));
        verify(realtimeActionsExecutorService).execute(any(Runnable.class));
    }

    /**
     * Test that plan actions are processing in planActionsExecutorService.
     */
    @Test
    public void testProcessingPlanActionsInPlanActionExecutorService() {
        // ARRANGE
        final ExecutorService planActionsExecutorService = Mockito.mock(ExecutorService.class);
        final ExecutorService realtimeActionsExecutorService = Mockito.mock(ExecutorService.class);

        final ActionPlan planActions = ActionPlan.newBuilder()
                .setId(1)
                .setInfo(ActionPlanInfo.newBuilder()
                        .setMarket(MarketActionPlanInfo.newBuilder()
                                .setSourceTopologyInfo(TopologyInfo.newBuilder()
                                        .setTopologyId(123)
                                        .setTopologyContextId(21312L))))
                .build();
        when(actionPlanAssessor.isActionPlanExpired(eq(planActions))).thenReturn(false);

        final MarketActionListener actionsListener = new MarketActionListener(orchestrator,
                actionPlanAssessor, planActionsExecutorService, realtimeActionsExecutorService,
                realtimeTopologyContextId);

        // ACT
        actionsListener.onActionsReceived(planActions, mock(SpanContext.class));

        // VERIFY
        verify(realtimeActionsExecutorService, Mockito.never()).execute(any(Runnable.class));
        verify(planActionsExecutorService).execute(any(Runnable.class));
    }
}