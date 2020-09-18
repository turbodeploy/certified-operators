package com.vmturbo.action.orchestrator.market;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.LocalDateTime;

import io.opentracing.SpanContext;

import org.junit.Test;

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

    /**
     * testOnActionsReceivedProcessActions.
     *
     * @throws Exception on exception.
     */
    @Test
    public void testOnActionsReceivedProcessActions() throws Exception {
        ActionPlan actionPlan = ActionPlan.newBuilder()
            .setId(1)
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyId(123)
                        .setTopologyContextId(realtimeTopologyContextId))))
            .build();
        when(actionPlanAssessor.isActionPlanExpired(eq(actionPlan))).thenReturn(false);

        MarketActionListener actionsListener =
                new MarketActionListener(orchestrator, actionPlanAssessor);
        actionsListener.onActionsReceived(actionPlan, mock(SpanContext.class));

        verify(orchestrator).processActions(eq(actionPlan), any(LocalDateTime.class), any(LocalDateTime.class));
    }

    @Test
    public void testDropExpiredActionPlan() throws Exception {
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
            new MarketActionListener(orchestrator, actionPlanAssessor);
        actionsListener.onActionsReceived(actionPlan, mock(SpanContext.class));

        verify(orchestrator, never()).processActions(any(ActionPlan.class),
            any(LocalDateTime.class), any(LocalDateTime.class));
    }

    @Test
    public void testDropLiveMarketActionPlanIfMoreRecentAvailable() throws Exception {
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
            new MarketActionListener(orchestrator, actionPlanAssessor);

        actionsListener.onAnalysisSummary(AnalysisSummary.newBuilder()
            .setActionPlanSummary(ActionPlanSummary.newBuilder()
                .setActionPlanId(actionPlan.getId() + 1))
            .setSourceTopologyInfo(TopologyInfo.newBuilder()
                .setTopologyType(TopologyType.REALTIME))
            .build());

        actionsListener.onActionsReceived(actionPlan, mock(SpanContext.class));

        verify(orchestrator, never()).processActions(any(ActionPlan.class),
            any(LocalDateTime.class), any(LocalDateTime.class));
    }

    @Test
    public void testPlanAnalysisNotIgnored() throws Exception {
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
            new MarketActionListener(orchestrator, actionPlanAssessor);

        // Got a NEWER live action plan
        actionsListener.onAnalysisSummary(AnalysisSummary.newBuilder()
            .setActionPlanSummary(ActionPlanSummary.newBuilder()
                .setActionPlanId(actionPlan.getId() + 1))
            .setSourceTopologyInfo(TopologyInfo.newBuilder()
                .setTopologyType(TopologyType.REALTIME))
            .build());

        actionsListener.onActionsReceived(actionPlan, mock(SpanContext.class));

        // We should still have saved the plan action plan.
        verify(orchestrator).processActions(eq(actionPlan),
            any(LocalDateTime.class), any(LocalDateTime.class));
    }

    @Test
    public void testPlanAnalysisSummaryNoAffectLiveActionPlan() throws Exception {
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
            new MarketActionListener(orchestrator, actionPlanAssessor);

        // Got a NEWER plan action plan
        actionsListener.onAnalysisSummary(AnalysisSummary.newBuilder()
            .setActionPlanSummary(ActionPlanSummary.newBuilder()
                .setActionPlanId(actionPlan.getId() + 1))
            .setSourceTopologyInfo(TopologyInfo.newBuilder()
                .setTopologyType(TopologyType.PLAN))
            .build());

        actionsListener.onActionsReceived(actionPlan, mock(SpanContext.class));

        // We should still have saved the realtime plan.
        verify(orchestrator).processActions(eq(actionPlan),
            any(LocalDateTime.class), any(LocalDateTime.class));
    }
}