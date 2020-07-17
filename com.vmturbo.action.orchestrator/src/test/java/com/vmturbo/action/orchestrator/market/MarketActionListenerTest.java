package com.vmturbo.action.orchestrator.market;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Optional;

import io.opentracing.SpanContext;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.action.orchestrator.api.ActionOrchestratorNotificationSender;
import com.vmturbo.action.orchestrator.approval.ActionApprovalSender;
import com.vmturbo.action.orchestrator.execution.AutomatedActionExecutor;
import com.vmturbo.action.orchestrator.store.ActionStore;
import com.vmturbo.action.orchestrator.store.ActionStorehouse;
import com.vmturbo.action.orchestrator.store.EntitySeverityCache;
import com.vmturbo.action.orchestrator.store.IActionStoreFactory;
import com.vmturbo.action.orchestrator.store.IActionStoreLoader;
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
    private final IActionStoreFactory actionStoreFactory = Mockito.mock(IActionStoreFactory.class);
    private final IActionStoreLoader actionStoreLoader = Mockito.mock(IActionStoreLoader.class);
    private final AutomatedActionExecutor executor = Mockito.mock(AutomatedActionExecutor.class);
    private ActionStorehouse actionStorehouse;
    private final ActionStore actionStore = mock(ActionStore.class);
    private final EntitySeverityCache severityCache = mock(EntitySeverityCache.class);
    private final ActionPlanAssessor actionPlanAssessor = mock(ActionPlanAssessor.class);

    @Before
    public void setup() {
        final ActionApprovalSender approvalSender = Mockito.mock(ActionApprovalSender.class);
        this.actionStorehouse = new ActionStorehouse(actionStoreFactory,
                executor, actionStoreLoader, approvalSender);
        when(actionStoreFactory.newStore(anyLong())).thenReturn(actionStore);
        when(actionStore.getEntitySeverityCache()).thenReturn(Optional.of(severityCache));
        when(actionStoreLoader.loadActionStores()).thenReturn(Collections.emptyList());
        when(actionStore.getStoreTypeName()).thenReturn("test");
    }

    @Test
    public void testOnActionsReceivedPopulatesActionStore() throws Exception {
        ActionOrchestratorNotificationSender notificationSender =
                mock(ActionOrchestratorNotificationSender.class);
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
                new MarketActionListener(notificationSender, actionStorehouse, actionPlanAssessor);
        actionsListener.onActionsReceived(actionPlan, mock(SpanContext.class));

        verify(actionStore).populateRecommendedActions(actionPlan);
    }

    @Test
    public void testOnActionsReceivedRefreshesSeverityCache() throws Exception {
        ActionOrchestratorNotificationSender notificationSender =
                mock(ActionOrchestratorNotificationSender.class);
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
                new MarketActionListener(notificationSender, actionStorehouse, actionPlanAssessor);
        actionsListener.onActionsReceived(actionPlan, mock(SpanContext.class));

        verify(severityCache).refresh(actionStore);
    }

    @Test
    public void testDropExpiredActionPlan() throws Exception {
        ActionOrchestratorNotificationSender notificationSender =
            mock(ActionOrchestratorNotificationSender.class);
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
            new MarketActionListener(notificationSender, actionStorehouse, actionPlanAssessor);
        actionsListener.onActionsReceived(actionPlan, mock(SpanContext.class));

        verify(actionStore, never()).populateRecommendedActions(any(ActionPlan.class));
        verify(severityCache, never()).refresh(any(ActionStore.class));
    }

    @Test
    public void testDropLiveMarketActionPlanIfMoreRecentAvailable() throws Exception {
        ActionOrchestratorNotificationSender notificationSender =
            mock(ActionOrchestratorNotificationSender.class);
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
            new MarketActionListener(notificationSender, actionStorehouse, actionPlanAssessor);

        actionsListener.onAnalysisSummary(AnalysisSummary.newBuilder()
            .setActionPlanSummary(ActionPlanSummary.newBuilder()
                .setActionPlanId(actionPlan.getId() + 1))
            .setSourceTopologyInfo(TopologyInfo.newBuilder()
                .setTopologyType(TopologyType.REALTIME))
            .build());

        actionsListener.onActionsReceived(actionPlan, mock(SpanContext.class));

        verify(actionStore, never()).populateRecommendedActions(any(ActionPlan.class));
        verify(severityCache, never()).refresh(any(ActionStore.class));
    }

    @Test
    public void testPlanAnalysisNotIgnored() throws Exception {
        ActionOrchestratorNotificationSender notificationSender =
            mock(ActionOrchestratorNotificationSender.class);
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
            new MarketActionListener(notificationSender, actionStorehouse, actionPlanAssessor);

        // Got a NEWER live action plan
        actionsListener.onAnalysisSummary(AnalysisSummary.newBuilder()
            .setActionPlanSummary(ActionPlanSummary.newBuilder()
                .setActionPlanId(actionPlan.getId() + 1))
            .setSourceTopologyInfo(TopologyInfo.newBuilder()
                .setTopologyType(TopologyType.REALTIME))
            .build());

        actionsListener.onActionsReceived(actionPlan, mock(SpanContext.class));

        // We should still have saved the plan action plan.
        verify(actionStore).populateRecommendedActions(actionPlan);
        verify(severityCache).refresh(any(ActionStore.class));
    }

    @Test
    public void testPlanAnalysisSummaryNoAffectLiveActionPlan() throws Exception {
        ActionOrchestratorNotificationSender notificationSender =
            mock(ActionOrchestratorNotificationSender.class);
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
            new MarketActionListener(notificationSender, actionStorehouse, actionPlanAssessor);

        // Got a NEWER plan action plan
        actionsListener.onAnalysisSummary(AnalysisSummary.newBuilder()
            .setActionPlanSummary(ActionPlanSummary.newBuilder()
                .setActionPlanId(actionPlan.getId() + 1))
            .setSourceTopologyInfo(TopologyInfo.newBuilder()
                .setTopologyType(TopologyType.PLAN))
            .build());

        actionsListener.onActionsReceived(actionPlan, mock(SpanContext.class));

        // We should still have saved the realtime plan.
        verify(actionStore).populateRecommendedActions(actionPlan);
        verify(severityCache).refresh(any(ActionStore.class));
    }

//    @Test
//    public void
}