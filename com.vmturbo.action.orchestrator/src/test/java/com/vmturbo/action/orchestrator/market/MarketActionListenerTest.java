package com.vmturbo.action.orchestrator.market;

import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.action.orchestrator.api.ActionOrchestratorNotificationSender;
import com.vmturbo.action.orchestrator.execution.AutomatedActionExecutor;
import com.vmturbo.action.orchestrator.store.ActionStore;
import com.vmturbo.action.orchestrator.store.ActionStorehouse;
import com.vmturbo.action.orchestrator.store.EntitySeverityCache;
import com.vmturbo.action.orchestrator.store.IActionStoreFactory;
import com.vmturbo.action.orchestrator.store.IActionStoreLoader;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;

/**
 * Test for the {@link MarketActionListener}.
 */
public class MarketActionListenerTest {

    private static final long realtimeTopologyContextId = 1234;
    private final IActionStoreFactory actionStoreFactory = Mockito.mock(IActionStoreFactory.class);
    private final IActionStoreLoader actionStoreLoader = Mockito.mock(IActionStoreLoader.class);
    private final AutomatedActionExecutor executor = Mockito.mock(AutomatedActionExecutor.class);
    private final ActionStorehouse actionStorehouse = new ActionStorehouse(actionStoreFactory,
            executor, actionStoreLoader);
    private final ActionStore actionStore = mock(ActionStore.class);
    private final EntitySeverityCache severityCache = mock(EntitySeverityCache.class);

    @Before
    public void setup() {
        when(actionStoreFactory.newStore(anyLong())).thenReturn(actionStore);
        when(actionStore.getEntitySeverityCache()).thenReturn(severityCache);
        when(actionStoreLoader.loadActionStores()).thenReturn(Collections.emptyList());
        when(actionStore.getStoreTypeName()).thenReturn("test");
    }

    @Test
    public void testOnActionsReceivedPopulatesActionStore() throws Exception {
        ActionOrchestratorNotificationSender notificationSender =
                mock(ActionOrchestratorNotificationSender.class);
        ActionPlan actionPlan = ActionPlan.newBuilder()
            .setId(1)
            .setTopologyContextId(realtimeTopologyContextId)
            .build();

        MarketActionListener actionsListener =
                new MarketActionListener(notificationSender, actionStorehouse);
        actionsListener.onActionsReceived(actionPlan);

        verify(actionStore).populateRecommendedActions(actionPlan);
    }

    @Test
    public void testOnActionsReceivedRefreshesSeverityCache() throws Exception {
        ActionOrchestratorNotificationSender notificationSender =
                mock(ActionOrchestratorNotificationSender.class);
        ActionPlan actionPlan = ActionPlan.newBuilder()
            .setId(1)
            .setTopologyContextId(realtimeTopologyContextId)
            .build();

        MarketActionListener actionsListener =
                new MarketActionListener(notificationSender, actionStorehouse);
        actionsListener.onActionsReceived(actionPlan);

        verify(severityCache).refresh(actionStore);
    }
}