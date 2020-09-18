package com.vmturbo.action.orchestrator.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.Optional;

import com.google.common.collect.ImmutableList;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import com.vmturbo.action.orchestrator.execution.ActionAutomationManager;
import com.vmturbo.action.orchestrator.store.ActionStorehouse.StoreDeletionException;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo.MarketActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;

/**
 * Tests for {@link ActionStorehouse}.
 */
public class ActionStorehouseTest {

    private static final long REALTIME_TOPOLOGY_CONTEXT_ID = 1234;
    private final ActionStore actionStore = Mockito.mock(ActionStore.class);
    private final EntitySeverityCache severityCache = Mockito.mock(EntitySeverityCache.class);
    private final IActionStoreFactory actionStoreFactory = Mockito.mock(IActionStoreFactory.class);
    private final IActionStoreLoader actionStoreLoader = Mockito.mock(IActionStoreLoader.class);
    private final long topologyContextId = 0xCAFE;
    private final ActionAutomationManager automationManager = mock(ActionAutomationManager.class);

    private ActionStorehouse actionStorehouse;
    private final Action moveAction = Action.newBuilder()
        .setId(9999L)
        .setDeprecatedImportance(0)
        .setExplanation(Explanation.getDefaultInstance())
        .setInfo(ActionInfo.getDefaultInstance())
        .build();
    private final ActionPlan actionPlan = ActionPlan.newBuilder()
        .setId(1234L)
        .setInfo(ActionPlanInfo.newBuilder()
            .setMarket(MarketActionPlanInfo.newBuilder()
                .setSourceTopologyInfo(TopologyInfo.newBuilder()
                    .setTopologyId(5678L)
                    .setTopologyContextId(topologyContextId))))
        .addAction(moveAction)
        .build();

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setup() {
        actionStorehouse = new ActionStorehouse(actionStoreFactory, actionStoreLoader, automationManager);
        when(actionStoreFactory.newStore(anyLong())).thenReturn(actionStore);
        when(actionStore.getEntitySeverityCache()).thenReturn(Optional.of(severityCache));
        when(actionStore.allowsExecution()).thenReturn(true);
        when(actionStore.getStoreTypeName()).thenReturn("test");
        when(actionStoreFactory.getContextTypeName(anyLong())).thenReturn("foo");
    }

    @Test
    public void testStoreActionsNewStore() throws Exception {
        actionStorehouse.storeActions(actionPlan);

        verify(actionStoreFactory).newStore(anyLong());
        assertEquals(actionStore, actionStorehouse.getStore(topologyContextId).get());
    }

    @Test
    public void testStoreActionsWithNoContextId() throws Exception {
        ActionPlan noContextPlan = ActionPlan.newBuilder()
            .setId(1234L)
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyId(5678L))))
            .addAction(moveAction)
            .build();

        expectedException.expect(IllegalArgumentException.class);
        actionStorehouse.storeActions(noContextPlan);
    }

    @Test
    public void testStoreActionsExistingStore() throws Exception {
        actionStorehouse.storeActions(actionPlan);
        actionStorehouse.storeActions(actionPlan);

        verify(actionStoreFactory, times(1)).newStore(eq(topologyContextId));
    }

    @Test
    public void testStoreActionsUsesPopulateMethod() throws Exception {
        actionStorehouse.storeActions(actionPlan);
        verify(actionStore).populateRecommendedActions(eq(actionPlan));
    }

    @Test
    public void testGetNonExistingStore() throws Exception {
        assertFalse(actionStorehouse.getStore(topologyContextId).isPresent());
    }

    @Test
    public void testGetNonExistingSeverityCache() throws Exception {
        assertFalse(actionStorehouse.getSeverityCache(topologyContextId).isPresent());
    }

    /**
     * testOnActionsReceivedStoreActions.
     *
     * @throws Exception on exception.
     */
    @Test
    public void testOnActionsReceivedStoreActions() throws Exception {
        ActionPlan actionPlan = ActionPlan.newBuilder()
            .setId(1)
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyId(123)
                        .setTopologyContextId(REALTIME_TOPOLOGY_CONTEXT_ID))))
            .build();

        actionStorehouse.storeActions(actionPlan);
        verify(actionStore).populateRecommendedActions(actionPlan);
    }

    @Test
    public void testRemoveExistingStore() throws Exception {
        actionStorehouse.storeActions(actionPlan);
        assertEquals(actionStore, actionStorehouse.getStore(topologyContextId).get());

        Optional<ActionStore> store = actionStorehouse.removeStore(topologyContextId);
        assertEquals(actionStore, store.get());
    }

    @Test
    public void testRemoveNonExistingStore() throws Exception {
        assertFalse(actionStorehouse.removeStore(topologyContextId).isPresent());
    }

    @Test
    public void testGetAllStores() throws Exception {
        assertTrue(actionStorehouse.getAllStores().isEmpty());
        actionStorehouse.storeActions(actionPlan);

        Map<Long, ActionStore> stores = actionStorehouse.getAllStores();
        assertEquals(1, stores.size());
        assertEquals(actionStore, stores.get(topologyContextId));
    }

    @Test
    public void testClearStore() throws Exception {
        actionStorehouse.storeActions(actionPlan);

        assertEquals(1, actionStorehouse.size());

        actionStorehouse.clearStore();

        assertEquals(0, actionStorehouse.size());
    }

    @Test
    public void testMultipleStores() throws Exception {
        final Action otherMoveAction = Action.newBuilder()
            .setId(19999L)
            .setDeprecatedImportance(0)
            .setExplanation(Explanation.getDefaultInstance())
            .setInfo(ActionInfo.getDefaultInstance())
            .build();
        final ActionPlan otherActionPlan = ActionPlan.newBuilder()
            .setId(11234L)
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyId(15678L)
                        .setTopologyContextId(0xDECADE))))
            .addAction(otherMoveAction)
            .build();

        assertEquals(0, actionStorehouse.size());

        actionStorehouse.storeActions(actionPlan);
        actionStorehouse.storeActions(otherActionPlan);

        assertEquals(2, actionStorehouse.size());
    }

    @Test
    public void testStorehouseLoadEmpty() throws Exception {
        assertEquals(0, actionStorehouse.size());
    }

    @Test
    public void testStorehouseLoadWithStores() throws Exception {
        ActionStore persistedStore = Mockito.mock(ActionStore.class);
        when(persistedStore.getTopologyContextId()).thenReturn(topologyContextId);

        final IActionStoreLoader actionStoreLoader = Mockito.mock(IActionStoreLoader.class);
        when(actionStoreLoader.loadActionStores()).thenReturn(ImmutableList.of(persistedStore));

        final ActionStorehouse actionStorehouse = new ActionStorehouse(actionStoreFactory,
                actionStoreLoader, automationManager);
        assertEquals(1, actionStorehouse.size());
        assertEquals(persistedStore, actionStorehouse.getStore(topologyContextId).get());
    }

    @Test
    public void testDeleteStoreNotPermitted() throws Exception {
        actionStorehouse.storeActions(actionPlan);
        assertEquals(actionStore, actionStorehouse.getStore(topologyContextId).get());
        when(actionStore.clear()).thenThrow(new IllegalStateException("not permitted"));

        expectedException.expect(IllegalStateException.class);
        actionStorehouse.deleteStore(topologyContextId);
    }

    @Test
    public void testDeleteStoreFailed() throws Exception {
        actionStorehouse.storeActions(actionPlan);
        assertEquals(actionStore, actionStorehouse.getStore(topologyContextId).get());
        when(actionStore.clear()).thenReturn(false);

        expectedException.expect(StoreDeletionException.class);
        actionStorehouse.deleteStore(topologyContextId);
    }

    @Test
    public void testDeleteStoreSucceeded() throws Exception {
        actionStorehouse.storeActions(actionPlan);
        assertEquals(actionStore, actionStorehouse.getStore(topologyContextId).get());
        when(actionStore.clear()).thenReturn(true);

        assertEquals(actionStore, actionStorehouse.deleteStore(topologyContextId).get());
        assertFalse(actionStorehouse.getStore(topologyContextId).isPresent());
    }

    @Test
    public void testDeleteNonExistingStore() throws Exception {
        assertEquals(Optional.empty(), actionStorehouse.deleteStore(topologyContextId));
    }

    /**
     * Test cancelling queued actions forwards to the automation manager.
     */
    @Test
    public void testCancelQueuedActions() {
        actionStorehouse.cancelQueuedActions();
        verify(automationManager).cancelQueuedActions();
    }
}
