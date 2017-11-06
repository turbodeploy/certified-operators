package com.vmturbo.action.orchestrator.store;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.InOrder;
import org.mockito.Mockito;

import com.vmturbo.action.orchestrator.execution.AutomatedActionExecutor;
import com.vmturbo.action.orchestrator.store.ActionStorehouse.StoreDeletionException;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link ActionStorehouse}.
 */
public class ActionStorehouseTest {

    private final ActionStore actionStore = Mockito.mock(ActionStore.class);
    private final EntitySeverityCache severityCache = Mockito.mock(EntitySeverityCache.class);
    private final IActionStoreFactory actionStoreFactory = Mockito.mock(IActionStoreFactory.class);
    private final IActionStoreLoader actionStoreLoader = Mockito.mock(IActionStoreLoader.class);
    private final AutomatedActionExecutor executor = Mockito.mock(AutomatedActionExecutor.class);
    private final long topologyContextId = 0xCAFE;

    private final ActionStorehouse actionStorehouse = new ActionStorehouse(actionStoreFactory,
            executor, actionStoreLoader);
    private final Action moveAction = Action.newBuilder()
        .setId(9999L)
        .setImportance(0)
        .setExplanation(Explanation.getDefaultInstance())
        .setInfo(ActionInfo.getDefaultInstance())
        .build();
    private final ActionPlan actionPlan = ActionPlan.newBuilder()
        .setId(1234L)
        .setTopologyId(5678L)
        .setTopologyContextId(topologyContextId)
        .addAction(moveAction)
        .build();

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setup() {
        when(actionStoreFactory.newStore(anyLong())).thenReturn(actionStore);
        when(actionStore.getEntitySeverityCache()).thenReturn(severityCache);
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
            .setTopologyId(5678L)
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
    public void testStoreActionsRefreshesSeverityCache() throws Exception {
        actionStorehouse.storeActions(actionPlan);
        verify(severityCache).refresh(eq(actionStore));
    }

    @Test
    public void testStoreActionsExecutesAutomaticActions() {
        actionStorehouse.storeActions(actionPlan);
        verify(executor).executeAutomatedFromStore(actionStore);
    }

    @Test
    public void testStoreActionsPopulateRefreshExecuteRefreshFlow() {
        InOrder inOrder = Mockito.inOrder(executor, severityCache, actionStore);
        actionStorehouse.storeActions(actionPlan);
        inOrder.verify(actionStore).populateRecommendedActions(actionPlan);
        inOrder.verify(executor).executeAutomatedFromStore(actionStore);
        inOrder.verify(severityCache).refresh(actionStore);
    }

    @Test
    public void testGetNonExistingStore() throws Exception {
        assertFalse(actionStorehouse.getStore(topologyContextId).isPresent());
    }

    @Test
    public void testGetNonExistingSeverityCache() throws Exception {
        assertFalse(actionStorehouse.getSeverityCache(topologyContextId).isPresent());
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
            .setImportance(0)
            .setExplanation(Explanation.getDefaultInstance())
            .setInfo(ActionInfo.getDefaultInstance())
            .build();
        final ActionPlan otherActionPlan = ActionPlan.newBuilder()
            .setId(11234L)
            .setTopologyId(15678L)
            .setTopologyContextId(0xDECADE)
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
        when(actionStoreLoader.loadActionStores()).thenReturn(Collections.singletonList(persistedStore));

        final ActionStorehouse actionStorehouse = new ActionStorehouse(actionStoreFactory,
                executor, actionStoreLoader);
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
}