package com.vmturbo.action.orchestrator.diagnostics;

import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.vmturbo.action.orchestrator.ActionOrchestratorTestUtils;
import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ActionEvent.BeginExecutionEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.CannotExecuteEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.FailureEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.ManualAcceptanceEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.NotRecommendedEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.PrepareExecutionEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.ProgressEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.SuccessEvent;
import com.vmturbo.action.orchestrator.action.ActionModeCalculator;
import com.vmturbo.action.orchestrator.store.ActionFactory;
import com.vmturbo.action.orchestrator.store.ActionStore;
import com.vmturbo.action.orchestrator.store.ActionStorehouse;
import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory.EntitiesAndSettingsSnapshot;
import com.vmturbo.action.orchestrator.store.EntitySeverityCache;
import com.vmturbo.action.orchestrator.store.IActionFactory;
import com.vmturbo.action.orchestrator.store.IActionStoreFactory;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan.ActionPlanType;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;

/**
 * Unit tests for {@link ActionOrchestratorDiagnostics}.
 */
public class ActionOrchestratorDiagnosticsTest {

    private final ActionStorehouse actionStorehouse = mock(ActionStorehouse.class);
    private final ActionStore actionStore = mock(ActionStore.class);
    private final EntitySeverityCache severityCache = mock(EntitySeverityCache.class);
    private ActionModeCalculator actionModeCalculator = new ActionModeCalculator();
    private final IActionFactory actionFactory = new ActionFactory(actionModeCalculator);
    private final IActionStoreFactory storeFactory = mock(IActionStoreFactory.class);
    private final ActionOrchestratorDiagnostics diagnostics =
            new ActionOrchestratorDiagnostics(actionStorehouse, actionModeCalculator);
    private final long realtimeTopologyContextId = 1234L;

    @Captor
    private ArgumentCaptor<Map<ActionPlanType, List<Action>>> actionCaptor;

    @Captor
    private ArgumentCaptor<Map<ActionPlanType, List<Action>>> planCaptor;

    @Before
    public void setup() {
        IdentityGenerator.initPrefix(0);
        MockitoAnnotations.initMocks(this);
        when(actionStorehouse.getActionStoreFactory()).thenReturn(storeFactory);
        when(actionStorehouse.getStore(eq(realtimeTopologyContextId))).thenReturn(
            Optional.of(actionStore));
        when(actionStore.getEntitySeverityCache()).thenReturn(severityCache);
    }

    @Test
    public void testNoActions() throws Exception {
        when(actionStorehouse.getAllStores()).thenReturn(
            ImmutableMap.<Long, ActionStore>builder().put(realtimeTopologyContextId, actionStore).build()
        );
        when(actionStore.getActions()).thenReturn(Collections.emptyMap());

        dumpAndRestore();
    }

    @Test
    public void testOneAction() throws Exception {
        testSingleAction(null);
    }

    @Test
    public void testQueuedAction() throws Exception {
        testSingleAction(action -> action.receive(new ManualAcceptanceEvent("0", 1)));
    }

    @Test
    public void testReadyAction() throws Exception {
        testSingleAction(action -> {
            action.receive(new ManualAcceptanceEvent("0", 1));
            action.receive(new PrepareExecutionEvent());
            action.receive(new BeginExecutionEvent());
        });
    }

    @Test
    public void testNotRecommendedAction() throws Exception {
        testSingleAction(action -> action.receive(new NotRecommendedEvent(10L)));
    }

    @Test
    public void testCannotExecuteAction() throws Exception {
        testSingleAction(action -> action.receive(new CannotExecuteEvent(10L)));
    }

    @Test
    public void testSuccessfulAction() throws Exception {
        testSingleAction(action -> {
            action.receive(new ManualAcceptanceEvent("0", 1L));
            action.receive(new PrepareExecutionEvent());
            action.receive(new BeginExecutionEvent());
            action.receive(new SuccessEvent());
        });
    }

    @Test
    public void testFailedAction() throws Exception {
        testSingleAction(action -> {
            action.receive(new ManualAcceptanceEvent("0", 1L));
            action.receive(new PrepareExecutionEvent());
            action.receive(new BeginExecutionEvent());
            action.receive(new FailureEvent("It was a trap!"));
        });
    }

    @Test
    public void testInProgressAction() throws Exception {
        testSingleAction(action -> {
            action.receive(new ManualAcceptanceEvent("0", 1L));
            action.receive(new PrepareExecutionEvent());
            action.receive(new BeginExecutionEvent());
            action.receive(new ProgressEvent(10, "Star date 20184..."));
        });
    }

    @Test
    public void testTwoActions() throws Exception {
        final Action action1 =
                actionFactory.newAction(ActionOrchestratorTestUtils.createMoveRecommendation(1), 0L,
                        IdentityGenerator.next());
        final Action action2 =
                actionFactory.newAction(ActionOrchestratorTestUtils.createMoveRecommendation(2), 0L,
                        IdentityGenerator.next());
        when(actionStorehouse.getAllStores()).thenReturn(
            ImmutableMap.<Long, ActionStore>builder().put(realtimeTopologyContextId, actionStore).build()
        );
        when(actionStore.getActionsByActionPlanType())
               .thenReturn(ImmutableMap.of(ActionPlanType.MARKET, ImmutableSet.of(action1, action2)));

        dumpAndRestore();

        Mockito.verify(actionStore).overwriteActions(actionCaptor.capture());

        final List<Action> deserializedActions = actionCaptor.getValue().get(ActionPlanType.MARKET);
        Assert.assertEquals(2, deserializedActions.size());

        ActionOrchestratorTestUtils.assertActionsEqual(action1, deserializedActions.get(0));
        ActionOrchestratorTestUtils.assertActionsEqual(action2, deserializedActions.get(1));
    }

    @Test
    public void testRestoreStoreNotInStorehouse() throws Exception {
        final long planTopologyContextId = 5678L;
        final ActionStore planStore = mock(ActionStore.class);
        final ActionStore newStore = mock(ActionStore.class);
        final EntitySeverityCache severityCache = mock(EntitySeverityCache.class);

        final Action action =
                actionFactory.newAction(ActionOrchestratorTestUtils.createMoveRecommendation(1), 0L,
                        IdentityGenerator.next());
        when(actionStorehouse.getAllStores()).thenReturn(
            ImmutableMap.<Long, ActionStore>builder()
                .put(planTopologyContextId, planStore)
                .build()
        );

        when(planStore.getActionsByActionPlanType())
            .thenReturn(ImmutableMap.of(ActionPlanType.MARKET, ImmutableSet.of(action)));
        when(actionStorehouse.getStore(eq(planTopologyContextId))).thenReturn(Optional.empty());
        when(storeFactory.newStore(anyLong())).thenReturn(newStore);
        when(newStore.getEntitySeverityCache()).thenReturn(severityCache);

        dumpAndRestore();

        Mockito.verify(newStore).overwriteActions(actionCaptor.capture());
        Mockito.verify(severityCache).refresh(newStore);

        final List<Action> actions = actionCaptor.getValue().get(ActionPlanType.MARKET);
        Assert.assertEquals(1, actions.size());
    }

    @Test
    public void testMultipleStores() throws Exception {
        final long planTopologyContextId = 5678L;
        final ActionStore planStore = mock(ActionStore.class);
        final EntitySeverityCache planSeverityCache = mock(EntitySeverityCache.class);

        final Action action1 =
                actionFactory.newAction(ActionOrchestratorTestUtils.createMoveRecommendation(1), 0L,
                        IdentityGenerator.next());
        final Action action2 =
                actionFactory.newAction(ActionOrchestratorTestUtils.createMoveRecommendation(2), 0L,
                        IdentityGenerator.next());
        final Action action3 =
                actionFactory.newAction(ActionOrchestratorTestUtils.createMoveRecommendation(3), 0L,
                        IdentityGenerator.next());
        final Action action4 =
                actionFactory.newAction(ActionOrchestratorTestUtils.createMoveRecommendation(4), 0L,
                        IdentityGenerator.next());
        when(actionStorehouse.getAllStores()).thenReturn(
            ImmutableMap.<Long, ActionStore>builder()
                .put(realtimeTopologyContextId, actionStore)
                .put(planTopologyContextId, planStore)
                .build()
        );

        when(actionStore.getActionsByActionPlanType())
            .thenReturn(ImmutableMap.of(
                ActionPlanType.MARKET, ImmutableSet.of(action1),
                ActionPlanType.BUY_RI, ImmutableSet.of(action2)));
        when(planStore.getActionsByActionPlanType())
            .thenReturn(ImmutableMap.of(
                ActionPlanType.MARKET, ImmutableSet.of(action3),
                ActionPlanType.BUY_RI, ImmutableSet.of(action4)));
        when(actionStorehouse.getStore(eq(planTopologyContextId)))
            .thenReturn(Optional.of(planStore));
        when(planStore.getEntitySeverityCache()).thenReturn(planSeverityCache);

        dumpAndRestore();

        Mockito.verify(actionStore).overwriteActions(actionCaptor.capture());
        Mockito.verify(planStore).overwriteActions(planCaptor.capture());

        Mockito.verify(severityCache).refresh(actionStore);
        Mockito.verify(planSeverityCache).refresh(planStore);

        final List<Action> deserializedMarketActions = actionCaptor.getValue().get(ActionPlanType.MARKET);
        Assert.assertEquals(1, deserializedMarketActions.size());
        Assert.assertEquals(1L, deserializedMarketActions.get(0).getId());
        final List<Action> deserializedBuyRIActions = actionCaptor.getValue().get(ActionPlanType.BUY_RI);
        Assert.assertEquals(1, deserializedBuyRIActions.size());
        Assert.assertEquals(2L, deserializedBuyRIActions.get(0).getId());
        final List<Action> planMarketActions = planCaptor.getValue().get(ActionPlanType.MARKET);
        Assert.assertEquals(1, planMarketActions.size());
        Assert.assertEquals(3L, planMarketActions.get(0).getId());
        final List<Action> planBuyRiActions = planCaptor.getValue().get(ActionPlanType.BUY_RI);
        Assert.assertEquals(1, planBuyRiActions.size());
        Assert.assertEquals(4L, planBuyRiActions.get(0).getId());
    }

    private void testSingleAction(@Nullable final Consumer<Action> actionModifier)
            throws Exception {
        final ActionDTO.Action rec = ActionOrchestratorTestUtils.createMoveRecommendation(1);
        final Action action = actionFactory.newAction(rec, 0L, IdentityGenerator.next());
        final EntitiesAndSettingsSnapshot snapshot = mock(EntitiesAndSettingsSnapshot.class);
        when(snapshot.getOwnerAccountOfEntity(anyLong())).thenReturn(Optional.empty());
        when(snapshot.getResourceGroupForEntity(anyLong())).thenReturn(Optional.empty());

        ActionOrchestratorTestUtils.setEntityAndSourceAndDestination(snapshot, action);

        action.getActionTranslation().setPassthroughTranslationSuccess();
        action.refreshAction(snapshot);

        if (actionModifier != null) {
            actionModifier.accept(action);
        }

        when(actionStorehouse.getAllStores()).thenReturn(
            ImmutableMap.<Long, ActionStore>builder().put(realtimeTopologyContextId, actionStore).build()
        );
        when(actionStore.getActionsByActionPlanType()).thenReturn(ImmutableMap.of(
                ActionPlanType.MARKET, ImmutableSet.of(action)));

        dumpAndRestore();

        Mockito.verify(actionStore).overwriteActions(actionCaptor.capture());

        final List<Action> deserializedActions = actionCaptor.getValue().get(ActionPlanType.MARKET);
        Assert.assertEquals(1, deserializedActions.size());
        final Action deserializedAction = deserializedActions.get(0);
        ActionOrchestratorTestUtils.assertActionsEqual(action, deserializedAction);
    }

    private void dumpAndRestore() throws Exception {
        final DiagnosticsAppender appender = Mockito.mock(DiagnosticsAppender.class);
        diagnostics.collectDiags(appender);
        final ArgumentCaptor<String> diags = ArgumentCaptor.forClass(String.class);
        Mockito.verify(appender).appendString(diags.capture());

        diagnostics.restoreDiags(diags.getAllValues());
    }
}
