package com.vmturbo.action.orchestrator.diagnostics;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import javax.annotation.Nullable;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.google.common.collect.ImmutableMap;

import io.prometheus.client.CollectorRegistry;

import com.vmturbo.action.orchestrator.ActionOrchestratorTestUtils;
import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ActionEvent.BeginExecutionEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.CannotExecuteEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.FailureEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.ManualAcceptanceEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.NotRecommendedEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.ProgressEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.SuccessEvent;
import com.vmturbo.action.orchestrator.action.ActionModeCalculator;
import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.action.orchestrator.store.ActionFactory;
import com.vmturbo.action.orchestrator.store.ActionStore;
import com.vmturbo.action.orchestrator.store.ActionStorehouse;
import com.vmturbo.action.orchestrator.store.EntitiesCache;
import com.vmturbo.action.orchestrator.store.EntitySeverityCache;
import com.vmturbo.action.orchestrator.store.IActionFactory;
import com.vmturbo.action.orchestrator.store.IActionStoreFactory;
import com.vmturbo.action.orchestrator.translation.ActionTranslator;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.components.common.DiagnosticsWriter;

/**
 * Unit tests for {@link ActionOrchestratorDiagnostics}.
 */
public class ActionOrchestratorDiagnosticsTest {

    private final ActionStorehouse actionStorehouse = mock(ActionStorehouse.class);
    private final ActionStore actionStore = mock(ActionStore.class);
    private final EntitySeverityCache severityCache = mock(EntitySeverityCache.class);
    private final ActionTranslator actionTranslator = Mockito.spy(new ActionTranslator(actionStream ->
            actionStream.map(action -> {
                action.getActionTranslation().setPassthroughTranslationSuccess();
                return action;
            })));
    private ActionModeCalculator actionModeCalculator = new ActionModeCalculator(actionTranslator);

    private final IActionFactory actionFactory = new ActionFactory(actionModeCalculator);
    private final IActionStoreFactory storeFactory = mock(IActionStoreFactory.class);
    private final DiagnosticsWriter diagnosticsWriter = Mockito.spy(new DiagnosticsWriter());
    private final ActionOrchestratorDiagnostics diagnostics =
            new ActionOrchestratorDiagnostics(actionStorehouse, actionFactory, diagnosticsWriter, actionModeCalculator);
    private final long realtimeTopologyContextId = 1234L;

    @Captor
    private ArgumentCaptor<List<Action>> actionCaptor;

    @Captor
    private ArgumentCaptor<List<Action>> planCaptor;

    @Before
    public void setup() {
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
            action.receive(new BeginExecutionEvent());
            action.receive(new SuccessEvent());
        });
    }

    @Test
    public void testFailedAction() throws Exception {
        testSingleAction(action -> {
            action.receive(new ManualAcceptanceEvent("0", 1L));
            action.receive(new BeginExecutionEvent());
            action.receive(new FailureEvent("It was a trap!"));
        });
    }

    @Test
    public void testInProgressAction() throws Exception {
        testSingleAction(action -> {
            action.receive(new ManualAcceptanceEvent("0", 1L));
            action.receive(new BeginExecutionEvent());
            action.receive(new ProgressEvent(10, "Star date 20184..."));
        });
    }

    @Test
    public void testTwoActions() throws Exception {
        final Action action1 = actionFactory.newAction(
                ActionOrchestratorTestUtils.createMoveRecommendation(1), 0L);
        final Action action2 = actionFactory.newAction(
                ActionOrchestratorTestUtils.createMoveRecommendation(2), 0L);
        when(actionStorehouse.getAllStores()).thenReturn(
            ImmutableMap.<Long, ActionStore>builder().put(realtimeTopologyContextId, actionStore).build()
        );
        when(actionStore.getActions())
               .thenReturn(ImmutableMap.of(action1.getId(), action1, action2.getId(), action2));

        dumpAndRestore();

        Mockito.verify(actionStore).overwriteActions(actionCaptor.capture());

        final List<Action> deserializedActions = actionCaptor.getValue();
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

        final Action action = actionFactory.newAction(
            ActionOrchestratorTestUtils.createMoveRecommendation(1), 0L);
        when(actionStorehouse.getAllStores()).thenReturn(
            ImmutableMap.<Long, ActionStore>builder()
                .put(planTopologyContextId, planStore)
                .build()
        );

        when(planStore.getActions())
            .thenReturn(ImmutableMap.of(action.getId(), action));
        when(actionStorehouse.getStore(eq(planTopologyContextId))).thenReturn(Optional.empty());
        when(storeFactory.newStore(anyLong())).thenReturn(newStore);
        when(newStore.getEntitySeverityCache()).thenReturn(severityCache);

        dumpAndRestore();

        Mockito.verify(newStore).overwriteActions(actionCaptor.capture());
        Mockito.verify(severityCache).refresh(newStore);

        final List<Action> actions = actionCaptor.getValue();
        Assert.assertEquals(1, actions.size());
    }

    @Test
    public void testMultipleStores() throws Exception {
        final long planTopologyContextId = 5678L;
        final ActionStore planStore = mock(ActionStore.class);
        final EntitySeverityCache planSeverityCache = mock(EntitySeverityCache.class);

        final Action action1 = actionFactory.newAction(
            ActionOrchestratorTestUtils.createMoveRecommendation(1), 0L);
        final Action action2 = actionFactory.newAction(
            ActionOrchestratorTestUtils.createMoveRecommendation(2), 0L);
        when(actionStorehouse.getAllStores()).thenReturn(
            ImmutableMap.<Long, ActionStore>builder()
                .put(realtimeTopologyContextId, actionStore)
                .put(planTopologyContextId, planStore)
                .build()
        );

        when(actionStore.getActions())
            .thenReturn(ImmutableMap.of(action1.getId(), action1));
        when(planStore.getActions())
            .thenReturn(ImmutableMap.of(action2.getId(), action2));
        when(actionStorehouse.getStore(eq(planTopologyContextId)))
            .thenReturn(Optional.of(planStore));
        when(planStore.getEntitySeverityCache()).thenReturn(planSeverityCache);

        dumpAndRestore();

        Mockito.verify(actionStore).overwriteActions(actionCaptor.capture());
        Mockito.verify(planStore).overwriteActions(planCaptor.capture());

        Mockito.verify(severityCache).refresh(actionStore);
        Mockito.verify(planSeverityCache).refresh(planStore);

        final List<Action> deserializedActions = actionCaptor.getValue();
        Assert.assertEquals(1, deserializedActions.size());

        final List<Action> planActions = planCaptor.getValue();
        Assert.assertEquals(1, planActions.size());
    }

    private void testSingleAction(@Nullable final Consumer<Action> actionModifier)
            throws Exception {
        final ActionDTO.Action rec = ActionOrchestratorTestUtils.createMoveRecommendation(1);
        final EntitiesCache settingsCache = mock(EntitiesCache.class);
        when(settingsCache.getSettingsForEntity(eq(rec.getInfo().getMove().getTarget().getId())))
                .thenReturn(ActionOrchestratorTestUtils.makeActionModeSetting(ActionMode.MANUAL));
        final Action action = actionFactory.newAction(rec, settingsCache, 0L);
        if (actionModifier != null) {
            actionModifier.accept(action);
        }

        when(actionStorehouse.getAllStores()).thenReturn(
            ImmutableMap.<Long, ActionStore>builder().put(realtimeTopologyContextId, actionStore).build()
        );
        when(actionStore.getActions()).thenReturn(ImmutableMap.of(action.getId(), action));

        dumpAndRestore();

        Mockito.verify(actionStore).overwriteActions(actionCaptor.capture());

        final List<Action> deserializedActions = actionCaptor.getValue();
        Assert.assertEquals(1, deserializedActions.size());
        final Action deserializedAction = deserializedActions.get(0);

        verify(diagnosticsWriter).writePrometheusMetrics(any(CollectorRegistry.class), any(ZipOutputStream.class));
        ActionOrchestratorTestUtils.assertActionsEqual(action, deserializedAction);
    }

    private void dumpAndRestore() throws Exception {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        final ZipOutputStream zipOutputStream = new ZipOutputStream(byteArrayOutputStream);
        diagnostics.dump(zipOutputStream);
        zipOutputStream.close();

        final String base64Str = Base64.getEncoder().encodeToString(byteArrayOutputStream.toByteArray());

        final ByteArrayInputStream inputStream = new ByteArrayInputStream(Base64.getDecoder().decode(base64Str));
        final ZipInputStream zipInputStream = new ZipInputStream(inputStream);
        diagnostics.restore(zipInputStream);
    }
}
