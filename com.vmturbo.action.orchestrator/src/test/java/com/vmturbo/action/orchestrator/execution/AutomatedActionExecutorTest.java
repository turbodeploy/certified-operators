package com.vmturbo.action.orchestrator.execution;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mockito;

import io.grpc.Channel;

import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ActionEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.AutomaticAcceptanceEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.BeginExecutionEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.FailureEvent;
import com.vmturbo.action.orchestrator.action.ActionTest;
import com.vmturbo.action.orchestrator.action.ActionTranslation;
import com.vmturbo.action.orchestrator.execution.AutomatedActionExecutor.ActionExecutionTask;
import com.vmturbo.action.orchestrator.store.ActionStore;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation;
import com.vmturbo.common.protobuf.topology.EntityInfoOuterClass.EntityInfo;

public class AutomatedActionExecutorTest {

    private final ActionTranslator actionTranslator = Mockito.mock(ActionTranslator.class);
    private final Channel channel = Mockito.mock(Channel.class);
    private final ActionTargetResolver resolver = Mockito.mock(ActionTargetResolver.class);
    private final ActionExecutor actionExecutor =
            Mockito.spy(new ActionExecutor(channel, resolver));
    private final ActionStore actionStore = Mockito.mock(ActionStore.class);
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();

    private final AutomatedActionExecutor automatedActionExecutor =
            new AutomatedActionExecutor(actionExecutor, executorService, actionTranslator);

    private final long timeout = 30L;
    private final TimeUnit unit = TimeUnit.SECONDS;

    private final long targetId1 = 49L;
    private final long targetId2 = 51L;

    private final long entityId1 = 111L;
    private final int entityType1 = 1;
    private final long entityId2 = 222L;
    private final int  entityType2 = 2;
    private final long entityId3 = 333L;
    private final int  entityType3 = 3;
    private final long entityId4 = 444L;
    private final int entityType4 = 4;

    private final EntityInfo entityInfo1 = makeEntityInfo(entityId1, targetId1);
    private final EntityInfo entityInfo2 = makeEntityInfo(entityId2, targetId1);
    private final EntityInfo entityInfo3 = makeEntityInfo(entityId3, targetId1);
    private final EntityInfo entityInfo4 = makeEntityInfo(entityId4, targetId2);

    private final Map<Long, Action> actionMap = new HashMap<>();

    private final Set<Long> entitySet = new HashSet<>();
    private final Map<Long, EntityInfo> entityMap = new HashMap<>();

    @Before
    public void setup() throws Exception {
        when(actionStore.getActions()).thenReturn(actionMap);
        Mockito.doReturn(entityMap).when(actionExecutor).getEntityInfo(entitySet);
        Mockito.doCallRealMethod()
                .when(actionExecutor).getEntitiesTarget(any(ActionDTO.Action.class), eq(entityMap));
        Mockito.doNothing().when(actionExecutor).executeSynchronously(anyLong(), any(ActionDTO.Action.class));
        when(actionStore.allowsExecution()).thenReturn(true);
    }

    @After
    public void teardown() {
        executorService.shutdownNow();
    }

    @Test
    public void testOnlyExecuteIfAllowed() {
        when(actionStore.allowsExecution()).thenReturn(false);

        automatedActionExecutor.executeAutomatedFromStore(actionStore);

        Mockito.verifyZeroInteractions(actionTranslator, actionExecutor);
    }

    @Test
    public void testAutomatedExecuteNoActions() {
        automatedActionExecutor.executeAutomatedFromStore(actionStore);

        Mockito.verifyZeroInteractions(actionTranslator, channel);
        Mockito.verify(actionStore).getActions();
        Mockito.verify(actionStore).allowsExecution();
        Mockito.verify(actionExecutor).getEntityInfo(Collections.emptySet());
        Mockito.verifyNoMoreInteractions(actionStore, actionExecutor);
    }

    @Test
    public void testAutomatedExecuteOneNonAutoAction() throws Exception {
        final Action nonAutoAction = Mockito.mock(Action.class);

        actionMap.put(99L, nonAutoAction);
        when(nonAutoAction.getMode()).thenReturn(ActionMode.MANUAL);

        automatedActionExecutor.executeAutomatedFromStore(actionStore);

        Mockito.verify(nonAutoAction, never()).receive(any(ActionEvent.class));
        Mockito.verifyZeroInteractions(actionTranslator, channel);
        Mockito.verify(actionStore).getActions();
        Mockito.verify(actionExecutor).getEntityInfo(Collections.emptySet());
        Mockito.verifyNoMoreInteractions(actionExecutor);
    }

    @Test
    public void testAutomatedExecuteOneUnsupported() {
        final Action unsupportedAction = Mockito.mock(Action.class);


        final ActionDTO.Action unsupportedRec = makeActionRec(99L, ActionInfo.newBuilder().build());
        setUpMocks(unsupportedAction, 99L, unsupportedRec);

        automatedActionExecutor.executeAutomatedFromStore(actionStore);

        final ArgumentCaptor<FailureEvent> captor = ArgumentCaptor.forClass(FailureEvent.class);
        Mockito.verify(unsupportedAction).receive(captor.capture());
        FailureEvent event = captor.getValue();

        String expectedFailure = String.format(AutomatedActionExecutor.UNSUPPORTED_MSG, 99,
                unsupportedRec.getInfo().getActionTypeCase());
        Assert.assertEquals(event.getErrorDescription(), expectedFailure);

        Mockito.verifyZeroInteractions(actionTranslator, channel);
        Mockito.verify(actionStore).getActions();
        Mockito.verify(actionStore).allowsExecution();
        Mockito.verify(actionExecutor).getEntityInfo(Collections.emptySet());
        Mockito.verifyNoMoreInteractions(actionStore, actionExecutor);
    }

    @Test
    public void testAutomatedExecuteOneUntargetedAction() throws TargetResolutionException {
        final Action noTargetAction = Mockito.mock(Action.class);

        final ActionDTO.Action noTargetRec = makeActionRec(99L,
                makeMoveInfo(entityId1, entityType1, entityId2, entityType2, entityId4));
        entityMap.put(entityId1, entityInfo1);
        entityMap.put(entityId2, entityInfo2);
        entityMap.put(entityId4, entityInfo4);
        entitySet.addAll(Arrays.asList(entityId4, entityId2, entityId1));

        setUpMocks(noTargetAction, 99L, noTargetRec);

        automatedActionExecutor.executeAutomatedFromStore(actionStore);

        final ArgumentCaptor<FailureEvent> captor = ArgumentCaptor.forClass(FailureEvent.class);
        Mockito.verify(noTargetAction).receive(captor.capture());
        FailureEvent event = captor.getValue();

        String expectedFailure = String.format(AutomatedActionExecutor.TARGET_RESOLUTION_MSG, 99);
        Assert.assertEquals(event.getErrorDescription(), expectedFailure);

        Mockito.verifyZeroInteractions(actionTranslator, channel);
        Mockito.verify(actionStore).getActions();
        Mockito.verify(actionStore).allowsExecution();
        Mockito.verify(actionExecutor).getEntityInfo(entitySet);
        Mockito.verify(actionExecutor).getEntitiesTarget(noTargetRec, entityMap);
        Mockito.verifyNoMoreInteractions(actionStore, actionExecutor);
    }

    @Test
    public void testAutomatedExecuteFailTranslation() throws Exception {
        final Action failedTranslationAction = Mockito.mock(Action.class);

        final ActionDTO.Action rec = makeActionRec(99L,
                makeMoveInfo(entityId1, entityType1, entityId2, entityType2, entityId3));
        entitySet.addAll(Arrays.asList(entityId1, entityId2, entityId3));
        entityMap.put(entityId1, entityInfo1);
        entityMap.put(entityId2, entityInfo2);
        entityMap.put(entityId3, entityInfo3);
        final ActionTranslation translation = new ActionTranslation(rec);
        translation.setTranslationFailure();

        setUpMocks(failedTranslationAction, 99L, rec);
        when(failedTranslationAction.getActionTranslation()).thenReturn(translation);

        automatedActionExecutor.executeAutomatedFromStore(actionStore);
        executorService.shutdown();
        executorService.awaitTermination(timeout, unit);

        InOrder inOrder = Mockito.inOrder(failedTranslationAction);
        inOrder.verify(failedTranslationAction).receive(isA(AutomaticAcceptanceEvent.class));
        inOrder.verify(failedTranslationAction).receive(isA(BeginExecutionEvent.class));
        final ArgumentCaptor<FailureEvent> failCaptor = ArgumentCaptor.forClass(FailureEvent.class);
        inOrder.verify(failedTranslationAction).receive(failCaptor.capture());

        String expectedFailure = String.format(AutomatedActionExecutor.FAILED_TRANSFORM_MSG, 99);
        Assert.assertEquals(failCaptor.getValue().getErrorDescription(), expectedFailure);

        Mockito.verifyZeroInteractions(channel);
        Mockito.verify(actionStore).getActions();
        Mockito.verify(actionStore).allowsExecution();
        Mockito.verify(actionExecutor).getEntityInfo(entitySet);
        Mockito.verify(actionExecutor).getEntitiesTarget(rec, entityMap);
        Mockito.verify(actionTranslator).translate(failedTranslationAction);
        Mockito.verifyNoMoreInteractions(actionTranslator, actionStore, actionExecutor);
    }

    @Test
    public void testAutomatedExecuteFail() throws Exception {

        final Action failedExecuteAction = Mockito.mock(Action.class);
        final ActionDTO.Action rec =
                makeActionRec(99L,
                    makeMoveInfo(entityId1, entityType1, entityId2, entityType2, entityId3));
        entitySet.addAll(Arrays.asList(entityId1, entityId2, entityId3));
        entityMap.put(entityId1, entityInfo1);
        entityMap.put(entityId2, entityInfo2);
        entityMap.put(entityId3, entityInfo3);
        final ActionTranslation translation = new ActionTranslation(rec);
        translation.setPassthroughTranslationSuccess();
        setUpMocks(failedExecuteAction, 99L, rec);
        when(failedExecuteAction.getActionTranslation()).thenReturn(translation);

        when(resolver.resolveExecutantTarget(any(), any()))
            .thenReturn(targetId1);
        Mockito.doThrow(new ExecutionStartException("EPIC FAIL!!!"))
                .when(actionExecutor).executeSynchronously(targetId1, rec);
        automatedActionExecutor.executeAutomatedFromStore(actionStore);

        executorService.shutdown();
        executorService.awaitTermination(timeout, unit);

        InOrder inOrder = Mockito.inOrder(failedExecuteAction);

        inOrder.verify(failedExecuteAction).receive(isA(AutomaticAcceptanceEvent.class));
        inOrder.verify(failedExecuteAction).receive(isA(BeginExecutionEvent.class));
        final ArgumentCaptor<FailureEvent> failCaptor = ArgumentCaptor.forClass(FailureEvent.class);
        inOrder.verify(failedExecuteAction).receive(failCaptor.capture());

        String expectedFailure = String.format(AutomatedActionExecutor.EXECUTION_START_MSG, 99);
        Assert.assertEquals(failCaptor.getValue().getErrorDescription(), expectedFailure);

        Mockito.verifyZeroInteractions(channel);
        Mockito.verify(actionStore).getActions();
        Mockito.verify(actionStore).allowsExecution();
        Mockito.verify(actionExecutor).getEntityInfo(entitySet);
        Mockito.verify(actionExecutor).getEntitiesTarget(rec, entityMap);
        Mockito.verify(actionTranslator).translate(any(Action.class));
        Mockito.verify(actionExecutor).executeSynchronously(targetId1, rec);
        Mockito.verifyNoMoreInteractions(actionTranslator, actionStore, actionExecutor);
    }

    @Test
    public void testAutomatedExecuteOneAction() throws Exception {

        final Action goodAction = Mockito.mock(Action.class);
        final ActionDTO.Action rec =
                makeActionRec(1L,
                    makeMoveInfo(entityId1, entityType1, entityId2, entityType2, entityId3));
        entitySet.addAll(Arrays.asList(entityId1, entityId2, entityId3));
        entityMap.put(entityId1, entityInfo1);
        entityMap.put(entityId2, entityInfo2);
        entityMap.put(entityId3, entityInfo3);
        final ActionTranslation translation = new ActionTranslation(rec);
        translation.setPassthroughTranslationSuccess();
        setUpMocks(goodAction, 1L, rec);
        when(goodAction.getActionTranslation()).thenReturn(translation);

        when(resolver.resolveExecutantTarget(any(), any()))
            .thenReturn(targetId1);

        automatedActionExecutor.executeAutomatedFromStore(actionStore);

        executorService.shutdown();
        executorService.awaitTermination(timeout, unit);

        InOrder order = Mockito.inOrder(goodAction);
        order.verify(goodAction).receive(isA(AutomaticAcceptanceEvent.class));
        order.verify(goodAction).receive(isA(BeginExecutionEvent.class));

        Mockito.verifyZeroInteractions(channel);
        Mockito.verify(actionStore).getActions();
        Mockito.verify(actionStore).allowsExecution();
        Mockito.verify(actionExecutor).getEntityInfo(entitySet);
        Mockito.verify(actionExecutor).getEntitiesTarget(rec, entityMap);
        Mockito.verify(actionTranslator).translate(any(Action.class));
        Mockito.verify(actionExecutor).executeSynchronously(targetId1, rec);
        Mockito.verifyNoMoreInteractions(actionTranslator, actionStore, actionExecutor);
    }

    @Test
    public void testAutomatedexecuteSynchronously() throws Exception {
        //all together now!

        final Action nonAutoAction = Mockito.mock(Action.class);
        actionMap.put(99L, nonAutoAction);
        when(nonAutoAction.getMode()).thenReturn(ActionMode.MANUAL);

        final Action unsupportedAction = Mockito.mock(Action.class);
        final long unsupportedId = 98;
        final ActionDTO.Action unsupportedRec =
                makeActionRec(unsupportedId, ActionInfo.newBuilder().build());
        setUpMocks(unsupportedAction, unsupportedId, unsupportedRec);

        final Action noTargetAction = Mockito.mock(Action.class);
        final long noTargetId = 97;
        final ActionDTO.Action noTargetRec =
                makeActionRec(noTargetId,
                    makeMoveInfo(entityId1, entityType1, entityId2, entityType2, entityId4));
        setUpMocks(noTargetAction, noTargetId, noTargetRec);

        final Action failedTranslationAction = Mockito.mock(Action.class);
        final long noTransId = 96;
        final ActionDTO.Action noTransRec =
                makeActionRec(noTransId,
                    makeMoveInfo(entityId4, 1, 555L, 1, 666L));
        final ActionTranslation badTrans = new ActionTranslation(noTransRec);
        badTrans.setTranslationFailure();
        setUpMocks(failedTranslationAction, noTransId, noTransRec);
        when(failedTranslationAction.getActionTranslation()).thenReturn(badTrans);

        final Action failedExecuteAction = Mockito.mock(Action.class);
        final long execFailId = 95;
        final ActionDTO.Action execFailRec =
                makeActionRec(execFailId,
                    makeMoveInfo(entityId4, entityType4, 555L, 1, 666L));
        final ActionTranslation execFailTrans = new ActionTranslation(execFailRec);
        execFailTrans.setPassthroughTranslationSuccess();
        setUpMocks(failedExecuteAction, execFailId, execFailRec);
        when(failedExecuteAction.getActionTranslation()).thenReturn(execFailTrans);
        Mockito.doThrow(new ExecutionStartException("EPIC FAIL!!!!"))
                .when(actionExecutor).executeSynchronously(targetId2, execFailRec);

        final Action goodAction = Mockito.mock(Action.class);
        final long goodId = 1L;
        final ActionDTO.Action goodRec =
                makeActionRec(goodId,
                    makeMoveInfo(entityId1, entityType1, entityId2, entityType2, entityId3));
        final ActionTranslation goodTrans = new ActionTranslation(goodRec);
        goodTrans.setPassthroughTranslationSuccess();
        setUpMocks(goodAction, goodId, goodRec);
        when(goodAction.getActionTranslation()).thenReturn(goodTrans);

        entitySet.addAll(Arrays.asList(entityId1, entityId2, entityId3, entityId4, 555L, 666L));
        entityMap.put(entityId1, entityInfo1);
        entityMap.put(entityId2, entityInfo2);
        entityMap.put(entityId3, entityInfo3);
        entityMap.put(entityId4, entityInfo4);
        entityMap.put(555L, makeEntityInfo(555L, targetId2));
        entityMap.put(666L, makeEntityInfo(666L, targetId2));

        when(resolver.resolveExecutantTarget(eq(goodRec), any()))
            .thenReturn(targetId1);
        when(resolver.resolveExecutantTarget(eq(execFailRec), any()))
                .thenReturn(targetId2);

        automatedActionExecutor.executeAutomatedFromStore(actionStore);

        executorService.shutdown();
        executorService.awaitTermination(timeout, unit);

        final ArgumentCaptor<FailureEvent> failureCaptor =
                ArgumentCaptor.forClass(FailureEvent.class);

        Mockito.verify(nonAutoAction, never()).receive(any(ActionEvent.class));

        Mockito.verify(unsupportedAction).receive(failureCaptor.capture());
        Assert.assertEquals(failureCaptor.getValue().getErrorDescription(),
                String.format(AutomatedActionExecutor.UNSUPPORTED_MSG, unsupportedId,
                        unsupportedRec.getInfo().getActionTypeCase()));

        Mockito.verify(noTargetAction).receive(failureCaptor.capture());
        Assert.assertEquals(failureCaptor.getValue().getErrorDescription(),
                String.format(AutomatedActionExecutor.TARGET_RESOLUTION_MSG, noTargetId));

        InOrder noTransOrder = Mockito.inOrder(failedTranslationAction);
        noTransOrder.verify(failedTranslationAction).receive(isA(AutomaticAcceptanceEvent.class));
        noTransOrder.verify(failedTranslationAction).receive(isA(BeginExecutionEvent.class));
        noTransOrder.verify(failedTranslationAction).receive(failureCaptor.capture());
        Assert.assertEquals(failureCaptor.getValue().getErrorDescription(),
                String.format(AutomatedActionExecutor.FAILED_TRANSFORM_MSG, noTransId));

        InOrder failExecOrder = Mockito.inOrder(failedExecuteAction);
        failExecOrder.verify(failedExecuteAction).receive(isA(AutomaticAcceptanceEvent.class));
        failExecOrder.verify(failedExecuteAction).receive(isA(BeginExecutionEvent.class));
        failExecOrder.verify(failedExecuteAction).receive(failureCaptor.capture());
        Assert.assertEquals(failureCaptor.getValue().getErrorDescription(),
                String.format(AutomatedActionExecutor.EXECUTION_START_MSG, execFailId));

        InOrder goodOrder = Mockito.inOrder(goodAction);
        goodOrder.verify(goodAction).receive(isA(AutomaticAcceptanceEvent.class));
        goodOrder.verify(goodAction).receive(isA(BeginExecutionEvent.class));

        Mockito.verifyZeroInteractions(channel);
        Mockito.verify(actionStore).getActions();
        Mockito.verify(actionStore).allowsExecution();
        Mockito.verify(actionExecutor).getEntityInfo(entitySet);
        Mockito.verify(actionExecutor, times(4))
                .getEntitiesTarget(any(ActionDTO.Action.class), anyMap());
        Mockito.verify(actionTranslator, times(3)).translate(any(Action.class));
        Mockito.verify(actionExecutor).executeSynchronously(targetId1, goodRec);
        Mockito.verify(actionExecutor).executeSynchronously(targetId2, execFailRec);
        Mockito.verifyNoMoreInteractions(actionTranslator, actionStore, actionExecutor);

    }

    @Test
    // Verify that only actions which are in READY state and which
    // have the executable flag set are submitted for execution.
    public void testexecuteAutomatedFromStoreExecutableActions() {
        ExecutorService testExecutor = mock(ExecutorService.class);
        ActionExecutor testActionExecutor = mock(ActionExecutor.class);
        final AutomatedActionExecutor automatedActionExecutor =
                new AutomatedActionExecutor(testActionExecutor, testExecutor, actionTranslator);
        long actionId = 1L;
        Callable<Action> mockCallable = new Callable<Action>() {
            @Override
            public Action call() throws Exception {
                return null;
            }
        };
        ActionDTO.Action testRecommendation =
                makeRec(ActionTest.makeMoveInfo(targetId1, entityId1, entityType1, entityId2, entityType2),
                        1, true, SupportLevel.SUPPORTED).build();
        Map<Long, EntityInfo> testEntityMap = new HashMap<>();
        testEntityMap.put(entityId1, entityInfo1);
        testEntityMap.put(entityId2, entityInfo2);
        testEntityMap.put(targetId1, entityInfo3);
        when(testExecutor.submit(mockCallable)).thenReturn(
                new FutureTask<Action>(new Callable<Action>() {
                    @Override
                    public Action call() throws Exception {
                        return null;
                    }
                }));

        Map<Long, Action> testActionMap = new HashMap<>();
        Action testAction  = mock(Action.class);
        testActionMap.put(actionId, testAction);
        when(actionStore.getActions()).thenReturn(testActionMap);
        Mockito.doReturn(testEntityMap).when(testActionExecutor).getEntityInfo(any());
        when(testActionExecutor.getEntitiesTarget(any(), any())).thenReturn(Optional.of(targetId1));
        // Case 1: when the action is executable and is in READY state.
        when(testAction.getId()).thenReturn(actionId);
        when(testAction.getState()).thenReturn(ActionState.READY);
        when(testAction.getMode()).thenReturn(ActionMode.AUTOMATIC);
        when(testAction.getRecommendation()).thenReturn(testRecommendation);
        when(testAction.determineExecutability()).thenReturn(true);
        List<ActionExecutionTask> actionFutures =
                automatedActionExecutor.executeAutomatedFromStore(actionStore);
        assertThat(actionFutures.size(), is(1));

        // Case 2: if action not executable, then no action should be submitted
        when(testAction.getState()).thenReturn(ActionState.QUEUED);
        when(testAction.determineExecutability()).thenReturn(false);
        actionFutures =
                automatedActionExecutor.executeAutomatedFromStore(actionStore);
        assertThat(actionFutures.size(), is(0));
        Mockito.verify(testExecutor, never()).submit(mockCallable);
    }

    private ActionDTO.Action makeActionRec(long actionId, ActionInfo info) {
        return ActionDTO.Action.newBuilder()
            .setId(actionId)
            .setImportance(0)
            .setExecutable(true)
            .setExplanation(Explanation.newBuilder()
                .setMove(Explanation.MoveExplanation.newBuilder()
                    .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                        .setInitialPlacement(
                            ChangeProviderExplanation.InitialPlacement.getDefaultInstance())
                        .build())
                    .build())
                .build())
            .setInfo(info)
            .build();
    }

    private ActionInfo makeMoveInfo(long sourceId, int sourceType,
                                    long destId, int destType,
                                    long targetId) {
        return ActionTest.makeMoveInfo(targetId, sourceId, sourceType,
                    destId, destType).build();
    }

    private EntityInfo makeEntityInfo(long entityId, long targetId) {
        return EntityInfo.newBuilder()
                .setEntityId(entityId).putTargetIdToProbeId(targetId, 2424L).build();
    }

    private void setUpMocks(Action action, long id, ActionDTO.Action rec) {
        actionMap.put(id, action);
        when(action.getMode()).thenReturn(ActionMode.AUTOMATIC);
        when(action.getId()).thenReturn(id);
        when(action.getRecommendation()).thenReturn(rec);
        when(action.determineExecutability()).thenReturn(true);
    }

    private ActionDTO.Action.Builder makeRec(ActionInfo.Builder infoBuilder,
                                             long id,
                                             boolean isExecutable,
                                             final SupportLevel supportLevel) {
        return ActionDTO.Action.newBuilder()
                .setId(id)
                .setImportance(0)
                .setExecutable(isExecutable)
                .setSupportingLevel(supportLevel)
                .setInfo(infoBuilder).setExplanation(Explanation.newBuilder().build());
    }
}
