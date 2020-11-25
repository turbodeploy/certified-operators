package com.vmturbo.action.orchestrator.execution;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import io.grpc.Channel;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mockito;

import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ActionEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.AutomaticAcceptanceEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.BeginExecutionEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.FailureEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.ManualAcceptanceEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.QueuedEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.RollBackToAcceptedEvent;
import com.vmturbo.action.orchestrator.action.ActionSchedule;
import com.vmturbo.action.orchestrator.action.ActionTranslation;
import com.vmturbo.action.orchestrator.action.TestActionBuilder;
import com.vmturbo.action.orchestrator.execution.ActionTargetSelector.ActionTargetInfo;
import com.vmturbo.action.orchestrator.execution.AutomatedActionExecutor.ActionExecutionTask;
import com.vmturbo.action.orchestrator.store.ActionStore;
import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory;
import com.vmturbo.action.orchestrator.workflow.store.WorkflowStore;
import com.vmturbo.action.orchestrator.workflow.store.WorkflowStoreException;
import com.vmturbo.auth.api.licensing.LicenseCheckClient;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation;
import com.vmturbo.common.protobuf.schedule.ScheduleProtoMoles.ScheduleServiceMole;
import com.vmturbo.common.protobuf.schedule.ScheduleServiceGrpc;
import com.vmturbo.common.protobuf.schedule.ScheduleServiceGrpc.ScheduleServiceBlockingStub;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class AutomatedActionExecutorTest {

    private final Channel channel = Mockito.mock(Channel.class);

    private final Clock clock = new MutableFixedClock(1_000_000);

    private final LicenseCheckClient licenseCheckClient = mock(LicenseCheckClient.class);

    private final ActionExecutor actionExecutor =
            Mockito.spy(new ActionExecutor(channel, clock, 1, TimeUnit.HOURS, licenseCheckClient));
    private final ActionTargetSelector actionTargetSelector =
            Mockito.mock(ActionTargetSelector.class);
    private final EntitiesAndSettingsSnapshotFactory entitySettingsCache =
        Mockito.mock(EntitiesAndSettingsSnapshotFactory.class);
    private final ActionStore actionStore = Mockito.mock(ActionStore.class);
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();

    private WorkflowStore workflowStore = Mockito.mock(WorkflowStore.class);

    private ScheduleServiceBlockingStub scheduleService;
    private AutomatedActionExecutor automatedActionExecutor;

    private final ScheduleServiceMole testScheduleService = spy(new ScheduleServiceMole());

    /**
     * Utility gRPC server to mock out gRPC service dependencies.
     */
    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(testScheduleService);

    private final long timeout = 30L;
    private final TimeUnit unit = TimeUnit.SECONDS;

    private final long targetId1 = 49L;
    private final long targetId2 = 51L;

    private final long entityId1 = 111L;
    private final long entityId2 = 222L;
    private final long entityId3 = 333L;
    private final long crossTargetEntity = 444L;

    private final int pmType = EntityType.PHYSICAL_MACHINE.getNumber();

    private final Map<Long, Action> actionMap = new HashMap<>();

    private Optional<WorkflowDTO.Workflow> workflowOpt = Optional.empty();

    @Before
    public void setup() throws Exception {
        scheduleService = ScheduleServiceGrpc.newBlockingStub(grpcServer.getChannel());
        automatedActionExecutor =
                new AutomatedActionExecutor(actionExecutor, executorService, workflowStore,
                        actionTargetSelector, entitySettingsCache, scheduleService);
        when(actionStore.getActions()).thenReturn(actionMap);
        Mockito.doNothing().when(actionExecutor).executeSynchronously(anyLong(),
                any(ActionDTO.Action.class), any(Optional.class));
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
    }

    @Test
    public void testAutomatedExecuteNoActions() {
        automatedActionExecutor.executeAutomatedFromStore(actionStore);

        Mockito.verify(actionStore).getActions();
        Mockito.verify(actionStore).allowsExecution();
        Mockito.verifyNoMoreInteractions(actionStore, actionExecutor);
    }

    @Test
    public void testAutomatedExecuteOneNonAutoAction() throws Exception {
        final Action nonAutoAction = Mockito.mock(Action.class);

        actionMap.put(99L, nonAutoAction);
        when(nonAutoAction.getMode()).thenReturn(ActionMode.MANUAL);
        when(nonAutoAction.getSchedule()).thenReturn(Optional.empty());

        automatedActionExecutor.executeAutomatedFromStore(actionStore);

        Mockito.verify(nonAutoAction, never()).receive(any(ActionEvent.class));
        Mockito.verify(actionStore).getActions();
        Mockito.verifyNoMoreInteractions(actionExecutor);
    }

    @Test
    public void testAutomatedExecuteOneUnsupported() throws Exception {
        final Action unsupportedAction = Mockito.mock(Action.class);


        final ActionDTO.Action unsupportedRec = makeActionRec(99L, ActionInfo.newBuilder().build());
        setUpMocksForAutomaticAction(unsupportedAction, 99L, unsupportedRec);
        when(unsupportedAction.getWorkflowExecutionTarget(workflowStore)).thenReturn(Optional.empty());

        automatedActionExecutor.executeAutomatedFromStore(actionStore);

        final ArgumentCaptor<FailureEvent> captor = ArgumentCaptor.forClass(FailureEvent.class);
        Mockito.verify(unsupportedAction).receive(captor.capture());
        FailureEvent event = captor.getValue();

        String expectedFailure = String.format(AutomatedActionExecutor.TARGET_RESOLUTION_MSG, 99);
        Assert.assertEquals(expectedFailure, event.getErrorDescription());

        Mockito.verify(actionStore).getActions();
        Mockito.verify(actionStore).allowsExecution();
        Mockito.verifyNoMoreInteractions(actionStore, actionExecutor);
    }

    @Test
    public void testAutomatedExecuteOneCrossTargetMove() throws Exception {
        final Action crossTargetAction = Mockito.mock(Action.class);

        final long actionId = 99;
        final ActionDTO.Action crossTargetRec = makeActionRec(actionId,
                makeMoveInfo(entityId1, pmType, crossTargetEntity, pmType, entityId2));

        setUpMocksForAutomaticAction(crossTargetAction, actionId, crossTargetRec);
        // Map this action to target #1
        when(actionTargetSelector.getTargetsForActions(any(), any(), any()))
            .thenReturn(Collections.singletonMap(actionId, actionTargetInfo(targetId1)));

        final ActionTranslation translation = new ActionTranslation(crossTargetRec);
        translation.setPassthroughTranslationSuccess();
        when(crossTargetAction.getActionTranslation()).thenReturn(translation);
        when(crossTargetAction.getWorkflow(workflowStore, crossTargetAction.getState())).thenReturn(
                Optional.empty());
        when(crossTargetAction.getWorkflowExecutionTarget(workflowStore)).thenReturn(
                Optional.empty());

        automatedActionExecutor.executeAutomatedFromStore(actionStore);

        executorService.shutdown();
        executorService.awaitTermination(timeout, unit);

        InOrder order = Mockito.inOrder(crossTargetAction);
        order.verify(crossTargetAction).receive(isA(AutomaticAcceptanceEvent.class));
        order.verify(crossTargetAction).receive(isA(BeginExecutionEvent.class));

        Mockito.verifyZeroInteractions(channel);
        Mockito.verify(actionStore).getActions();
        Mockito.verify(actionStore).allowsExecution();
        Mockito.verify(actionTargetSelector).getTargetsForActions(any(), any(), any());
        Mockito.verify(actionExecutor).executeSynchronously(targetId1, crossTargetRec, workflowOpt);
        Mockito.verifyNoMoreInteractions(actionStore, actionExecutor, actionTargetSelector);
    }

    private ActionTargetInfo actionTargetInfo(final long targetId) {
        return ImmutableActionTargetInfo.builder()
            .targetId(targetId)
            .supportingLevel(SupportLevel.SUPPORTED)
            .build();
    }

    @Test
    public void testAutomatedExecuteFailTranslation() throws Exception {
        final Action failedTranslationAction = Mockito.mock(Action.class);

        final ActionDTO.Action rec = makeActionRec(99L,
                makeMoveInfo(entityId1, pmType, entityId2, pmType, entityId3));

        final ActionTranslation translation = new ActionTranslation(rec);
        translation.setTranslationFailure();

        setUpMocksForAutomaticAction(failedTranslationAction, 99L, rec);
        // Map this action to target #1
        when(actionTargetSelector.getTargetsForActions(any(), any(), any()))
            .thenReturn(Collections.singletonMap(99L, actionTargetInfo(targetId1)));
        when(failedTranslationAction.getActionTranslation()).thenReturn(translation);
        when(failedTranslationAction.getWorkflowExecutionTarget(workflowStore)).thenReturn(Optional.empty());

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
        Mockito.verify(actionTargetSelector).getTargetsForActions(any(), any(), any());
        Mockito.verifyNoMoreInteractions(actionStore, actionExecutor, actionTargetSelector);
    }

    /**
     * Tests that if action from action queue has non active execution schedule then we miss
     * executing of this action.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testExecutingActionsFromActionQueue() throws Exception {
        final Action scheduleAction1 = Mockito.mock(Action.class);

        final ActionSchedule actionSchedule1 = mock(ActionSchedule.class);
        final long action1Id = 1;
        final ActionDTO.Action recommendation1 = makeActionRec(action1Id,
                makeMoveInfo(entityId1, pmType, entityId2, pmType, entityId3));
        final ActionTranslation translation1 = new ActionTranslation(recommendation1);
        translation1.setPassthroughTranslationSuccess();
        setUpMocksForManuallyAcceptedAction(scheduleAction1, action1Id, recommendation1,
                actionSchedule1, true);

        final Action scheduleAction2 = Mockito.mock(Action.class);
        final ActionSchedule actionSchedule2 = mock(ActionSchedule.class);
        final long action2Id = 2;
        final ActionDTO.Action recommendation2 = makeActionRec(action2Id,
                makeMoveInfo(entityId1, pmType, entityId2, pmType, entityId3));
        final ActionTranslation translation2 = new ActionTranslation(recommendation2);
        translation2.setPassthroughTranslationSuccess();
        setUpMocksForManuallyAcceptedAction(scheduleAction2, action2Id, recommendation2,
                actionSchedule2, true);

        final Action actionWithoutSchedule = Mockito.mock(Action.class);
        final long actionWithoutScheduleId = 3;
        final ActionDTO.Action recommendation3 = makeActionRec(actionWithoutScheduleId,
                makeMoveInfo(entityId1, pmType, entityId2, pmType, entityId3));
        final ActionTranslation translation3 = new ActionTranslation(recommendation3);
        translation3.setPassthroughTranslationSuccess();
        setUpMocksForAutomaticAction(actionWithoutSchedule, actionWithoutScheduleId,
                recommendation3);

        final Map<Long, ActionTargetInfo> actionToTargetMap = new HashMap<>();
        actionToTargetMap.put(action1Id, actionTargetInfo(targetId1));
        actionToTargetMap.put(action2Id, actionTargetInfo(targetId1));
        actionToTargetMap.put(actionWithoutScheduleId, actionTargetInfo(targetId1));

        when(actionTargetSelector.getTargetsForActions(any(), any(), any())).thenReturn(actionToTargetMap);
        when(scheduleAction2.getActionTranslation()).thenReturn(translation1);
        when(scheduleAction1.getActionTranslation()).thenReturn(translation2);
        when(actionWithoutSchedule.getActionTranslation()).thenReturn(translation3);
        when(scheduleAction2.getWorkflowExecutionTarget(workflowStore)).thenReturn(Optional.empty());
        when(scheduleAction1.getWorkflowExecutionTarget(workflowStore)).thenReturn(Optional.empty());
        when(actionWithoutSchedule.getWorkflowExecutionTarget(workflowStore)).thenReturn(Optional.empty());

        final MockExecutorService mockExecutorService = new MockExecutorService();
        final AutomatedActionExecutor automatedActionExecutor =
                new AutomatedActionExecutor(actionExecutor, mockExecutorService, workflowStore,
                        actionTargetSelector, entitySettingsCache, scheduleService);

        automatedActionExecutor.executeAutomatedFromStore(actionStore);

        Mockito.when(actionSchedule1.isActiveScheduleNow()).thenReturn(true);
        Mockito.when(actionSchedule2.isActiveScheduleNow()).thenReturn(false);
        mockExecutorService.executeTasks();

        // checks that action with active execution window and action without schedule
        // start executing and action with non active schedule doesn't start executing
        final InOrder inOrder1 = Mockito.inOrder(scheduleAction1);
        inOrder1.verify(scheduleAction1).receive(isA(QueuedEvent.class));
        inOrder1.verify(scheduleAction1).receive(isA(BeginExecutionEvent.class));

        final InOrder inOrder2 = Mockito.inOrder(scheduleAction2);
        inOrder2.verify(scheduleAction2).receive(isA(QueuedEvent.class));
        inOrder2.verify(scheduleAction2, Mockito.never()).receive(isA(BeginExecutionEvent.class));
        inOrder2.verify(scheduleAction2).receive(isA(RollBackToAcceptedEvent.class));

        final InOrder inOrder3 = Mockito.inOrder(actionWithoutSchedule);
        inOrder3.verify(actionWithoutSchedule).receive(isA(AutomaticAcceptanceEvent.class));
        inOrder3.verify(actionWithoutSchedule).receive(isA(BeginExecutionEvent.class));
    }

    @Test
    public void testAutomatedExecuteFail() throws Exception {
        final Action failedExecuteAction = Mockito.mock(Action.class);
        final ActionDTO.Action rec =
                makeActionRec(99L,
                    makeMoveInfo(entityId1, pmType, entityId2, pmType, entityId3));

        final ActionTranslation translation = new ActionTranslation(rec);
        translation.setPassthroughTranslationSuccess();
        setUpMocksForAutomaticAction(failedExecuteAction, 99L, rec);
        // Map this action to target #1
        when(actionTargetSelector.getTargetsForActions(any(), any(), any()))
            .thenReturn(Collections.singletonMap(99L, actionTargetInfo(targetId1)));
        when(failedExecuteAction.getActionTranslation()).thenReturn(translation);
        when(failedExecuteAction.getWorkflow(workflowStore,
                failedExecuteAction.getState())).thenReturn(Optional.empty());
        when(failedExecuteAction.getWorkflowExecutionTarget(workflowStore)).thenReturn(Optional.empty());
        Mockito.doThrow(new ExecutionStartException("EPIC FAIL!!!"))
                .when(actionExecutor).executeSynchronously(targetId1, rec, workflowOpt);
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
        Mockito.verify(actionTargetSelector).getTargetsForActions(any(), any(), any());
        Mockito.verify(actionExecutor).executeSynchronously(targetId1, rec, workflowOpt);
        Mockito.verifyNoMoreInteractions(actionStore, actionExecutor, actionTargetSelector);
    }

    @Test
    public void testAutomatedExecuteOneAction() throws Exception {

        final Action goodAction = Mockito.mock(Action.class);
        final long actionId = 1;
        final ActionDTO.Action rec =
                makeActionRec(actionId,
                    makeMoveInfo(entityId1, pmType, entityId2, pmType, entityId3));

        final ActionTranslation translation = new ActionTranslation(rec);
        translation.setPassthroughTranslationSuccess();
        setUpMocksForAutomaticAction(goodAction, actionId, rec);
        // Map this action to target #1
        when(actionTargetSelector.getTargetsForActions(any(), any(), any()))
            .thenReturn(Collections.singletonMap(actionId, actionTargetInfo(targetId1)));
        when(goodAction.getActionTranslation()).thenReturn(translation);
        when(goodAction.getWorkflow(workflowStore, goodAction.getState())).thenReturn(Optional.empty());
        when(goodAction.getWorkflowExecutionTarget(workflowStore)).thenReturn(Optional.empty());


        automatedActionExecutor.executeAutomatedFromStore(actionStore);

        executorService.shutdown();
        executorService.awaitTermination(timeout, unit);

        InOrder order = Mockito.inOrder(goodAction);
        order.verify(goodAction).receive(isA(AutomaticAcceptanceEvent.class));
        order.verify(goodAction).receive(isA(BeginExecutionEvent.class));

        Mockito.verifyZeroInteractions(channel);
        Mockito.verify(actionStore).getActions();
        Mockito.verify(actionStore).allowsExecution();
        Mockito.verify(actionTargetSelector).getTargetsForActions(any(), any(), any());
        Mockito.verify(actionExecutor).executeSynchronously(targetId1, rec, workflowOpt);
        Mockito.verifyNoMoreInteractions(actionStore, actionExecutor, actionTargetSelector);
    }

    @Test
    public void testAutomatedexecuteSynchronously() throws Exception {
        //all together now!

        final Action nonAutoAction = Mockito.mock(Action.class);
        actionMap.put(99L, nonAutoAction);
        when(nonAutoAction.getMode()).thenReturn(ActionMode.MANUAL);
        when(nonAutoAction.getSchedule()).thenReturn(Optional.empty());
        when(nonAutoAction.getWorkflow(workflowStore, nonAutoAction.getState())).thenReturn(
                Optional.empty());
        when(nonAutoAction.getWorkflowExecutionTarget(workflowStore)).thenReturn(Optional.empty());

        final Action unsupportedAction = Mockito.mock(Action.class);
        final long unsupportedId = 98;
        final ActionDTO.Action unsupportedRec =
                makeActionRec(unsupportedId, ActionInfo.newBuilder().build());
        when(unsupportedAction.getWorkflowExecutionTarget(workflowStore)).thenReturn(Optional.empty());
        setUpMocksForAutomaticAction(unsupportedAction, unsupportedId, unsupportedRec);

        final Action crossTargetAction = Mockito.mock(Action.class);
        final long crossTargetId = 97;
        final ActionDTO.Action crossTargetRec =
                makeActionRec(crossTargetId,
                    makeMoveInfo(entityId1, pmType, crossTargetEntity, pmType, entityId2));
        when(crossTargetAction.getWorkflowExecutionTarget(workflowStore)).thenReturn(Optional.empty());
        setUpMocksForAutomaticAction(crossTargetAction, crossTargetId, crossTargetRec);

        final Action failedTranslationAction = Mockito.mock(Action.class);
        final long noTransId = 96;
        final ActionDTO.Action noTransRec =
                makeActionRec(noTransId,
                    makeMoveInfo(crossTargetEntity, 1, 555L, 1, 666L));
        final ActionTranslation badTrans = new ActionTranslation(noTransRec);
        when(failedTranslationAction.getWorkflowExecutionTarget(workflowStore)).thenReturn(Optional.empty());
        badTrans.setTranslationFailure();
        setUpMocksForAutomaticAction(failedTranslationAction, noTransId, noTransRec);
        when(failedTranslationAction.getActionTranslation()).thenReturn(badTrans);

        final Action failedExecuteAction = Mockito.mock(Action.class);
        final long execFailId = 95;
        final ActionDTO.Action execFailRec =
                makeActionRec(
                        execFailId,
                        makeMoveInfo(crossTargetEntity, pmType, 555L, pmType, 666L));
        final ActionTranslation execFailTrans = new ActionTranslation(execFailRec);
        when(failedExecuteAction.getWorkflowExecutionTarget(workflowStore)).thenReturn(Optional.empty());
        execFailTrans.setPassthroughTranslationSuccess();
        setUpMocksForAutomaticAction(failedExecuteAction, execFailId, execFailRec);
        when(failedExecuteAction.getActionTranslation()).thenReturn(execFailTrans);
        Mockito.doThrow(new ExecutionStartException("EPIC FAIL!!!!"))
                .when(actionExecutor).executeSynchronously(targetId2, execFailRec, workflowOpt);

        final Action goodAction = Mockito.mock(Action.class);
        final long goodId = 1L;
        final ActionDTO.Action goodRec =
                makeActionRec(goodId,
                    makeMoveInfo(entityId1, pmType, entityId2, pmType, entityId3));
        final ActionTranslation goodTrans = new ActionTranslation(goodRec);
        goodTrans.setPassthroughTranslationSuccess();
        setUpMocksForAutomaticAction(goodAction, goodId, goodRec);
        when(goodAction.getActionTranslation()).thenReturn(goodTrans);
        when(goodAction.getWorkflow(workflowStore, goodAction.getState())).thenReturn(
                Optional.empty());
        when(goodAction.getWorkflowExecutionTarget(workflowStore)).thenReturn(Optional.empty());


        // Map the actions to targets
        Map<Long, ActionTargetInfo> actionToTargetMap = new HashMap<>();
        actionToTargetMap.put(crossTargetAction.getId(), actionTargetInfo(targetId1));
        actionToTargetMap.put(failedExecuteAction.getId(), actionTargetInfo(targetId2));
        actionToTargetMap.put(failedTranslationAction.getId(), actionTargetInfo(targetId1));
        actionToTargetMap.put(goodAction.getId(), actionTargetInfo(targetId1));
        when(actionTargetSelector.getTargetsForActions(any(), any(), any())).thenReturn(actionToTargetMap);

        automatedActionExecutor.executeAutomatedFromStore(actionStore);

        executorService.shutdown();
        executorService.awaitTermination(timeout, unit);

        final ArgumentCaptor<FailureEvent> failureCaptor =
                ArgumentCaptor.forClass(FailureEvent.class);

        Mockito.verify(nonAutoAction, never()).receive(any(ActionEvent.class));

        Mockito.verify(unsupportedAction).receive(failureCaptor.capture());
        Assert.assertEquals(failureCaptor.getValue().getErrorDescription(),
                String.format(AutomatedActionExecutor.TARGET_RESOLUTION_MSG, unsupportedId));

        InOrder crossOrder = Mockito.inOrder(crossTargetAction);
        crossOrder.verify(crossTargetAction).receive(isA(AutomaticAcceptanceEvent.class));
        crossOrder.verify(crossTargetAction).receive(isA(BeginExecutionEvent.class));

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
        // This mapping happens once, for all actions
        Mockito.verify(actionTargetSelector).getTargetsForActions(any(), any(), any());
        Mockito.verify(actionExecutor).executeSynchronously(targetId1, goodRec, workflowOpt);
        Mockito.verify(actionExecutor).executeSynchronously(targetId2, execFailRec, workflowOpt);
        Mockito.verifyNoMoreInteractions(actionStore, actionExecutor, actionTargetSelector);

    }

    @Test
    // Verify that only actions which are in READY state and which
    // have the executable flag set are submitted for execution.
    public void testexecuteAutomatedFromStoreExecutableActions() throws Exception {
        ExecutorService testExecutor = mock(ExecutorService.class);
        ActionExecutor testActionExecutor = mock(ActionExecutor.class);
        final AutomatedActionExecutor automatedActionExecutor =
                new AutomatedActionExecutor(testActionExecutor, testExecutor,
                        workflowStore, actionTargetSelector, entitySettingsCache, scheduleService);
        long actionId = 1L;
        Callable<Action> mockCallable = new Callable<Action>() {
            @Override
            public Action call() throws Exception {
                return null;
            }
        };
        ActionDTO.Action testRecommendation = makeRec(
                TestActionBuilder.makeMoveInfo(targetId1, entityId1, pmType, entityId2, pmType),
                1, true, SupportLevel.SUPPORTED).build();

        when(testExecutor.submit(mockCallable)).thenReturn(
                new FutureTask<Action>(new Callable<Action>() {
                    @Override
                    public Action call() throws Exception {
                        return null;
                    }
                }));

        Map<Long, Action> testActionMap = new HashMap<>();
        Action testAction  = mock(Action.class);
        when(testAction.getWorkflowExecutionTarget(workflowStore)).thenReturn(Optional.empty());
        testActionMap.put(actionId, testAction);
        when(actionStore.getActions()).thenReturn(testActionMap);

        // Map the action (if present) to targetId1
        when(actionTargetSelector.getTargetsForActions(any(), any(), any()))
            .thenAnswer(invocation -> {
                Stream<ActionDTO.Action> actions = invocation.getArgumentAt(0, Stream.class);
                if (actions.count() > 0) {
                    return Collections.singletonMap(testAction.getId(), actionTargetInfo(targetId1));
                } else {
                    return Collections.emptyMap();
                }
            });

        // Case 1: when the action is executable and is in READY state.
        when(testAction.getId()).thenReturn(actionId);
        when(testAction.getState()).thenReturn(ActionState.READY);
        when(testAction.getMode()).thenReturn(ActionMode.AUTOMATIC);
        when(testAction.getSchedule()).thenReturn(Optional.empty());
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

    /**
     * Test that automatically start execution for manually accepted action with active execution
     * schedule.
     *
     * @throws Exception if something goes wrong
     */
    @Test
    public void testExecutionOfManuallyAccepted() throws Exception {
        final Action manualAcceptedAction = Mockito.mock(Action.class);
        final long actionId = 1;
        final long scheduleId = 2;
        final ActionDTO.Action recommendation =
                makeActionRec(actionId, makeMoveInfo(entityId1, pmType, entityId2, pmType, entityId3));
        final ActionSchedule actionSchedule = mock(ActionSchedule.class);

        final ActionTranslation translation = new ActionTranslation(recommendation);
        translation.setPassthroughTranslationSuccess();

        setUpMocksForManuallyAcceptedAction(manualAcceptedAction, actionId, recommendation,
                actionSchedule, true);

        // Map this action to target #1
        when(actionTargetSelector.getTargetsForActions(any(), any(), any())).thenReturn(Collections.singletonMap(actionId, actionTargetInfo(targetId1)));
        when(manualAcceptedAction.getActionTranslation()).thenReturn(translation);
        when(manualAcceptedAction.getWorkflowExecutionTarget(workflowStore)).thenReturn(Optional.empty());
        when(actionSchedule.getScheduleId()).thenReturn(scheduleId);
        when(actionSchedule.isActiveScheduleNow()).thenReturn(true);

        automatedActionExecutor.executeAutomatedFromStore(actionStore);

        executorService.shutdown();
        executorService.awaitTermination(timeout, unit);

        InOrder order = Mockito.inOrder(manualAcceptedAction);
        order.verify(manualAcceptedAction).receive(isA(QueuedEvent.class));
        order.verify(manualAcceptedAction).receive(isA(BeginExecutionEvent.class));

        Mockito.verifyZeroInteractions(channel);
        Mockito.verify(actionStore).getActions();
        Mockito.verify(actionStore).allowsExecution();
        Mockito.verify(actionTargetSelector).getTargetsForActions(any(), any(), any());
        Mockito.verify(actionExecutor).executeSynchronously(targetId1, recommendation, workflowOpt);
        Mockito.verifyNoMoreInteractions(actionStore, actionExecutor, actionTargetSelector);
    }

    /**
     * Test that we don't start execution for manually accepted action if execution schedule
     * window is not active.
     *
     * @throws Exception if something goes wrong
     */
    @Test
    public void testAwaitingExecutionManuallyAcceptedAction() throws Exception {
        final Action manualAcceptedAction = Mockito.mock(Action.class);
        final ActionSchedule actionSchedule = mock(ActionSchedule.class);
        final long actionId = 1;
        final ActionDTO.Action recommendation =
                makeActionRec(actionId, makeMoveInfo(entityId1, pmType, entityId2, pmType, entityId3));
        when(manualAcceptedAction.getWorkflowExecutionTarget(workflowStore)).thenReturn(Optional.empty());

        final ActionTranslation translation = new ActionTranslation(recommendation);
        translation.setPassthroughTranslationSuccess();
        setUpMocksForManuallyAcceptedAction(manualAcceptedAction, actionId, recommendation,
                actionSchedule, false);

        automatedActionExecutor.executeAutomatedFromStore(actionStore);

        executorService.shutdown();
        executorService.awaitTermination(timeout, unit);

        Mockito.verify(manualAcceptedAction, Mockito.never()).receive(isA(ManualAcceptanceEvent.class));
    }

    private ActionDTO.Action makeActionRec(long actionId, ActionInfo info) {
        return ActionDTO.Action.newBuilder()
            .setId(actionId)
            .setDeprecatedImportance(0)
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
        return TestActionBuilder.makeMoveInfo(targetId, sourceId, sourceType,
                    destId, destType).build();
    }

    private void setUpMocksForAutomaticAction(@Nonnull Action action, long id,
            @Nonnull ActionDTO.Action recommendation) throws WorkflowStoreException {
        setUpMocks(action, id, recommendation);
        when(action.getMode()).thenReturn(ActionMode.AUTOMATIC);
        when(action.getSchedule()).thenReturn(Optional.empty());
    }

    private void setUpMocksForManuallyAcceptedAction(@Nonnull Action action, long id,
            @Nonnull ActionDTO.Action recommendation, @Nonnull ActionSchedule actionSchedule,
            boolean isActiveExecutionSchedule) throws WorkflowStoreException {
        setUpMocks(action, id, recommendation);
        when(action.getMode()).thenReturn(ActionMode.MANUAL);

        when(actionSchedule.isActiveSchedule()).thenReturn(isActiveExecutionSchedule);
        when(actionSchedule.getAcceptingUser()).thenReturn("administrator");

        when(action.getSchedule()).thenReturn(Optional.of(actionSchedule));
    }

    private void setUpMocks(Action action, long id, ActionDTO.Action rec)
            throws WorkflowStoreException {
        actionMap.put(id, action);
        when(action.getId()).thenReturn(id);
        when(action.getRecommendation()).thenReturn(rec);
        when(action.determineExecutability()).thenReturn(true);
        when(action.getWorkflow(workflowStore, action.getState())).thenReturn(Optional.empty());
    }

    private ActionDTO.Action.Builder makeRec(ActionInfo.Builder infoBuilder,
                                             long id,
                                             boolean isExecutable,
                                             final SupportLevel supportLevel) {
        return ActionDTO.Action.newBuilder()
                .setId(id)
                .setDeprecatedImportance(0)
                .setExecutable(isExecutable)
                .setSupportingLevel(supportLevel)
                .setInfo(infoBuilder).setExplanation(Explanation.newBuilder().build());
    }
}
