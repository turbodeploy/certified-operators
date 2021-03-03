package com.vmturbo.topology.processor.actions;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nonnull;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.vmturbo.action.orchestrator.dto.ActionMessages.ActionEvent;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.topology.ActionExecution.ExecuteActionRequest;
import com.vmturbo.platform.common.dto.ActionExecution.ActionErrorDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionEventDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionExecutionDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionResponseState;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionErrorsResponse;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.actions.data.context.ActionExecutionContextFactory;
import com.vmturbo.topology.processor.actions.data.context.ContextCreationException;
import com.vmturbo.topology.processor.actions.data.context.ResizeContext;
import com.vmturbo.topology.processor.api.util.ImmutableThinProbeInfo;
import com.vmturbo.topology.processor.api.util.ImmutableThinTargetInfo;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;
import com.vmturbo.topology.processor.operation.IOperationManager;
import com.vmturbo.topology.processor.operation.IOperationManager.OperationCallback;

/**
 * Test class for {@link TargetActionAuditService}.
 */
public class TargetActionAuditServiceTest {

    private static final long SERVICENOW_TARGET_ID = 1L;
    private static final long KAFKA_TARGET_ID = 2L;

    private static final ImmutableThinTargetInfo KAFKA_TARGET = ImmutableThinTargetInfo.builder()
            .oid(KAFKA_TARGET_ID)
            .displayName("Kafka")
            .isHidden(false)
            .probeInfo(ImmutableThinProbeInfo.builder()
                    .type("Kafka")
                    .oid(KAFKA_TARGET_ID)
                    .category(ProbeCategory.ORCHESTRATOR.getCategory())
                    .uiCategory(ProbeCategory.ORCHESTRATOR.getCategory())
                    .build())
            .build();

    private static final ImmutableThinTargetInfo SERVICENOW_TARGET = ImmutableThinTargetInfo.builder()
            .oid(SERVICENOW_TARGET_ID)
            .displayName("ServiceNow")
            .isHidden(false)
            .probeInfo(ImmutableThinProbeInfo.builder()
                    .type(SDKProbeType.SERVICENOW.getProbeType())
                    .oid(SERVICENOW_TARGET_ID)
                    .category(ProbeCategory.ORCHESTRATOR.getCategory())
                    .uiCategory(ProbeCategory.ORCHESTRATOR.getCategory())
                    .build())
            .build();
    private static final long ACTION_ID = 1L;
    private static final Runnable EMPTY_RUNNABLE = () -> { };

    @Mock
    private ThinTargetCache thinTargetCache;
    @Mock
    private IOperationManager operationManager;
    @Mock
    private ActionExecutionContextFactory contextFactory;
    private final MockScheduledService executorService = new MockScheduledService();

    @Captor
    private ArgumentCaptor<OperationCallback<ActionErrorsResponse>> operationCallbackCaptor;

    @Captor
    private ArgumentCaptor<Collection<ActionEventDTO>> auditedActionsCaptor;

    /**
     * Rule for checking expected exceptions.
     */
    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    /**
     * Initialize test configuration.
     */
    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);
        Mockito.when(thinTargetCache.getTargetInfo(SERVICENOW_TARGET_ID))
                .thenReturn(Optional.of(SERVICENOW_TARGET));
        Mockito.when(thinTargetCache.getTargetInfo(KAFKA_TARGET_ID))
                .thenReturn(Optional.of(KAFKA_TARGET));
    }

    /**
     * Tests logic that if we send action event for audit and it wasn't received by external
     * audit system (e.g. external kafka) then we return it back to the queue in order to resend
     * it again. We need to do it because in AO we have bookkeeping mechanism which allows to send
     * action event for audit only once.
     *
     * @throws Exception if something goes wrong
     */
    @Test
    public void testAuditLogic() throws Exception {
        // ARRANGE
        final TargetActionAuditService targetActionAuditService =
                new TargetActionAuditService(KAFKA_TARGET_ID, operationManager, contextFactory,
                        executorService, 0, 1, new AtomicBoolean(true), thinTargetCache);
        final ActionSpec actionSpec =
                ActionSpec.newBuilder().setRecommendation(createResizeAction()).build();
        final ExecuteActionRequest onGenAuditRequest = ExecuteActionRequest.newBuilder()
                .setActionId(ACTION_ID)
                .setTargetId(KAFKA_TARGET_ID)
                .setActionSpec(actionSpec)
                .setActionType(ActionType.RESIZE)
                .build();
        // for CLEARED event we send only minimal required information about action
        final ExecuteActionRequest clearedActionRequest = ExecuteActionRequest.newBuilder()
                .setActionId(ACTION_ID)
                .setTargetId(KAFKA_TARGET_ID)
                .setActionType(ActionType.NONE)
                .build();
        final ActionEvent onGenEvent =
                createAuditEvent(onGenAuditRequest, ActionResponseState.PENDING_ACCEPT,
                        ActionResponseState.PENDING_ACCEPT);
        final ActionEvent clearedEvent =
                createAuditEvent(clearedActionRequest, ActionResponseState.PENDING_ACCEPT,
                        ActionResponseState.CLEARED);
        final ActionErrorsResponse succeedResponse = ActionErrorsResponse.getDefaultInstance();
        final ActionErrorsResponse failedResponse = ActionErrorsResponse.newBuilder()
                .addErrors(ActionErrorDTO.newBuilder()
                        .setActionOid(ACTION_ID)
                        .setMessage("Audit target is not available")
                        .build())
                .build();
        arrangeStuffForCreatingActionExecutionDTO(onGenAuditRequest);
        arrangeStuffForCreatingActionExecutionDTO(clearedActionRequest);

        // ACT 1 (successfully send ON_GEN event)
        targetActionAuditService.onNewEvent(onGenEvent, EMPTY_RUNNABLE);
        executorService.executeTasks();
        Mockito.verify(operationManager)
                .sendActionAuditEvents(Mockito.eq(KAFKA_TARGET_ID),
                        Mockito.anyCollectionOf(ActionEventDTO.class),
                        operationCallbackCaptor.capture());
        final OperationCallback<ActionErrorsResponse> onGenCallback =
                operationCallbackCaptor.getValue();
        onGenCallback.onSuccess(succeedResponse);

        // ASSERT 1 (successfully send ON_GEN event)
        // approves that action was successfully delivered to external audit system
        Mockito.verify(operationManager, Mockito.never())
                .sendActionAuditEvents(Mockito.eq(SERVICENOW_TARGET_ID),
                        auditedActionsCaptor.capture(), operationCallbackCaptor.capture());

        // clear stuff
        Mockito.reset(operationManager);
        executorService.removeAllTasks();

        // ACT 2 (failed to send CLEARED event)
        targetActionAuditService.onNewEvent(clearedEvent, EMPTY_RUNNABLE);
        executorService.executeTasks();
        Mockito.verify(operationManager)
                .sendActionAuditEvents(Mockito.eq(KAFKA_TARGET_ID),
                        Mockito.anyCollectionOf(ActionEventDTO.class),
                        operationCallbackCaptor.capture());
        Mockito.reset(operationManager);
        final OperationCallback<ActionErrorsResponse> clearedCallback =
                operationCallbackCaptor.getValue();
        clearedCallback.onSuccess(failedResponse);
        executorService.executeTasks();

        // ASSERT 2 (failed to send CLEARED event)
        // approves that if audit response from probe has errors then we returned sent actions to
        // the queue in order to resend them again (resend audit events automatically after
        // receiving response with errors - TargetActionAuditService.ActionAuditCallback.onSuccess)
        Mockito.verify(operationManager)
                .sendActionAuditEvents(Mockito.eq(KAFKA_TARGET_ID), auditedActionsCaptor.capture(),
                        operationCallbackCaptor.capture());
        final Collection<ActionEventDTO> actionForAudit = auditedActionsCaptor.getValue();
        Assert.assertEquals(1, actionForAudit.size());
        final ActionEventDTO auditedAction = actionForAudit.iterator().next();
        Assert.assertEquals(ACTION_ID, auditedAction.getAction().getActionOid());
        Assert.assertEquals(ActionResponseState.CLEARED, auditedAction.getNewState());
        Assert.assertEquals(ActionResponseState.PENDING_ACCEPT, auditedAction.getOldState());
    }

    /**
     * Tests specific audit logic for ServiceNow.
     * In general for audit feature if we send action event for audit and it wasn't received by
     * external audit system (e.g. external kafka) then we return it back to the queue in order to
     * resend it again. We need to do it because in AO we have bookkeeping mechanism which allows to send
     * action event for audit only once.
     * But for ServiceNow we need to send action event every market cycle and all bookkeeping
     * logic we have in ServiceNow app.
     * We need to update/remove this test after finishing TODO OM-64606
     * (where we plan to change the ServiceNow app and having  generic audit logic for orchestration
     * targets)
     *
     * @throws Exception if something goes wrong
     */
    @Test
    public void testServiceNowSpecificLogic() throws Exception {
        // ARRANGE
        final TargetActionAuditService targetActionAuditService =
                new TargetActionAuditService(SERVICENOW_TARGET_ID, operationManager, contextFactory,
                        executorService, 0, 1, new AtomicBoolean(true), thinTargetCache);
        final ActionSpec actionSpec =
                ActionSpec.newBuilder().setRecommendation(createResizeAction()).build();
        final ExecuteActionRequest onGenAuditRequest = ExecuteActionRequest.newBuilder()
                .setActionId(ACTION_ID)
                .setTargetId(SERVICENOW_TARGET_ID)
                .setActionSpec(actionSpec)
                .setActionType(ActionType.RESIZE)
                .build();
        final ActionEvent onGenEvent =
                createAuditEvent(onGenAuditRequest, ActionResponseState.PENDING_ACCEPT,
                        ActionResponseState.PENDING_ACCEPT);
        final ActionErrorsResponse failedResponse = ActionErrorsResponse.newBuilder()
                .addErrors(ActionErrorDTO.newBuilder()
                        .setActionOid(ACTION_ID)
                        .setMessage("Audit target is not available")
                        .build())
                .build();
        arrangeStuffForCreatingActionExecutionDTO(onGenAuditRequest);

        // ACT 1 (failed to send ON_GEN event to ServiceNow)
        targetActionAuditService.onNewEvent(onGenEvent, EMPTY_RUNNABLE);
        executorService.executeTasks();
        Mockito.verify(operationManager)
                .sendActionAuditEvents(Mockito.eq(SERVICENOW_TARGET_ID),
                        Mockito.anyCollectionOf(ActionEventDTO.class),
                        operationCallbackCaptor.capture());
        Mockito.reset(operationManager);
        final OperationCallback<ActionErrorsResponse> onGenCallback =
                operationCallbackCaptor.getValue();
        onGenCallback.onSuccess(failedResponse);

        // ASSERT 2 (failed to send ON_GEN event to ServiceNow)
        // If audit response from ServiceNow probe has errors then we don't need to returned sent
        // action events to the queue, because for ServiceNow we send all audited actions every
        // market cycle. And in case of long unavailability of ServiceNow target our queue can be
        // overflowed.
        Mockito.verify(operationManager, Mockito.never())
                .sendActionAuditEvents(Mockito.eq(SERVICENOW_TARGET_ID),
                        auditedActionsCaptor.capture(), operationCallbackCaptor.capture());
    }

    /**
     * Tests that we don't send actions for audit if we don't have information about audit target,
     * because we have different audit logic depends on target and without getting target info
     * we couldn't select appropriate audit scenario.
     */
    @Test
    public void failedToAuditActionsDueToMissingTargetInfo() {
        expectedException.expect(UnsupportedOperationException.class);
        Mockito.when(thinTargetCache.getTargetInfo(SERVICENOW_TARGET_ID))
                .thenReturn(Optional.empty());
        new TargetActionAuditService(SERVICENOW_TARGET_ID, operationManager, contextFactory,
                executorService, 0, 1, new AtomicBoolean(true), thinTargetCache);
    }

    private void arrangeStuffForCreatingActionExecutionDTO(
            @Nonnull ExecuteActionRequest auditRequest) throws ContextCreationException {
        final ResizeContext resizeContext = Mockito.mock(ResizeContext.class);
        Mockito.when(resizeContext.buildActionExecutionDto())
                .thenReturn(ActionExecutionDTO.newBuilder()
                        .setActionType(ActionItemDTO.ActionType.NONE)
                        .setActionOid(auditRequest.getActionId())
                        .build());
        Mockito.when(contextFactory.getActionExecutionContext(auditRequest))
                .thenReturn(resizeContext);
    }

    private static ActionEvent createAuditEvent(@Nonnull ExecuteActionRequest executeActionRequest,
            @Nonnull ActionResponseState oldState, @Nonnull ActionResponseState newState) {
        return ActionEvent.newBuilder()
                .setTimestamp(System.currentTimeMillis())
                .setOldState(oldState)
                .setNewState(newState)
                .setActionRequest(executeActionRequest)
                .build();
    }

    private static Action createResizeAction() {
        return Action.newBuilder()
                .setId(ACTION_ID)
                .setDeprecatedImportance(2)
                .setInfo(ActionInfo.newBuilder()
                        .setResize(Resize.newBuilder()
                                .setTarget(ActionEntity.newBuilder().setId(12L).setType(1).build())
                                .build())
                        .build())
                .setExplanation(Explanation.getDefaultInstance())
                .build();
    }

}
