package com.vmturbo.action.orchestrator.audit;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

import javax.annotation.Nullable;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ActionModeCalculator;
import com.vmturbo.action.orchestrator.dto.ActionMessages.ActionEvent;
import com.vmturbo.action.orchestrator.workflow.store.WorkflowStore;
import com.vmturbo.action.orchestrator.workflow.store.WorkflowStoreException;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPhase;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.Delete;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.DeactivateExplanation;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.Workflow;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.WorkflowInfo;
import com.vmturbo.components.api.server.IMessageSender;
import com.vmturbo.platform.common.dto.ActionExecution.ActionResponseState;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.api.util.ImmutableThinProbeInfo;
import com.vmturbo.topology.processor.api.util.ImmutableThinTargetInfo;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;
import com.vmturbo.topology.processor.api.util.ThinTargetCache.ThinTargetInfo;

/**
 * Unit test for {@link ActionAuditSender}.
 */
public class ActionAuditSenderTest {

    private static final long ACTION_PLAN_ID = 1001L;
    private static final long WORKFLOW_ID1 = 2001L;
    private static final long WORKFLOW_ID2 = 2002L;
    private static final long WORKFLOW_ID3 = 2003L;
    private static final long TARGET_1 = 3001L;
    private static final long TARGET_2 = 3002L;
    private static final long ACTION1_ID = 4001L;
    private static final long ACTION2_ID = 4002L;
    private static final long ACTION3_ID = 4003L;
    private static final long ACTION4_ID = 4004L;

    @Mock
    private IMessageSender<ActionEvent> messageSender;
    @Mock
    private WorkflowStore workflowStore;
    @Mock
    private ThinTargetCache thinTargetCache;
    @Captor
    private ArgumentCaptor<ActionEvent> messageCaptor;

    private static final ImmutableThinTargetInfo KAFKA_TARGET = ImmutableThinTargetInfo.builder()
            .oid(TARGET_1)
            .displayName("Kafka")
            .isHidden(false)
            .probeInfo(ImmutableThinProbeInfo.builder()
                    .type("Kafka")
                    .oid(TARGET_1)
                    .category(ProbeCategory.ORCHESTRATOR.getCategory())
                    .uiCategory(ProbeCategory.ORCHESTRATOR.getCategory())
                    .build())
            .build();

    private static final ImmutableThinTargetInfo AUDIT_TARGET = ImmutableThinTargetInfo.builder()
            .oid(TARGET_2)
            .displayName("ServiceNow")
            .isHidden(false)
            .probeInfo(ImmutableThinProbeInfo.builder()
                    .type(SDKProbeType.SERVICENOW.getProbeType())
                    .oid(TARGET_2)
                    .category(ProbeCategory.ORCHESTRATOR.getCategory())
                    .uiCategory(ProbeCategory.ORCHESTRATOR.getCategory())
                    .build())
            .build();

    /**
     * Initialises the tests.
     */
    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);
    }

    /**
     * Tests sending action events.
     *
     * @throws Exception on exceptions occurred.
     */
    @Test
    public void testSendingActionEvents() throws Exception {
        final ActionAuditSender sender = new ActionAuditSender(workflowStore, messageSender,
                thinTargetCache);
        final Workflow workflow1 = Workflow.newBuilder().setId(WORKFLOW_ID1).setWorkflowInfo(
                WorkflowInfo.newBuilder()
                        .setActionPhase(ActionPhase.ON_GENERATION)
                        .setTargetId(TARGET_1))
                .build();
        final Workflow workflow2 = Workflow.newBuilder().setId(WORKFLOW_ID2).setWorkflowInfo(
                WorkflowInfo.newBuilder()
                        .setActionPhase(ActionPhase.AFTER_EXECUTION)
                        .setTargetId(TARGET_2))
                .build();
        final Workflow workflow3 = Workflow.newBuilder().setId(WORKFLOW_ID3).setWorkflowInfo(
                WorkflowInfo.newBuilder()
                        .setActionPhase(ActionPhase.REPLACE)
                        .setTargetId(TARGET_1))
                .build();
        final Action action1 = createAction(ACTION1_ID, workflow1);
        final Action action2 = createAction(ACTION2_ID, workflow2);
        Mockito.when(action2.getState()).thenReturn(ActionState.SUCCEEDED);
        final Action action3 = createAction(ACTION3_ID, workflow3);
        final Action action4 = createAction(ACTION4_ID, null);
        Optional<ThinTargetInfo> auditTarget = Optional.of(AUDIT_TARGET);
        Mockito.when(thinTargetCache.getTargetInfo(Mockito.any(Long.class)))
                .thenReturn(auditTarget);
        sender.sendActionEvents(Arrays.asList(action1, action2, action3, action4));
        Mockito.verify(messageSender, Mockito.times(2)).sendMessage(messageCaptor.capture());

        final ActionEvent event1 = messageCaptor.getAllValues().get(0);
        Assert.assertEquals(TARGET_1, event1.getActionRequest().getTargetId());
        Assert.assertEquals(action1.getRecommendationOid(), event1.getActionRequest().getActionId());
        Assert.assertEquals(ActionResponseState.PENDING_ACCEPT, event1.getOldState());
        Assert.assertEquals(ActionResponseState.PENDING_ACCEPT, event1.getNewState());

        final ActionEvent event2 = messageCaptor.getAllValues().get(1);
        Assert.assertEquals(TARGET_2, event2.getActionRequest().getTargetId());
        Assert.assertEquals(action2.getRecommendationOid(), event2.getActionRequest().getActionId());
        Assert.assertEquals(ActionResponseState.IN_PROGRESS, event2.getOldState());
        Assert.assertEquals(ActionResponseState.SUCCEEDED, event2.getNewState());
    }

    /**
     * Tests that for ServiceNow we send audit actions every time when they recommend.
     * NOTE: this test can be removed after changing ServiceNow app and introducing generic audit
     * logic (sending ON_GEN actions only one time).
     *
     * @throws Exception on exceptions occurred.
     */
    @Test
    public void testSendingActionsForServiceNowAuditEveryTime() throws Exception {
        final ActionAuditSender sender =
                new ActionAuditSender(workflowStore, messageSender, thinTargetCache);
        final Workflow workflow1 = Workflow.newBuilder()
                .setId(WORKFLOW_ID1)
                .setWorkflowInfo(WorkflowInfo.newBuilder()
                        .setActionPhase(ActionPhase.ON_GENERATION)
                        .setTargetId(TARGET_2))
                .build();
        final Workflow workflow2 = Workflow.newBuilder()
                .setId(WORKFLOW_ID2)
                .setWorkflowInfo(WorkflowInfo.newBuilder()
                        .setActionPhase(ActionPhase.ON_GENERATION)
                        .setTargetId(TARGET_2))
                .build();
        final Action action1 = createAction(ACTION1_ID, workflow1);
        final Action action2 = createAction(ACTION2_ID, workflow1);
        final Action action3 = createAction(ACTION3_ID, workflow2);
        Mockito.when(thinTargetCache.getTargetInfo(TARGET_2)).thenReturn(Optional.of(AUDIT_TARGET));
        sender.sendActionEvents(Arrays.asList(action1, action2));
        Mockito.verify(messageSender, Mockito.times(2)).sendMessage(messageCaptor.capture());

        final ActionEvent event1 = messageCaptor.getAllValues().get(0);
        Assert.assertEquals(action1.getRecommendationOid(),
                event1.getActionRequest().getActionId());

        final ActionEvent event2 = messageCaptor.getAllValues().get(1);
        Assert.assertEquals(action2.getRecommendationOid(),
                event2.getActionRequest().getActionId());

        Mockito.reset(messageSender);
        sender.sendActionEvents(Arrays.asList(action1, action2, action3));
        // all actions will be send for ServiceNow audit
        Mockito.verify(messageSender, Mockito.times(3)).sendMessage(messageCaptor.capture());

        final ActionEvent event1Again = messageCaptor.getAllValues().get(2);
        Assert.assertEquals(action1.getRecommendationOid(),
                event1Again.getActionRequest().getActionId());

        final ActionEvent event2Again = messageCaptor.getAllValues().get(3);
        Assert.assertEquals(action2.getRecommendationOid(),
                event2Again.getActionRequest().getActionId());

        final ActionEvent event3 = messageCaptor.getAllValues().get(4);
        Assert.assertEquals(action3.getRecommendationOid(),
                event3.getActionRequest().getActionId());
    }

    /**
     * Tests sending action events only one time to certain audit target (defined by workflow id).
     *
     * @throws Exception on exceptions occurred.
     */
    @Test
    public void testSendingOnlyNewActionEvents() throws Exception {
        final ActionAuditSender sender = new ActionAuditSender(workflowStore, messageSender,
                thinTargetCache);
        final Workflow workflow1 = Workflow.newBuilder().setId(WORKFLOW_ID1).setWorkflowInfo(
                WorkflowInfo.newBuilder()
                        .setActionPhase(ActionPhase.ON_GENERATION)
                        .setTargetId(TARGET_1))
                .build();
        final Workflow workflow2 = Workflow.newBuilder().setId(WORKFLOW_ID2).setWorkflowInfo(
                WorkflowInfo.newBuilder()
                        .setActionPhase(ActionPhase.ON_GENERATION)
                        .setTargetId(TARGET_1))
                .build();
        final Action action1 = createAction(ACTION1_ID, workflow1);
        final Action action2 = createAction(ACTION2_ID, workflow1);
        final Action action3 = createAction(ACTION3_ID, workflow2);
        Mockito.when(thinTargetCache.getTargetInfo(TARGET_1)).thenReturn(Optional.of(KAFKA_TARGET));
        sender.sendActionEvents(Arrays.asList(action1, action2));
        Mockito.verify(messageSender, Mockito.times(2)).sendMessage(messageCaptor.capture());

        final ActionEvent event1 = messageCaptor.getAllValues().get(0);
        Assert.assertEquals(action1.getRecommendationOid(), event1.getActionRequest().getActionId());

        final ActionEvent event2 = messageCaptor.getAllValues().get(1);
        Assert.assertEquals(action2.getRecommendationOid(), event2.getActionRequest().getActionId());

        Mockito.reset(messageSender);
        sender.sendActionEvents(Arrays.asList(action1, action2, action3));
        // only action3 will be send for audit. action1 and action2 were already sent
        Mockito.verify(messageSender, Mockito.times(1)).sendMessage(messageCaptor.capture());
        final ActionEvent event3 = messageCaptor.getAllValues().get(2);
        Assert.assertEquals(action3.getRecommendationOid(),
                event3.getActionRequest().getActionId());
    }

    /**
     * Tests sending CLEARED action events when action wasn't recommended by market.
     *
     * @throws Exception on exceptions occurred.
     */
    @Test
    public void testSendingClearedEvents() throws Exception {
        final ActionAuditSender sender = new ActionAuditSender(workflowStore, messageSender,
                thinTargetCache);
        final Workflow workflow1 = Workflow.newBuilder().setId(WORKFLOW_ID1).setWorkflowInfo(
                WorkflowInfo.newBuilder()
                        .setActionPhase(ActionPhase.ON_GENERATION)
                        .setTargetId(TARGET_1))
                .build();
        final Workflow workflow2 = Workflow.newBuilder().setId(WORKFLOW_ID2).setWorkflowInfo(
                WorkflowInfo.newBuilder()
                        .setActionPhase(ActionPhase.ON_GENERATION)
                        .setTargetId(TARGET_1))
                .build();
        final Action action1 = createAction(ACTION1_ID, workflow1);
        final Action action2 = createAction(ACTION2_ID, workflow1);
        final Action action3 = createAction(ACTION3_ID, workflow2);
        Mockito.when(thinTargetCache.getTargetInfo(TARGET_1)).thenReturn(Optional.of(KAFKA_TARGET));
        sender.sendActionEvents(Arrays.asList(action1, action2));
        Mockito.verify(messageSender, Mockito.times(2)).sendMessage(messageCaptor.capture());

        final ActionEvent event1 = messageCaptor.getAllValues().get(0);
        Assert.assertEquals(action1.getRecommendationOid(), event1.getActionRequest().getActionId());

        final ActionEvent event2 = messageCaptor.getAllValues().get(1);
        Assert.assertEquals(action2.getRecommendationOid(), event2.getActionRequest().getActionId());

        Mockito.reset(messageSender);
        sender.sendActionEvents(Arrays.asList(action3));
        // action3 will be send for audit as a new action.
        // action1 and action2 will be send as CLEARED actions
        Mockito.verify(messageSender, Mockito.times(3)).sendMessage(messageCaptor.capture());
        final ActionEvent event3 = messageCaptor.getAllValues().get(2);
        Assert.assertEquals(action3.getRecommendationOid(), event3.getActionRequest().getActionId());
        final ActionEvent clearedEvent1 = messageCaptor.getAllValues().get(3);
        Assert.assertEquals(action1.getRecommendationOid(), clearedEvent1.getActionRequest().getActionId());
        Assert.assertEquals(ActionResponseState.CLEARED, clearedEvent1.getNewState());

        final ActionEvent clearedEvent2 = messageCaptor.getAllValues().get(4);
        Assert.assertEquals(action2.getRecommendationOid(), clearedEvent2.getActionRequest().getActionId());
        Assert.assertEquals(ActionResponseState.CLEARED, clearedEvent2.getNewState());

    }

    /**
     * Tests that after sending for audit CLEARED action we remove them from cache. So if the
     * action will be recommended again we send it for audit as ON_GEN.
     *
     * @throws Exception on exceptions occurred.
     */
    @Test
    public void testRemovingSentClearedActionsFromCache() throws Exception {
        final ActionAuditSender sender = new ActionAuditSender(workflowStore, messageSender,
                thinTargetCache);
        final Workflow workflow1 = Workflow.newBuilder().setId(WORKFLOW_ID1).setWorkflowInfo(
                WorkflowInfo.newBuilder()
                        .setActionPhase(ActionPhase.ON_GENERATION)
                        .setTargetId(TARGET_1))
                .build();
        final Action action1 = createAction(ACTION1_ID, workflow1);
        final Action action2 = createAction(ACTION2_ID, workflow1);
        Mockito.when(thinTargetCache.getTargetInfo(TARGET_1)).thenReturn(Optional.of(KAFKA_TARGET));
        sender.sendActionEvents(Arrays.asList(action1));
        Mockito.verify(messageSender, Mockito.times(1)).sendMessage(messageCaptor.capture());

        final ActionEvent event1 = messageCaptor.getAllValues().get(0);
        Assert.assertEquals(action1.getRecommendationOid(), event1.getActionRequest().getActionId());

        Mockito.reset(messageSender);
        sender.sendActionEvents(Collections.singletonList(action2));
        // action2 will be send for audit as a new action.
        // action1 will be send as CLEARED actions
        Mockito.verify(messageSender, Mockito.times(2)).sendMessage(messageCaptor.capture());
        final ActionEvent event2 = messageCaptor.getAllValues().get(1);
        Assert.assertEquals(action2.getRecommendationOid(),
                event2.getActionRequest().getActionId());
        final ActionEvent clearedEvent1 = messageCaptor.getAllValues().get(2);
        Assert.assertEquals(action1.getRecommendationOid(), clearedEvent1.getActionRequest().getActionId());
        Assert.assertEquals(ActionResponseState.CLEARED, clearedEvent1.getNewState());

        Mockito.reset(messageSender);
        sender.sendActionEvents(Arrays.asList(action1, action2));
        // action1 should be send again as new generated actions
        Mockito.verify(messageSender, Mockito.times(1)).sendMessage(messageCaptor.capture());
        final ActionEvent againEvent1 = messageCaptor.getAllValues().get(3);
        Assert.assertEquals(action1.getRecommendationOid(),
                againEvent1.getActionRequest().getActionId());
        Assert.assertEquals(ActionResponseState.PENDING_ACCEPT, againEvent1.getNewState());
    }

    private Action createAction(long oid, @Nullable Workflow workflow) throws
            WorkflowStoreException {
        final ActionDTO.Action actionDTO = ActionDTO.Action.newBuilder().setId(oid).setInfo(
                ActionInfo.newBuilder()
                        .setDelete(Delete.newBuilder().setTarget(ActionEntity.newBuilder()
                                .setId(10)
                                .setType(12)
                                .build()))).setExplanation(Explanation.newBuilder()
                .setDeactivate(DeactivateExplanation.newBuilder().build())
                .build()).setDeprecatedImportance(23).build();
        final Action action = Mockito.spy(
                new Action(actionDTO, ACTION_PLAN_ID, Mockito.mock(ActionModeCalculator.class),
                        oid));
        action.getActionTranslation().setTranslationSuccess(actionDTO);
        Mockito.when(action.getWorkflow(Mockito.any(), Mockito.any()))
                .thenReturn(Optional.ofNullable(workflow));
        return action;
    }
}
