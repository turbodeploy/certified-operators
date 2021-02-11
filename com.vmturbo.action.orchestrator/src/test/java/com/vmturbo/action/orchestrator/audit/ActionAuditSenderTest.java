package com.vmturbo.action.orchestrator.audit;

import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.vmturbo.action.orchestrator.ActionOrchestratorTestUtils;
import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ActionModeCalculator;
import com.vmturbo.action.orchestrator.action.AuditedActionInfo;
import com.vmturbo.action.orchestrator.action.AuditedActionsManager;
import com.vmturbo.action.orchestrator.action.AuditedActionsManager.AuditedActionsUpdate;
import com.vmturbo.action.orchestrator.dto.ActionMessages.ActionEvent;
import com.vmturbo.action.orchestrator.translation.ActionTranslator;
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
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.api.server.IMessageSender;
import com.vmturbo.platform.common.dto.ActionExecution.ActionResponseState;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.api.util.ImmutableThinProbeInfo;
import com.vmturbo.topology.processor.api.util.ImmutableThinTargetInfo;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;

/**
 * Unit test for {@link ActionAuditSender}.
 */
public class ActionAuditSenderTest {

    private static final long ACTION_PLAN_ID = 1001L;
    private static final long KAFKA_ONGEN_WORKFLOW_ID = 2001L;
    private static final long SERVICENOW_ONGEN_WORKFLOW_ID = 2002L;
    private static final long WORKFLOW_ID3 = 2003L;
    private static final long KAFKA_TARGET_ID = 3001L;
    private static final long SERVICENOW_TARGET_ID = 3002L;
    private static final long ACTION1_ID = 4001L;
    private static final long ACTION2_ID = 4002L;
    private static final long ACTION3_ID = 4003L;
    private static final long ACTION4_ID = 4004L;

    /**
     * We override the current time in milliseconds to Mon Jan 18 2021 20:25:18 EST for convenience
     * of unit testing.
     */
    private static final long CURRENT_TIME = 1611001518345L;

    @Mock
    private IMessageSender<ActionEvent> messageSender;
    @Mock
    private WorkflowStore workflowStore;
    @Mock
    private ThinTargetCache thinTargetCache;

    @Mock
    private AuditedActionsManager auditedActionsManager;

    private final ActionTranslator actionTranslator =
            ActionOrchestratorTestUtils.passthroughTranslator();

    private static final long MINUTES_UNTIL_MARKED_CLEARED = TimeUnit.DAYS.toMinutes(1L);

    @Mock
    private Clock clock;

    private ActionAuditSender actionAuditSender;

    @Captor
    private ArgumentCaptor<ActionEvent> sentActionEventMessageCaptor;

    @Captor
    private ArgumentCaptor<AuditedActionsUpdate> persistedActionsCaptor;

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

    private static final Workflow KAFKA_ONGEN_WORKFLOW = Workflow.newBuilder().setId(KAFKA_ONGEN_WORKFLOW_ID).setWorkflowInfo(
        WorkflowInfo.newBuilder()
            .setActionPhase(ActionPhase.ON_GENERATION)
            .setTargetId(KAFKA_TARGET_ID))
        .build();

    private static final Workflow SERVICENOW_ONGEN_WORKFLOW = Workflow.newBuilder().setId(SERVICENOW_ONGEN_WORKFLOW_ID).setWorkflowInfo(
        WorkflowInfo.newBuilder()
            .setActionPhase(ActionPhase.AFTER_EXECUTION)
            .setTargetId(SERVICENOW_TARGET_ID))
        .build();

    /**
     * Initialises the tests.
     *
     * @throws WorkflowStoreException should not be thrown.
     */
    @Before
    public void init() throws WorkflowStoreException {
        MockitoAnnotations.initMocks(this);

        when(clock.millis()).thenReturn(CURRENT_TIME);

        when(workflowStore.fetchWorkflow(KAFKA_ONGEN_WORKFLOW_ID)).thenReturn(Optional.of(KAFKA_ONGEN_WORKFLOW));

        when(thinTargetCache.getTargetInfo(KAFKA_TARGET_ID)).thenReturn(Optional.of(KAFKA_TARGET));
        when(thinTargetCache.getTargetInfo(SERVICENOW_TARGET_ID)).thenReturn(Optional.of(SERVICENOW_TARGET));

        actionAuditSender = new ActionAuditSender(
            workflowStore,
            messageSender,
            thinTargetCache,
            actionTranslator,
            auditedActionsManager,
            MINUTES_UNTIL_MARKED_CLEARED,
            clock);
    }

    /**
     * Tests sending action events.
     *
     * @throws Exception on exceptions occurred.
     */
    @Test
    public void testSendingActionEvents() throws Exception {
        final Workflow workflow3 = Workflow.newBuilder().setId(WORKFLOW_ID3).setWorkflowInfo(
                WorkflowInfo.newBuilder()
                        .setActionPhase(ActionPhase.REPLACE)
                        .setTargetId(KAFKA_TARGET_ID))
                .build();
        final Action action1 = createAction(ACTION1_ID, KAFKA_ONGEN_WORKFLOW);
        final Action action2 = createAction(ACTION2_ID, SERVICENOW_ONGEN_WORKFLOW);
        when(action2.getState()).thenReturn(ActionState.SUCCEEDED);
        final Action action3 = createAction(ACTION3_ID, workflow3);
        final Action action4 = createAction(ACTION4_ID, null);
        actionAuditSender.sendActionEvents(Arrays.asList(action1, action2, action3, action4));
        Mockito.verify(messageSender, Mockito.times(2)).sendMessage(sentActionEventMessageCaptor.capture());

        final ActionEvent event1 = sentActionEventMessageCaptor.getAllValues().get(0);
        Assert.assertEquals(KAFKA_TARGET_ID, event1.getActionRequest().getTargetId());
        Assert.assertEquals(action1.getRecommendationOid(), event1.getActionRequest().getActionId());
        Assert.assertEquals(ActionResponseState.PENDING_ACCEPT, event1.getOldState());
        Assert.assertEquals(ActionResponseState.PENDING_ACCEPT, event1.getNewState());

        final ActionEvent event2 = sentActionEventMessageCaptor.getAllValues().get(1);
        Assert.assertEquals(SERVICENOW_TARGET_ID, event2.getActionRequest().getTargetId());
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
        final Action action1 = createAction(ACTION1_ID, KAFKA_ONGEN_WORKFLOW);
        final Action action2 = createAction(ACTION2_ID, KAFKA_ONGEN_WORKFLOW);
        final Action action3 = createAction(ACTION3_ID, SERVICENOW_ONGEN_WORKFLOW);
        actionAuditSender.sendActionEvents(Arrays.asList(action1, action2));
        Mockito.verify(messageSender, Mockito.times(2)).sendMessage(sentActionEventMessageCaptor.capture());

        final ActionEvent event1 = sentActionEventMessageCaptor.getAllValues().get(0);
        Assert.assertEquals(action1.getRecommendationOid(),
                event1.getActionRequest().getActionId());

        final ActionEvent event2 = sentActionEventMessageCaptor.getAllValues().get(1);
        Assert.assertEquals(action2.getRecommendationOid(),
                event2.getActionRequest().getActionId());

        Mockito.reset(messageSender);
        actionAuditSender.sendActionEvents(Arrays.asList(action1, action2, action3));
        // all actions will be send for ServiceNow audit
        Mockito.verify(messageSender, Mockito.times(3)).sendMessage(sentActionEventMessageCaptor.capture());

        final ActionEvent event1Again = sentActionEventMessageCaptor.getAllValues().get(2);
        Assert.assertEquals(action1.getRecommendationOid(),
                event1Again.getActionRequest().getActionId());

        final ActionEvent event2Again = sentActionEventMessageCaptor.getAllValues().get(3);
        Assert.assertEquals(action2.getRecommendationOid(),
                event2Again.getActionRequest().getActionId());

        final ActionEvent event3 = sentActionEventMessageCaptor.getAllValues().get(4);
        Assert.assertEquals(action3.getRecommendationOid(),
                event3.getActionRequest().getActionId());
    }

    /**
     * When there are no actions under book keeping, all actions should be sent.
     *
     * @throws Exception should not be thrown.
     */
    @Test
    public void testNoActionsBookKeptYet() throws Exception {
        final Action action1 = createAction(ACTION1_ID, KAFKA_ONGEN_WORKFLOW);
        final Action action2 = createAction(ACTION2_ID, KAFKA_ONGEN_WORKFLOW);
        final Action action3 = createAction(ACTION3_ID, SERVICENOW_ONGEN_WORKFLOW);
        actionAuditSender.sendActionEvents(Arrays.asList(action1, action2, action3));
        Mockito.verify(messageSender, Mockito.times(3)).sendMessage(sentActionEventMessageCaptor.capture());

        final ActionEvent event1 = sentActionEventMessageCaptor.getAllValues().get(0);
        Assert.assertEquals(action1.getRecommendationOid(), event1.getActionRequest().getActionId());

        final ActionEvent event2 = sentActionEventMessageCaptor.getAllValues().get(1);
        Assert.assertEquals(action2.getRecommendationOid(), event2.getActionRequest().getActionId());

        final ActionEvent event3 = sentActionEventMessageCaptor.getAllValues().get(2);
        Assert.assertEquals(action3.getRecommendationOid(), event3.getActionRequest().getActionId());


        // Check which actions ActionAuditSender wanted to persist
        Mockito.verify(auditedActionsManager, Mockito.times(1))
            .persistAuditedActionsUpdates(persistedActionsCaptor.capture());
        AuditedActionsUpdate actualUpdate = persistedActionsCaptor.getValue();
        Assert.assertEquals(
            Arrays.asList(
                new AuditedActionInfo(ACTION1_ID, KAFKA_ONGEN_WORKFLOW_ID, Optional.empty()),
                new AuditedActionInfo(ACTION2_ID, KAFKA_ONGEN_WORKFLOW_ID, Optional.empty())
            ),
            actualUpdate.getAuditedActions());
        Assert.assertEquals(Collections.emptyList(), actualUpdate.getRecentlyClearedActions());
        Assert.assertEquals(Collections.emptyList(), actualUpdate.getExpiredClearedActions());
    }

    /**
     * For actions destined for a ServiceNOW workflow, they should always be sent, even though
     * they might be in the cache. In this setup actionSentToServiceNOW is destined for ServiceNOW. As a result,
     * we expect actionSentToServiceNOW to be sent.
     *
     * <p>For actions destined for a non-ServiceNOW workflow should only be if they have
     * not been sent before. In this setup actionAlreadySentToKafka was been before, but actionNotSentToKafka was not.
     * As a result, we expect action2 to be sent.</p>
     *
     *
     * @throws Exception should not be thrown.
     */
    @Test
    public void testSendingOnlyNewActionEvents() throws Exception {
        when(auditedActionsManager.isAlreadySent(ACTION1_ID, KAFKA_ONGEN_WORKFLOW_ID)).thenReturn(true);
        when(auditedActionsManager.isAlreadySent(ACTION2_ID, KAFKA_ONGEN_WORKFLOW_ID)).thenReturn(false);
        when(auditedActionsManager.isAlreadySent(ACTION3_ID, SERVICENOW_ONGEN_WORKFLOW_ID)).thenReturn(true);

        final Action actionAlreadySentToKafka = createAction(ACTION1_ID, KAFKA_ONGEN_WORKFLOW);
        final Action actionNotSentToKafka = createAction(ACTION2_ID, KAFKA_ONGEN_WORKFLOW);
        final Action actionSentToServiceNOW = createAction(ACTION3_ID, SERVICENOW_ONGEN_WORKFLOW);
        actionAuditSender.sendActionEvents(Arrays.asList(actionAlreadySentToKafka,
                actionNotSentToKafka, actionSentToServiceNOW));

        // only action3 will be send for audit. action1 and action2 were already sent
        Mockito.verify(messageSender, Mockito.times(2)).sendMessage(sentActionEventMessageCaptor.capture());
        Assert.assertEquals(actionNotSentToKafka.getRecommendationOid(),
            sentActionEventMessageCaptor.getAllValues().get(0).getActionRequest().getActionId());
        Assert.assertEquals(actionSentToServiceNOW.getRecommendationOid(),
            sentActionEventMessageCaptor.getAllValues().get(1).getActionRequest().getActionId());

        // Check which actions ActionAuditSender wanted to persist
        Mockito.verify(auditedActionsManager, Mockito.times(1))
            .persistAuditedActionsUpdates(persistedActionsCaptor.capture());
        AuditedActionsUpdate actualUpdate = persistedActionsCaptor.getValue();
        Assert.assertEquals(
            Arrays.asList(new AuditedActionInfo(ACTION2_ID, KAFKA_ONGEN_WORKFLOW_ID, Optional.empty())),
            actualUpdate.getAuditedActions());
        Assert.assertEquals(Collections.emptyList(), actualUpdate.getRecentlyClearedActions());
        Assert.assertEquals(Collections.emptyList(), actualUpdate.getExpiredClearedActions());
    }

    /**
     * <ol>
     * <li>The action actionClearedButNotExpired should have not affect on ActionAuditSender because
     * the duration between it's cleared time and CURRENT_TIME is less than than MINUTES_UNTIL_MARKED_CLEARED.</li>
     * <li>The action actionClearedAndExpired should result in a CLEARED message because the duration
     * between it's cleared time and CURRENT_TIME is greater than MINUTES_UNTIL_MARKED_CLEARED.</li>
     * <li>The action actionJustExpiring is not present in the action list for the first time, so it's
     * clear time should be CURRENT_TIME.</li>
     * <li>The action actionGoingToServiceNOW is always sent to ServiceNOW and does not participate in
     * book keeping.</li>
     * </ol>
     *
     * @throws Exception should not be thrown.
     */
    @Test
    public void testSendingClearedEvents() throws Exception {
        final Action actionClearedButNotExpired = createAction(ACTION1_ID, KAFKA_ONGEN_WORKFLOW);
        final Action actionClearedAndExpired = createAction(ACTION2_ID, KAFKA_ONGEN_WORKFLOW);
        final Action actionJustExpiring = createAction(ACTION3_ID, KAFKA_ONGEN_WORKFLOW);
        final Action actionGoingToServiceNOW = createAction(ACTION4_ID, SERVICENOW_ONGEN_WORKFLOW);
        final List<AuditedActionInfo> alreadySentActions = Arrays.asList(
                new AuditedActionInfo(actionClearedButNotExpired.getRecommendationOid(),
                        KAFKA_ONGEN_WORKFLOW_ID,
                        // still 1 millisecond until expiry, so this action should not be cleared
                        Optional.of(CURRENT_TIME - TimeUnit.MINUTES.toMillis(MINUTES_UNTIL_MARKED_CLEARED) + 1)),
                new AuditedActionInfo(actionClearedAndExpired.getRecommendationOid(),
                        KAFKA_ONGEN_WORKFLOW_ID,
                        // still 1 millisecond after expiry, so this action should be cleared
                        Optional.of(CURRENT_TIME - TimeUnit.MINUTES.toMillis(MINUTES_UNTIL_MARKED_CLEARED) - 1)),
                // actionJustExpiring was sent last cycle, but not this cycle
                new AuditedActionInfo(actionJustExpiring.getRecommendationOid(), KAFKA_ONGEN_WORKFLOW_ID,
                        // was recommended in the last market cycle, so no cleared timestamp
                        Optional.empty()));
        when(auditedActionsManager.getAlreadySentActions()).thenReturn(alreadySentActions);
        actionAuditSender.sendActionEvents(Collections.singletonList(actionGoingToServiceNOW));

        // action3 will be send for audit as a new action.
        // action1 will be send as CLEARED actions
        Mockito.verify(messageSender, Mockito.times(2))
                .sendMessage(sentActionEventMessageCaptor.capture());
        final ActionEvent sendEvent = sentActionEventMessageCaptor.getAllValues().get(0);
        Assert.assertEquals(actionGoingToServiceNOW.getRecommendationOid(),
                sendEvent.getActionRequest().getActionId());
        final ActionEvent clearedEvent = sentActionEventMessageCaptor.getAllValues().get(1);
        Assert.assertEquals(actionClearedAndExpired.getRecommendationOid(),
                clearedEvent.getActionRequest().getActionId());
        Assert.assertEquals(ActionResponseState.CLEARED, clearedEvent.getNewState());

        // Check which actions ActionAuditSender wanted to persist
        Mockito.verify(auditedActionsManager, Mockito.times(1))
                .persistAuditedActionsUpdates(persistedActionsCaptor.capture());
        AuditedActionsUpdate actualUpdate = persistedActionsCaptor.getValue();
        Assert.assertEquals(Collections.emptyList(), actualUpdate.getAuditedActions());
        Assert.assertEquals(Collections.singletonList(
                new AuditedActionInfo(ACTION3_ID, KAFKA_ONGEN_WORKFLOW_ID,
                        Optional.of(CURRENT_TIME))), actualUpdate.getRecentlyClearedActions());
        Assert.assertEquals(Collections.singletonList(
                new AuditedActionInfo(ACTION2_ID, KAFKA_ONGEN_WORKFLOW_ID, Optional.empty())),
                actualUpdate.getExpiredClearedActions());
    }

    /**
     * Tests that we reset cleared_timestamp for action which was not recommended during certain
     * time but wasn't send as CLEARED because don't met cleared criteria (wasn't recommended
     * less time then expected for sending action as CLEARED).
     * Test has 3 main steps:
     * I. market recommends `actionForAudit` so we need to send it for ON_GEN audit
     * II. market don't recommend `actionForAudit` and we need to persist cleared_timestamp for
     *     it (without sending it as CLEARED because action don't met cleared criteria yet)
     * III. market again recommends `actionForAudit` so we need to reset cleared_timestamp for it
     *
     * @throws WorkflowStoreException shouldn't be thrown
     * @throws CommunicationException shouldn't be thrown
     * @throws InterruptedException shouldn't be thrown
     */
    @Test
    public void testResetOfClearedTimestamp()
            throws WorkflowStoreException, CommunicationException, InterruptedException {
        final Action actionForAudit = createAction(ACTION1_ID, KAFKA_ONGEN_WORKFLOW);
        when(auditedActionsManager.getAlreadySentActions()).thenReturn(Collections.emptyList());
        // I. send `actionForAudit` first time
        actionAuditSender.sendActionEvents(Collections.singletonList(actionForAudit));

        // `actionForAudit` will be send for audit as a new action.
        Mockito.verify(messageSender, Mockito.times(1))
                .sendMessage(sentActionEventMessageCaptor.capture());
        final ActionEvent sendEvent = sentActionEventMessageCaptor.getValue();
        Assert.assertEquals(actionForAudit.getRecommendationOid(),
                sendEvent.getActionRequest().getActionId());

        // Check which actions ActionAuditSender wanted to persist
        Mockito.verify(auditedActionsManager, Mockito.times(1))
                .persistAuditedActionsUpdates(persistedActionsCaptor.capture());
        final AuditedActionsUpdate actualUpdate = persistedActionsCaptor.getValue();
        Assert.assertEquals(1, actualUpdate.getAuditedActions().size());
        final AuditedActionInfo auditedAction = actualUpdate.getAuditedActions().get(0);
        Assert.assertEquals(ACTION1_ID, auditedAction.getRecommendationId());
        Assert.assertEquals(KAFKA_ONGEN_WORKFLOW_ID, auditedAction.getWorkflowId());
        Assert.assertEquals(Optional.empty(), auditedAction.getClearedTimestamp());

        Mockito.reset(auditedActionsManager, messageSender);
        final List<AuditedActionInfo> alreadySentActions =
                Collections.singletonList(
                        new AuditedActionInfo(actionForAudit.getRecommendationOid(), KAFKA_ONGEN_WORKFLOW_ID,
                                Optional.empty()));

        when(auditedActionsManager.getAlreadySentActions()).thenReturn(alreadySentActions);
        when(auditedActionsManager.isAlreadySent(ACTION1_ID, KAFKA_ONGEN_WORKFLOW_ID)).thenReturn(true);
        // II. `actionForAudit` is not recommended now, but we audited it before, so we need to
        // update cleared_timestamp for it (without sending it as CLEARED, because don't met
        // cleared criteria)
        actionAuditSender.sendActionEvents(Collections.emptyList());

        // don't send any actions for audit
        Mockito.verify(messageSender, Mockito.times(0))
                .sendMessage(Mockito.any());

        // Check which actions ActionAuditSender wanted to persist
        Mockito.verify(auditedActionsManager, Mockito.times(1))
                .persistAuditedActionsUpdates(persistedActionsCaptor.capture());
        final AuditedActionsUpdate actualUpdate2 = persistedActionsCaptor.getValue();
        Assert.assertEquals(0, actualUpdate2.getAuditedActions().size());
        Assert.assertEquals(1, actualUpdate2.getRecentlyClearedActions().size());
        final AuditedActionInfo recentlyClearedAction =
                actualUpdate2.getRecentlyClearedActions().get(0);
        Assert.assertEquals(ACTION1_ID, recentlyClearedAction.getRecommendationId());
        Assert.assertEquals(KAFKA_ONGEN_WORKFLOW_ID, recentlyClearedAction.getWorkflowId());
        Assert.assertTrue(recentlyClearedAction.getClearedTimestamp().isPresent());

        Mockito.reset(auditedActionsManager, messageSender);
        final List<AuditedActionInfo> alreadySentActions2 =
                Collections.singletonList(
                        new AuditedActionInfo(actionForAudit.getRecommendationOid(), KAFKA_ONGEN_WORKFLOW_ID,
                                // still 1 millisecond until expiry, so this action should not be cleared
                                Optional.of(CURRENT_TIME - MINUTES_UNTIL_MARKED_CLEARED + 1)));
        when(auditedActionsManager.getAlreadySentActions()).thenReturn(alreadySentActions2);
        when(auditedActionsManager.isAlreadySent(ACTION1_ID, KAFKA_ONGEN_WORKFLOW_ID)).thenReturn(true);
        // III. `actionForAudit` is come back before meeting CLEARED criteria, so we don't send
        // it as CLEARED and reset cleared_timestamp
        actionAuditSender.sendActionEvents(Collections.singletonList(actionForAudit));

        // don't send any actions for audit (`actionForAudit` was already sent for ON_GEN audit
        // and it shouldn't be send as CLEARED)
        Mockito.verify(messageSender, Mockito.times(0))
                .sendMessage(Mockito.any());

        // Check which actions ActionAuditSender wanted to persist
        Mockito.verify(auditedActionsManager, Mockito.times(1))
                .persistAuditedActionsUpdates(persistedActionsCaptor.capture());
        final AuditedActionsUpdate actualUpdate3 = persistedActionsCaptor.getValue();
        Assert.assertEquals(1, actualUpdate3.getAuditedActions().size());
        final AuditedActionInfo actionWithResetClearedTimestamp =
                actualUpdate3.getAuditedActions().get(0);
        Assert.assertEquals(ACTION1_ID, actionWithResetClearedTimestamp.getRecommendationId());
        Assert.assertEquals(KAFKA_ONGEN_WORKFLOW_ID,
                actionWithResetClearedTimestamp.getWorkflowId());
        Assert.assertEquals(Optional.empty(),
                actionWithResetClearedTimestamp.getClearedTimestamp());
    }

    /**
     * Tests resending of earlier audited action events.
     *
     * @throws Exception if something goes wrong
     */
    @Test
    public void testResendAuditedActions() throws Exception {
        final Action actionForAudit = createAction(ACTION1_ID, KAFKA_ONGEN_WORKFLOW);
        when(auditedActionsManager.getAlreadySentActions()).thenReturn(Collections.emptyList());
        // I. send `actionForAudit` first time
        actionAuditSender.sendActionEvents(Collections.singletonList(actionForAudit));

        // `actionForAudit` will be send for audit as a new action.
        Mockito.verify(messageSender, Mockito.times(1))
                .sendMessage(sentActionEventMessageCaptor.capture());
        final ActionEvent sendEvent = sentActionEventMessageCaptor.getValue();
        Assert.assertEquals(actionForAudit.getRecommendationOid(),
                sendEvent.getActionRequest().getActionId());

        Mockito.reset(auditedActionsManager, messageSender);

        // II. resend `actionForAudit`
        actionAuditSender.resendActionEvents(Collections.singletonList(actionForAudit));
        Mockito.verify(messageSender, Mockito.times(1))
                .sendMessage(sentActionEventMessageCaptor.capture());
        final ActionEvent reSentEvent = sentActionEventMessageCaptor.getValue();
        Assert.assertEquals(actionForAudit.getRecommendationOid(),
                reSentEvent.getActionRequest().getActionId());

        // tests that doing resend we don't modify data in bookkeeping cache
        Mockito.verify(auditedActionsManager, Mockito.never())
                .persistAuditedActionsUpdates(Mockito.any(AuditedActionsUpdate.class));
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
        when(action.getWorkflow(Mockito.any(), Mockito.any()))
                .thenReturn(Optional.ofNullable(workflow));
        return action;
    }
}
