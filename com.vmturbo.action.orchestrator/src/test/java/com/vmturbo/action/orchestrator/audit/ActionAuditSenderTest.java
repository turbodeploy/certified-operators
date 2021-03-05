package com.vmturbo.action.orchestrator.audit;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;

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
import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.action.orchestrator.action.AuditedActionInfo;
import com.vmturbo.action.orchestrator.action.AuditedActionsManager;
import com.vmturbo.action.orchestrator.action.AuditedActionsManager.AuditedActionsUpdate;
import com.vmturbo.action.orchestrator.dto.ActionMessages.ActionEvent;
import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory.EntitiesAndSettingsSnapshot;
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
import com.vmturbo.common.protobuf.setting.SettingProto;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.StringSettingValue;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.Workflow;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.WorkflowInfo;
import com.vmturbo.components.api.server.IMessageSender;
import com.vmturbo.components.common.setting.ActionSettingSpecs;
import com.vmturbo.components.common.setting.ActionSettingType;
import com.vmturbo.components.common.setting.ConfigurableActionSettings;
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
    private static final long KAFKA_ONGEN_WORKFLOW_ID_2 = 2002L;
    private static final long KAFKA_AFTEREXEC_WORKFLOW_ID = 2003L;
    private static final long SERVICENOW_ONGEN_WORKFLOW_ID = 2004L;
    private static final long SERVICENOW_ONGEN_WORKFLOW_ID_2 = 2005L;
    private static final long SERVICENOW_AFTEREXEC_WORKFLOW_ID = 2006L;
    private static final long WORKFLOW_ID3 = 2003L;
    private static final long KAFKA_TARGET_ID = 3001L;
    private static final long SERVICENOW_TARGET_ID = 3002L;
    private static final long ACTION1_ID = 4001L;
    private static final long ACTION2_ID = 4002L;
    private static final long ACTION3_ID = 4003L;
    private static final long ACTION4_ID = 4004L;
    private static final long ACTION5_ID = 4005L;
    private static final long ACTION6_ID = 4006L;
    private static final long ACTION7_ID = 4007L;
    private static final long ACTION8_ID = 4008L;
    private static final long TARGET_ENTITY_ID_1 = 5001L;
    private static final long TARGET_ENTITY_ID_2 = 5002L;
    private static final long TARGET_ENTITY_ID_3 = 5003L;
    private static final long TARGET_ENTITY_ID_4 = 5004L;
    private static final long TARGET_ENTITY_ID_5 = 5005L;
    private static final long TARGET_ENTITY_ID_6 = 5006L;
    private static final long TARGET_ENTITY_ID_7 = 5007L;
    private static final long TARGET_ENTITY_ID_8 = 5008L;
    private static final String VMEM_RESIZE_UP_ONGEN = ActionSettingSpecs.getSubSettingFromActionModeSetting(
        ConfigurableActionSettings.ResizeVmemUpInBetweenThresholds,
        ActionSettingType.ON_GEN);
    private static final String UNRELATED_SETTING_NAME = "unrelatedSetting";
    private static final String INVALID_SETTING_VALUE = "ThisIsNotANumber";

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
    @Mock
    private EntitiesAndSettingsSnapshot entitiesAndSettingsSnapshot;

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

    private static final Workflow KAFKA_ONGEN_WORKFLOW = Workflow.newBuilder()
        .setId(KAFKA_ONGEN_WORKFLOW_ID)
        .setWorkflowInfo(
            WorkflowInfo.newBuilder()
                .setActionPhase(ActionPhase.ON_GENERATION)
                .setTargetId(KAFKA_TARGET_ID))
        .build();
    private static final Workflow KAFKA_ONGEN_WORKFLOW_2 = Workflow.newBuilder()
        .setId(KAFKA_ONGEN_WORKFLOW_ID_2)
        .setWorkflowInfo(
            WorkflowInfo.newBuilder()
                .setActionPhase(ActionPhase.ON_GENERATION)
                .setTargetId(KAFKA_TARGET_ID))
        .build();

    private static final Workflow KAFKA_AFTEREXEC_WORKFLOW = Workflow.newBuilder()
        .setId(KAFKA_AFTEREXEC_WORKFLOW_ID)
        .setWorkflowInfo(
            WorkflowInfo.newBuilder()
                .setActionPhase(ActionPhase.AFTER_EXECUTION)
                .setTargetId(KAFKA_TARGET_ID))
        .build();

    private static final Workflow SERVICENOW_ONGEN_WORKFLOW = Workflow.newBuilder()
        .setId(SERVICENOW_ONGEN_WORKFLOW_ID)
        .setWorkflowInfo(
            WorkflowInfo.newBuilder()
                .setActionPhase(ActionPhase.ON_GENERATION)
                .setTargetId(SERVICENOW_TARGET_ID))
        .build();
    private static final Workflow SERVICENOW_ONGEN_WORKFLOW_2 = Workflow.newBuilder()
        .setId(SERVICENOW_ONGEN_WORKFLOW_ID_2)
        .setWorkflowInfo(
            WorkflowInfo.newBuilder()
                .setActionPhase(ActionPhase.ON_GENERATION)
                .setTargetId(SERVICENOW_TARGET_ID))
        .build();

    private static final Workflow SERVICENOW_AFTEREXEC_WORKFLOW = Workflow.newBuilder()
        .setId(SERVICENOW_AFTEREXEC_WORKFLOW_ID).setWorkflowInfo(
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
        when(workflowStore.fetchWorkflow(SERVICENOW_ONGEN_WORKFLOW_ID)).thenReturn(Optional.of(SERVICENOW_ONGEN_WORKFLOW));
        when(workflowStore.fetchWorkflow(SERVICENOW_ONGEN_WORKFLOW_ID_2)).thenReturn(Optional.of(SERVICENOW_ONGEN_WORKFLOW_2));

        when(thinTargetCache.getTargetInfo(KAFKA_TARGET_ID)).thenReturn(Optional.of(KAFKA_TARGET));
        when(thinTargetCache.getTargetInfo(SERVICENOW_TARGET_ID)).thenReturn(Optional.of(SERVICENOW_TARGET));

        when(entitiesAndSettingsSnapshot.getSettingsForEntity(anyLong())).thenReturn(Collections.emptyMap());

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
        actionAuditSender.sendOnGenerationEvents(Arrays.asList(action1, action2, action3, action4), entitiesAndSettingsSnapshot);
        Mockito.verify(messageSender, Mockito.times(2)).sendMessage(sentActionEventMessageCaptor.capture());

        final ActionEvent event1 = sentActionEventMessageCaptor.getAllValues().get(0);
        Assert.assertEquals(KAFKA_TARGET_ID, event1.getActionRequest().getTargetId());
        Assert.assertEquals(action1.getRecommendationOid(), event1.getActionRequest().getActionId());
        Assert.assertEquals(ActionResponseState.PENDING_ACCEPT, event1.getOldState());
        Assert.assertEquals(ActionResponseState.PENDING_ACCEPT, event1.getNewState());

        final ActionEvent event2 = sentActionEventMessageCaptor.getAllValues().get(1);
        Assert.assertEquals(SERVICENOW_TARGET_ID, event2.getActionRequest().getTargetId());
        Assert.assertEquals(action2.getRecommendationOid(), event2.getActionRequest().getActionId());
        Assert.assertEquals(ActionResponseState.PENDING_ACCEPT, event2.getOldState());
        Assert.assertEquals(ActionResponseState.PENDING_ACCEPT, event2.getNewState());
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
        actionAuditSender.sendOnGenerationEvents(Arrays.asList(action1, action2), entitiesAndSettingsSnapshot);
        Mockito.verify(messageSender, Mockito.times(2)).sendMessage(sentActionEventMessageCaptor.capture());

        final ActionEvent event1 = sentActionEventMessageCaptor.getAllValues().get(0);
        Assert.assertEquals(action1.getRecommendationOid(),
                event1.getActionRequest().getActionId());

        final ActionEvent event2 = sentActionEventMessageCaptor.getAllValues().get(1);
        Assert.assertEquals(action2.getRecommendationOid(),
                event2.getActionRequest().getActionId());

        Mockito.reset(messageSender);
        actionAuditSender.sendOnGenerationEvents(Arrays.asList(action1, action2, action3), entitiesAndSettingsSnapshot);
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
        final Action action1 = createAction(ACTION1_ID, TARGET_ENTITY_ID_1, KAFKA_ONGEN_WORKFLOW);
        when(action1.getWorkflowSetting(ActionState.READY))
            .thenReturn(Optional.of(createWorkflowSetting(VMEM_RESIZE_UP_ONGEN, KAFKA_ONGEN_WORKFLOW_ID)));
        final Action action2 = createAction(ACTION2_ID, TARGET_ENTITY_ID_2, KAFKA_ONGEN_WORKFLOW);
        when(action2.getWorkflowSetting(ActionState.READY))
            .thenReturn(Optional.of(createWorkflowSetting(VMEM_RESIZE_UP_ONGEN, KAFKA_ONGEN_WORKFLOW_ID)));
        final Action action3 = createAction(ACTION3_ID, TARGET_ENTITY_ID_3, SERVICENOW_ONGEN_WORKFLOW);
        when(action3.getWorkflowSetting(ActionState.READY))
            .thenReturn(Optional.of(createWorkflowSetting(VMEM_RESIZE_UP_ONGEN, SERVICENOW_ONGEN_WORKFLOW_ID)));
        final Action action4 = createAction(ACTION4_ID, TARGET_ENTITY_ID_4, SERVICENOW_AFTEREXEC_WORKFLOW);
        when(action4.getWorkflowSetting(ActionState.READY))
            .thenReturn(Optional.of(createWorkflowSetting(VMEM_RESIZE_UP_ONGEN, SERVICENOW_ONGEN_WORKFLOW_ID)));
        actionAuditSender.sendOnGenerationEvents(
            Arrays.asList(action1, action2, action3, action4),
            entitiesAndSettingsSnapshot);

        // Only 3 actions because the 4th action is AFTEREXEC. AFTEREXEC needs to come through
        // Action sendAfterExecutionEvents instead of sendOnGenerationEvents.
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
                new AuditedActionInfo(ACTION1_ID, KAFKA_ONGEN_WORKFLOW_ID, TARGET_ENTITY_ID_1, VMEM_RESIZE_UP_ONGEN, Optional.empty()),
                new AuditedActionInfo(ACTION2_ID, KAFKA_ONGEN_WORKFLOW_ID, TARGET_ENTITY_ID_2, VMEM_RESIZE_UP_ONGEN, Optional.empty())
            ),
            actualUpdate.getAuditedActions());
        Assert.assertEquals(Collections.emptyList(), actualUpdate.getRecentlyClearedActions());
        Assert.assertEquals(Collections.emptyList(), actualUpdate.getRemovedAudits());
        Assert.assertEquals(Collections.emptyList(), actualUpdate.getRemovedActionRecommendationOid());
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

        final Action actionAlreadySentToKafka = createAction(ACTION1_ID, TARGET_ENTITY_ID_1, KAFKA_ONGEN_WORKFLOW);
        when(actionAlreadySentToKafka.getWorkflowSetting(ActionState.READY))
            .thenReturn(Optional.of(createWorkflowSetting(VMEM_RESIZE_UP_ONGEN, KAFKA_ONGEN_WORKFLOW_ID)));
        final Action actionNotSentToKafka = createAction(ACTION2_ID, TARGET_ENTITY_ID_2, KAFKA_ONGEN_WORKFLOW);
        when(actionNotSentToKafka.getWorkflowSetting(ActionState.READY))
            .thenReturn(Optional.of(createWorkflowSetting(VMEM_RESIZE_UP_ONGEN, KAFKA_ONGEN_WORKFLOW_ID)));
        final Action actionSentToServiceNOW = createAction(ACTION3_ID, TARGET_ENTITY_ID_3, SERVICENOW_ONGEN_WORKFLOW);
        actionAuditSender.sendOnGenerationEvents(Arrays.asList(actionAlreadySentToKafka,
                actionNotSentToKafka, actionSentToServiceNOW), entitiesAndSettingsSnapshot);

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
            Arrays.asList(new AuditedActionInfo(ACTION2_ID, KAFKA_ONGEN_WORKFLOW_ID, TARGET_ENTITY_ID_2, VMEM_RESIZE_UP_ONGEN, Optional.empty())),
            actualUpdate.getAuditedActions());
        Assert.assertEquals(Collections.emptyList(), actualUpdate.getRecentlyClearedActions());
        Assert.assertEquals(Collections.emptyList(), actualUpdate.getRemovedAudits());
        Assert.assertEquals(Collections.emptyList(), actualUpdate.getRemovedActionRecommendationOid());
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
                        TARGET_ENTITY_ID_1, VMEM_RESIZE_UP_ONGEN,
                        // still 1 millisecond until expiry, so this action should not be cleared
                        Optional.of(CURRENT_TIME - TimeUnit.MINUTES.toMillis(MINUTES_UNTIL_MARKED_CLEARED) + 1)),
                new AuditedActionInfo(actionClearedAndExpired.getRecommendationOid(),
                        KAFKA_ONGEN_WORKFLOW_ID,
                        TARGET_ENTITY_ID_2, VMEM_RESIZE_UP_ONGEN,
                        // still 1 millisecond after expiry, so this action should be cleared
                        Optional.of(CURRENT_TIME - TimeUnit.MINUTES.toMillis(MINUTES_UNTIL_MARKED_CLEARED) - 1)),
                // actionJustExpiring was sent last cycle, but not this cycle
                new AuditedActionInfo(actionJustExpiring.getRecommendationOid(), KAFKA_ONGEN_WORKFLOW_ID,
                        TARGET_ENTITY_ID_3, VMEM_RESIZE_UP_ONGEN,
                        // was recommended in the last market cycle, so no cleared timestamp
                        Optional.empty()));
        when(auditedActionsManager.getAlreadySentActions()).thenReturn(alreadySentActions);
        final Map<String, Setting> settingsMap = new HashMap<>();
        settingsMap.put(VMEM_RESIZE_UP_ONGEN, Setting.newBuilder()
                .setStringSettingValue(StringSettingValue.newBuilder()
                        .setValue(String.valueOf(KAFKA_ONGEN_WORKFLOW_ID))
                        .build())
                .setSettingSpecName(VMEM_RESIZE_UP_ONGEN)
                .build());
        when(entitiesAndSettingsSnapshot.getSettingsForEntity(TARGET_ENTITY_ID_1)).thenReturn(
                settingsMap);
        when(entitiesAndSettingsSnapshot.getSettingsForEntity(TARGET_ENTITY_ID_2)).thenReturn(
                settingsMap);
        when(entitiesAndSettingsSnapshot.getSettingsForEntity(TARGET_ENTITY_ID_3)).thenReturn(
                settingsMap);

        actionAuditSender.sendOnGenerationEvents(Collections.singletonList(actionGoingToServiceNOW), entitiesAndSettingsSnapshot);

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
                        TARGET_ENTITY_ID_3, VMEM_RESIZE_UP_ONGEN,
                        Optional.of(CURRENT_TIME))), actualUpdate.getRecentlyClearedActions());
        Assert.assertEquals(Collections.singletonList(
                new AuditedActionInfo(ACTION2_ID, KAFKA_ONGEN_WORKFLOW_ID,
                    TARGET_ENTITY_ID_2, VMEM_RESIZE_UP_ONGEN,
                    Optional.empty())),
                actualUpdate.getRemovedAudits());
        Assert.assertEquals(Collections.emptyList(), actualUpdate.getRemovedActionRecommendationOid());
    }

    /**
     * Test removing all earlier audited actions from bookkeeping cache because they don't have
     * associated audit ON_GEN workflow.
     *
     * @throws Exception shouldn't be thrown
     */
    @Test
    public void testRemovingClearedActionWithoutAssociatedWorkflow() throws Exception {
        final Action actionClearedButNotExpired = createAction(ACTION1_ID, KAFKA_ONGEN_WORKFLOW);
        final AuditedActionInfo auditClearedButNotExpired = new AuditedActionInfo(
            actionClearedButNotExpired.getRecommendationOid(),
            KAFKA_ONGEN_WORKFLOW_ID, TARGET_ENTITY_ID_1, VMEM_RESIZE_UP_ONGEN,
            // still 1 millisecond until expiry, so this action should not be cleared
            Optional.of(CURRENT_TIME - TimeUnit.MINUTES.toMillis(
                MINUTES_UNTIL_MARKED_CLEARED) + 1));
        final Action actionClearedAndExpired = createAction(ACTION2_ID, KAFKA_ONGEN_WORKFLOW);
        final AuditedActionInfo auditClearedAndExpired = new AuditedActionInfo(
            actionClearedAndExpired.getRecommendationOid(),
            KAFKA_ONGEN_WORKFLOW_ID, TARGET_ENTITY_ID_2, VMEM_RESIZE_UP_ONGEN,
            // still 1 millisecond after expiry, so this action should be cleared
            Optional.of(CURRENT_TIME - TimeUnit.MINUTES.toMillis(
                MINUTES_UNTIL_MARKED_CLEARED) - 1));
        final Action actionJustExpiring = createAction(ACTION3_ID, KAFKA_ONGEN_WORKFLOW);
        // actionJustExpiring was sent last cycle, but not this cycle
        final AuditedActionInfo auditJustExpiring = new AuditedActionInfo(
            actionJustExpiring.getRecommendationOid(),
                KAFKA_ONGEN_WORKFLOW_ID, TARGET_ENTITY_ID_3, VMEM_RESIZE_UP_ONGEN,
                // was recommended in the last market cycle, so no cleared timestamp
                Optional.empty());
        final Action actionGoingToServiceNOW = createAction(ACTION4_ID, SERVICENOW_ONGEN_WORKFLOW);

        final List<AuditedActionInfo> alreadySentActions = Arrays.asList(
            auditClearedButNotExpired,
            auditClearedAndExpired,
            auditJustExpiring);
        when(auditedActionsManager.getAlreadySentActions()).thenReturn(alreadySentActions);
        when(entitiesAndSettingsSnapshot.getSettingsForEntity(TARGET_ENTITY_ID_1)).thenReturn(
                Collections.emptyMap());
        when(entitiesAndSettingsSnapshot.getSettingsForEntity(TARGET_ENTITY_ID_2)).thenReturn(
                Collections.emptyMap());
        when(entitiesAndSettingsSnapshot.getSettingsForEntity(TARGET_ENTITY_ID_3)).thenReturn(
                Collections.emptyMap());

        actionAuditSender.sendOnGenerationEvents(Collections.singletonList(actionGoingToServiceNOW),
                entitiesAndSettingsSnapshot);

        // Check which actions ActionAuditSender wanted to persist
        Mockito.verify(auditedActionsManager, Mockito.times(1))
                .persistAuditedActionsUpdates(persistedActionsCaptor.capture());
        AuditedActionsUpdate actualUpdate = persistedActionsCaptor.getValue();

        // Check that we remove all actions (besides actionGoingToServiceNOW, because we don't
        // persist SNOW-related actions in bookkeeping) from bookkeeping cache because they don't have
        // associated audit ON_GEN workflow
        Assert.assertEquals(Arrays.asList(auditClearedButNotExpired, auditClearedAndExpired, auditJustExpiring),
                actualUpdate.getRemovedAudits());
        Assert.assertEquals(Collections.emptyList(), actualUpdate.getRemovedActionRecommendationOid());
    }

    /**
     * Only non-ServiceNOW actions with changed or removed workflows should have their book keeping
     * removed.
     * <ol>
     *     <li>When the action stays the same, no message should be sent and no new book keeping is
     *         needed.</li>
     *     <li>When the action has a new workflow, a message for the new workflow should be sent,
     *         the old book keeping removed, and the new book keeping added.</li>
     *     <li>When the workflow is removed from the action, no message should be sent and the
     *         book keeping should be removed.</li>
     *     <li>When all the settings for the action is removed, a message should not be sent and the
     *         book keeping should be removed.</li>
     *     <li>All ServiceNOW actions should always be resent and no book keeping should be recorded.</li>
     * </ol>
     *
     * @throws Exception should not be thrown.
     */
    @Test
    public void testRemovedOrChangedWorkflows() throws Exception {
        // Actions we previously sent under the previous workflow.
        when(auditedActionsManager.isAlreadySent(ACTION1_ID, KAFKA_ONGEN_WORKFLOW_ID)).thenReturn(true);
        when(auditedActionsManager.isAlreadySent(ACTION2_ID, KAFKA_ONGEN_WORKFLOW_ID)).thenReturn(true);
        when(auditedActionsManager.isAlreadySent(ACTION3_ID, KAFKA_ONGEN_WORKFLOW_ID)).thenReturn(true);
        when(auditedActionsManager.isAlreadySent(ACTION4_ID, KAFKA_ONGEN_WORKFLOW_ID)).thenReturn(true);
        when(auditedActionsManager.getAlreadySentActions()).thenReturn(Arrays.asList(
            new AuditedActionInfo(ACTION1_ID, KAFKA_ONGEN_WORKFLOW_ID, TARGET_ENTITY_ID_1, VMEM_RESIZE_UP_ONGEN, Optional.empty()),
            new AuditedActionInfo(ACTION2_ID, KAFKA_ONGEN_WORKFLOW_ID, TARGET_ENTITY_ID_2, VMEM_RESIZE_UP_ONGEN, Optional.empty()),
            new AuditedActionInfo(ACTION3_ID, KAFKA_ONGEN_WORKFLOW_ID, TARGET_ENTITY_ID_3, VMEM_RESIZE_UP_ONGEN, Optional.empty()),
            new AuditedActionInfo(ACTION4_ID, KAFKA_ONGEN_WORKFLOW_ID, TARGET_ENTITY_ID_4, VMEM_RESIZE_UP_ONGEN, Optional.empty())
        ));

        final Action actionStaysTheSame = createAction(ACTION1_ID, TARGET_ENTITY_ID_1, KAFKA_ONGEN_WORKFLOW);
        when(entitiesAndSettingsSnapshot.getSettingsForEntity(TARGET_ENTITY_ID_1)).thenReturn(
            ImmutableMap.<String, SettingProto.Setting>builder()
                .put(VMEM_RESIZE_UP_ONGEN, createWorkflowSetting(VMEM_RESIZE_UP_ONGEN, KAFKA_ONGEN_WORKFLOW_ID))
                .build());
        final Action actionNewWorkflow = createAction(ACTION2_ID, TARGET_ENTITY_ID_2, KAFKA_ONGEN_WORKFLOW_2);
        when(actionNewWorkflow.getWorkflowSetting(ActionState.READY))
            .thenReturn(Optional.of(createWorkflowSetting(VMEM_RESIZE_UP_ONGEN, KAFKA_ONGEN_WORKFLOW_ID_2)));
        when(entitiesAndSettingsSnapshot.getSettingsForEntity(TARGET_ENTITY_ID_2)).thenReturn(
            ImmutableMap.<String, SettingProto.Setting>builder()
                .put(VMEM_RESIZE_UP_ONGEN, createWorkflowSetting(VMEM_RESIZE_UP_ONGEN, KAFKA_ONGEN_WORKFLOW_ID_2))
                .build());
        final Action actionRemovedWorkflow = createAction(ACTION3_ID, TARGET_ENTITY_ID_3, null);
        when(entitiesAndSettingsSnapshot.getSettingsForEntity(TARGET_ENTITY_ID_3)).thenReturn(
            ImmutableMap.<String, SettingProto.Setting>builder()
                .put(UNRELATED_SETTING_NAME, createWorkflowSetting(UNRELATED_SETTING_NAME, KAFKA_ONGEN_WORKFLOW_ID))
                .build());
        // actionRemovedPolicy intentionally has no settings
        final Action actionRemovedPolicy = createAction(ACTION4_ID, TARGET_ENTITY_ID_4, null);
        final Action actionSNOWStaysTheSame = createAction(ACTION5_ID, TARGET_ENTITY_ID_5, SERVICENOW_ONGEN_WORKFLOW);
        final Action actionSNOWNewWorkflow = createAction(ACTION6_ID, TARGET_ENTITY_ID_6, SERVICENOW_ONGEN_WORKFLOW_2);
        actionAuditSender.sendOnGenerationEvents(
            Arrays.asList(
                actionStaysTheSame,
                actionNewWorkflow,
                actionRemovedWorkflow,
                actionRemovedPolicy,
                actionSNOWStaysTheSame,
                actionSNOWNewWorkflow
            ), entitiesAndSettingsSnapshot);

        // One action that changed ActionStream workflows, and 2 ServiceNOW actions that
        // ActionAuditSender always resends.
        Mockito.verify(messageSender, Mockito.times(3)).sendMessage(sentActionEventMessageCaptor.capture());
        Assert.assertEquals(actionNewWorkflow.getRecommendationOid(),
            sentActionEventMessageCaptor.getAllValues().get(0).getActionRequest().getActionId());
        Assert.assertEquals(actionSNOWStaysTheSame.getRecommendationOid(),
            sentActionEventMessageCaptor.getAllValues().get(1).getActionRequest().getActionId());
        Assert.assertEquals(actionSNOWNewWorkflow.getRecommendationOid(),
            sentActionEventMessageCaptor.getAllValues().get(2).getActionRequest().getActionId());

        // Check which actions ActionAuditSender wanted to persist
        Mockito.verify(auditedActionsManager, Mockito.times(1))
            .persistAuditedActionsUpdates(persistedActionsCaptor.capture());
        AuditedActionsUpdate actualUpdate = persistedActionsCaptor.getValue();
        Assert.assertEquals(
            Arrays.asList(new AuditedActionInfo(ACTION2_ID, KAFKA_ONGEN_WORKFLOW_ID_2, TARGET_ENTITY_ID_2, VMEM_RESIZE_UP_ONGEN, Optional.empty())),
            actualUpdate.getAuditedActions());
        Assert.assertEquals(Collections.emptyList(), actualUpdate.getRecentlyClearedActions());
        Assert.assertEquals(Collections.emptyList(), actualUpdate.getRemovedActionRecommendationOid());
        final List<AuditedActionInfo> expectedRemovedAudits = Arrays.asList(
            new AuditedActionInfo(actionNewWorkflow.getRecommendationOid(), KAFKA_ONGEN_WORKFLOW_ID, TARGET_ENTITY_ID_2, VMEM_RESIZE_UP_ONGEN, Optional.empty()),
            new AuditedActionInfo(actionRemovedWorkflow.getRecommendationOid(), KAFKA_ONGEN_WORKFLOW_ID, TARGET_ENTITY_ID_3, VMEM_RESIZE_UP_ONGEN, Optional.empty()),
            new AuditedActionInfo(actionRemovedPolicy.getRecommendationOid(), KAFKA_ONGEN_WORKFLOW_ID, TARGET_ENTITY_ID_4, VMEM_RESIZE_UP_ONGEN, Optional.empty())
        );
        Assert.assertEquals(expectedRemovedAudits, actualUpdate.getRemovedAudits());
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
     * @throws Exception shouldn't be thrown
     */
    @Test
    public void testResetOfClearedTimestamp() throws Exception {
        final Action actionForAudit = createAction(ACTION1_ID, KAFKA_ONGEN_WORKFLOW);
        when(auditedActionsManager.getAlreadySentActions()).thenReturn(Collections.emptyList());
        // I. send `actionForAudit` first time
        actionAuditSender.sendOnGenerationEvents(Collections.singletonList(actionForAudit), entitiesAndSettingsSnapshot);

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
                                TARGET_ENTITY_ID_1, VMEM_RESIZE_UP_ONGEN,
                                Optional.empty()));

        when(auditedActionsManager.getAlreadySentActions()).thenReturn(alreadySentActions);
        when(auditedActionsManager.isAlreadySent(ACTION1_ID, KAFKA_ONGEN_WORKFLOW_ID)).thenReturn(true);
        final Map<String, Setting> settingsMap = new HashMap<>();
        settingsMap.put(VMEM_RESIZE_UP_ONGEN, Setting.newBuilder()
                .setStringSettingValue(StringSettingValue.newBuilder()
                        .setValue(String.valueOf(KAFKA_ONGEN_WORKFLOW_ID))
                        .build())
                .setSettingSpecName(VMEM_RESIZE_UP_ONGEN)
                .build());
        when(entitiesAndSettingsSnapshot.getSettingsForEntity(TARGET_ENTITY_ID_1)).thenReturn(
                settingsMap);
        // II. `actionForAudit` is not recommended now, but we audited it before, so we need to
        // update cleared_timestamp for it (without sending it as CLEARED, because don't met
        // cleared criteria)
        actionAuditSender.sendOnGenerationEvents(Collections.emptyList(), entitiesAndSettingsSnapshot);

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
                                TARGET_ENTITY_ID_1, VMEM_RESIZE_UP_ONGEN,
                                // still 1 millisecond until expiry, so this action should not be cleared
                                Optional.of(CURRENT_TIME - MINUTES_UNTIL_MARKED_CLEARED + 1)));
        when(auditedActionsManager.getAlreadySentActions()).thenReturn(alreadySentActions2);
        when(auditedActionsManager.isAlreadySent(ACTION1_ID, KAFKA_ONGEN_WORKFLOW_ID)).thenReturn(true);
        // III. `actionForAudit` is come back before meeting CLEARED criteria, so we don't send
        // it as CLEARED and reset cleared_timestamp
        actionAuditSender.sendOnGenerationEvents(Collections.singletonList(actionForAudit), entitiesAndSettingsSnapshot);

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
        actionAuditSender.sendOnGenerationEvents(Collections.singletonList(actionForAudit), entitiesAndSettingsSnapshot);

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

    /**
     * Calling {@link ActionAuditSender#sendAfterExecutionEvents(ActionView)} should not send
     * on-gen action workflows.
     *
     * @throws Exception exceptions should not be thrown.
     */
    @Test
    public void testCallAfterExecWithOnGen() throws Exception {
        final Action kafkaOnGenAction = createAction(ACTION1_ID, KAFKA_ONGEN_WORKFLOW);
        final Action serviceNowOnGenAction = createAction(ACTION2_ID, SERVICENOW_ONGEN_WORKFLOW);

        actionAuditSender.sendAfterExecutionEvents(kafkaOnGenAction);
        actionAuditSender.sendAfterExecutionEvents(serviceNowOnGenAction);
        verify(auditedActionsManager, times(0)).persistAuditedActionsUpdates(any());
        verify(messageSender, times(0)).sendMessage(any());
    }

    /**
     * Calling {@link ActionAuditSender#sendAfterExecutionEvents(ActionView)} should send messages
     * to topology processor. However, only non-servicenow workflows should handle book keeping.
     *
     * @throws Exception exceptions should not be thrown.
     */
    @Test
    public void testAfterExec() throws Exception {
        final Action kafkaAfterExecAction = createAction(ACTION1_ID, KAFKA_AFTEREXEC_WORKFLOW, ActionState.SUCCEEDED);
        final Action serviceNowAfterExecAction = createAction(ACTION2_ID, SERVICENOW_AFTEREXEC_WORKFLOW, ActionState.FAILED);
        final Action unsupportedActionState = createAction(ACTION3_ID, KAFKA_AFTEREXEC_WORKFLOW, null);

        actionAuditSender.sendAfterExecutionEvents(kafkaAfterExecAction);
        actionAuditSender.sendAfterExecutionEvents(serviceNowAfterExecAction);
        actionAuditSender.sendAfterExecutionEvents(unsupportedActionState);

        // serviceNowAfterExecAction not included because it's not supported by book keeping yet
        // unsupportedActionState not included because it's not in an after exec state
        verify(auditedActionsManager, times(1))
            .persistAuditedActionsUpdates(persistedActionsCaptor.capture());
        Assert.assertEquals(Arrays.asList(kafkaAfterExecAction.getRecommendationOid()),
            persistedActionsCaptor.getValue().getRemovedActionRecommendationOid());

        // unsupportedActionState not included because it's not in an after exec state
        verify(messageSender, times(2))
            .sendMessage(sentActionEventMessageCaptor.capture());
        ActionEvent kafkaActionSent = sentActionEventMessageCaptor.getAllValues().get(0);
        Assert.assertEquals(kafkaAfterExecAction.getRecommendationOid(),
            kafkaActionSent.getActionRequest().getActionId());
        Assert.assertEquals(ActionResponseState.IN_PROGRESS,
            kafkaActionSent.getOldState());
        Assert.assertEquals(ActionResponseState.SUCCEEDED,
            kafkaActionSent.getNewState());
        ActionEvent serviceNowActionSent = sentActionEventMessageCaptor.getAllValues().get(1);
        Assert.assertEquals(serviceNowAfterExecAction.getRecommendationOid(),
            serviceNowActionSent.getActionRequest().getActionId());
        Assert.assertEquals(ActionResponseState.IN_PROGRESS,
            serviceNowActionSent.getOldState());
        Assert.assertEquals(ActionResponseState.FAILED,
            serviceNowActionSent.getNewState());
    }

    /**
     * When the actions' target entity has a setting value that is not an integer, skip processing
     * it and do not fail action processing.
     *
     * @throws Exception should not be thrown.
     */
    @Test
    public void testNonLongSettingShouldNotBreakAuditSender() throws Exception {
        // Actions we previously sent under the previous workflow.
        when(auditedActionsManager.isAlreadySent(ACTION1_ID, KAFKA_ONGEN_WORKFLOW_ID)).thenReturn(true);
        when(auditedActionsManager.isAlreadySent(ACTION2_ID, KAFKA_ONGEN_WORKFLOW_ID)).thenReturn(true);
        when(auditedActionsManager.getAlreadySentActions()).thenReturn(Arrays.asList(
            new AuditedActionInfo(ACTION1_ID, KAFKA_ONGEN_WORKFLOW_ID, TARGET_ENTITY_ID_1, VMEM_RESIZE_UP_ONGEN, Optional.empty()),
            new AuditedActionInfo(ACTION2_ID, KAFKA_ONGEN_WORKFLOW_ID, TARGET_ENTITY_ID_2, VMEM_RESIZE_UP_ONGEN, Optional.empty())
        ));

        final Action actionGetsInvalidSetting = createAction(ACTION1_ID, TARGET_ENTITY_ID_1, KAFKA_ONGEN_WORKFLOW);
        when(entitiesAndSettingsSnapshot.getSettingsForEntity(TARGET_ENTITY_ID_1)).thenReturn(
            ImmutableMap.<String, SettingProto.Setting>builder()
                .put(VMEM_RESIZE_UP_ONGEN, createWorkflowSetting(VMEM_RESIZE_UP_ONGEN, INVALID_SETTING_VALUE))
                .build());
        // action cleared, but there is still a setting for the target entity
        when(entitiesAndSettingsSnapshot.getSettingsForEntity(TARGET_ENTITY_ID_2)).thenReturn(
            ImmutableMap.<String, SettingProto.Setting>builder()
                .put(VMEM_RESIZE_UP_ONGEN, createWorkflowSetting(VMEM_RESIZE_UP_ONGEN, KAFKA_ONGEN_WORKFLOW_ID))
                .build());
        actionAuditSender.sendOnGenerationEvents(
            Arrays.asList(actionGetsInvalidSetting),
            entitiesAndSettingsSnapshot);

        // No action is sent because they are already in book keeping.
        Mockito.verify(messageSender, Mockito.times(0)).sendMessage(sentActionEventMessageCaptor.capture());
        // Check which actions ActionAuditSender wanted to persist
        Mockito.verify(auditedActionsManager, Mockito.times(1))
            .persistAuditedActionsUpdates(persistedActionsCaptor.capture());
        AuditedActionsUpdate actualUpdate = persistedActionsCaptor.getValue();
        // no new audits since the action was already sent and had an invalid setting value
        Assert.assertEquals(Collections.emptyList(), actualUpdate.getAuditedActions());
        // the other action was not in the most recent broadcast so it recently cleared
        Assert.assertEquals(
            Arrays.asList(
                new AuditedActionInfo(ACTION2_ID, KAFKA_ONGEN_WORKFLOW_ID, TARGET_ENTITY_ID_2, VMEM_RESIZE_UP_ONGEN, Optional.of(CURRENT_TIME))
            ),
            actualUpdate.getRecentlyClearedActions());
        Assert.assertEquals(Collections.emptyList(), actualUpdate.getRemovedActionRecommendationOid());
        Assert.assertEquals(Collections.emptyList(), actualUpdate.getRemovedAudits());
    }

    private Setting createWorkflowSetting(String settingName, long workflowOid) {
        return createWorkflowSetting(settingName, String.valueOf(workflowOid));
    }

    private Setting createWorkflowSetting(String settingName, String settingValue) {
        return Setting.newBuilder()
            .setSettingSpecName(settingName)
            .setStringSettingValue(
                StringSettingValue.newBuilder()
                    .setValue(String.valueOf(settingValue))
                    .build()
            )
            .build();
    }

    private Action createAction(
        long oid,
        @Nullable Workflow workflow) throws WorkflowStoreException {
        return createAction(oid, null, workflow);
    }

    private Action createAction(
        long oid,
        @Nullable Long nullableTargetEntityId,
        @Nullable Workflow workflow) throws WorkflowStoreException {
        return createAction(oid, nullableTargetEntityId, workflow, null);
    }

    private Action createAction(
        long oid,
        @Nullable Workflow workflow,
        @Nullable ActionState state) throws WorkflowStoreException {
        return createAction(oid, null, workflow, state);
    }

    private Action createAction(
            long oid,
            @Nullable Long nullableTargetEntityId,
            @Nullable Workflow workflow,
            @Nullable ActionState state) throws WorkflowStoreException {
        final long targetEntityId;
        if (nullableTargetEntityId == null) {
            targetEntityId = 10;
        } else {
            targetEntityId = nullableTargetEntityId;
        }
        final ActionDTO.Action actionDTO = ActionDTO.Action.newBuilder().setId(oid).setInfo(
                ActionInfo.newBuilder()
                        .setDelete(Delete.newBuilder().setTarget(ActionEntity.newBuilder()
                                .setId(targetEntityId)
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
        if (state != null) {
            when(action.getState()).thenReturn(state);
        }
        return action;
    }
}
