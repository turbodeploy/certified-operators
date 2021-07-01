package com.vmturbo.action.orchestrator.translation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableMap;

import io.grpc.Context;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.security.core.context.SecurityContextHolder;

import com.vmturbo.action.orchestrator.ActionOrchestratorTestUtils;
import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ActionEvent;
import com.vmturbo.action.orchestrator.action.ActionModeCalculator;
import com.vmturbo.action.orchestrator.action.ActionTranslation.TranslationStatus;
import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory.EntitiesAndSettingsSnapshot;
import com.vmturbo.action.orchestrator.topology.ActionGraphEntity;
import com.vmturbo.action.orchestrator.topology.ActionTopologyStore;
import com.vmturbo.auth.api.authorization.jwt.SecurityConstant;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionDecision;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionDecision.ClearingDecision;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.Compliance;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.MoveExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ReconfigureExplanation;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.TopologyType;
import com.vmturbo.common.protobuf.setting.SettingProto.ListSettingPoliciesRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.common.protobuf.setting.SettingProtoMoles.SettingPolicyServiceMole;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity.ActionEntityTypeSpecificInfo;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.graph.TopologyGraph;

public class ActionTranslatorTest {

    private ActionTranslator translator;
    private ActionModeCalculator actionModeCalculator;

    private static final long actionPlanId = 1234;

    private static final int CPU_SPEED_MHZ = 2000;
    private static final long VM_TARGET_ID = 2;
    private static final long HOST_ID = 99;
    private static final long CONTAINER_ID = 3;
    private static final long CONTAINER_POD_ID = 4;
    private static final double CONTAINER_CPU_SPEED = 2000;

    private static final long TOPOLOGY_CONTEXT_ID = 777777;

    private static final int OLD_VCPU_MHZ = 2000;
    private static final int NEW_VCPU_MHZ = 4000;

    private final SettingPolicyServiceMole settingPolicyServiceSpy = spy(new SettingPolicyServiceMole());
    private EntitiesAndSettingsSnapshot mockSnapshot = mock(EntitiesAndSettingsSnapshot.class);
    private final ActionTopologyStore actionTopologyStore = mock(ActionTopologyStore.class);

    @Rule
    public GrpcTestServer server = GrpcTestServer.newServer(settingPolicyServiceSpy);

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        when(actionTopologyStore.getSourceTopology()).thenReturn(Optional.empty());
        translator = new ActionTranslator(server.getChannel(), actionTopologyStore);
        actionModeCalculator = new ActionModeCalculator();

        when(mockSnapshot.getTopologyType()).thenReturn(TopologyType.SOURCE);
        when(mockSnapshot.getTopologyContextId()).thenReturn(TOPOLOGY_CONTEXT_ID);
    }

    @Test
    public void testTranslateMovePassthrough() {
        final Action move = new Action(ActionOrchestratorTestUtils.createMoveRecommendation(1), actionPlanId, actionModeCalculator, 2244L);
        translator.translate(Stream.of(move), mock(EntitiesAndSettingsSnapshot.class)).findFirst().get();
        assertEquals(move.getTranslationStatus(), TranslationStatus.TRANSLATION_SUCCEEDED);
    }

    @Test
    public void testTranslateResizeMemoryPassthrough() {
        final Action resize = new Action(
            ActionOrchestratorTestUtils.createResizeRecommendation(1, CommodityType.VMEM), actionPlanId, actionModeCalculator, 2244L);
        translator.translate(Stream.of(resize), mock(EntitiesAndSettingsSnapshot.class)).findFirst().get();
        assertEquals(resize.getTranslationStatus(), TranslationStatus.TRANSLATION_SUCCEEDED);
    }

    @Test
    public void testToSpecWithoutDecision() {
        final Action action = new Action(ActionOrchestratorTestUtils.createMoveRecommendation(1), actionPlanId, actionModeCalculator, 2244L);
        action.setDescription("Move VM1 from PM1 to PM2");
        final ActionSpec spec = translator.translateToSpec(action);

        assertTrue(spec.getIsExecutable());
        assertEquals(ActionState.READY, spec.getActionState());
        assertEquals(actionPlanId, spec.getActionPlanId());
        assertEquals(action.getRecommendation().getId(), spec.getRecommendation().getId());
        assertFalse(spec.hasDecision());
    }

    @Test
    public void testToSpecWithDecision() {
        final Action action = new Action(ActionOrchestratorTestUtils.createMoveRecommendation(1), actionPlanId, actionModeCalculator, 2244L);
        final long clearingPlanId = 5;
        action.receive(new ActionEvent.NotRecommendedEvent(clearingPlanId));
        action.setDescription("Move VM1 from PM1 to PM2");
        final ActionSpec spec = translator.translateToSpec(action);

        assertFalse(spec.getIsExecutable());
        assertEquals(ActionState.CLEARED, spec.getActionState());
        assertEquals(actionPlanId, spec.getActionPlanId());
        assertEquals(action.getRecommendation().getId(), spec.getRecommendation().getId());

        ActionDecision decision = spec.getDecision();
        assertEquals(clearingPlanId, decision.getClearingDecision().getActionPlanId());
        assertEquals(ClearingDecision.Reason.NO_LONGER_RECOMMENDED,
            decision.getClearingDecision().getReason());
    }

    @Test
    public void testToSpecUntranslated() {
        final Action action = new Action(ActionOrchestratorTestUtils.createMoveRecommendation(1), actionPlanId, actionModeCalculator, 2244L);
        action.setDescription("Move VM1 from PM1 to PM2");
        final ActionSpec spec = translator.translateToSpec(action);
        assertEquals(action.getRecommendation(), spec.getRecommendation());
    }

    @Test
    public void testToSpecTranslationFailed() {
        final Action action = new Action(ActionOrchestratorTestUtils.createMoveRecommendation(1), actionPlanId, actionModeCalculator, 2244L);
        action.getActionTranslation().setTranslationFailure();
        action.setDescription("Move VM1 from PM1 to PM2");
        final ActionSpec spec = translator.translateToSpec(action);

        assertEquals(action.getRecommendation(), spec.getRecommendation());
    }

    @Test
    public void testToSpecTranslationSucceeded() {
        final Action action = new Action(ActionOrchestratorTestUtils.createMoveRecommendation(1), actionPlanId, actionModeCalculator, 2244L);
        final ActionDTO.Action translatedRecommendation = ActionOrchestratorTestUtils.createMoveRecommendation(2);
        action.getActionTranslation().setTranslationSuccess(translatedRecommendation);

        action.setDescription("Move VM1 from PM1 to PM2");
        final ActionSpec spec = translator.translateToSpec(action);

        assertNotEquals(action.getRecommendation(), spec.getRecommendation());
        assertEquals(translatedRecommendation, spec.getRecommendation());
    }

    /**
     * Test case when the user has an observer role, all actions should be dropped to RECOMMEND as
     * the highest mode (DISABLED are generally filtered out, but if they are returned from AO,
     * their mode can stay DISABLED).
     */
    @Test
    public void testUserHasObserverRoleAllActionsDroppedToRecommend() {
        SecurityContextHolder.getContext().setAuthentication(null);
        final Action action = Mockito.spy(
                new Action(ActionOrchestratorTestUtils.createMoveRecommendation(1), actionPlanId,
                        actionModeCalculator, 2244L));
        // Roles, initial action mode, expected action mode.
        Stream.of(new Object[][]{
                {null, ActionMode.DISABLED, ActionMode.DISABLED},
                {Collections.singletonList(SecurityConstant.ADMINISTRATOR), ActionMode.AUTOMATIC,
                        ActionMode.AUTOMATIC},
                {Collections.singletonList(SecurityConstant.SITE_ADMIN), ActionMode.RECOMMEND,
                        ActionMode.RECOMMEND},
                {Collections.singletonList(SecurityConstant.AUTOMATOR), ActionMode.MANUAL,
                        ActionMode.MANUAL},
                {Collections.singletonList(SecurityConstant.OBSERVER), ActionMode.MANUAL,
                        ActionMode.RECOMMEND},
                {Collections.singletonList(SecurityConstant.ADVISOR), ActionMode.MANUAL,
                        ActionMode.RECOMMEND},
                {Collections.singletonList(SecurityConstant.OBSERVER), ActionMode.DISABLED,
                        ActionMode.DISABLED}}).forEach(data -> {
            final Context testContext = Context.current()
                    .withValue(SecurityConstant.USER_ROLES_KEY, (List<String>)data[0]);
            final Context previous = testContext.attach();
            try {
                Mockito.when(action.getMode()).thenReturn((ActionMode)data[1]);
                final ActionSpec actionSpec = translator.translateToSpec(action);
                Assert.assertEquals("expected action mode", data[2], actionSpec.getActionMode());
            } finally {
                testContext.detach(previous);
            }
        });
    }

    /**
     * Test {@link ActionTranslator#getReasonSettingPolicyIdToSettingPolicyNameMap}.
     */
    @Test
    public void testGetAllReasonSettings() {
        long reasonSetting1 = 1L;
        long reasonSetting2 = 2L;
        long reasonSetting3 = 3L;

        // Move action with a compliance explanation which is primary.
        Action moveAction1 = new Action(ActionDTO.Action.newBuilder()
            .setId(0).setInfo(ActionInfo.newBuilder())
            .setDeprecatedImportance(0)
            .setExplanation(Explanation.newBuilder()
                .setMove(MoveExplanation.newBuilder()
                    .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                        .setIsPrimaryChangeProviderExplanation(true)
                        .setCompliance(Compliance.newBuilder()
                        .addReasonSettings(reasonSetting1))))).build(),
            actionPlanId, actionModeCalculator, 2244L);

        // Move action with a compliance explanation which is not primary.
        Action moveAction2 = new Action(ActionDTO.Action.newBuilder()
            .setId(0).setInfo(ActionInfo.newBuilder())
            .setDeprecatedImportance(0)
            .setExplanation(Explanation.newBuilder()
                .setMove(MoveExplanation.newBuilder()
                    .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                        .setIsPrimaryChangeProviderExplanation(false)
                        .setCompliance(Compliance.newBuilder()
                            .addReasonSettings(reasonSetting2))))).build(),
            actionPlanId, actionModeCalculator, 2245L);

        Action reconfigureAction = new Action(ActionDTO.Action.newBuilder()
            .setId(0).setInfo(ActionInfo.newBuilder())
            .setDeprecatedImportance(0)
            .setExplanation(Explanation.newBuilder()
                .setReconfigure(ReconfigureExplanation.newBuilder()
                    .addReasonSettings(reasonSetting3))).build(),
            actionPlanId, actionModeCalculator, 2246L);

        // Return a list of SettingPolicies only when
        // the input request has reasonSetting1 and reasonSetting3 as the idFilter.
        // If the assertEquals() is true,
        // it means that we get the correct reasonSettings from the Actions.
        when(settingPolicyServiceSpy.listSettingPolicies(ListSettingPoliciesRequest.newBuilder()
            .addAllIdFilter(Arrays.asList(reasonSetting1, reasonSetting3)).build()))
            .thenReturn(Arrays.asList(
                SettingPolicy.newBuilder().setId(reasonSetting1).setInfo(
                    SettingPolicyInfo.newBuilder().setDisplayName("reasonSetting1")).build(),
                SettingPolicy.newBuilder().setId(reasonSetting3).setInfo(
                    SettingPolicyInfo.newBuilder().setDisplayName("reasonSetting3")).build()));

        final Map<Long, String> settingPolicyIdToSettingPolicyName =
            translator.getReasonSettingPolicyIdToSettingPolicyNameMap(
                Arrays.asList(moveAction1, moveAction2, reconfigureAction));
        assertEquals(ImmutableMap.of(reasonSetting1, "reasonSetting1", reasonSetting3, "reasonSetting3"),
            settingPolicyIdToSettingPolicyName);
    }

    /**
     * Failing actions should set hasFailure on {@link ActionSpec}. Actions that did not fail yet
     * should set the flag to false.
     */
    @Test
    public void testFailingState() {
        final Action actionFailing = spy(new Action(ActionOrchestratorTestUtils.createMoveRecommendation(1), actionPlanId, actionModeCalculator, 2244L));
        actionFailing.setDescription("Move VM1 from PM1 to PM2");
        when(actionFailing.getState()).thenReturn(ActionState.FAILING);
        final ActionSpec actionSpecFailing = translator.translateToSpec(actionFailing);
        assertEquals(ActionState.FAILING, actionSpecFailing.getActionState());
    }

    private TopologyGraph<ActionGraphEntity> mockTopologyGraph() {
        TopologyGraph<ActionGraphEntity> topologyGraph = mock(TopologyGraph.class);
        ActionGraphEntity containerPodEntity = ActionOrchestratorTestUtils.mockActionGraphEntity(CONTAINER_POD_ID, EntityType.CONTAINER_POD_VALUE);
        when(topologyGraph.getProviders(CONTAINER_ID)).thenReturn(Stream.of(containerPodEntity));

        ActionGraphEntity vmEntity = ActionOrchestratorTestUtils.mockActionGraphEntity(VM_TARGET_ID, EntityType.VIRTUAL_MACHINE_VALUE);
        when(topologyGraph.getEntity(vmEntity.getOid())).thenReturn(Optional.of(vmEntity));
        when(vmEntity.getActionEntityInfo()).thenReturn(ActionEntityTypeSpecificInfo.newBuilder().build());
        when(topologyGraph.getProviders(CONTAINER_POD_ID)).thenReturn(Stream.of(vmEntity));
        return topologyGraph;
    }

    private Action setupDefaultResizeAction() {
        when(mockSnapshot.getEntityFromOid(VM_TARGET_ID)).thenReturn(Optional.of(
            ActionPartialEntity.newBuilder()
                .setOid(VM_TARGET_ID)
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setPrimaryProviderId(HOST_ID).build()));
        when(mockSnapshot.getTopologyContextId()).thenReturn(actionPlanId);

        return new Action(ActionOrchestratorTestUtils
            .createResizeRecommendation(1, VM_TARGET_ID, CommodityType.VCPU, OLD_VCPU_MHZ, NEW_VCPU_MHZ),
            actionPlanId, actionModeCalculator, 2244L);
    }
}
