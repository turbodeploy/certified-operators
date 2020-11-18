package com.vmturbo.action.orchestrator.translation;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import io.grpc.Context;
import io.grpc.Status;

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
import com.vmturbo.action.orchestrator.topology.ActionRealtimeTopology;
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
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.TopologyType;
import com.vmturbo.common.protobuf.repository.RepositoryDTOMoles.RepositoryServiceMole;
import com.vmturbo.common.protobuf.setting.SettingProto.ListSettingPoliciesRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.common.protobuf.setting.SettingProtoMoles.SettingPolicyServiceMole;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity.ActionEntityTypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity.ActionEntityTypeSpecificInfo.ActionPhysicalMachineInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity.ActionEntityTypeSpecificInfo.ActionVirtualMachineInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.Type;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntityBatch;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.PhysicalMachineInfo;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.graph.TopologyGraphCreator;

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

    private final RepositoryServiceMole repositoryServiceSpy = spy(new RepositoryServiceMole());
    private final SettingPolicyServiceMole settingPolicyServiceSpy = spy(new SettingPolicyServiceMole());
    private EntitiesAndSettingsSnapshot mockSnapshot = mock(EntitiesAndSettingsSnapshot.class);
    private final ActionTopologyStore actionTopologyStore = mock(ActionTopologyStore.class);

    @Rule
    public GrpcTestServer server = GrpcTestServer.newServer(repositoryServiceSpy, settingPolicyServiceSpy);

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        when(actionTopologyStore.getSourceTopology()).thenReturn(Optional.empty());
        translator = new ActionTranslator(server.getChannel(), server.getChannel(), actionTopologyStore);
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
    public void testTranslateResizeVcpuInfoTranslation() {
        final Action resize = setupDefaultResizeAction();

        translator.translate(Stream.of(resize), mockSnapshot).findFirst().get();

        assertEquals(resize.getTranslationStatus(), TranslationStatus.TRANSLATION_SUCCEEDED);
        final ActionDTO.Action translatedResize = resize.getActionTranslation().getTranslatedRecommendation().get();
        verifyDefaultTranslatedResize(translatedResize);
    }

    @Test
    public void testInfoUnavailableDuringTranslation() {
        when(repositoryServiceSpy.retrieveTopologyEntities(
            RetrieveTopologyEntitiesRequest.newBuilder()
                .addAllEntityOids(Arrays.asList(HOST_ID))
                .setTopologyContextId(actionPlanId)
                .setReturnType(Type.ACTION)
                .setTopologyType(TopologyType.SOURCE).build()
        )).thenReturn(Arrays.asList(PartialEntityBatch.newBuilder().build()));
        when(mockSnapshot.getEntityFromOid(VM_TARGET_ID)).thenReturn(Optional.of(
            ActionPartialEntity.newBuilder()
                .setOid(VM_TARGET_ID)
                .setPrimaryProviderId(HOST_ID).build()));
        when(mockSnapshot.getTopologyContextId()).thenReturn(actionPlanId);
        final Action resize = new Action(
            ActionOrchestratorTestUtils.createResizeRecommendation(1, VM_TARGET_ID,
                CommodityType.VCPU, OLD_VCPU_MHZ, NEW_VCPU_MHZ), actionPlanId, actionModeCalculator, 2244L);

        translator.translate(Stream.of(resize), mockSnapshot).findFirst().get();
        assertEquals(TranslationStatus.TRANSLATION_FAILED, resize.getTranslationStatus());
        assertFalse(resize.getActionTranslation().getTranslatedRecommendation().isPresent());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testExceptionDuringTranslation() {
        when(repositoryServiceSpy.retrieveTopologyEntitiesError(any()))
            .thenReturn(Optional.of(Status.INTERNAL.asException()));
        final Action resize = new Action(
            ActionOrchestratorTestUtils.createResizeRecommendation(1, VM_TARGET_ID,
                CommodityType.VCPU, OLD_VCPU_MHZ, NEW_VCPU_MHZ), actionPlanId, actionModeCalculator, 2244L);
        when(mockSnapshot.getEntityFromOid(VM_TARGET_ID)).thenReturn(Optional.of(
            ActionPartialEntity.newBuilder()
                .setOid(VM_TARGET_ID)
                .setPrimaryProviderId(HOST_ID).build()));
        when(mockSnapshot.getTopologyContextId()).thenReturn(actionPlanId);
        translator.translate(Stream.of(resize), mockSnapshot);
        assertSame(resize.getTranslationStatus(), TranslationStatus.TRANSLATION_FAILED);
        assertFalse(resize.getActionTranslation().getTranslatedRecommendation().isPresent());
    }

    @Test
    public void testTranslateMixedActionTypes() {
        final Action resize = setupDefaultResizeAction();
        final Action move = new Action(ActionOrchestratorTestUtils.createMoveRecommendation(4), actionPlanId, actionModeCalculator, 2244L);

        assertEquals(2, translator.translate(Stream.of(resize, move), mockSnapshot).count());
        final ActionDTO.Action translatedResize = resize.getActionTranslation().getTranslatedRecommendation().get();

        verifyDefaultTranslatedResize(translatedResize);
    }

    @Test
    public void testTranslateResizeActionTypesWithSameVcpuValue() {
        final Action resize = setupDefaultResizeActionNegativeScenario();
        // Action should appear, and translation status should be failed.
        final Action action = translator.translate(Stream.of(resize), mockSnapshot).findFirst().get();
        assertThat(action.getTranslationStatus(), is(TranslationStatus.TRANSLATION_FAILED));
    }

    @Test
    public void testAlreadyTranslatedDoesNotCallService() {
        final Action resize = setupDefaultResizeAction();
        final Action move = new Action(ActionOrchestratorTestUtils.createMoveRecommendation(4), actionPlanId, actionModeCalculator, 2244L);
        resize.getActionTranslation().setPassthroughTranslationSuccess();

        assertEquals(2, translator.translate(Stream.of(resize, move), mockSnapshot).count());
        verify(repositoryServiceSpy, never()).retrieveTopologyEntities(any());
    }

    /**
     * When the actionTopologyStore already has the information to translate an action,
     * we should not perform extra RPC calls to the repository to fetch the information
     * we already have.
     */
    @Test
    public void testInTopologyDoesNotCallService() {
        final Action resize = setupDefaultResizeAction();
        final Action move = new Action(ActionOrchestratorTestUtils.createMoveRecommendation(4), actionPlanId, actionModeCalculator, 2244L);
        final ActionRealtimeTopology actionRealtimeTopology = actionRealtimeTopologyForDefaultResize();
        when(actionTopologyStore.getSourceTopology()).thenReturn(Optional.of(actionRealtimeTopology));

        assertEquals(2, translator.translate(Stream.of(resize, move), mockSnapshot).count());
        verify(repositoryServiceSpy, never()).retrieveTopologyEntities(any());
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
     * Test container VCPU resize translation without source topology in actionTopologyStore.
     * Translation will be failed if source topology is not found in actionTopologyStore.
     */
    @Test
    public void testContainerVCPUResizeBatchTranslateWithoutSourceTopology() {
        when(mockSnapshot.getEntityFromOid(CONTAINER_ID)).thenReturn(Optional.of(
            ActionPartialEntity.newBuilder()
                .setEntityType(EntityType.CONTAINER_VALUE)
                .setOid(CONTAINER_ID)
                .build()));
        Action resizeAction = new Action(ActionOrchestratorTestUtils.createResizeRecommendation(
            1, CONTAINER_ID, CommodityType.VCPU_VALUE, 10, 20, EntityType.CONTAINER_VALUE),
            actionPlanId, actionModeCalculator, 2248L);
        List<Action> translatedActions = translator.translate(Stream.of(resizeAction), mockSnapshot).collect(Collectors.toList());
        assertEquals(1, translatedActions.size());
        assertEquals(TranslationStatus.TRANSLATION_FAILED, translatedActions.get(0).getTranslationStatus());
    }

    /**
     * Test container VCPU resize translation. VM ActionGraphEntity has no cpuMillicoreMhz, so
     * translation will be failed.
     */
    @Test
    public void testContainerVCPUResizeBatchTranslateWithoutVMCpuMillicoreMhz() {
        when(mockSnapshot.getEntityFromOid(CONTAINER_ID)).thenReturn(Optional.of(
            ActionPartialEntity.newBuilder()
                .setEntityType(EntityType.CONTAINER_VALUE)
                .setOid(CONTAINER_ID)
                .build()));

        final ActionRealtimeTopology sourceTopology = mock(ActionRealtimeTopology.class);
        final TopologyGraph<ActionGraphEntity> topologyGraph = mockTopologyGraph();

        when(sourceTopology.entityGraph()).thenReturn(topologyGraph);
        when(sourceTopology.topologyInfo()).thenReturn(TopologyInfo.newBuilder()
            .setTopologyType(TopologyDTO.TopologyType.REALTIME)
            .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
            .build());

        when(actionTopologyStore.getSourceTopology()).thenReturn(Optional.of(sourceTopology));

        Action vCPUResizeAction = new Action(ActionOrchestratorTestUtils.createResizeRecommendation(
            1, CONTAINER_ID, CommodityType.VCPU_VALUE, OLD_VCPU_MHZ, NEW_VCPU_MHZ, EntityType.CONTAINER_VALUE),
            actionPlanId, actionModeCalculator, 2249L);

        List<Action> translatedActions =
            translator.translate(Stream.of(vCPUResizeAction), mockSnapshot).collect(Collectors.toList());
        assertEquals(1, translatedActions.size());
        assertEquals(TranslationStatus.TRANSLATION_FAILED, translatedActions.get(0).getTranslationStatus());
    }

    /**
     * Test container VCPU resize translation from topology.
     */
    @Test
    public void testContainerVCPUResizeBatchTranslateInTopologyGraph() {
        when(mockSnapshot.getEntityFromOid(CONTAINER_ID)).thenReturn(Optional.of(
            ActionPartialEntity.newBuilder()
                .setEntityType(EntityType.CONTAINER_VALUE)
                .setOid(CONTAINER_ID)
                .build()));

        final ActionRealtimeTopology sourceTopology = mock(ActionRealtimeTopology.class);
        final TopologyGraph<ActionGraphEntity> topologyGraph = mockTopologyGraph();

        // Set cpuMillicoreMhz to ActionVirtualMachineInfo of vmEntity.
        final ActionGraphEntity vmEntity = topologyGraph.getEntity(VM_TARGET_ID).get();
        ActionEntityTypeSpecificInfo actionEntityInfo = ActionEntityTypeSpecificInfo.newBuilder()
            .setVirtualMachine(ActionVirtualMachineInfo.newBuilder()
                .setCpuCoreMhz(CONTAINER_CPU_SPEED))
            .build();
        when(vmEntity.getActionEntityInfo()).thenReturn(actionEntityInfo);

        when(sourceTopology.entityGraph()).thenReturn(topologyGraph);
        when(sourceTopology.topologyInfo()).thenReturn(TopologyInfo.newBuilder()
            .setTopologyType(TopologyDTO.TopologyType.REALTIME)
            .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
            .build());

        when(actionTopologyStore.getSourceTopology()).thenReturn(Optional.of(sourceTopology));

        Action vCPUResizeAction = new Action(ActionOrchestratorTestUtils.createResizeRecommendation(
            1, CONTAINER_ID, CommodityType.VCPU_VALUE, OLD_VCPU_MHZ, NEW_VCPU_MHZ, EntityType.CONTAINER_VALUE),
            actionPlanId, actionModeCalculator, 2249L);

        List<Action> translatedActions =
            translator.translate(Stream.of(vCPUResizeAction), mockSnapshot).collect(Collectors.toList());
        assertEquals(1, translatedActions.size());
        assertEquals(TranslationStatus.TRANSLATION_SUCCEEDED, translatedActions.get(0).getTranslationStatus());
        ActionDTO.Action translatedVCPUAction = translatedActions.get(0).getActionTranslation().getTranslatedRecommendation().get();
        ActionDTO.Resize translatedVCPUResize = translatedVCPUAction.getInfo().getResize();
        assertEquals(1000 * OLD_VCPU_MHZ / CONTAINER_CPU_SPEED, translatedVCPUResize.getOldCapacity(), 0);
        assertEquals(1000 * NEW_VCPU_MHZ / CONTAINER_CPU_SPEED, translatedVCPUResize.getNewCapacity(), 0);
    }

    /**
     * Test container VCPU resize translation when fetching data from repository.
     * We have two containers running in the same pod. The action for both containers
     * should be successfully translated.
     */
    @Test
    public void testContainerVCPUResizeBatchTranslateFromRepo() {
        when(mockSnapshot.getEntityFromOid(CONTAINER_ID)).thenReturn(Optional.of(
            ActionPartialEntity.newBuilder()
                .setEntityType(EntityType.CONTAINER_VALUE)
                .setOid(CONTAINER_ID)
                .setPrimaryProviderId(CONTAINER_POD_ID)
                .build()));
        when(mockSnapshot.getEntityFromOid(CONTAINER_ID + 1)).thenReturn(Optional.of(
            ActionPartialEntity.newBuilder()
                .setEntityType(EntityType.CONTAINER_VALUE)
                .setOid(CONTAINER_ID + 1)
                .setPrimaryProviderId(CONTAINER_POD_ID)
                .build()));
        when(mockSnapshot.getTopologyContextId()).thenReturn(actionPlanId);

        final ActionPartialEntity.Builder pod = ActionPartialEntity.newBuilder()
            .setOid(CONTAINER_POD_ID)
            .setPrimaryProviderId(VM_TARGET_ID);
        final ActionPartialEntity.Builder vm = ActionPartialEntity.newBuilder()
            .setOid(VM_TARGET_ID)
            .setTypeSpecificInfo(ActionEntityTypeSpecificInfo.newBuilder()
                .setVirtualMachine(ActionVirtualMachineInfo.newBuilder()
                    .setCpuCoreMhz(CONTAINER_CPU_SPEED)));

        when(actionTopologyStore.getSourceTopology()).thenReturn(Optional.empty());
        setupRepoEntity(pod.getOid(), pod);
        setupRepoEntity(vm.getOid(), vm);

        final Action vCPUResizeAction1 = new Action(ActionOrchestratorTestUtils.createResizeRecommendation(
            1, CONTAINER_ID, CommodityType.VCPU_VALUE, OLD_VCPU_MHZ, NEW_VCPU_MHZ, EntityType.CONTAINER_VALUE),
            actionPlanId, actionModeCalculator, 2249L);
        final Action vCPUResizeAction2 = new Action(ActionOrchestratorTestUtils.createResizeRecommendation(
            1, CONTAINER_ID + 1, CommodityType.VCPU_VALUE, OLD_VCPU_MHZ, NEW_VCPU_MHZ, EntityType.CONTAINER_VALUE),
            actionPlanId, actionModeCalculator, 2249L);

        List<Action> translatedActions =
            translator.translate(Stream.of(vCPUResizeAction1, vCPUResizeAction2), mockSnapshot).collect(Collectors.toList());
        assertEquals(2, translatedActions.size());
        translatedActions.forEach(action -> {
            assertEquals(TranslationStatus.TRANSLATION_SUCCEEDED, action.getTranslationStatus());
            ActionDTO.Action translatedVCPUAction = action.getActionTranslation().getTranslatedRecommendation().get();
            ActionDTO.Resize translatedVCPUResize = translatedVCPUAction.getInfo().getResize();
            assertEquals(1000 * OLD_VCPU_MHZ / CONTAINER_CPU_SPEED, translatedVCPUResize.getOldCapacity(), 0);
            assertEquals(1000 * NEW_VCPU_MHZ / CONTAINER_CPU_SPEED, translatedVCPUResize.getNewCapacity(), 0);
        });
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

    private void setupRepoEntity(@Nonnull final long queryOid,
                                 @Nonnull final ActionPartialEntity.Builder entityResponse) {
        when(repositoryServiceSpy.retrieveTopologyEntities(
            eq(RetrieveTopologyEntitiesRequest.newBuilder()
                .addAllEntityOids(Arrays.asList(queryOid))
                .setTopologyContextId(actionPlanId)
                .setReturnType(Type.ACTION)
                .setTopologyType(TopologyType.SOURCE).build())
        )).thenReturn(Arrays.asList(PartialEntityBatch.newBuilder().addEntities(
            PartialEntity.newBuilder().setAction(entityResponse)).build()));
    }

    private Action setupDefaultResizeAction() {
        when(repositoryServiceSpy.retrieveTopologyEntities(
            RetrieveTopologyEntitiesRequest.newBuilder()
                .addAllEntityOids(Arrays.asList(HOST_ID))
                .setTopologyContextId(actionPlanId)
                .setReturnType(Type.ACTION)
                .setTopologyType(TopologyType.SOURCE).build()
        )).thenReturn(Arrays.asList(PartialEntityBatch.newBuilder().addEntities(
            PartialEntity.newBuilder()
                .setAction(ActionPartialEntity.newBuilder()
                    .setTypeSpecificInfo(ActionEntityTypeSpecificInfo.newBuilder().setPhysicalMachine(
                        ActionPhysicalMachineInfo.newBuilder().setCpuCoreMhz(CPU_SPEED_MHZ)))
                    .setOid(HOST_ID))).build()));

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

    private ActionRealtimeTopology actionRealtimeTopologyForDefaultResize() {
        final TopologyGraphCreator<ActionGraphEntity.Builder, ActionGraphEntity> graphCreator =
            new TopologyGraphCreator<>();
        TopologyEntityDTO.Builder host = TopologyEntityDTO.newBuilder()
            .setOid(HOST_ID)
            .setDisplayName("host")
            .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder().setPhysicalMachine(
                PhysicalMachineInfo.newBuilder().setCpuCoreMhz(CPU_SPEED_MHZ)));
        graphCreator.addEntity( new ActionGraphEntity.Builder(host.build()));

        ActionRealtimeTopology actionRealtimeTopology = mock(ActionRealtimeTopology.class);
        when(actionRealtimeTopology.entityGraph())
            .thenReturn(graphCreator.build());
        when(actionRealtimeTopology.topologyInfo())
            .thenReturn(TopologyInfo.newBuilder()
                .setTopologyType(TopologyDTO.TopologyType.REALTIME)
                .setTopologyContextId(actionPlanId)
                .build());

        return actionRealtimeTopology;
    }

    /* Host CPU 2k (MHZ)
       Market generated VCPU size down action:
            old_capacity: 4k
            new_capacity: 2.5k
       VCPU from value is 2.0 (Math.round(originalResize.getOldCapacity() / hostInfo.getCpuCoreMhz()))
            to value is 2.0 ((float)Math.ceil(originalResize.getNewCapacity() / hostInfo.getCpuCoreMhz()))
     */
    private Action setupDefaultResizeActionNegativeScenario() {
        when(repositoryServiceSpy.retrieveTopologyEntities(
            RetrieveTopologyEntitiesRequest.newBuilder()
                .addAllEntityOids(Arrays.asList(HOST_ID))
                .setTopologyContextId(actionPlanId)
                .setReturnType(Type.ACTION)
                .setTopologyType(TopologyType.SOURCE).build()
        )).thenReturn(Arrays.asList(PartialEntityBatch.newBuilder().addEntities(
            PartialEntity.newBuilder()
                .setAction(ActionPartialEntity.newBuilder()
                    .setTypeSpecificInfo(ActionEntityTypeSpecificInfo.newBuilder().setPhysicalMachine(
                        ActionPhysicalMachineInfo.newBuilder().setCpuCoreMhz(1234)))
                    .setOid(HOST_ID))).build()));
        when(mockSnapshot.getEntityFromOid(VM_TARGET_ID)).thenReturn(Optional.of(
            ActionPartialEntity.newBuilder()
                .setOid(VM_TARGET_ID)
                .setPrimaryProviderId(HOST_ID).build()));
        when(mockSnapshot.getTopologyContextId()).thenReturn(actionPlanId);

        return new Action(ActionOrchestratorTestUtils
                .createResizeRecommendation(1, VM_TARGET_ID, CommodityType.VCPU, 4000, 2500),
                actionPlanId, actionModeCalculator, 2244L);
    }

    private void verifyDefaultTranslatedResize(ActionDTO.Action translatedResize) {
        assertEquals(NEW_VCPU_MHZ / CPU_SPEED_MHZ, translatedResize.getInfo().getResize().getNewCapacity(), 0.0f);
        assertEquals(OLD_VCPU_MHZ / CPU_SPEED_MHZ, translatedResize.getInfo().getResize().getOldCapacity(), 0.0f);
        assertEquals(CommodityType.VCPU.getNumber(),
            translatedResize.getInfo().getResize().getCommodityType().getType());
        assertEquals(translatedResize.getInfo().getResize().getTarget().getId(),
            translatedResize.getInfo().getResize().getTarget().getId());
    }
}
