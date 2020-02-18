package com.vmturbo.action.orchestrator.translation;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
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
import java.util.stream.Stream;

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
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.Type;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.TypeSpecificPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntityBatch;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.PhysicalMachineInfo;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;

public class ActionTranslatorTest {

    private ActionTranslator translator;
    private ActionModeCalculator actionModeCalculator;

    private static final long actionPlanId = 1234;

    private static final int CPU_SPEED_MHZ = 2000;
    private static final long VM_TARGET_ID = 2;
    private static final long HOST_ID = 99;

    private static final int OLD_VCPU_MHZ = 2000;
    private static final int NEW_VCPU_MHZ = 4000;

    private final RepositoryServiceMole repositoryServiceSpy = spy(new RepositoryServiceMole());
    private final SettingPolicyServiceMole settingPolicyServiceSpy = spy(new SettingPolicyServiceMole());
    private EntitiesAndSettingsSnapshot mockSnapshot = mock(EntitiesAndSettingsSnapshot.class);

    @Rule
    public GrpcTestServer server = GrpcTestServer.newServer(repositoryServiceSpy, settingPolicyServiceSpy);

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        translator = new ActionTranslator(server.getChannel(), server.getChannel());
        actionModeCalculator = new ActionModeCalculator();

        when(mockSnapshot.getTopologyType()).thenReturn(TopologyType.SOURCE);
    }

    @Test
    public void testTranslateMovePassthrough() {
        final Action move = new Action(ActionOrchestratorTestUtils.createMoveRecommendation(1), actionPlanId, actionModeCalculator);
        translator.translate(Stream.of(move), mock(EntitiesAndSettingsSnapshot.class)).findFirst().get();
        assertEquals(move.getTranslationStatus(), TranslationStatus.TRANSLATION_SUCCEEDED);
    }

    @Test
    public void testTranslateResizeMemoryPassthrough() {
        final Action resize = new Action(
            ActionOrchestratorTestUtils.createResizeRecommendation(1, CommodityType.VMEM), actionPlanId, actionModeCalculator);
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
                .setReturnType(Type.TYPE_SPECIFIC)
                .setTopologyType(TopologyType.SOURCE).build()
        )).thenReturn(Arrays.asList(PartialEntityBatch.newBuilder().build()));
        when(mockSnapshot.getEntityFromOid(VM_TARGET_ID)).thenReturn(Optional.of(
            ActionPartialEntity.newBuilder()
                .setOid(VM_TARGET_ID)
                .setPrimaryProviderId(HOST_ID).build()));
        when(mockSnapshot.getToologyContextId()).thenReturn(actionPlanId);
        final Action resize = new Action(
            ActionOrchestratorTestUtils.createResizeRecommendation(1, VM_TARGET_ID,
                CommodityType.VCPU, OLD_VCPU_MHZ, NEW_VCPU_MHZ), actionPlanId, actionModeCalculator);

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
                CommodityType.VCPU, OLD_VCPU_MHZ, NEW_VCPU_MHZ), actionPlanId, actionModeCalculator);
        when(mockSnapshot.getEntityFromOid(VM_TARGET_ID)).thenReturn(Optional.of(
            ActionPartialEntity.newBuilder()
                .setOid(VM_TARGET_ID)
                .setPrimaryProviderId(HOST_ID).build()));
        when(mockSnapshot.getToologyContextId()).thenReturn(actionPlanId);
        translator.translate(Stream.of(resize), mockSnapshot);
        assertSame(resize.getTranslationStatus(), TranslationStatus.TRANSLATION_FAILED);
        assertFalse(resize.getActionTranslation().getTranslatedRecommendation().isPresent());
    }

    @Test
    public void testTranslateMixedActionTypes() {
        final Action resize = setupDefaultResizeAction();
        final Action move = new Action(ActionOrchestratorTestUtils.createMoveRecommendation(4), actionPlanId, actionModeCalculator);

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
        final Action move = new Action(ActionOrchestratorTestUtils.createMoveRecommendation(4), actionPlanId, actionModeCalculator);
        resize.getActionTranslation().setPassthroughTranslationSuccess();

        assertEquals(2, translator.translate(Stream.of(resize, move), mockSnapshot).count());
        verify(repositoryServiceSpy, never()).retrieveTopologyEntities(any());
    }

    @Test
    public void testToSpecWithoutDecision() {
        final Action action = new Action(ActionOrchestratorTestUtils.createMoveRecommendation(1), actionPlanId, actionModeCalculator);
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
        final Action action = new Action(ActionOrchestratorTestUtils.createMoveRecommendation(1), actionPlanId, actionModeCalculator);
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
        final Action action = new Action(ActionOrchestratorTestUtils.createMoveRecommendation(1), actionPlanId, actionModeCalculator);
        action.setDescription("Move VM1 from PM1 to PM2");
        final ActionSpec spec = translator.translateToSpec(action);
        assertEquals(action.getRecommendation(), spec.getRecommendation());
    }

    @Test
    public void testToSpecTranslationFailed() {
        final Action action = new Action(ActionOrchestratorTestUtils.createMoveRecommendation(1), actionPlanId, actionModeCalculator);
        action.getActionTranslation().setTranslationFailure();
        action.setDescription("Move VM1 from PM1 to PM2");
        final ActionSpec spec = translator.translateToSpec(action);

        assertEquals(action.getRecommendation(), spec.getRecommendation());
    }

    @Test
    public void testToSpecTranslationSucceeded() {
        final Action action = new Action(ActionOrchestratorTestUtils.createMoveRecommendation(1), actionPlanId, actionModeCalculator);
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
                        actionModeCalculator));
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
            actionPlanId, actionModeCalculator);

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
            actionPlanId, actionModeCalculator);

        Action reconfigureAction = new Action(ActionDTO.Action.newBuilder()
            .setId(0).setInfo(ActionInfo.newBuilder())
            .setDeprecatedImportance(0)
            .setExplanation(Explanation.newBuilder()
                .setReconfigure(ReconfigureExplanation.newBuilder()
                    .addReasonSettings(reasonSetting3))).build(),
            actionPlanId, actionModeCalculator);

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

    private Action setupDefaultResizeAction() {
        when(repositoryServiceSpy.retrieveTopologyEntities(
            RetrieveTopologyEntitiesRequest.newBuilder()
                .addAllEntityOids(Arrays.asList(HOST_ID))
                .setTopologyContextId(actionPlanId)
                .setReturnType(Type.TYPE_SPECIFIC)
                .setTopologyType(TopologyType.SOURCE).build()
        )).thenReturn(Arrays.asList(PartialEntityBatch.newBuilder().addEntities(
            PartialEntity.newBuilder()
                .setTypeSpecific(TypeSpecificPartialEntity.newBuilder()
                    .setTypeSpecificInfo(TypeSpecificInfo.newBuilder().setPhysicalMachine(
                        PhysicalMachineInfo.newBuilder().setCpuCoreMhz(CPU_SPEED_MHZ)))
                    .setOid(HOST_ID))).build()));

        when(mockSnapshot.getEntityFromOid(VM_TARGET_ID)).thenReturn(Optional.of(
            ActionPartialEntity.newBuilder()
                .setOid(VM_TARGET_ID)
                .setPrimaryProviderId(HOST_ID).build()));
        when(mockSnapshot.getToologyContextId()).thenReturn(actionPlanId);

        return new Action(ActionOrchestratorTestUtils
            .createResizeRecommendation(1, VM_TARGET_ID, CommodityType.VCPU, OLD_VCPU_MHZ, NEW_VCPU_MHZ),
            actionPlanId, actionModeCalculator);
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
                .setReturnType(Type.TYPE_SPECIFIC)
                .setTopologyType(TopologyType.SOURCE).build()
        )).thenReturn(Arrays.asList(PartialEntityBatch.newBuilder().addEntities(
            PartialEntity.newBuilder()
                .setTypeSpecific(TypeSpecificPartialEntity.newBuilder()
                    .setTypeSpecificInfo(TypeSpecificInfo.newBuilder().setPhysicalMachine(
                        PhysicalMachineInfo.newBuilder().setCpuCoreMhz(1234)))
                    .setOid(HOST_ID))).build()));
        when(mockSnapshot.getEntityFromOid(VM_TARGET_ID)).thenReturn(Optional.of(
            ActionPartialEntity.newBuilder()
                .setOid(VM_TARGET_ID)
                .setPrimaryProviderId(HOST_ID).build()));
        when(mockSnapshot.getToologyContextId()).thenReturn(actionPlanId);

        return new Action(ActionOrchestratorTestUtils
                .createResizeRecommendation(1, VM_TARGET_ID, CommodityType.VCPU, 4000, 2500),
                actionPlanId, actionModeCalculator);
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
