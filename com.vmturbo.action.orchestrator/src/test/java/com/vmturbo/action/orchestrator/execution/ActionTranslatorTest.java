package com.vmturbo.action.orchestrator.execution;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.MockitoAnnotations;

import io.grpc.Status;

import com.vmturbo.action.orchestrator.ActionOrchestratorTestUtils;
import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ActionEvent;
import com.vmturbo.action.orchestrator.action.ActionModeCalculator;
import com.vmturbo.action.orchestrator.action.ActionTranslation.TranslationStatus;
import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory.EntitiesAndSettingsSnapshot;
import com.vmturbo.action.orchestrator.translation.ActionTranslator;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionDecision;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionDecision.ClearingDecision;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest.TopologyType;
import com.vmturbo.common.protobuf.repository.RepositoryDTOMoles.RepositoryServiceMole;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.EntityInfo.GetHostInfoRequest;
import com.vmturbo.common.protobuf.topology.EntityInfo.GetHostInfoResponse;
import com.vmturbo.common.protobuf.topology.EntityInfo.HostInfo;
import com.vmturbo.common.protobuf.topology.EntityInfoMoles.EntityServiceMole;
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

    private final long actionPlanId = 1234;

    private static final int CPU_SPEED_MHZ = 2000;
    private static final long VM_TARGET_ID = 2;
    private static final long HOST_ID = 99;

    final int OLD_VCPU_MHZ = 2000;
    final int NEW_VPCU_MHZ = 4000;

    private RepositoryServiceMole repositoryServiceSpy = spy(new RepositoryServiceMole());
    EntitiesAndSettingsSnapshot mockSnapshot = mock(EntitiesAndSettingsSnapshot.class);

    @Rule
    public GrpcTestServer server = GrpcTestServer.newServer(repositoryServiceSpy);

    @Before
    public void setup() throws IOException {
        MockitoAnnotations.initMocks(this);
        translator = new ActionTranslator(server.getChannel());
        actionModeCalculator = new ActionModeCalculator();
    }

    @Test
    public void testTranslateMovePassthrough() throws Exception {
        final Action move = new Action(ActionOrchestratorTestUtils.createMoveRecommendation(1), actionPlanId, actionModeCalculator);
        Action translatedAction = translator.translate(Stream.of(move), mock(EntitiesAndSettingsSnapshot.class)).findFirst().get();
        assertEquals(move.getTranslationStatus(), TranslationStatus.TRANSLATION_SUCCEEDED);
    }

    @Test
    public void testTranslateResizeMemoryPassthrough() throws Exception {
        final Action resize = new Action(
            ActionOrchestratorTestUtils.createResizeRecommendation(1, CommodityType.VMEM), actionPlanId, actionModeCalculator);
        Action translatedAction = translator.translate(Stream.of(resize), mock(EntitiesAndSettingsSnapshot.class)).findFirst().get();
        assertEquals(resize.getTranslationStatus(), TranslationStatus.TRANSLATION_SUCCEEDED);
    }

    @Test
    public void testTranslateResizeVcpuInfoTranslation() throws Exception {
        final Action resize = setupDefaultResizeAction();

        Action translatedAction = translator.translate(Stream.of(resize), mockSnapshot).findFirst().get();

        assertEquals(resize.getTranslationStatus(), TranslationStatus.TRANSLATION_SUCCEEDED);
        final ActionDTO.Action translatedResize = resize.getActionTranslation().getTranslatedRecommendation().get();
        verifyDefaultTranslatedResize(translatedResize);
    }

    @Test
    public void testInfoUnavailableDuringTranslation() throws Exception {
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
                CommodityType.VCPU, OLD_VCPU_MHZ, NEW_VPCU_MHZ), actionPlanId, actionModeCalculator);

        Action translatedAction = translator.translate(Stream.of(resize), mockSnapshot).findFirst().get();
        assertEquals(TranslationStatus.TRANSLATION_FAILED, resize.getTranslationStatus());
        assertFalse(resize.getActionTranslation().getTranslatedRecommendation().isPresent());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testExceptionDuringTranslation() throws Exception {
        when(repositoryServiceSpy.retrieveTopologyEntitiesError(any()))
            .thenReturn(Optional.of(Status.INTERNAL.asException()));
        final Action resize = new Action(
            ActionOrchestratorTestUtils.createResizeRecommendation(1, VM_TARGET_ID,
                CommodityType.VCPU, OLD_VCPU_MHZ, NEW_VPCU_MHZ), actionPlanId, actionModeCalculator);
        when(mockSnapshot.getEntityFromOid(VM_TARGET_ID)).thenReturn(Optional.of(
            ActionPartialEntity.newBuilder()
                .setOid(VM_TARGET_ID)
                .setPrimaryProviderId(HOST_ID).build()));
        when(mockSnapshot.getToologyContextId()).thenReturn(actionPlanId);
        Stream<Action> translatedActions = translator.translate(Stream.of(resize), mockSnapshot);
        assertTrue(resize.getTranslationStatus() == TranslationStatus.TRANSLATION_FAILED);
        assertFalse(resize.getActionTranslation().getTranslatedRecommendation().isPresent());
    }

    @Test
    public void testTranslateMixedActionTypes() throws Exception {
        final Action resize = setupDefaultResizeAction();
        final Action move = new Action(ActionOrchestratorTestUtils.createMoveRecommendation(4), actionPlanId, actionModeCalculator);

        assertEquals(2, translator.translate(Stream.of(resize, move), mockSnapshot).count());
        final ActionDTO.Action translatedResize = resize.getActionTranslation().getTranslatedRecommendation().get();

        verifyDefaultTranslatedResize(translatedResize);
    }

    @Test
    public void testTranslateResizeActionTypesWithSameVcpuValue() throws Exception {
        final Action resize = setupDefaultResizeActionNegativeScenario();
        // Action should appear, and translation status should be failed.
        final Action action = translator.translate(Stream.of(resize), mockSnapshot).findFirst().get();
        assertThat(action.getTranslationStatus(), is(TranslationStatus.TRANSLATION_FAILED));
    }

    @Test
    public void testAlreadyTranslatedDoesNotCallService() throws Exception {
        final Action resize = setupDefaultResizeAction();
        final Action move = new Action(ActionOrchestratorTestUtils.createMoveRecommendation(4), actionPlanId, actionModeCalculator);
        resize.getActionTranslation().setPassthroughTranslationSuccess();

        assertEquals(2, translator.translate(Stream.of(resize, move), mockSnapshot).count());
        verify(repositoryServiceSpy, never()).retrieveTopologyEntities(any());
    }

    @Test
    public void testToSpecWithoutDecision() throws Exception {
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
    public void testToSpecWithDecision() throws Exception {
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

    private Action setupDefaultResizeAction() {
        when(repositoryServiceSpy.retrieveTopologyEntities(
            RetrieveTopologyEntitiesRequest.newBuilder()
                .addAllEntityOids(Arrays.asList(HOST_ID))
                .setTopologyContextId(actionPlanId)
                .setReturnType(Type.TYPE_SPECIFIC)
                .setTopologyType(TopologyType.PROJECTED).build()
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
            .createResizeRecommendation(1, VM_TARGET_ID, CommodityType.VCPU, OLD_VCPU_MHZ, NEW_VPCU_MHZ),
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
        assertEquals(NEW_VPCU_MHZ / CPU_SPEED_MHZ, translatedResize.getInfo().getResize().getNewCapacity(), 0.0f);
        assertEquals(OLD_VCPU_MHZ / CPU_SPEED_MHZ, translatedResize.getInfo().getResize().getOldCapacity(), 0.0f);
        assertEquals(CommodityType.VCPU.getNumber(),
            translatedResize.getInfo().getResize().getCommodityType().getType());
        assertEquals(translatedResize.getInfo().getResize().getTarget().getId(),
            translatedResize.getInfo().getResize().getTarget().getId());
    }

    private static HostInfo hostInfo(final int cpuSpeedMhz, final long hostId) {
        return HostInfo.newBuilder()
            .setCpuCoreMhz(cpuSpeedMhz)
            .setNumCpuSockets(1)
            .setNumCpuCores(4)
            .setNumCpuThreads(2)
            .setHostId(hostId)
            .build();
    }
}
