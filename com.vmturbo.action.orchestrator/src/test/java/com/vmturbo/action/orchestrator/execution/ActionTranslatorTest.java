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
import com.vmturbo.action.orchestrator.translation.ActionTranslator;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionDecision;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionDecision.ClearingDecision;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.topology.EntityInfoMoles.EntityServiceMole;
import com.vmturbo.common.protobuf.topology.EntityInfoOuterClass.GetHostInfoRequest;
import com.vmturbo.common.protobuf.topology.EntityInfoOuterClass.GetHostInfoResponse;
import com.vmturbo.common.protobuf.topology.EntityInfoOuterClass.HostInfo;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;

public class ActionTranslatorTest {

    private ActionTranslator translator;
    private ActionModeCalculator actionModeCalculator;

    private final long actionPlanId = 1234;

    private static final int CPU_SPEED_MHZ = 2000;
    private static final long VM_TARGET_ID = 2;

    final int OLD_VCPU_MHZ = 2000;
    final int NEW_VPCU_MHZ = 4000;

    private EntityServiceMole entityServiceSpy = spy(new EntityServiceMole());

    @Rule
    public GrpcTestServer server = GrpcTestServer.newServer(entityServiceSpy);

    @Before
    public void setup() throws IOException {
        MockitoAnnotations.initMocks(this);
        translator = new ActionTranslator(server.getChannel());
        actionModeCalculator = new ActionModeCalculator(translator);
    }

    @Test
    public void testTranslateMovePassthrough() throws Exception {
        final Action move = new Action(ActionOrchestratorTestUtils.createMoveRecommendation(1), actionPlanId, actionModeCalculator);
        assertTrue(translator.translate(move));
    }

    @Test
    public void testTranslateResizeMemoryPassthrough() throws Exception {
        final Action resize = new Action(
            ActionOrchestratorTestUtils.createResizeRecommendation(1, CommodityType.VMEM), actionPlanId, actionModeCalculator);
        assertTrue(translator.translate(resize));
    }

    @Test
    public void testTranslateResizeVcpuInfoTranslation() throws Exception {
        final Action resize = setupDefaultResizeAction();

        assertTrue(translator.translate(resize));
        final ActionDTO.Action translatedResize = resize.getActionTranslation().getTranslatedRecommendation().get();

        verifyDefaultTranslatedResize(translatedResize);
    }

    @Test
    public void testTranslateResizeVcpuExplanationTranslation() throws Exception {
        final Action resize = setupDefaultResizeAction();

        assertTrue(translator.translate(resize));
        final ActionDTO.Action translatedResize = resize.getActionTranslation().getTranslatedRecommendation().get();

        verifyDefaultTranslatedResize(translatedResize);
    }

    @Test
    public void testInfoUnavailableDuringTranslation() throws Exception {
        when(entityServiceSpy.getHostsInfo(eq(GetHostInfoRequest.newBuilder()
                .addVirtualMachineIds(ActionOrchestratorTestUtils.TARGET_ID)
                .build())))
                .thenReturn(Collections.singletonList(GetHostInfoResponse.newBuilder()
                        .setVirtualMachineId(ActionOrchestratorTestUtils.TARGET_ID)
                        // No host info
                        .build()));
        final Action resize = new Action(
            ActionOrchestratorTestUtils.createResizeRecommendation(1, CommodityType.VCPU), actionPlanId, actionModeCalculator);

        assertFalse(translator.translate(resize));
        assertEquals(TranslationStatus.TRANSLATION_FAILED, resize.getTranslationStatus());
        assertFalse(resize.getActionTranslation().getTranslatedRecommendation().isPresent());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testExceptionDuringTranslation() throws Exception {
        when(entityServiceSpy.getHostsInfoError(any()))
            .thenReturn(Optional.of(Status.INTERNAL.asException()));
        final Action resize = new Action(
            ActionOrchestratorTestUtils.createResizeRecommendation(1, CommodityType.VCPU), actionPlanId, actionModeCalculator);

        assertFalse(translator.translate(resize));
        assertEquals(TranslationStatus.TRANSLATION_FAILED, resize.getTranslationStatus());
        assertFalse(resize.getActionTranslation().getTranslatedRecommendation().isPresent());
    }

    @Test
    public void testTranslateMixedActionTypes() throws Exception {
        final Action resize = setupDefaultResizeAction();
        final Action move = new Action(ActionOrchestratorTestUtils.createMoveRecommendation(4), actionPlanId, actionModeCalculator);

        assertEquals(2, translator.translate(Stream.of(resize, move)).count());
        final ActionDTO.Action translatedResize = resize.getActionTranslation().getTranslatedRecommendation().get();

        verifyDefaultTranslatedResize(translatedResize);
    }

    @Test
    public void testTranslateResizeActionTypesWithSameVcpuValue() throws Exception {
        final Action resize = setupDefaultResizeActionNegativeScenario();
        // Action should appear, and translation status should be failed.
        final Action action = translator.translate(Stream.of(resize)).findFirst().get();
        assertThat(action.getTranslationStatus(), is(TranslationStatus.TRANSLATION_FAILED));
    }

    @Test
    public void testAlreadyTranslatedDoesNotCallService() throws Exception {
        final Action resize = setupDefaultResizeAction();
        final Action move = new Action(ActionOrchestratorTestUtils.createMoveRecommendation(4), actionPlanId, actionModeCalculator);
        resize.getActionTranslation().setPassthroughTranslationSuccess();

        assertEquals(2, translator.translate(Stream.of(resize, move)).count());
        verify(entityServiceSpy, never()).getHostsInfo(any());
    }

    @Test
    public void testToSpecWithoutDecision() throws Exception {
        final Action action = new Action(ActionOrchestratorTestUtils.createMoveRecommendation(1), actionPlanId, actionModeCalculator);
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
        final ActionSpec spec = translator.translateToSpec(action);

        assertEquals(action.getRecommendation(), spec.getRecommendation());
    }

    @Test
    public void testToSpecTranslationFailed() {
        final Action action = new Action(ActionOrchestratorTestUtils.createMoveRecommendation(1), actionPlanId, actionModeCalculator);
        action.getActionTranslation().setTranslationFailure();
        final ActionSpec spec = translator.translateToSpec(action);

        assertEquals(action.getRecommendation(), spec.getRecommendation());
    }

    @Test
    public void testToSpecTranslationSucceeded() {
        final Action action = new Action(ActionOrchestratorTestUtils.createMoveRecommendation(1), actionPlanId, actionModeCalculator);
        final ActionDTO.Action translatedRecommendation = ActionOrchestratorTestUtils.createMoveRecommendation(2);
        action.getActionTranslation().setTranslationSuccess(translatedRecommendation);

        final ActionSpec spec = translator.translateToSpec(action);

        assertNotEquals(action.getRecommendation(), spec.getRecommendation());
        assertEquals(translatedRecommendation, spec.getRecommendation());
    }

    private Action setupDefaultResizeAction() {
        when(entityServiceSpy.getHostsInfo(eq(GetHostInfoRequest.newBuilder()
                .addVirtualMachineIds(VM_TARGET_ID)
                .build())))
            .thenReturn(Collections.singletonList(GetHostInfoResponse.newBuilder()
                .setVirtualMachineId(VM_TARGET_ID)
                .setHostInfo(hostInfo(CPU_SPEED_MHZ, 1234))
                .build()));

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
        when(entityServiceSpy.getHostsInfo(eq(GetHostInfoRequest.newBuilder()
                .addVirtualMachineIds(VM_TARGET_ID)
                .build())))
                .thenReturn(Collections.singletonList(GetHostInfoResponse.newBuilder()
                        .setVirtualMachineId(VM_TARGET_ID)
                        .setHostInfo(hostInfo(CPU_SPEED_MHZ, 1234))
                        .build()));

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
