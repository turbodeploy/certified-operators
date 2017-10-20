package com.vmturbo.action.orchestrator.execution;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import io.grpc.stub.StreamObserver;

import com.vmturbo.action.orchestrator.ActionOrchestratorTestUtils;
import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ActionEvent;
import com.vmturbo.action.orchestrator.action.ActionTranslation.TranslationStatus;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionDecision;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionDecision.ClearingDecision;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.topology.EntityInfoOuterClass.GetHostInfoRequest;
import com.vmturbo.common.protobuf.topology.EntityInfoOuterClass.GetHostInfoResponse;
import com.vmturbo.common.protobuf.topology.EntityInfoOuterClass.HostInfo;
import com.vmturbo.common.protobuf.topology.EntityServiceGrpc;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;

public class ActionTranslatorTest {

    private ActionTranslator translator;

    private final long actionPlanId = 1234;

    private final Map<Long, HostInfo> hostInfos = new HashMap<>();

    private static final int CPU_SPEED_MHZ = 2000;
    private static final long VM_TARGET_ID = 2;

    final int OLD_VCPU_MHZ = 2000;
    final int NEW_VPCU_MHZ = 4000;

    private EntityServiceGrpc.EntityServiceImplBase entityServiceBackend =
        Mockito.spy(new EntityServiceGrpc.EntityServiceImplBase() {
            @Override
            public void getHostsInfo(GetHostInfoRequest request,
                                        StreamObserver<GetHostInfoResponse> responseObserver) {
                request.getVirtualMachineIdsList().forEach(vmId -> {
                    final GetHostInfoResponse.Builder builder = GetHostInfoResponse.newBuilder()
                        .setVirtualMachineId(vmId);
                    Optional.ofNullable(hostInfos.get(vmId)).ifPresent(builder::setHostInfo);

                    responseObserver.onNext(builder.build());
                });

                responseObserver.onCompleted();
            }
        });

    @Rule
    public GrpcTestServer server = GrpcTestServer.newServer(entityServiceBackend);

    @Before
    public void setup() throws IOException {
        MockitoAnnotations.initMocks(this);

        translator = new ActionTranslator(server.getChannel());
    }

    @Test
    public void testTranslateMovePassthrough() throws Exception {
        final Action move = new Action(ActionOrchestratorTestUtils.createMoveRecommendation(1), actionPlanId);
        assertTrue(translator.translate(move));
    }

    @Test
    public void testTranslateResizeMemoryPassthrough() throws Exception {
        final Action resize = new Action(
            ActionOrchestratorTestUtils.createResizeRecommendation(1, CommodityType.VMEM), actionPlanId);
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
        final Action resize = new Action(
            ActionOrchestratorTestUtils.createResizeRecommendation(1, CommodityType.VCPU), actionPlanId);

        assertFalse(translator.translate(resize));
        assertEquals(TranslationStatus.TRANSLATION_FAILED, resize.getTranslationStatus());
        assertFalse(resize.getActionTranslation().getTranslatedRecommendation().isPresent());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testExceptionDuringTranslation() throws Exception {
        doThrow(new RuntimeException("Failure")).when(entityServiceBackend)
            .getHostsInfo(any(GetHostInfoRequest.class), any(StreamObserver.class));
        final Action resize = new Action(
            ActionOrchestratorTestUtils.createResizeRecommendation(1, CommodityType.VCPU), actionPlanId);

        assertFalse(translator.translate(resize));
        assertEquals(TranslationStatus.TRANSLATION_FAILED, resize.getTranslationStatus());
        assertFalse(resize.getActionTranslation().getTranslatedRecommendation().isPresent());
    }

    @Test
    public void testTranslateMixedActionTypes() throws Exception {
        final Action resize = setupDefaultResizeAction();
        final Action move = new Action(ActionOrchestratorTestUtils.createMoveRecommendation(4), actionPlanId);

        assertEquals(2, translator.translate(Stream.of(resize, move)).count());
        final ActionDTO.Action translatedResize = resize.getActionTranslation().getTranslatedRecommendation().get();

        verifyDefaultTranslatedResize(translatedResize);
    }

    @Test
    public void testAlreadyTranslatedDoesNotCallService() throws Exception {
        final Action resize = setupDefaultResizeAction();
        final Action move = new Action(ActionOrchestratorTestUtils.createMoveRecommendation(4), actionPlanId);
        resize.getActionTranslation().setPassthroughTranslationSuccess();

        assertEquals(2, translator.translate(Stream.of(resize, move)).count());
        verify(entityServiceBackend, never()).getHostsInfo(any(GetHostInfoRequest.class), any(StreamObserver.class));
    }

    @Test
    public void testToSpecWithoutDecision() throws Exception {
        final Action action = new Action(ActionOrchestratorTestUtils.createMoveRecommendation(1), actionPlanId);
        final ActionSpec spec = translator.translateToSpec(action);

        assertTrue(spec.getIsExecutable());
        assertEquals(ActionState.READY, spec.getActionState());
        assertEquals(actionPlanId, spec.getActionPlanId());
        assertEquals(action.getRecommendation().getId(), spec.getRecommendation().getId());
        assertFalse(spec.hasDecision());
    }

    @Test
    public void testToSpecWithDecision() throws Exception {
        final Action action = new Action(ActionOrchestratorTestUtils.createMoveRecommendation(1), actionPlanId);
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
        final Action action = new Action(ActionOrchestratorTestUtils.createMoveRecommendation(1), actionPlanId);
        final ActionSpec spec = translator.translateToSpec(action);

        assertEquals(action.getRecommendation(), spec.getRecommendation());
    }

    @Test
    public void testToSpecTranslationFailed() {
        final Action action = new Action(ActionOrchestratorTestUtils.createMoveRecommendation(1), actionPlanId);
        action.getActionTranslation().setTranslationFailure();
        final ActionSpec spec = translator.translateToSpec(action);

        assertEquals(action.getRecommendation(), spec.getRecommendation());
    }

    @Test
    public void testToSpecTranslationSucceeded() {
        final Action action = new Action(ActionOrchestratorTestUtils.createMoveRecommendation(1), actionPlanId);
        final ActionDTO.Action translatedRecommendation = ActionOrchestratorTestUtils.createMoveRecommendation(2);
        action.getActionTranslation().setTranslationSuccess(translatedRecommendation);

        final ActionSpec spec = translator.translateToSpec(action);

        assertNotEquals(action.getRecommendation(), spec.getRecommendation());
        assertEquals(translatedRecommendation, spec.getRecommendation());
    }

    private Action setupDefaultResizeAction() {
        hostInfos.put(VM_TARGET_ID, hostInfo(CPU_SPEED_MHZ, 1234));

        return new Action(ActionOrchestratorTestUtils
            .createResizeRecommendation(1, VM_TARGET_ID, CommodityType.VCPU, OLD_VCPU_MHZ, NEW_VPCU_MHZ),
            actionPlanId);
    }

    private void verifyDefaultTranslatedResize(ActionDTO.Action translatedResize) {
        assertEquals(NEW_VPCU_MHZ / CPU_SPEED_MHZ, translatedResize.getInfo().getResize().getNewCapacity(), 0.0f);
        assertEquals(OLD_VCPU_MHZ / CPU_SPEED_MHZ, translatedResize.getInfo().getResize().getOldCapacity(), 0.0f);
        assertEquals(CommodityType.VCPU.getNumber(),
            translatedResize.getInfo().getResize().getCommodityType().getType());
        assertEquals(translatedResize.getInfo().getResize().getTargetId(),
            translatedResize.getInfo().getResize().getTargetId());
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