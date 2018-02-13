package com.vmturbo.action.orchestrator.store;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;
import io.grpc.stub.StreamObserver;

import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ActionTest;
import com.vmturbo.action.orchestrator.execution.ActionExecutor;
import com.vmturbo.action.orchestrator.execution.TargetResolutionException;
import com.vmturbo.common.protobuf.UnsupportedActionException;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo.ActionTypeCase;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.action.ActionDTO.Activate;
import com.vmturbo.common.protobuf.action.ActionDTO.Deactivate;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.topology.Probe.ActionCapabilitiesList;
import com.vmturbo.common.protobuf.topology.Probe.GetProbeActionCapabilitiesRequest;
import com.vmturbo.common.protobuf.topology.Probe.GetProbeActionCapabilitiesResponse;
import com.vmturbo.common.protobuf.topology.Probe.ListProbeActionCapabilitiesRequest;
import com.vmturbo.common.protobuf.topology.Probe.ProbeActionCapabilities;
import com.vmturbo.common.protobuf.topology.Probe.ProbeActionCapability;
import com.vmturbo.common.protobuf.topology.Probe.ProbeActionCapability.ActionCapability;
import com.vmturbo.common.protobuf.topology.Probe.ProbeActionCapability.ActionCapabilityElement;
import com.vmturbo.common.protobuf.topology.ProbeActionCapabilitiesServiceGrpc;
import com.vmturbo.common.protobuf.topology.ProbeActionCapabilitiesServiceGrpc.ProbeActionCapabilitiesServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.ProbeActionCapabilitiesServiceGrpc.ProbeActionCapabilitiesServiceImplBase;
import com.vmturbo.components.api.test.GrpcTestServer;

/**
 * Tests for class which resolve whether action supported or not.
 */
public class ActionSupportResolverTest {

    private final long probeId = 1L;

    private final ActionExecutor actionExecutor = Mockito.mock(ActionExecutor.class);

    private final TestActionCapabilitiesRpcService actionCapabilitiesService =
            new TestActionCapabilitiesRpcService(probeId, createActionCapabilities());

    @Rule
    public GrpcTestServer testServer = GrpcTestServer.newServer(actionCapabilitiesService);

    private ProbeActionCapabilitiesServiceBlockingStub actionCapabilitiesStub;

    private ActionSupportResolver filter;

    private Collection<Action> resolvedBySupportActions;

    @Before
    public void setup() throws TargetResolutionException, IOException, UnsupportedActionException {
        actionCapabilitiesStub =
                ProbeActionCapabilitiesServiceGrpc.newBlockingStub(testServer.getChannel());
        final ActionCapabilitiesStore actionCapabilitiesStore = new
                ProbeActionCapabilitiesStore(actionCapabilitiesStub);
        filter = new ActionSupportResolver(actionCapabilitiesStore, actionExecutor);
        resolvedBySupportActions = filter.resolveActionsSupporting(getTestedActions());
    }

    private List<ProbeActionCapability> createActionCapabilities() {
        ProbeActionCapability supportedCapability = ProbeActionCapability.newBuilder()
                .addCapabilityElement(ActionCapabilityElement.newBuilder()
                        .setActionType(ActionType.MOVE)
                        .setActionCapability(ActionCapability.SUPPORTED)
                        .build())
                .build();
        ProbeActionCapability notSupported = ProbeActionCapability.newBuilder()
                .addCapabilityElement(ActionCapabilityElement.newBuilder()
                        .setActionType(ActionType.ACTIVATE)
                        .setActionCapability(ActionCapability.NOT_SUPPORTED)
                        .build())
                .build();
        ProbeActionCapability notExecutable = ProbeActionCapability.newBuilder()
                .addCapabilityElement(ActionCapabilityElement.newBuilder()
                        .setActionType(ActionType.DEACTIVATE)
                        .setActionCapability(ActionCapability.NOT_EXECUTABLE)
                        .build())
                .build();
        return ImmutableList.of(supportedCapability, notSupported, notExecutable);
    }

    private List<Action> getTestedActions() {
        ActionDTO.Action move = ActionDTO.Action.newBuilder()
                .setInfo(ActionTest.makeMoveInfo(probeId, probeId, probeId))
                .setId(1)
                .setImportance(1)
                .setExplanation(Explanation.newBuilder().build())
                .build();
        Action moveAction = new Action(move, LocalDateTime.now(), 1);

        ActionDTO.Action activate = ActionDTO.Action.newBuilder()
                .setInfo(ActionInfo.newBuilder()
                        .setActivate(Activate.newBuilder().setTargetId(probeId).build())
                        .build())
                .setId(1)
                .setImportance(1)
                .setExplanation(Explanation.newBuilder().build())
                .build();
        Action activateAction = new Action(activate, LocalDateTime.now(), 1);

        ActionDTO.Action deactivate = ActionDTO.Action.newBuilder()
                .setInfo(ActionInfo.newBuilder()
                        .setDeactivate(Deactivate.newBuilder().setTargetId(probeId).build())
                        .build())
                .setId(1)
                .setImportance(1)
                .setExplanation(Explanation.newBuilder().build())
                .build();
        Action deactivateAction = new Action(deactivate, LocalDateTime.now(), 1);

        return ImmutableList.of(moveAction, activateAction, deactivateAction);
    }

    /**
     * Tests resolving of actions supporting.
     */
    @Test
    public void testResolveActionsSupporting() {
        for (Action action : resolvedBySupportActions) {
            if (action.getRecommendation().getInfo().getActionTypeCase() ==
                    ActionTypeCase.ACTIVATE) {
                Assert.assertEquals(SupportLevel.UNSUPPORTED, action.getSupportLevel());
            } else if (action.getRecommendation().getInfo().getActionTypeCase() ==
                    ActionTypeCase.DEACTIVATE) {
                Assert.assertEquals(SupportLevel.SHOW_ONLY, action.getSupportLevel());
            } else {
                Assert.assertEquals(SupportLevel.SUPPORTED, action.getSupportLevel());
            }
        }
    }

    @Test
    public void testActionModeForOnlyShowActions() {
        Assert.assertTrue(resolvedBySupportActions.stream().allMatch(action ->
                showOnlyActionHasRecommendMode(action) || action.getSupportLevel() !=
                        SupportLevel.SHOW_ONLY));
    }

    private boolean showOnlyActionHasRecommendMode(Action action) {
        return action.getSupportLevel() == SupportLevel.SHOW_ONLY
                && action.getMode() == ActionMode.RECOMMEND;
    }

    private static class TestActionCapabilitiesRpcService extends
            ProbeActionCapabilitiesServiceImplBase {

        private final List<ProbeActionCapability> actionCapabilities;

        private final long probeId;

        public TestActionCapabilitiesRpcService(long probeId, List<ProbeActionCapability>
                actionCapabilities) {
            this.probeId = probeId;
            this.actionCapabilities = actionCapabilities;
        }

        @Override
        public void getProbeActionCapabilities(GetProbeActionCapabilitiesRequest request,
                StreamObserver<GetProbeActionCapabilitiesResponse> responseObserver) {
            responseObserver.onNext(GetProbeActionCapabilitiesResponse.newBuilder()
                    .addAllActionCapabilities(actionCapabilities)
                    .build());
            responseObserver.onCompleted();
        }

        @Override
        public void listProbeActionCapabilities(ListProbeActionCapabilitiesRequest request,
                StreamObserver<ProbeActionCapabilities> responseObserver) {
            ProbeActionCapabilities probeCapabilities = ProbeActionCapabilities.newBuilder()
                    .setProbeId(probeId).setActionCapabilitiesList(
                            ActionCapabilitiesList.newBuilder()
                            .addAllActionCapabilities(actionCapabilities)
                            .build())
                    .build();
            responseObserver.onNext(probeCapabilities);
            responseObserver.onCompleted();
        }
    }
}
