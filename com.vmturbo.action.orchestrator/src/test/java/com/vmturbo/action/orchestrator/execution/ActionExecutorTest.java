package com.vmturbo.action.orchestrator.execution;

import java.io.IOException;
import java.util.stream.Stream;

import io.grpc.stub.StreamObserver;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo.ActionTypeCase;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.topology.ActionExecution.ExecuteActionRequest;
import com.vmturbo.common.protobuf.topology.ActionExecutionServiceGrpc.ActionExecutionServiceImplBase;
import com.vmturbo.common.protobuf.topology.EntityInfoOuterClass;
import com.vmturbo.common.protobuf.topology.EntityServiceGrpc;
import com.vmturbo.components.api.test.GrpcTestServer;

/**
 * Unit tests for the {@link ActionExecutor} class.
 */
public class ActionExecutorTest {

    private ActionExecutor actionExecutor;

    private final long probeId = 10;
    private final long targetId = 7;

    private ActionExecutionServiceImplBase actionExecutionBackend =
            Mockito.spy(new ActionExecutionServiceImplBase() {});

    private EntityServiceGrpc.EntityServiceImplBase entityServiceBackend =
            Mockito.spy(new EntityServiceGrpc.EntityServiceImplBase() {
                @Override
                public void getEntitiesInfo(EntityInfoOuterClass.GetEntitiesInfoRequest request,
                                            StreamObserver<EntityInfoOuterClass.EntityInfo> responseObserver) {
                    Stream.of(1, 2, 3).forEach(id ->
                            responseObserver.onNext(EntityInfoOuterClass.EntityInfo.newBuilder()
                                    .setEntityId(id)
                                    .putTargetIdToProbeId(targetId, probeId)
                                    .build())
                    );

                    responseObserver.onCompleted();
                }
            });

    @Captor
    private ArgumentCaptor<ExecuteActionRequest> actionSpecCaptor;

    @Rule
    public GrpcTestServer server =
            GrpcTestServer.newServer(actionExecutionBackend, entityServiceBackend);

    @Before
    public void setup() throws IOException {
        MockitoAnnotations.initMocks(this);

        actionExecutor = new ActionExecutor(server.getChannel());
    }

    @Test
    public void testMove() throws Exception {
        final ActionDTO.Action action = ActionDTO.Action.newBuilder()
                .setId(44)
                .setImportance(0)
                .setInfo(ActionDTO.ActionInfo.newBuilder()
                    .setMove(ActionDTO.Move.newBuilder()
                        .setTargetId(1)
                        .setSourceId(2)
                        .setDestinationId(3)))
                .setExplanation(Explanation.newBuilder().build())
                .build();

        Assert.assertEquals(targetId, actionExecutor.getTargetId(action));

        try {
            actionExecutor.execute(targetId, action);
        } catch (ExecutionStartException e) {
            // We expect this to happen, since the backend implementation
            // is not implemented.
        }

        // However, the backend should have been called, and we can capture
        // and examine the arguments.
        Mockito.verify(actionExecutionBackend).executeAction(actionSpecCaptor.capture(), Mockito.any());
        ExecuteActionRequest sentSpec = actionSpecCaptor.getValue();
        Assert.assertTrue(sentSpec.hasActionInfo());
        Assert.assertEquals(ActionTypeCase.MOVE, sentSpec.getActionInfo().getActionTypeCase());
        Move move= sentSpec.getActionInfo().getMove();
        Assert.assertEquals(targetId, sentSpec.getTargetId());
        Assert.assertEquals(1, move.getTargetId());
        Assert.assertEquals(2, move.getSourceId());
        Assert.assertEquals(3, move.getDestinationId());
    }

    @Test(expected=TargetResolutionException.class)
    public void testMoveWithNotExistEnity() throws Exception {
        final ActionDTO.Action action = ActionDTO.Action.newBuilder()
                .setId(44)
                .setImportance(0)
                .setInfo(ActionDTO.ActionInfo.newBuilder()
                        .setMove(ActionDTO.Move.newBuilder()
                                .setTargetId(1)
                                .setSourceId(2)
                                .setDestinationId(4)))
                .setExplanation(Explanation.newBuilder().build())
                .build();

        actionExecutor.getTargetId(action);
    }
}
