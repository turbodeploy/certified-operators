package com.vmturbo.action.orchestrator.execution;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import io.grpc.stub.StreamObserver;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo.ActionTypeCase;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.topology.ActionExecution.ExecuteActionRequest;
import com.vmturbo.common.protobuf.topology.ActionExecutionServiceGrpc.ActionExecutionServiceImplBase;
import com.vmturbo.common.protobuf.topology.EntityInfoMoles.EntityServiceMole;
import com.vmturbo.common.protobuf.topology.EntityInfoOuterClass;
import com.vmturbo.common.protobuf.topology.EntityInfoOuterClass.EntityInfo;
import com.vmturbo.common.protobuf.topology.EntityInfoOuterClass.GetEntitiesInfoRequest;
import com.vmturbo.common.protobuf.topology.EntityServiceGrpc;
import com.vmturbo.common.protobuf.topology.EntityServiceGrpc.EntityServiceImplBase;
import com.vmturbo.components.api.test.GrpcTestServer;

/**
 * Unit tests for the {@link ActionExecutor} class.
 */
public class ActionExecutorTest {

    private ActionExecutor actionExecutor;

    private final long probeId = 10;
    private final long targetId = 7;

    private final long moveTargetEntityId = 11L;
    private final long moveSourceEntityId = 22L;
    private final long moveDestinationEntityId = 33L;

    private final long resolvedTargetId = moveSourceEntityId;

    private ActionTargetResolver actionTargetResolver;

    private final Map<Long, Long> targetToProbeMap = ImmutableMap.of(moveTargetEntityId,
            moveTargetEntityId, moveSourceEntityId, moveSourceEntityId,
            moveDestinationEntityId, moveDestinationEntityId);


    private final ActionExecutionServiceImplBase actionExecutionBackend =
            Mockito.spy(new ActionExecutionServiceImplBase() {} );

    private final EntityServiceGrpc.EntityServiceImplBase entityServiceBackend =
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

    private final EntityServiceMole targetsConflictEntityService = Mockito.spy(new
            EntityServiceMole());

    @Captor
    private ArgumentCaptor<ExecuteActionRequest> actionSpecCaptor;

    @Rule
    public final GrpcTestServer server =
            GrpcTestServer.newServer(actionExecutionBackend, entityServiceBackend);

    @Rule
    public final GrpcTestServer targetConflictServer =
            GrpcTestServer.newServer(actionExecutionBackend, targetsConflictEntityService);

    @Before
    public void setup() throws IOException, TargetResolutionException {
        actionTargetResolver = Mockito.mock(ActionTargetResolver.class);
        MockitoAnnotations.initMocks(this);
        Mockito.when(actionTargetResolver.resolveExecutantTarget(Mockito.any(ActionDTO.Action
                .class), Mockito.anySet())).thenReturn(resolvedTargetId);
        actionExecutor = new ActionExecutor(server.getChannel(), actionTargetResolver);
        Mockito.when(targetsConflictEntityService.getEntitiesInfo(Mockito.any()))
                .thenReturn(ImmutableList.of(
                        buidEntityInfo(moveTargetEntityId),
                        buidEntityInfo(moveSourceEntityId),
                        buidEntityInfo(moveDestinationEntityId)));
    }

    @Nonnull
    private EntityInfo buidEntityInfo(long id) {
        return EntityInfo.newBuilder().setEntityId(id)
                .putAllTargetIdToProbeId(targetToProbeMap).build();
    }

    @Test
    public void testMove() throws Exception {
        final ActionDTO.Action action = buildMoveAction(1, 2, 3);

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
        final ExecuteActionRequest sentSpec = actionSpecCaptor.getValue();
        Assert.assertTrue(sentSpec.hasActionInfo());
        Assert.assertEquals(ActionTypeCase.MOVE, sentSpec.getActionInfo().getActionTypeCase());
        final Move move = sentSpec.getActionInfo().getMove();
        Assert.assertEquals(targetId, sentSpec.getTargetId());
        Assert.assertEquals(1, move.getTargetId());
        Assert.assertEquals(2, move.getSourceId());
        Assert.assertEquals(3, move.getDestinationId());
    }

    @Test(expected = TargetResolutionException.class)
    public void testMoveWithNotExistEnity() throws Exception {
        final ActionDTO.Action action = buildMoveAction(1, 2, 4);

        actionExecutor.getTargetId(action);
    }

    /**
     * Tests getTarget(Action) for the case when there a multiple targets which can execute the
     * action.
     *
     * @throws TargetResolutionException if provided action was null or there are no entities for
     * action
     */
    @Test
    public void testGetTargetForActionWithConflict() throws TargetResolutionException {
        final ActionDTO.Action action = buildMoveAction(moveTargetEntityId, moveSourceEntityId,
                moveDestinationEntityId);

        actionExecutor = new ActionExecutor(targetConflictServer.getChannel(), actionTargetResolver);
        Assert.assertEquals(resolvedTargetId, actionExecutor.getTargetId(action));
    }

    @Nonnull
    private Action buildMoveAction(long targetId, long sourceId, long destinationId) {
        return Action.newBuilder().setId(1).setImportance(1)
                .setExplanation(Explanation.newBuilder().build()).setInfo(ActionDTO.ActionInfo
                        .newBuilder().setMove(Move.newBuilder().setTargetId(targetId)
                                .setSourceId(sourceId).setDestinationId(destinationId)
                                .build()).build()).build();
    }
}
