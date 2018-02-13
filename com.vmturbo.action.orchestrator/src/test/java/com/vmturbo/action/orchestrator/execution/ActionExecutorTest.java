package com.vmturbo.action.orchestrator.execution;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.google.common.collect.ImmutableSet;

import com.vmturbo.action.orchestrator.action.ActionTest;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo.ActionTypeCase;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.topology.ActionExecution.ExecuteActionRequest;
import com.vmturbo.common.protobuf.topology.ActionExecutionMoles.ActionExecutionServiceMole;
import com.vmturbo.common.protobuf.topology.EntityInfoMoles.EntityServiceMole;
import com.vmturbo.common.protobuf.topology.EntityInfoOuterClass.EntityInfo;
import com.vmturbo.components.api.test.GrpcTestServer;

/**
 * Unit tests for the {@link ActionExecutor} class.
 */
public class ActionExecutorTest {

    private ActionExecutor actionExecutor;

    private final long probeId = 10;
    private final long targetId = 7;

    private ActionTargetResolver actionTargetResolver;

    private final ActionExecutionServiceMole actionExecutionBackend =
            Mockito.spy(new ActionExecutionServiceMole());

    private final EntityServiceMole entityServiceMole =
            Mockito.spy(new EntityServiceMole());

    @Captor
    private ArgumentCaptor<ExecuteActionRequest> actionSpecCaptor;

    @Rule
    public final GrpcTestServer server =
            GrpcTestServer.newServer(actionExecutionBackend, entityServiceMole);

    @Before
    public void setup() throws IOException, TargetResolutionException {
        actionTargetResolver = Mockito.mock(ActionTargetResolver.class);
        MockitoAnnotations.initMocks(this);
        actionExecutor = new ActionExecutor(server.getChannel(), actionTargetResolver);
    }

    @Test
    public void testMove() throws Exception {
        when(entityServiceMole.getEntitiesInfo(any()))
            .thenReturn(Stream.of(1, 2, 3).map(id ->
                EntityInfo.newBuilder()
                    .setEntityId(id)
                    .putTargetIdToProbeId(targetId, probeId)
                    .build()).collect(Collectors.toList()));
        when(actionTargetResolver.resolveExecutantTarget(any(), eq(ImmutableSet.of(targetId))))
                .thenReturn(targetId);

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
        Mockito.verify(actionExecutionBackend).executeAction(actionSpecCaptor.capture(), any());
        final ExecuteActionRequest sentSpec = actionSpecCaptor.getValue();
        Assert.assertTrue(sentSpec.hasActionInfo());
        Assert.assertEquals(ActionTypeCase.MOVE, sentSpec.getActionInfo().getActionTypeCase());
        final Move move = sentSpec.getActionInfo().getMove();
        Assert.assertEquals(targetId, sentSpec.getTargetId());
        Assert.assertEquals(1, move.getTargetId());
        Assert.assertEquals(1, move.getChangesCount());
        Assert.assertEquals(2, move.getChanges(0).getSourceId());
        Assert.assertEquals(3, move.getChanges(0).getDestinationId());
    }

    @Test(expected = TargetResolutionException.class)
    public void testMoveWithNotExistEntity() throws Exception {
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
        when(entityServiceMole.getEntitiesInfo(any()))
            .thenReturn(Stream.of(1, 2, 3)
                .map(id ->
                    EntityInfo.newBuilder()
                            .setEntityId(id)
                            .putTargetIdToProbeId(targetId, probeId)
                            .putTargetIdToProbeId(targetId + 1, probeId)
                            .build())
                .collect(Collectors.toList()));
        when(actionTargetResolver.resolveExecutantTarget(any(),
                eq(ImmutableSet.of(targetId, targetId + 1))))
            .thenReturn(targetId);
        final ActionDTO.Action action = buildMoveAction(1, 2,
                3);

        actionExecutor = new ActionExecutor(server.getChannel(), actionTargetResolver);
        Assert.assertEquals(targetId, actionExecutor.getTargetId(action));
    }

    @Nonnull
    private Action buildMoveAction(long targetId, long sourceId, long destinationId) {
        return Action.newBuilder().setId(1).setImportance(1)
                .setExplanation(Explanation.newBuilder().build())
                .setInfo(ActionTest.makeMoveInfo(targetId, sourceId, destinationId))
                .build();
    }

    @Test
    public void testGetEntitiesTarget() throws Exception {
        Map<Long, EntityInfo> mapArg = new HashMap<>();
        EntityInfo info1 = EntityInfo.newBuilder()
                .setEntityId(1)
                .putTargetIdToProbeId(targetId, probeId)
                .build();
        EntityInfo info2 = EntityInfo.newBuilder()
                .setEntityId(2)
                .putTargetIdToProbeId(targetId, probeId)
                .build();
        mapArg.put(1L, info1);
        mapArg.put(2L, info2);

        when(actionTargetResolver.resolveExecutantTarget(any(),
                eq(ImmutableSet.of(targetId))))
                .thenReturn(targetId);

        Assert.assertEquals(Long.valueOf(targetId),
                actionExecutor.getEntitiesTarget(buildMoveAction(1, 2, 3), mapArg));
    }

    @Test
    public void testGetEntitiesNoTarget() {
        long targetId2 = 20;
        Map<Long, EntityInfo> mapArg = new HashMap<>();
        EntityInfo info1 = EntityInfo.newBuilder()
                .setEntityId(1)
                .putTargetIdToProbeId(targetId, probeId)
                .build();
        EntityInfo info2 = EntityInfo.newBuilder()
                .setEntityId(2)
                .putTargetIdToProbeId(targetId2, probeId)
                .build();
        mapArg.put(1L, info1);
        mapArg.put(2L, info2);
        try {
            actionExecutor.getEntitiesTarget(buildMoveAction(1, 2, 3), mapArg);
            Assert.fail();
        } catch (TargetResolutionException e) {
            Assert.assertTrue(e.getMessage()
                    .endsWith(" has no overlapping targets between the entities involved."));
        }
    }
}
