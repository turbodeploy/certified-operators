package com.vmturbo.action.orchestrator.execution;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.google.common.collect.ImmutableSet;

import com.vmturbo.action.orchestrator.action.TestActionBuilder;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.topology.EntityInfoMoles.EntityServiceMole;
import com.vmturbo.common.protobuf.topology.EntityInfoOuterClass.EntityInfo;
import com.vmturbo.common.protobuf.topology.EntityInfoOuterClass.GetEntitiesInfoRequest;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Unit tests for the {@link ActionTargetSelector} class.
 */
public class ActionTargetSelectorTest {

    // The class under test
    private ActionTargetSelector actionTargetSelector;

    // Create a testable instance of the remote connection to Topology Processor
    private final EntityServiceMole entityServiceMole = Mockito.spy(new EntityServiceMole());

    @Rule
    public final GrpcTestServer server = GrpcTestServer.newServer(entityServiceMole);

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    // To capture the arguments sent to the entityServiceMole
    @Captor
    private ArgumentCaptor<GetEntitiesInfoRequest> entitiesRequestCaptor;

    private ActionTargetResolver actionTargetResolverMock;

    private ActionExecutionEntitySelector targetEntitySelectorMock;

    // A test helper class for building move actions
    TestActionBuilder testActionBuilder = new TestActionBuilder();

    @Before
    public void setup() {
        actionTargetResolverMock = Mockito.mock(ActionTargetResolver.class);
        targetEntitySelectorMock = Mockito.mock(ActionExecutionEntitySelector.class);
        MockitoAnnotations.initMocks(this);
        // The class under test
        actionTargetSelector = new ActionTargetSelector(actionTargetResolverMock,
                targetEntitySelectorMock,
                server.getChannel());
    }

    /**
     * Helper method to build a TopologyEntityDTO, populating only the entityId and the entityType
     *
     * @param entityId the id of the entity
     * @param entityType the type of the entity (used for target resolution)
     * @return
     */
    private static TopologyEntityDTO buildEntityDTO(final long entityId, final int entityType) {
        return TopologyEntityDTO.newBuilder()
                .setOid(entityId)
                .setEntityType(entityType)
                .build();
    }

    @Test
    public void testMove() throws Exception {
        final long probeId = 10;
        final long targetId = 7;
        final long selectedEntityId = 1;
        final int selectedEntityType = EntityType.VIRTUAL_MACHINE.getNumber();
        // No target selection for action execution special cases apply, return the target entity
        when(targetEntitySelectorMock.getEntityId(any()))
                .thenReturn(Optional.of(selectedEntityId));
        when(entityServiceMole.getEntitiesInfo(any()))
                .thenReturn(Stream.of(1, 2, 3).map(id ->
                        EntityInfo.newBuilder()
                                .setEntityId(id)
                                .putTargetIdToProbeId(targetId, probeId)
                                .build()).collect(Collectors.toList()));
        when(actionTargetResolverMock.resolveExecutantTarget(any(), eq(ImmutableSet.of(targetId))))
                .thenReturn(targetId);

        final ActionDTO.Action action =
                testActionBuilder.buildMoveAction(selectedEntityId, 2L, 1, 3L, 1);

        Assert.assertEquals(targetId, actionTargetSelector.getTargetId(action));

        // However, the backend should have been called, and we can capture
        // and examine the arguments.
        Mockito.verify(entityServiceMole).getEntitiesInfo(entitiesRequestCaptor.capture());
        final GetEntitiesInfoRequest sentRequest = entitiesRequestCaptor.getValue();
        // Verify that one entity info was requested...
        Assert.assertEquals(1, sentRequest.getEntityIdsCount());
        // ...and that its id matches the selectedEntityId
        Assert.assertEquals(selectedEntityId, sentRequest.getEntityIds(0));
    }

    @Test
    public void testMoveWithNotExistEntity() throws Exception {
        final long probeId = 10;
        final long targetId = 7;
        final long selectedEntityId = 1;
        final int selectedEntityType = EntityType.VIRTUAL_MACHINE.getNumber();
        // No target selection for action execution special cases apply, return the target entity
        when(targetEntitySelectorMock.getEntityId(any()))
                .thenReturn(Optional.of(selectedEntityId));
        // The entity info returned will not include the requested entity (the selectedEntityId)
        when(entityServiceMole.getEntitiesInfo(any()))
                .thenReturn(Stream.of(2, 3, 4).map(id ->
                        EntityInfo.newBuilder()
                                .setEntityId(id)
                                .putTargetIdToProbeId(targetId, probeId)
                                .build()).collect(Collectors.toList()));
        final ActionDTO.Action action =
                testActionBuilder.buildMoveAction(selectedEntityId, 2L, 1, 4L, 1);
        // This exception will be thrown since the entityServiceMole will not return the requested
        // entity data
        expectedException.expect(EntitiesResolutionException.class);
        actionTargetSelector.getTargetId(action);
    }

    /**
     * Tests getTarget(Action) for the case when there a multiple targets which can execute the
     * action.
     *
     * @throws TargetResolutionException if provided action was null or there are no entities for
     * action
     */
    @Test
    public void testGetTargetForActionWithMultipleTargets() throws Exception {
        final long probeId = 10;
        final long firstTargetId = 7;
        final long secondTargetId = 8;
        final long selectedEntityId = 1;
        final int selectedEntityType = EntityType.VIRTUAL_MACHINE.getNumber();
        // No target selection for action execution special cases apply
        when(targetEntitySelectorMock.getEntityId(any()))
                .thenReturn(Optional.of(selectedEntityId));
        when(entityServiceMole.getEntitiesInfo(any()))
                .thenReturn(Stream.of(1, 2, 3)
                        .map(id ->
                                EntityInfo.newBuilder()
                                        .setEntityId(id)
                                        .putTargetIdToProbeId(firstTargetId, probeId)
                                        .putTargetIdToProbeId(secondTargetId, probeId)
                                        .build())
                        .collect(Collectors.toList()));
        when(actionTargetResolverMock.resolveExecutantTarget(any(),
                eq(ImmutableSet.of(firstTargetId, secondTargetId))))
                .thenReturn(firstTargetId);
        final ActionDTO.Action action =
                testActionBuilder.buildMoveAction(selectedEntityId, 2L, 1, 3L, 1);

        Assert.assertEquals(firstTargetId, actionTargetSelector.getTargetId(action));
    }

    @Test
    public void testUnsupportedAction() throws Exception {
        final long probeId = 10;
        final long targetId = 7;
        // Prepare some entity data to return when the Topology Processor is queried
        when(entityServiceMole.getEntitiesInfo(any()))
                .thenReturn(Stream.of(1, 2, 3).map(id ->
                        EntityInfo.newBuilder()
                                .setEntityId(id)
                                .putTargetIdToProbeId(targetId, probeId)
                                .build()).collect(Collectors.toList()));
        when(actionTargetResolverMock.resolveExecutantTarget(any(),
                eq(ImmutableSet.of(targetId))))
                .thenReturn(targetId);
        // Target entity selection throws exception.
        when(targetEntitySelectorMock.getEntityId(any()))
                .thenThrow(UnsupportedActionException.class);
        final Action bogusAction = Action.newBuilder()
                .setId(23)
                .setImportance(1)
                .setExplanation(Explanation.newBuilder().build())
                .setInfo(ActionInfo.newBuilder()
                        // Explicitly clearing this for clarity to the reader
                        // An action with no actionType is bogus and not executable
                        .clearActionType()
                        .build()
                )
                .build();
        // Expect an UnsupportedActionException, because this action doesn't have a valid type
        expectedException.expect(UnsupportedActionException.class);
        actionTargetSelector.getTargetId(bogusAction);
    }

    /**
     * Test that the target cannot be resolved if the provided entity data does not match the action
     *
     * @throws TargetResolutionException because the provided entity data does not match the action
     */
    @Test
    public void testNoTargetForEntity() throws TargetResolutionException {
        final long anotherEntityId = 2;
        EntityInfo info = EntityInfo.newBuilder()
                .setEntityId(anotherEntityId)
                .build();
        final Action action = testActionBuilder.buildMoveAction(1L, 2L, 1, 3L, 1);
        expectedException.expect(TargetResolutionException.class);
        actionTargetSelector.getTargetId(action, info);
    }
}
