package com.vmturbo.action.orchestrator.execution;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
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

import com.google.common.collect.Lists;

import com.vmturbo.action.orchestrator.action.TestActionBuilder;
import com.vmturbo.action.orchestrator.execution.ActionTargetSelector.ActionTargetInfo;
import com.vmturbo.action.orchestrator.execution.ActionTargetSelector.TargetInfoResolver;
import com.vmturbo.action.orchestrator.execution.ProbeCapabilityCache.CachedCapabilities;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.topology.EntityInfoMoles.EntityServiceMole;
import com.vmturbo.common.protobuf.topology.EntityInfoOuterClass.EntityInfo;
import com.vmturbo.common.protobuf.topology.EntityInfoOuterClass.GetEntitiesInfoRequest;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;

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

    private ActionExecutionEntitySelector targetEntitySelectorMock;

    private ProbeCapabilityCache probeCapabilityCache = mock(ProbeCapabilityCache.class);

    private CachedCapabilities cachedCapabilities = mock(CachedCapabilities.class);

    private TargetInfoResolver mockTargetInfoResolver = mock(TargetInfoResolver.class);

    // A test helper class for building move actions
    TestActionBuilder testActionBuilder = new TestActionBuilder();

    @Before
    public void setup() {
        when(probeCapabilityCache.getCachedCapabilities()).thenReturn(cachedCapabilities);
        targetEntitySelectorMock = mock(ActionExecutionEntitySelector.class);
        MockitoAnnotations.initMocks(this);
        // The class under test
        actionTargetSelector = new ActionTargetSelector(mockTargetInfoResolver,
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
    public void testTargetInfoResolverMaxSupportingLevel() throws UnsupportedActionException {
        final TargetInfoResolver targetInfoResolver = new TargetInfoResolver(probeCapabilityCache);
        final ActionDTO.Action action = testActionBuilder.buildMoveAction(1, 2L, 1, 3L, 1);
        final ActionEntity actionEntity = ActionDTOUtil.getPrimaryEntity(action);
        final long target1Id = 1;
        final long probe1Id = 11;
        final long target2Id = 2;
        final long probe2Id = 22;
        final EntityInfo entityInfo = EntityInfo.newBuilder()
            .setEntityId(actionEntity.getId())
            .putTargetIdToProbeId(target1Id, probe1Id)
            .putTargetIdToProbeId(target2Id, probe2Id)
            .build();

        when(cachedCapabilities.getSupportLevel(action, actionEntity, probe1Id))
            .thenReturn(SupportLevel.SHOW_ONLY);
        when(cachedCapabilities.getProbeCategory(probe1Id))
            .thenReturn(Optional.of(TargetInfoResolver.PROBE_CATEGORY_PRIORITIES.get(0)));
        when(cachedCapabilities.getSupportLevel(action, actionEntity, probe2Id))
            .thenReturn(SupportLevel.SUPPORTED);
        // Target 2 has a "lower" probe category priority, but a higher support level.
        // The higher support level should win out.
        when(cachedCapabilities.getProbeCategory(probe2Id))
            .thenReturn(Optional.of(TargetInfoResolver.PROBE_CATEGORY_PRIORITIES.get(1)));

        final ActionTargetInfo targetInfo =
            targetInfoResolver.getTargetInfoForAction(action, actionEntity, entityInfo);
        assertThat(targetInfo.supportingLevel(), is(SupportLevel.SUPPORTED));
        assertThat(targetInfo.targetId().get(), is(target2Id));

    }

    @Test
    public void testTargetInfoResolverUnsupportedTarget() throws UnsupportedActionException {
        final TargetInfoResolver targetInfoResolver = new TargetInfoResolver(probeCapabilityCache);
        final ActionDTO.Action action = testActionBuilder.buildMoveAction(1, 2L, 1, 3L, 1);
        final ActionEntity actionEntity = ActionDTOUtil.getPrimaryEntity(action);
        final long target1Id = 1;
        final long probe1Id = 11;
        final EntityInfo entityInfo = EntityInfo.newBuilder()
            .setEntityId(actionEntity.getId())
            .putTargetIdToProbeId(target1Id, probe1Id)
            .build();

        when(cachedCapabilities.getSupportLevel(action, actionEntity, probe1Id))
            .thenReturn(SupportLevel.UNSUPPORTED);
        when(cachedCapabilities.getProbeCategory(probe1Id))
            .thenReturn(Optional.of(ProbeCategory.HYPERVISOR));

        final ActionTargetInfo targetInfo =
            targetInfoResolver.getTargetInfoForAction(action, actionEntity, entityInfo);
        assertThat(targetInfo.supportingLevel(), is(SupportLevel.UNSUPPORTED));
        assertThat(targetInfo.targetId().get(), is(target1Id));
    }

    @Test
    public void testTargetInfoResolverNoTargets() throws UnsupportedActionException {
        final TargetInfoResolver targetInfoResolver = new TargetInfoResolver(probeCapabilityCache);
        final ActionDTO.Action action = testActionBuilder.buildMoveAction(1, 2L, 1, 3L, 1);
        final ActionEntity actionEntity = ActionDTOUtil.getPrimaryEntity(action);
        final EntityInfo entityInfo = EntityInfo.newBuilder()
            .setEntityId(actionEntity.getId())
            // No targets discovered this entity... somehow.
            .build();

        final ActionTargetInfo targetInfo =
            targetInfoResolver.getTargetInfoForAction(action, actionEntity, entityInfo);
        assertThat(targetInfo.supportingLevel(), is(SupportLevel.UNSUPPORTED));
        assertFalse(targetInfo.targetId().isPresent());
    }

    @Test
    public void testTargetInfoResolverProbeCategoryPriority() throws UnsupportedActionException {
        final TargetInfoResolver targetInfoResolver = new TargetInfoResolver(probeCapabilityCache);
        final ActionDTO.Action action = testActionBuilder.buildMoveAction(1, 2L, 1, 3L, 1);
        final ActionEntity actionEntity = ActionDTOUtil.getPrimaryEntity(action);
        final long target1Id = 1;
        final long probe1Id = 11;
        final long target2Id = 2;
        final long probe2Id = 22;
        final EntityInfo entityInfo = EntityInfo.newBuilder()
            .setEntityId(actionEntity.getId())
            .putTargetIdToProbeId(target1Id, probe1Id)
            .putTargetIdToProbeId(target2Id, probe2Id)
            .build();

        when(cachedCapabilities.getSupportLevel(action, actionEntity, probe1Id))
            .thenReturn(SupportLevel.SUPPORTED);
        // Target 1 has a lower priority.
        when(cachedCapabilities.getProbeCategory(probe1Id))
            .thenReturn(Optional.of(TargetInfoResolver.PROBE_CATEGORY_PRIORITIES.get(1)));
        when(cachedCapabilities.getSupportLevel(action, actionEntity, probe2Id))
            .thenReturn(SupportLevel.SUPPORTED);
        // Target 2 has a higher priority.
        when(cachedCapabilities.getProbeCategory(probe2Id))
            .thenReturn(Optional.of(TargetInfoResolver.PROBE_CATEGORY_PRIORITIES.get(0)));

        final ActionTargetInfo targetInfo =
            targetInfoResolver.getTargetInfoForAction(action, actionEntity, entityInfo);
        assertThat(targetInfo.supportingLevel(), is(SupportLevel.SUPPORTED));
        // The selected target should be the one with higher priority.
        assertThat(targetInfo.targetId().get(), is(target2Id));
    }

    @Test
    public void testTargetInfoResolverNoProbeCategory() throws UnsupportedActionException {
        final TargetInfoResolver targetInfoResolver = new TargetInfoResolver(probeCapabilityCache);
        final ActionDTO.Action action = testActionBuilder.buildMoveAction(1, 2L, 1, 3L, 1);
        final ActionEntity actionEntity = ActionDTOUtil.getPrimaryEntity(action);
        final long target1Id = 1;
        final long probe1Id = 11;
        final long target2Id = 2;
        final long probe2Id = 22;
        final EntityInfo entityInfo = EntityInfo.newBuilder()
            .setEntityId(actionEntity.getId())
            .putTargetIdToProbeId(target1Id, probe1Id)
            .putTargetIdToProbeId(target2Id, probe2Id)
            .build();

        when(cachedCapabilities.getSupportLevel(action, actionEntity, probe1Id))
            .thenReturn(SupportLevel.SUPPORTED);
        // Target 1 has the lowest explicitly-specified priority (last probe in the list).
        when(cachedCapabilities.getProbeCategory(probe1Id))
            .thenReturn(Optional.of(TargetInfoResolver.PROBE_CATEGORY_PRIORITIES.get(
                TargetInfoResolver.PROBE_CATEGORY_PRIORITIES.size() - 1)));
        when(cachedCapabilities.getSupportLevel(action, actionEntity, probe2Id))
            .thenReturn(SupportLevel.SUPPORTED);
        // Target 2 has no known probe category. This shouldn't happen regularly, but
        // might happen based on the interface definition.
        when(cachedCapabilities.getProbeCategory(probe2Id))
            .thenReturn(Optional.empty());

        final ActionTargetInfo targetInfo =
            targetInfoResolver.getTargetInfoForAction(action, actionEntity, entityInfo);
        assertThat(targetInfo.supportingLevel(), is(SupportLevel.SUPPORTED));
        // The selected target should be the one with the explicitly-specified priority.
        assertThat(targetInfo.targetId().get(), is(target1Id));
    }

    @Test
    public void testTargetInfoResolverUnknownProbeCategoryLowerPriority() throws UnsupportedActionException {
        final TargetInfoResolver targetInfoResolver = new TargetInfoResolver(probeCapabilityCache);
        final ActionDTO.Action action = testActionBuilder.buildMoveAction(1, 2L, 1, 3L, 1);
        final ActionEntity actionEntity = ActionDTOUtil.getPrimaryEntity(action);
        final long target1Id = 1;
        final long probe1Id = 11;
        final long target2Id = 2;
        final long probe2Id = 22;
        final EntityInfo entityInfo = EntityInfo.newBuilder()
            .setEntityId(actionEntity.getId())
            .putTargetIdToProbeId(target1Id, probe1Id)
            .putTargetIdToProbeId(target2Id, probe2Id)
            .build();

        when(cachedCapabilities.getSupportLevel(action, actionEntity, probe1Id))
            .thenReturn(SupportLevel.SUPPORTED);
        // Target 1 has the lowest explicitly-specified priority (last probe in the list).
        when(cachedCapabilities.getProbeCategory(probe1Id))
            .thenReturn(Optional.of(TargetInfoResolver.PROBE_CATEGORY_PRIORITIES.get(
                TargetInfoResolver.PROBE_CATEGORY_PRIORITIES.size() - 1)));
        when(cachedCapabilities.getSupportLevel(action, actionEntity, probe2Id))
            .thenReturn(SupportLevel.SUPPORTED);
        // Target 2 has some unknown probe category.
        when(cachedCapabilities.getProbeCategory(probe2Id))
            .thenReturn(Optional.of(ProbeCategory.UNKNOWN));

        final ActionTargetInfo targetInfo =
            targetInfoResolver.getTargetInfoForAction(action, actionEntity, entityInfo);
        assertThat(targetInfo.supportingLevel(), is(SupportLevel.SUPPORTED));
        // The selected target should be the one with the explicitly-specified priority.
        assertThat(targetInfo.targetId().get(), is(target1Id));
    }

    @Test
    public void testMove() throws Exception {
        final long probeId = 10;
        final long targetId = 7;
        final long selectedEntityId = 1;
        final ActionDTO.Action action =
            testActionBuilder.buildMoveAction(selectedEntityId, 2L, 1, 3L, 1);
        final ActionDTO.ActionEntity selectedEntity = action.getInfo().getMove().getTarget();
        // No target selection for action execution special cases apply, return the normal
        // primary entity.
        when(targetEntitySelectorMock.getEntity(any()))
            .thenReturn(Optional.of(selectedEntity));
        final Map<Long, EntityInfo> entityInfoMap = Stream.of(1, 2, 3)
            .map(id -> EntityInfo.newBuilder()
                .setEntityId(id)
                .putTargetIdToProbeId(targetId, probeId)
                .build())
            .collect(Collectors.toMap(EntityInfo::getEntityId, Function.identity()));
        when(entityServiceMole.getEntitiesInfo(any()))
            .thenReturn(Lists.newArrayList(entityInfoMap.values()));

        final ActionTargetInfo actionTargetInfo = ImmutableActionTargetInfo.builder()
            .targetId(targetId)
            .supportingLevel(SupportLevel.SUPPORTED)
            .build();
        when(mockTargetInfoResolver.getTargetInfoForAction(action,
                selectedEntity, entityInfoMap.get(selectedEntityId)))
            .thenReturn(actionTargetInfo);

        Assert.assertEquals(actionTargetInfo, actionTargetSelector.getTargetForAction(action));

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
        final ActionDTO.Action action =
            testActionBuilder.buildMoveAction(selectedEntityId, 2L, 1, 4L, 1);
        final ActionDTO.ActionEntity selectedEntity = action.getInfo().getMove().getTarget();

        // No target selection for action execution special cases apply, return the target entity
        when(targetEntitySelectorMock.getEntity(any()))
                .thenReturn(Optional.of(selectedEntity));
        // The entity info returned will not include the requested entity (the selectedEntityId)
        when(entityServiceMole.getEntitiesInfo(any()))
                .thenReturn(Stream.of(2, 3, 4).map(id ->
                        EntityInfo.newBuilder()
                                .setEntityId(id)
                                .putTargetIdToProbeId(targetId, probeId)
                                .build()).collect(Collectors.toList()));
        // No target will be selected since we don't have entity data for the selected entity.
        assertThat(actionTargetSelector.getTargetForAction(action).supportingLevel(), is(SupportLevel.UNSUPPORTED));
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
        // Target entity selection throws exception.
        when(targetEntitySelectorMock.getEntity(any()))
                .thenThrow(UnsupportedActionException.class);
        final Action bogusAction = Action.newBuilder()
                .setId(23)
                .setDeprecatedImportance(1)
                .setExplanation(Explanation.newBuilder().build())
                .setInfo(ActionInfo.newBuilder()
                        // Explicitly clearing this for clarity to the reader
                        // An action with no actionType is bogus and not executable
                        .clearActionType()
                        .build()
                )
                .build();
        // Expect a no target, because this action doesn't have a valid type.
        assertThat(actionTargetSelector.getTargetForAction(bogusAction).supportingLevel(), is(SupportLevel.UNSUPPORTED));
    }

    @Test
    public void testNoSelectedEntity() throws Exception {
        final long probeId = 10;
        final long targetId = 7;
        final ActionDTO.Action action =
            testActionBuilder.buildMoveAction(1L, 2L, 1, 4L, 1);
        // Prepare some entity data to return when the Topology Processor is queried
        when(entityServiceMole.getEntitiesInfo(any()))
            .thenReturn(Stream.of(1, 2, 3).map(id ->
                EntityInfo.newBuilder()
                    .setEntityId(id)
                    .putTargetIdToProbeId(targetId, probeId)
                    .build()).collect(Collectors.toList()));
        // Target entity selection returns empty optional.
        when(targetEntitySelectorMock.getEntity(any()))
            .thenReturn(Optional.empty());
        // Expect a no target, because we can't select an entity to be the primary entity.
        assertThat(actionTargetSelector.getTargetForAction(action).supportingLevel(), is(SupportLevel.UNSUPPORTED));
    }
}
