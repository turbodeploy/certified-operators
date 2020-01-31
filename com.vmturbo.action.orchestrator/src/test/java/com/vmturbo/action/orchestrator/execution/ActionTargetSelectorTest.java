package com.vmturbo.action.orchestrator.execution;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.google.common.collect.Maps;

import com.vmturbo.action.orchestrator.action.TestActionBuilder;
import com.vmturbo.action.orchestrator.action.constraint.ActionConstraintStoreFactory;
import com.vmturbo.action.orchestrator.execution.ActionTargetSelector.ActionTargetInfo;
import com.vmturbo.action.orchestrator.execution.ActionTargetSelector.TargetInfoResolver;
import com.vmturbo.action.orchestrator.execution.ProbeCapabilityCache.CachedCapabilities;
import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory;
import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory.EntitiesAndSettingsSnapshot;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.TopologyType;
import com.vmturbo.common.protobuf.repository.RepositoryDTOMoles.RepositoryServiceMole;
import com.vmturbo.common.protobuf.topology.EntityInfoMoles.EntityServiceMole;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntityBatch;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;

/**
 * Unit tests for the {@link ActionTargetSelector} class.
 */
public class ActionTargetSelectorTest {

    private static final long REALTIME_CONTEXT_ID = 777;

    // The class under test
    private ActionTargetSelector actionTargetSelector;

    private final EntitiesAndSettingsSnapshotFactory entitySettingsCache =
        mock(EntitiesAndSettingsSnapshotFactory.class);

    private final EntitiesAndSettingsSnapshot snapshot = mock(EntitiesAndSettingsSnapshot.class);

    private RepositoryServiceMole repositoryServiceSpy = spy(new RepositoryServiceMole());

    // Create a testable instance of the remote connection to Topology Processor
    private final EntityServiceMole entityServiceMole = Mockito.spy(new EntityServiceMole());

    @Rule
    public final GrpcTestServer repoServer = GrpcTestServer.newServer(
        repositoryServiceSpy);

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    // To capture the arguments sent to the entityServiceMole
    @Captor
    private ArgumentCaptor<RetrieveTopologyEntitiesRequest> entitiesRequestCaptor;

    private ActionExecutionEntitySelector targetEntitySelectorMock;

    private ProbeCapabilityCache probeCapabilityCache = mock(ProbeCapabilityCache.class);

    private ActionConstraintStoreFactory actionConstraintStoreFactory =
        mock(ActionConstraintStoreFactory.class);

    private CachedCapabilities cachedCapabilities = mock(CachedCapabilities.class);

    private TargetInfoResolver mockTargetInfoResolver = mock(TargetInfoResolver.class);

    // A test helper class for building move actions
    TestActionBuilder testActionBuilder = new TestActionBuilder();

    @Before
    public void setup() {
        when(snapshot.getOwnerAccountOfEntity(anyLong())).thenReturn(Optional.empty());
        when(probeCapabilityCache.getCachedCapabilities()).thenReturn(cachedCapabilities);
        when(entitySettingsCache.emptySnapshot()).thenReturn(new EntitiesAndSettingsSnapshot(
            Collections.emptyMap(), Maps.newHashMap(), null, Maps.newHashMap(), 0, TopologyType.SOURCE));
        targetEntitySelectorMock = mock(ActionExecutionEntitySelector.class);
        MockitoAnnotations.initMocks(this);
        // The class under test
        actionTargetSelector = new ActionTargetSelector(mockTargetInfoResolver,
            targetEntitySelectorMock,
            repoServer.getChannel(),
            REALTIME_CONTEXT_ID);
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
        final TargetInfoResolver targetInfoResolver =
            new TargetInfoResolver(probeCapabilityCache, actionConstraintStoreFactory);
        final ActionDTO.Action action = testActionBuilder.buildMoveAction(1, 2L, 1, 3L, 1);
        final ActionEntity actionEntity = ActionDTOUtil.getPrimaryEntity(action);
        final long target1Id = 1;
        final long probe1Id = 11;
        final long target2Id = 2;
        final long probe2Id = 22;
        final ActionPartialEntity entityInfo = ActionPartialEntity.newBuilder()
            .setOid(actionEntity.getId())
            .addAllDiscoveringTargetIds(Arrays.asList(target1Id, target2Id))
            .build();
        final Map<Long,Long> targetToProbe = new HashMap<>();
        targetToProbe.put(target1Id, probe1Id);
        targetToProbe.put(target2Id, probe2Id);

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
        when(cachedCapabilities.getProbeFromTarget(target1Id))
            .thenReturn(Optional.of(probe1Id));
        when(cachedCapabilities.getProbeFromTarget(target2Id))
            .thenReturn(Optional.of(probe2Id));

        final ActionTargetInfo targetInfo =
            targetInfoResolver.getTargetInfoForAction(action, actionEntity, entityInfo, snapshot);
        assertThat(targetInfo.supportingLevel(), is(SupportLevel.SUPPORTED));
        assertThat(targetInfo.targetId().get(), is(target2Id));

    }

    @Test
    public void testTargetInfoResolverUnsupportedTarget() throws UnsupportedActionException {
        final TargetInfoResolver targetInfoResolver =
            new TargetInfoResolver(probeCapabilityCache, actionConstraintStoreFactory);
        final ActionDTO.Action action = testActionBuilder.buildMoveAction(1, 2L, 1, 3L, 1);
        final ActionEntity actionEntity = ActionDTOUtil.getPrimaryEntity(action);
        final long target1Id = 1;
        final long probe1Id = 11;
        final ActionPartialEntity actionPartialEntity = ActionPartialEntity.newBuilder()
            .setOid(actionEntity.getId())
            .addAllDiscoveringTargetIds(Arrays.asList(target1Id))
            .build();
        final Map<Long,Long> targetToProbe = new HashMap<>();
        targetToProbe.put(target1Id, probe1Id);

        when(cachedCapabilities.getSupportLevel(action, actionEntity, probe1Id))
            .thenReturn(SupportLevel.UNSUPPORTED);
        when(cachedCapabilities.getProbeCategory(probe1Id))
            .thenReturn(Optional.of(ProbeCategory.HYPERVISOR));
        when(cachedCapabilities.getProbeFromTarget(target1Id))
            .thenReturn(Optional.of(probe1Id));

        final ActionTargetInfo targetInfo = targetInfoResolver.getTargetInfoForAction(
            action, actionEntity, actionPartialEntity, snapshot);
        assertThat(targetInfo.supportingLevel(), is(SupportLevel.UNSUPPORTED));
        assertThat(targetInfo.targetId().get(), is(target1Id));
    }

    @Test
    public void testTargetInfoResolverNoTargets() throws UnsupportedActionException {
        final TargetInfoResolver targetInfoResolver =
            new TargetInfoResolver(probeCapabilityCache, actionConstraintStoreFactory);
        final ActionDTO.Action action = testActionBuilder.buildMoveAction(1, 2L, 1, 3L, 1);
        final ActionEntity actionEntity = ActionDTOUtil.getPrimaryEntity(action);
        final ActionPartialEntity actionPartialEntity = ActionPartialEntity.newBuilder()
            .setOid(actionEntity.getId())
            // No targets discovered this entity... somehow.
            .build();
        final Map<Long,Long> targetToProbe = new HashMap<>();
        targetToProbe.put(1L, 2L);
        when(cachedCapabilities.getProbeFromTarget(1L))
            .thenReturn(Optional.of(2L));
        final ActionTargetInfo targetInfo = targetInfoResolver.getTargetInfoForAction(
            action, actionEntity, actionPartialEntity, snapshot);
        assertThat(targetInfo.supportingLevel(), is(SupportLevel.UNSUPPORTED));
        assertFalse(targetInfo.targetId().isPresent());
    }

    @Test
    public void testTargetInfoResolverProbeCategoryPriority() throws UnsupportedActionException {
        final TargetInfoResolver targetInfoResolver =
            new TargetInfoResolver(probeCapabilityCache, actionConstraintStoreFactory);
        final ActionDTO.Action action = testActionBuilder.buildMoveAction(1, 2L, 1, 3L, 1);
        final ActionEntity actionEntity = ActionDTOUtil.getPrimaryEntity(action);
        final long target1Id = 1;
        final long probe1Id = 11;
        final long target2Id = 2;
        final long probe2Id = 22;
        final ActionPartialEntity actionPartialEntity = ActionPartialEntity.newBuilder()
            .setOid(actionEntity.getId())
            .addAllDiscoveringTargetIds(Arrays.asList(target1Id, target2Id))
            .build();
        final Map<Long,Long> targetToProbe = new HashMap<>();
        targetToProbe.put(target1Id, probe1Id);
        targetToProbe.put(target2Id, probe2Id);

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
        when(cachedCapabilities.getProbeFromTarget(target1Id))
            .thenReturn(Optional.of(probe1Id));
        when(cachedCapabilities.getProbeFromTarget(target2Id))
            .thenReturn(Optional.of(probe2Id));

        final ActionTargetInfo targetInfo = targetInfoResolver.getTargetInfoForAction(
            action, actionEntity, actionPartialEntity, snapshot);
        assertThat(targetInfo.supportingLevel(), is(SupportLevel.SUPPORTED));
        // The selected target should be the one with higher priority.
        assertThat(targetInfo.targetId().get(), is(target2Id));
    }

    @Test
    public void testTargetInfoResolverNoProbeCategory() throws UnsupportedActionException {
        final TargetInfoResolver targetInfoResolver =
            new TargetInfoResolver(probeCapabilityCache, actionConstraintStoreFactory);
        final ActionDTO.Action action = testActionBuilder.buildMoveAction(1, 2L, 1, 3L, 1);
        final ActionEntity actionEntity = ActionDTOUtil.getPrimaryEntity(action);
        final long target1Id = 1;
        final long probe1Id = 11;
        final long target2Id = 2;
        final long probe2Id = 22;
        final ActionPartialEntity actionPartialEntity = ActionPartialEntity.newBuilder()
            .setOid(actionEntity.getId())
            .addAllDiscoveringTargetIds(Arrays.asList(target1Id, target2Id))
            .build();
        final Map<Long,Long> targetToProbe = new HashMap<>();
        targetToProbe.put(target1Id, probe1Id);
        targetToProbe.put(target2Id, probe2Id);
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
        when(cachedCapabilities.getProbeFromTarget(target1Id))
            .thenReturn(Optional.of(probe1Id));
        when(cachedCapabilities.getProbeFromTarget(target2Id))
            .thenReturn(Optional.of(probe2Id));
        final ActionTargetInfo targetInfo = targetInfoResolver.getTargetInfoForAction(
            action, actionEntity, actionPartialEntity, snapshot);
        assertThat(targetInfo.supportingLevel(), is(SupportLevel.SUPPORTED));
        // The selected target should be the one with the explicitly-specified priority.
        assertThat(targetInfo.targetId().get(), is(target1Id));
    }

    @Test
    public void testTargetInfoResolverUnknownProbeCategoryLowerPriority() throws UnsupportedActionException {
        final TargetInfoResolver targetInfoResolver =
            new TargetInfoResolver(probeCapabilityCache, actionConstraintStoreFactory);
        final ActionDTO.Action action = testActionBuilder.buildMoveAction(1, 2L, 1, 3L, 1);
        final ActionEntity actionEntity = ActionDTOUtil.getPrimaryEntity(action);
        final long target1Id = 1;
        final long probe1Id = 11;
        final long target2Id = 2;
        final long probe2Id = 22;
        final ActionPartialEntity actionPartialEntity = ActionPartialEntity.newBuilder()
            .setOid(actionEntity.getId())
            .addAllDiscoveringTargetIds(Arrays.asList(target1Id))
            .build();
        final Map<Long,Long> targetToProbe = new HashMap<>();
        targetToProbe.put(target1Id, probe1Id);
        targetToProbe.put(target2Id, probe2Id);


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
        when(cachedCapabilities.getProbeFromTarget(target1Id))
            .thenReturn(Optional.of(probe1Id));
        when(cachedCapabilities.getProbeFromTarget(target2Id))
            .thenReturn(Optional.of(probe2Id));

        final ActionTargetInfo targetInfo = targetInfoResolver.getTargetInfoForAction(
            action, actionEntity, actionPartialEntity, snapshot);
        assertThat(targetInfo.supportingLevel(), is(SupportLevel.SUPPORTED));
        // The selected target should be the one with the explicitly-specified priority.
        assertThat(targetInfo.targetId().get(), is(target1Id));
    }

    @Test
    public void testMove() throws Exception {
        final long probeId = 10;
        final long targetId = 7;
        final long selectedEntityId = 1;
        final Map<Long,Long> targetToProbe = new HashMap<>();
        targetToProbe.put(targetId, probeId);
        final ActionDTO.Action action =
            testActionBuilder.buildMoveAction(selectedEntityId, 2L, 1, 3L, 1);
        final ActionDTO.ActionEntity selectedEntity = action.getInfo().getMove().getTarget();
        final ActionPartialEntity actionPartialEntity =
            ActionPartialEntity.newBuilder().setOid(selectedEntityId).addAllDiscoveringTargetIds(Arrays.asList(targetId)).build();
        // No target selection for action execution special cases apply, return the normal
        // primary entity.
        PartialEntityBatch batch =
            PartialEntityBatch.newBuilder().addEntities(PartialEntity.newBuilder().setAction(actionPartialEntity).build()).build();
        when(repositoryServiceSpy.retrieveTopologyEntities(any())).thenReturn(Arrays.asList(batch));
        when(targetEntitySelectorMock.getEntity(any()))
            .thenReturn(Optional.of(selectedEntity));

        final ActionTargetInfo actionTargetInfo = ImmutableActionTargetInfo.builder()
            .targetId(targetId)
            .supportingLevel(SupportLevel.SUPPORTED)
            .build();

        when(cachedCapabilities.getProbeFromTarget(targetId)).thenReturn(Optional.of(probeId));
        when(cachedCapabilities.getProbeFromTarget(targetId))
            .thenReturn(Optional.of(probeId));
        when(mockTargetInfoResolver.getTargetInfoForAction(eq(action),
                eq(selectedEntity), eq(actionPartialEntity), any()))
            .thenReturn(actionTargetInfo);
        Assert.assertEquals(actionTargetInfo,
            actionTargetSelector.getTargetForAction(action, entitySettingsCache));

        // However, the backend should have been called, and we can capture
        // and examine the arguments.
        Mockito.verify(repositoryServiceSpy).retrieveTopologyEntities(entitiesRequestCaptor.capture());
        final RetrieveTopologyEntitiesRequest sentRequest = entitiesRequestCaptor.getValue();
        // Verify that one entity info was requested...
        Assert.assertEquals(3, sentRequest.getEntityOidsCount());
        // ...and that its id matches the selectedEntityId
        Assert.assertEquals(selectedEntityId, sentRequest.getEntityOids(0));
    }

    @Test
    public void testMoveWithNotExistEntity() throws Exception {
        final long probeId = 10;
        final long targetId = 7;
        final long selectedEntityId = 1;
        final ActionDTO.Action action =
            testActionBuilder.buildMoveAction(selectedEntityId, 2L, 1, 4L, 1);
        final ActionDTO.ActionEntity selectedEntity = action.getInfo().getMove().getTarget();
        final ActionPartialEntity actionPartialEntity =
            ActionPartialEntity.newBuilder().setOid(2).addAllDiscoveringTargetIds(Arrays.asList(targetId)).build();
        // No target selection for action execution special cases apply, return the normal
        // primary entity.
        PartialEntityBatch batch =
            PartialEntityBatch.newBuilder().addEntities(PartialEntity.newBuilder().setAction(actionPartialEntity).build()).build();
        when(repositoryServiceSpy.retrieveTopologyEntities(any())).thenReturn(Arrays.asList(batch));
        // No target selection for action execution special cases apply, return the target entity
        when(targetEntitySelectorMock.getEntity(any()))
                .thenReturn(Optional.of(selectedEntity));
        // The entity info returned will not include the requested entity (the selectedEntityId)
        when(cachedCapabilities.getProbeFromTarget(targetId)).thenReturn(Optional.of(probeId));

        when(repositoryServiceSpy.retrieveTopologyEntities(any())).thenReturn(Arrays.asList(batch));
        // No target will be selected since we don't have entity data for the selected entity.
        assertThat(actionTargetSelector.getTargetForAction(action, entitySettingsCache).supportingLevel(),
            is(SupportLevel.UNSUPPORTED));
    }

    @Test
    public void testUnsupportedAction() throws Exception {
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
        assertThat(actionTargetSelector.getTargetForAction(bogusAction, entitySettingsCache).supportingLevel(),
            is(SupportLevel.UNSUPPORTED));
    }

    @Test
    public void testNoSelectedEntity() throws Exception {
        final ActionDTO.Action action =
            testActionBuilder.buildMoveAction(1L, 2L, 1, 4L, 1);
        // Target entity selection returns empty optional.
        when(targetEntitySelectorMock.getEntity(any()))
            .thenReturn(Optional.empty());
        // Expect a no target, because we can't select an entity to be the primary entity.
        assertThat(actionTargetSelector.getTargetForAction(action, entitySettingsCache).supportingLevel(),
            is(SupportLevel.UNSUPPORTED));
    }
}
