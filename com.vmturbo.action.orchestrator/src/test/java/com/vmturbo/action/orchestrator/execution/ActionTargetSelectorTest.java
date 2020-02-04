package com.vmturbo.action.orchestrator.execution;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

import com.google.common.collect.ImmutableMap;

import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.vmturbo.action.orchestrator.action.TestActionBuilder;
import com.vmturbo.action.orchestrator.action.constraint.ActionConstraintStoreFactory;
import com.vmturbo.action.orchestrator.execution.ActionTargetSelector.ActionTargetInfo;
import com.vmturbo.action.orchestrator.execution.ProbeCapabilityCache.CachedCapabilities;
import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory;
import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory.EntitiesAndSettingsSnapshot;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.TopologyType;
import com.vmturbo.common.protobuf.repository.RepositoryDTOMoles.RepositoryServiceMole;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntityBatch;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;

/**
 * Unit tests for the {@link ActionTargetSelector} class.
 */
public class ActionTargetSelectorTest {

    private static final long REALTIME_CONTEXT_ID = 777;

    // The class under test
    private ActionTargetSelector actionTargetSelector;

    private final EntitiesAndSettingsSnapshotFactory entitySettingsCache =
            Mockito.mock(EntitiesAndSettingsSnapshotFactory.class);

    private final EntitiesAndSettingsSnapshot snapshot =
            Mockito.mock(EntitiesAndSettingsSnapshot.class);

    private RepositoryServiceMole repositoryServiceSpy = Mockito.spy(new RepositoryServiceMole());

    /**
     * GRPC test server.
     */
    @Rule
    public final GrpcTestServer repoServer = GrpcTestServer.newServer(repositoryServiceSpy);

    /**
     * Expected exception rule.
     */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private ProbeCapabilityCache probeCapabilityCache = Mockito.mock(ProbeCapabilityCache.class);

    private ActionConstraintStoreFactory actionConstraintStoreFactory =
            Mockito.mock(ActionConstraintStoreFactory.class);

    private CachedCapabilities cachedCapabilities = Mockito.mock(CachedCapabilities.class);

    // A test helper class for building move actions
    TestActionBuilder testActionBuilder = new TestActionBuilder();
    private TargetInfoResolver targetInfoResolver;

    /**
     * Setup.
     */
    @Before
    public void setup() {
        Mockito.when(snapshot.getOwnerAccountOfEntity(Mockito.anyLong()))
                .thenReturn(Optional.empty());
        Mockito.when(probeCapabilityCache.getCachedCapabilities()).thenReturn(cachedCapabilities);
        Mockito.when(entitySettingsCache.emptySnapshot())
                .thenReturn(new EntitiesAndSettingsSnapshot(Collections.emptyMap(),
                        Collections.emptyMap(), null, Collections.emptyMap(), 0,
                        TopologyType.SOURCE));
        MockitoAnnotations.initMocks(this);
        // The class under test
        actionTargetSelector = new ActionTargetSelector(probeCapabilityCache,
                Mockito.mock(ActionConstraintStoreFactory.class), repoServer.getChannel(),
                REALTIME_CONTEXT_ID);
        targetInfoResolver =
                new TargetInfoResolver(probeCapabilityCache, actionConstraintStoreFactory,
                        new EntityAndActionTypeBasedEntitySelector());
    }

    @Test
    public void testTargetInfoResolverMaxSupportingLevel() throws UnsupportedActionException {
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

        Mockito.when(cachedCapabilities.getSupportLevel(action, actionEntity, probe1Id))
                .thenReturn(SupportLevel.SHOW_ONLY);
        Mockito.when(cachedCapabilities.getProbeCategory(probe1Id))
                .thenReturn(Optional.of(ProbeCategory.CLOUD_NATIVE));
        Mockito.when(cachedCapabilities.getSupportLevel(action, actionEntity, probe2Id))
                .thenReturn(SupportLevel.SUPPORTED);
        // Target 2 has a "lower" probe category priority, but a higher support level.
        // The higher support level should win out.
        Mockito.when(cachedCapabilities.getProbeCategory(probe2Id))
                .thenReturn(Optional.of(ProbeCategory.CLOUD_MANAGEMENT));
        Mockito.when(cachedCapabilities.getProbeFromTarget(target1Id))
                .thenReturn(Optional.of(probe1Id));
        Mockito.when(cachedCapabilities.getProbeFromTarget(target2Id))
                .thenReturn(Optional.of(probe2Id));

        final ActionTargetInfo targetInfo = targetInfoResolver.getTargetInfoForAction(action,
                Collections.singletonMap(entityInfo.getOid(), entityInfo), snapshot);
        Assert.assertThat(targetInfo.supportingLevel(), Matchers.is(SupportLevel.SUPPORTED));
        Assert.assertThat(targetInfo.targetId().get(), Matchers.is(target2Id));
    }

    @Test
    public void testTargetInfoResolverUnsupportedTarget() throws UnsupportedActionException {
        final ActionDTO.Action action = testActionBuilder.buildMoveAction(1, 2L, 1, 3L, 1);
        final ActionEntity actionEntity = ActionDTOUtil.getPrimaryEntity(action);
        final long target1Id = 1;
        final long probe1Id = 11;
        final ActionPartialEntity actionPartialEntity = ActionPartialEntity.newBuilder()
                .setOid(actionEntity.getId())
                .addAllDiscoveringTargetIds(Collections.singletonList(target1Id))
                .build();

        Mockito.when(cachedCapabilities.getSupportLevel(action, actionEntity, probe1Id))
                .thenReturn(SupportLevel.UNSUPPORTED);
        Mockito.when(cachedCapabilities.getProbeCategory(probe1Id))
                .thenReturn(Optional.of(ProbeCategory.HYPERVISOR));
        Mockito.when(cachedCapabilities.getProbeFromTarget(target1Id))
                .thenReturn(Optional.of(probe1Id));

        final ActionTargetInfo targetInfo = targetInfoResolver.getTargetInfoForAction(action,
                Collections.singletonMap(actionPartialEntity.getOid(), actionPartialEntity),
                snapshot);
        Assert.assertThat(targetInfo.supportingLevel(), Matchers.is(SupportLevel.UNSUPPORTED));
        Assert.assertThat(targetInfo.targetId().get(), Matchers.is(target1Id));
    }

    @Test
    public void testTargetInfoResolverNoTargets() throws UnsupportedActionException {
        final ActionDTO.Action action = testActionBuilder.buildMoveAction(1, 2L, 1, 3L, 1);
        final ActionEntity actionEntity = ActionDTOUtil.getPrimaryEntity(action);
        final ActionPartialEntity actionPartialEntity =
                ActionPartialEntity.newBuilder().setOid(actionEntity.getId())
                        // No targets discovered this entity... somehow.
                        .build();
        Mockito.when(cachedCapabilities.getProbeFromTarget(1L)).thenReturn(Optional.of(2L));
        final ActionTargetInfo targetInfo = targetInfoResolver.getTargetInfoForAction(action,
                Collections.singletonMap(actionPartialEntity.getOid(), actionPartialEntity),
                snapshot);
        Assert.assertThat(targetInfo.supportingLevel(), Matchers.is(SupportLevel.UNSUPPORTED));
        Assert.assertFalse(targetInfo.targetId().isPresent());
    }

    @Test
    public void testTargetInfoResolverProbeCategoryPriority() throws UnsupportedActionException {
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

        Mockito.when(cachedCapabilities.getSupportLevel(action, actionEntity, probe1Id))
                .thenReturn(SupportLevel.SUPPORTED);
        // Target 1 has a lower priority.
        Mockito.when(cachedCapabilities.getProbeCategory(probe1Id))
                .thenReturn(Optional.of(ProbeCategory.CLOUD_MANAGEMENT));
        Mockito.when(cachedCapabilities.getSupportLevel(action, actionEntity, probe2Id))
                .thenReturn(SupportLevel.SUPPORTED);
        // Target 2 has a higher priority.
        Mockito.when(cachedCapabilities.getProbeCategory(probe2Id))
                .thenReturn(Optional.of(ProbeCategory.CLOUD_NATIVE));
        Mockito.when(cachedCapabilities.getProbeFromTarget(target1Id))
                .thenReturn(Optional.of(probe1Id));
        Mockito.when(cachedCapabilities.getProbeFromTarget(target2Id))
                .thenReturn(Optional.of(probe2Id));

        final ActionTargetInfo targetInfo = targetInfoResolver.getTargetInfoForAction(action,
                Collections.singletonMap(actionPartialEntity.getOid(), actionPartialEntity),
                snapshot);
        Assert.assertThat(targetInfo.supportingLevel(), Matchers.is(SupportLevel.SUPPORTED));
        // The selected target should be the one with higher priority.
        Assert.assertThat(targetInfo.targetId().get(), Matchers.is(target2Id));
    }

    @Test
    public void testTargetInfoResolverNoProbeCategory() throws UnsupportedActionException {
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
        Mockito.when(cachedCapabilities.getSupportLevel(action, actionEntity, probe1Id))
                .thenReturn(SupportLevel.SUPPORTED);
        // Target 1 has the lowest explicitly-specified priority (last probe in the list).
        Mockito.when(cachedCapabilities.getProbeCategory(probe1Id))
                .thenReturn(Optional.of(ProbeCategory.GUEST_OS_PROCESSES));
        Mockito.when(cachedCapabilities.getSupportLevel(action, actionEntity, probe2Id))
                .thenReturn(SupportLevel.SUPPORTED);
        // Target 2 has no known probe category. This shouldn't happen regularly, but
        // might happen based on the interface definition.
        Mockito.when(cachedCapabilities.getProbeCategory(probe2Id)).thenReturn(Optional.empty());
        Mockito.when(cachedCapabilities.getProbeFromTarget(target1Id))
                .thenReturn(Optional.of(probe1Id));
        Mockito.when(cachedCapabilities.getProbeFromTarget(target2Id))
                .thenReturn(Optional.of(probe2Id));
        final ActionTargetInfo targetInfo = targetInfoResolver.getTargetInfoForAction(action,
                Collections.singletonMap(actionPartialEntity.getOid(), actionPartialEntity),
                snapshot);
        Assert.assertThat(targetInfo.supportingLevel(), Matchers.is(SupportLevel.SUPPORTED));
        // The selected target should be the one with the explicitly-specified priority.
        Assert.assertThat(targetInfo.targetId().get(), Matchers.is(target1Id));
    }

    @Test
    public void testTargetInfoResolverUnknownProbeCategoryLowerPriority()
            throws UnsupportedActionException {
        final ActionDTO.Action action = testActionBuilder.buildMoveAction(1, 2L, 1, 3L, 1);
        final ActionEntity actionEntity = ActionDTOUtil.getPrimaryEntity(action);
        final long target1Id = 1;
        final long probe1Id = 11;
        final long target2Id = 2;
        final long probe2Id = 22;
        final ActionPartialEntity actionPartialEntity = ActionPartialEntity.newBuilder()
                .setOid(actionEntity.getId())
                .addAllDiscoveringTargetIds(Collections.singletonList(target1Id))
                .build();

        Mockito.when(cachedCapabilities.getSupportLevel(action, actionEntity, probe1Id))
                .thenReturn(SupportLevel.SUPPORTED);
        // Target 1 has the lowest explicitly-specified priority (last probe in the list).
        Mockito.when(cachedCapabilities.getProbeCategory(probe1Id))
                .thenReturn(Optional.of(ProbeCategory.GUEST_OS_PROCESSES));
        Mockito.when(cachedCapabilities.getSupportLevel(action, actionEntity, probe2Id))
                .thenReturn(SupportLevel.SUPPORTED);
        // Target 2 has some unknown probe category.
        Mockito.when(cachedCapabilities.getProbeCategory(probe2Id))
                .thenReturn(Optional.of(ProbeCategory.UNKNOWN));
        Mockito.when(cachedCapabilities.getProbeFromTarget(target1Id))
                .thenReturn(Optional.of(probe1Id));
        Mockito.when(cachedCapabilities.getProbeFromTarget(target2Id))
                .thenReturn(Optional.of(probe2Id));

        final ActionTargetInfo targetInfo = targetInfoResolver.getTargetInfoForAction(action,
                Collections.singletonMap(actionPartialEntity.getOid(), actionPartialEntity),
                snapshot);
        Assert.assertThat(targetInfo.supportingLevel(), Matchers.is(SupportLevel.SUPPORTED));
        // The selected target should be the one with the explicitly-specified priority.
        Assert.assertThat(targetInfo.targetId().get(), Matchers.is(target1Id));
    }

    @Test
    public void testMove() {
        final long probeId = 10;
        final long targetId = 7;
        final long selectedEntityId = 1;
        final ActionDTO.Action action =
                testActionBuilder.buildMoveAction(selectedEntityId, 2L, 1, 3L, 1);
        final ActionDTO.ActionEntity selectedEntity = action.getInfo().getMove().getTarget();
        final ActionPartialEntity actionPartialEntity = ActionPartialEntity.newBuilder()
                .setOid(selectedEntityId)
                .addAllDiscoveringTargetIds(Collections.singletonList(targetId))
                .build();
        // No target selection for action execution special cases apply, return the normal
        // primary entity.
        PartialEntityBatch batch = PartialEntityBatch.newBuilder()
                .addEntities(PartialEntity.newBuilder().setAction(actionPartialEntity).build())
                .build();
        Mockito.when(repositoryServiceSpy.retrieveTopologyEntities(Mockito.any()))
                .thenReturn(Collections.singletonList(batch));

        final ActionTargetInfo actionTargetInfo = ImmutableActionTargetInfo.builder()
                .targetId(targetId)
                .supportingLevel(SupportLevel.SUPPORTED)
                .build();

        Mockito.when(cachedCapabilities.getProbeFromTarget(targetId))
                .thenReturn(Optional.of(probeId));
        Mockito.when(cachedCapabilities.getSupportLevel(action, selectedEntity, probeId))
                .thenReturn(SupportLevel.SUPPORTED);
        Mockito.when(cachedCapabilities.getProbeCategory(probeId))
                .thenReturn(Optional.of(ProbeCategory.HYPERVISOR));

        Assert.assertEquals(actionTargetInfo,
                actionTargetSelector.getTargetForAction(action, entitySettingsCache));
        // However, the backend should have been called, and we can capture
        // and examine the arguments.
        final ArgumentCaptor<RetrieveTopologyEntitiesRequest> entitiesRequestCaptor =
                ArgumentCaptor.forClass(RetrieveTopologyEntitiesRequest.class);
        Mockito.verify(repositoryServiceSpy)
                .retrieveTopologyEntities(entitiesRequestCaptor.capture());
        final RetrieveTopologyEntitiesRequest sentRequest = entitiesRequestCaptor.getValue();

        // Verify that one entity info was requested...
        Assert.assertEquals(3, sentRequest.getEntityOidsCount());
        // ...and that its id matches the selectedEntityId
        Assert.assertEquals(selectedEntityId, sentRequest.getEntityOids(0));
    }

    @Test
    public void testMoveWithNotExistEntity() {
        final long probeId = 10;
        final long targetId = 7;
        final long selectedEntityId = 1;
        final ActionDTO.Action action =
                testActionBuilder.buildMoveAction(selectedEntityId, 2L, 1, 4L, 1);
        final ActionDTO.ActionEntity selectedEntity = action.getInfo().getMove().getTarget();
        final ActionPartialEntity actionPartialEntity = ActionPartialEntity.newBuilder()
                .setOid(2)
                .addAllDiscoveringTargetIds(Collections.singletonList(targetId))
                .build();
        // No target selection for action execution special cases apply, return the normal
        // primary entity.
        PartialEntityBatch batch = PartialEntityBatch.newBuilder()
                .addEntities(PartialEntity.newBuilder().setAction(actionPartialEntity).build())
                .build();
        Mockito.when(repositoryServiceSpy.retrieveTopologyEntities(Mockito.any()))
                .thenReturn(Collections.singletonList(batch));
        // The entity info returned will not include the requested entity (the selectedEntityId)
        Mockito.when(cachedCapabilities.getProbeFromTarget(targetId))
                .thenReturn(Optional.of(probeId));

        Mockito.when(repositoryServiceSpy.retrieveTopologyEntities(Mockito.any()))
                .thenReturn(Collections.singletonList(batch));
        // No target will be selected since we don't have entity data for the selected entity.
        Assert.assertThat(actionTargetSelector.getTargetForAction(action, entitySettingsCache)
                .supportingLevel(), Matchers.is(SupportLevel.UNSUPPORTED));
    }

    @Test
    public void testUnsupportedAction() {
        final Action bogusAction = Action.newBuilder()
                .setId(23)
                .setDeprecatedImportance(1)
                .setExplanation(Explanation.newBuilder().build())
                .setInfo(ActionInfo.newBuilder()
                        // Explicitly clearing this for clarity to the reader
                        // An action with no actionType is bogus and not executable
                        .clearActionType().build())
                .build();
        // Expect a no target, because this action doesn't have a valid type.
        Assert.assertThat(actionTargetSelector.getTargetForAction(bogusAction, entitySettingsCache)
                .supportingLevel(), Matchers.is(SupportLevel.UNSUPPORTED));
    }

    @Test
    public void testNoSelectedEntity() {
        final ActionDTO.Action action = testActionBuilder.buildMoveAction(1L, 2L, 1, 4L, 1);
        // Expect a no target, because we can't select an entity to be the primary entity.
        Assert.assertThat(actionTargetSelector.getTargetForAction(action, entitySettingsCache)
                .supportingLevel(), Matchers.is(SupportLevel.UNSUPPORTED));
    }

    /**
     * Test select target for move action, when target entity discovered from 2 equal probe types.
     *
     * @throws UnsupportedActionException if action is not supported
     */
    @Test
    public void testTargetFrom2EqualsProbeTypes() throws UnsupportedActionException {
        final long targetOid = 1;
        final long sourceOid = 2L;
        final long destinationOid = 3L;
        final long probeId = 10L;
        final long targetId1 = 11L;
        final long targetId2 = 12L;

        final ActionDTO.Action action = Action.newBuilder()
                .setId(777)
                .setInfo(ActionInfo.newBuilder()
                        .setMove(Move.newBuilder()
                                .setTarget(ActionEntity.newBuilder()
                                        .setId(targetOid)
                                        .setType(EntityType.BUSINESS_USER_VALUE)
                                        .build())
                                .addChanges(ChangeProvider.newBuilder()
                                        .setSource(ActionEntity.newBuilder()
                                                .setId(sourceOid)
                                                .setType(EntityDTO.EntityType.DESKTOP_POOL_VALUE)
                                                .build())
                                        .setDestination(ActionEntity.newBuilder()
                                                .setId(destinationOid)
                                                .setType(EntityDTO.EntityType.DESKTOP_POOL_VALUE)
                                                .build())
                                        .build())
                                .build()))
                .setDeprecatedImportance(0)
                .setExplanation(Explanation.newBuilder().build())
                .setSupportingLevel(SupportLevel.SUPPORTED)
                .build();

        final ActionPartialEntity targetActionPartialEntity = ActionPartialEntity.newBuilder()
                .setOid(targetOid)
                .addAllDiscoveringTargetIds(Arrays.asList(targetId1, targetId2))
                .build();
        final ActionPartialEntity sourceActionPartialEntity = ActionPartialEntity.newBuilder()
                .setOid(sourceOid)
                .addAllDiscoveringTargetIds(Collections.singletonList(targetId2))
                .build();
        Mockito.when(
                cachedCapabilities.getSupportLevel(action, ActionDTOUtil.getPrimaryEntity(action),
                        probeId)).thenReturn(SupportLevel.SUPPORTED);
        Mockito.when(cachedCapabilities.getProbeCategory(probeId))
                .thenReturn(Optional.of(ProbeCategory.VIRTUAL_DESKTOP_INFRASTRUCTURE));
        Mockito.when(cachedCapabilities.getProbeFromTarget(targetId1))
                .thenReturn(Optional.of(probeId));
        Mockito.when(cachedCapabilities.getProbeFromTarget(targetId2))
                .thenReturn(Optional.of(probeId));

        final ActionTargetInfo targetInfo = targetInfoResolver.getTargetInfoForAction(action,
                ImmutableMap.of(targetActionPartialEntity.getOid(), targetActionPartialEntity,
                        sourceActionPartialEntity.getOid(), sourceActionPartialEntity), snapshot);
        Assert.assertEquals(targetId2, (long)targetInfo.targetId().get());
    }
}
