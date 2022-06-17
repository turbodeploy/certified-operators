package com.vmturbo.cost.component.savings.tem;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.cloud.common.topology.CloudTopology;
import com.vmturbo.cloud.common.topology.TopologyEntityCloudTopology;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.ExecutedActionsChangeWindow;
import com.vmturbo.common.protobuf.action.ActionDTO.ExecutedActionsChangeWindow.LivenessState;
import com.vmturbo.common.protobuf.action.ActionDTO.ExecutionStep;
import com.vmturbo.common.protobuf.action.ActionDTO.ExecutionStep.Status;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.ResizeInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.Scale;
import com.vmturbo.common.protobuf.action.ActionDTO.UpdateActionChangeWindowRequest.ActionLivenessInfo;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.DatabaseInfo;
import com.vmturbo.cost.component.savings.CachedSavingsActionStore;
import com.vmturbo.cost.component.savings.SavingsException;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DeploymentType;

public class TopologyEntityMonitorTest {

    private static final Map<Integer, Integer> ENTITY_TYPE_TO_PROVIDER_TYPE = ImmutableMap.of(
            EntityType.VIRTUAL_MACHINE_VALUE, EntityType.COMPUTE_TIER_VALUE,
            EntityType.VIRTUAL_VOLUME_VALUE, EntityType.STORAGE_TIER_VALUE,
            EntityType.DATABASE_VALUE, EntityType.DATABASE_TIER_VALUE,
            EntityType.DATABASE_SERVER_VALUE, EntityType.DATABASE_SERVER_TIER_VALUE
    );

    private CloudTopology<TopologyEntityDTO> cloudTopology;
    private TopologyEntityMonitor topologyMonitor;
    private static final TopologyInfo topologyInfo = TopologyInfo.newBuilder()
            .setCreationTime(LocalDateTime.now().toInstant(ZoneOffset.UTC).toEpochMilli()).build();
    private CachedSavingsActionStore cachedSavingsActionStore = mock(CachedSavingsActionStore.class);

    /**
         * Maps entity ID to provider ID.
         */
        private Map<Long, Long> entityIdToProviderId = new HashMap<Long, Long>();

    @Before
    public void setup() throws IOException {
        topologyMonitor = new TopologyEntityMonitor(cachedSavingsActionStore);
        final Map<Long, TopologyEntityDTO> entityMap = createTopologyMap();
        cloudTopology = createCloudTopology(entityMap);
    }

    private ActionLivenessInfo createLivenessInfo(@Nonnull final Long actionOid, @Nonnull final LivenessState livenessState,
                                            final Long timeStamp) {
        final ActionLivenessInfo actionLivenessInfo = ActionLivenessInfo.newBuilder()
                .setActionOid(actionOid)
                .setLivenessState(livenessState)
                .setTimestamp(timeStamp).build();
        return actionLivenessInfo;
    }

    /**
     * Test updates of Liveness states of executed actions.
     */
    @Test
    public void testLivenessUpdates() throws SavingsException {
        Scale.Builder scaleBuilder = Scale.newBuilder();
        final Set<ExecutedActionsChangeWindow> initialChangeWindows = ImmutableSet.of(
                createExecutedActionsChangeWindow(1L, 201L, LivenessState.LIVE, LocalDateTime.of(2022, 5, 22, 10, 30),
                        EntityType.VIRTUAL_MACHINE_VALUE, EntityType.COMPUTE_TIER_VALUE, 1001L, 3001L, scaleBuilder),
                createExecutedActionsChangeWindow(1L, 301L, LivenessState.LIVE, LocalDateTime.of(2022, 5, 23, 9, 30),
                        EntityType.VIRTUAL_MACHINE_VALUE, EntityType.COMPUTE_TIER_VALUE, 3001L, 4001L, scaleBuilder),
                createExecutedActionsChangeWindow(1L, 401L, LivenessState.NEW, LocalDateTime.of(2022, 5, 24, 11, 30),
                        EntityType.VIRTUAL_MACHINE_VALUE, EntityType.COMPUTE_TIER_VALUE, 4001L, 1001L, scaleBuilder),
                createExecutedActionsChangeWindow(1L, 501L, LivenessState.NEW, LocalDateTime.of(2022, 5, 25, 12, 0),
                        EntityType.VIRTUAL_MACHINE_VALUE, EntityType.COMPUTE_TIER_VALUE, 1001L, 2001L, scaleBuilder));

        doReturn(initialChangeWindows)
                .when(cachedSavingsActionStore)
                .getActions(any(LivenessState.class));

        topologyMonitor.process(cloudTopology, topologyInfo);

        verify(cachedSavingsActionStore,  times(1)).activateAction(501L, topologyInfo.getCreationTime());
        verify(cachedSavingsActionStore,  times(1)).deactivateAction(201L, topologyInfo.getCreationTime(), LivenessState.SUPERSEDED);
        verify(cachedSavingsActionStore,  times(1)).deactivateAction(301L, topologyInfo.getCreationTime(), LivenessState.SUPERSEDED);
        verify(cachedSavingsActionStore,  times(1)).deactivateAction(401L, topologyInfo.getCreationTime(), LivenessState.SUPERSEDED);
        verify(cachedSavingsActionStore,  times(1)).saveChanges();
    }

    /**
     * Test updates of Liveness states of executed actions with commodity resizes.
     */
    @Test
    public void testLivenessUpdatesWithCommResizes() throws SavingsException {
        List<ResizeInfo> resizeInfoList = new ArrayList<>();
        resizeInfoList.add(ResizeInfo.newBuilder()
                .setCommodityType(TopologyDTO.CommodityType.newBuilder().setType(CommodityDTO.CommodityType.STORAGE_ACCESS_VALUE))
                .setOldCapacity(100)
                .setNewCapacity(300).build());
        resizeInfoList.add(ResizeInfo.newBuilder()
                .setCommodityType(TopologyDTO.CommodityType.newBuilder().setType(CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE))
                .setOldCapacity(50)
                .setNewCapacity(100).build());
        resizeInfoList.add(ResizeInfo.newBuilder()
                .setCommodityType(TopologyDTO.CommodityType.newBuilder().setType(CommodityDTO.CommodityType.IO_THROUGHPUT_VALUE))
                .setOldCapacity(400)
                .setNewCapacity(200).build());
        Scale.Builder scaleBuilder = Scale.newBuilder();
        scaleBuilder.addAllCommodityResizes(resizeInfoList);
        final Set<ExecutedActionsChangeWindow> initialChangeWindows = ImmutableSet.of(
                createExecutedActionsChangeWindow(4L, 201L, LivenessState.LIVE, LocalDateTime.of(2022, 5, 22, 10, 30),
                        EntityType.VIRTUAL_VOLUME_VALUE, EntityType.STORAGE_TIER_VALUE, 1004L, 3004L, scaleBuilder),
                createExecutedActionsChangeWindow(4L, 301L, LivenessState.LIVE, LocalDateTime.of(2022, 5, 23, 9, 30),
                        EntityType.VIRTUAL_VOLUME_VALUE, EntityType.STORAGE_TIER_VALUE, 3004L, 4004L, scaleBuilder),
                createExecutedActionsChangeWindow(4L, 401L, LivenessState.NEW, LocalDateTime.of(2022, 5, 24, 11, 30),
                        EntityType.VIRTUAL_VOLUME_VALUE, EntityType.STORAGE_TIER_VALUE, 4004L, 1004L, scaleBuilder),
                createExecutedActionsChangeWindow(4L, 501L, LivenessState.NEW, LocalDateTime.of(2022, 5, 25, 12, 00),
                        EntityType.VIRTUAL_VOLUME_VALUE, EntityType.STORAGE_TIER_VALUE, 1004L, 2004L, scaleBuilder));

        doReturn(initialChangeWindows)
                .when(cachedSavingsActionStore)
                .getActions(any(LivenessState.class));

        topologyMonitor.process(cloudTopology, topologyInfo);

        verify(cachedSavingsActionStore,  times(1)).activateAction(501L, topologyInfo.getCreationTime());
        verify(cachedSavingsActionStore,  times(1)).deactivateAction(201L, topologyInfo.getCreationTime(), LivenessState.SUPERSEDED);
        verify(cachedSavingsActionStore,  times(1)).deactivateAction(301L, topologyInfo.getCreationTime(), LivenessState.SUPERSEDED);
        verify(cachedSavingsActionStore,  times(1)).deactivateAction(401L, topologyInfo.getCreationTime(), LivenessState.SUPERSEDED);
        verify(cachedSavingsActionStore,  times(1)).saveChanges();
    }

    /**
     * Test no updates required to of Liveness states of executed actions.
     */
    @Test
    public void testNoUpdatesRequired() throws SavingsException {
        Scale.Builder scaleBuilder = Scale.newBuilder();
        final Set<ExecutedActionsChangeWindow> initialChangeWindows = ImmutableSet.of(
                createExecutedActionsChangeWindow(1L, 101L, LivenessState.SUPERSEDED, LocalDateTime.of(2022, 5, 21, 10, 30),
                        EntityType.VIRTUAL_MACHINE_VALUE, EntityType.COMPUTE_TIER_VALUE, 1001L, 3001L, scaleBuilder),
                createExecutedActionsChangeWindow(1L, 201L, LivenessState.LIVE, LocalDateTime.of(2022, 5, 22, 10, 30),
                        EntityType.VIRTUAL_MACHINE_VALUE, EntityType.COMPUTE_TIER_VALUE, 3001L, 4001L, scaleBuilder));

        doReturn(initialChangeWindows)
                .when(cachedSavingsActionStore)
                .getActions(any(LivenessState.class));

        topologyMonitor.process(cloudTopology, topologyInfo);

        verify(cachedSavingsActionStore,  times(0)).activateAction(201L, topologyInfo.getCreationTime());
        verify(cachedSavingsActionStore,  times(0)).deactivateAction(101L, topologyInfo.getCreationTime(), LivenessState.SUPERSEDED);
        verify(cachedSavingsActionStore,  times(1)).saveChanges();
    }

    /**
     * Tests external Revert of executed action.
     */
    @Test
    public void testRevert() throws SavingsException {
        Scale.Builder scaleBuilder = Scale.newBuilder();
        final Set<ExecutedActionsChangeWindow> initialChangeWindows = ImmutableSet.of(
                createExecutedActionsChangeWindow(1L, 201L, LivenessState.LIVE, LocalDateTime.of(2022, 5, 22, 10, 30),
                        EntityType.VIRTUAL_MACHINE_VALUE, EntityType.COMPUTE_TIER_VALUE, 2001L, 3001L, scaleBuilder));

        doReturn(initialChangeWindows)
                .when(cachedSavingsActionStore)
                .getActions(any(LivenessState.class));

        topologyMonitor.process(cloudTopology, topologyInfo);

        verify(cachedSavingsActionStore,  times(1)).deactivateAction(201L, topologyInfo.getCreationTime(), LivenessState.REVERTED);
        verify(cachedSavingsActionStore,  times(1)).saveChanges();
    }

    /**
     * Tests external Revert of executed action, with commodity resizes.
     */
    @Test
    public void testRevertWithCommResizes() throws SavingsException {
        List<ResizeInfo> resizeInfoList = new ArrayList<>();
        resizeInfoList.add(ResizeInfo.newBuilder()
                .setCommodityType(TopologyDTO.CommodityType.newBuilder().setType(CommodityDTO.CommodityType.STORAGE_ACCESS_VALUE))
                .setOldCapacity(300)
                .setNewCapacity(100).build());
        resizeInfoList.add(ResizeInfo.newBuilder()
                .setCommodityType(TopologyDTO.CommodityType.newBuilder().setType(CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE))
                .setOldCapacity(100)
                .setNewCapacity(50).build());
        resizeInfoList.add(ResizeInfo.newBuilder()
                .setCommodityType(TopologyDTO.CommodityType.newBuilder().setType(CommodityDTO.CommodityType.IO_THROUGHPUT_VALUE))
                .setOldCapacity(200)
                .setNewCapacity(400).build());
        Scale.Builder scaleBuilder = Scale.newBuilder();
        scaleBuilder.addAllCommodityResizes(resizeInfoList);

        final Set<ExecutedActionsChangeWindow> initialChangeWindows = ImmutableSet.of(
                createExecutedActionsChangeWindow(4L, 201L, LivenessState.LIVE, LocalDateTime.of(2022, 5, 22, 10, 30),
                        EntityType.VIRTUAL_VOLUME_VALUE, EntityType.STORAGE_TIER_VALUE, 2004L, 3004L, scaleBuilder));

        doReturn(initialChangeWindows)
                .when(cachedSavingsActionStore)
                .getActions(any(LivenessState.class));

        topologyMonitor.process(cloudTopology, topologyInfo);

        verify(cachedSavingsActionStore,  times(1)).deactivateAction(201L, topologyInfo.getCreationTime(), LivenessState.REVERTED);
        verify(cachedSavingsActionStore,  times(1)).saveChanges();
    }

    private ExecutedActionsChangeWindow createExecutedActionsChangeWindow(final long entityOid,
                                                                          final long actionId,
                                                                          final LivenessState livenessState,
                                                                          final LocalDateTime localDateTime,
                                                                          final int entityType,
                                                                          final int providerType,
                                                                          final long srcProviderId,
                                                                          final long destProviderId,
                                                                          final Scale.Builder scaleBuilder) {
        final ActionEntity actionEntity = ActionEntity.newBuilder().setId(entityOid).setType(
                entityType).setEnvironmentType(
                EnvironmentTypeEnum.EnvironmentType.CLOUD).build();

        final ChangeProvider.Builder changeBuilder = ChangeProvider.newBuilder()
                .setSource(ActionEntity.newBuilder()
                        .setId(srcProviderId)
                        .setType(providerType)
                        .build())
                .setDestination(ActionEntity.newBuilder()
                        .setId(destProviderId)
                        .setType(providerType)
                        .build());

       scaleBuilder.setTarget(actionEntity).addChanges(changeBuilder.build());

        return ExecutedActionsChangeWindow.newBuilder()
                .setEntityOid(entityOid)
                .setActionOid(actionId)
                .setActionSpec(ActionSpec.newBuilder().setRecommendation(
                                Action.newBuilder()
                                        .setId(actionId)
                                        .setInfo(ActionInfo.newBuilder()
                                                .setScale(scaleBuilder
                                                        .build()))
                                        .setDeprecatedImportance(0)
                                        .setExplanation(Explanation.newBuilder().build()))
                        .setExecutionStep(ExecutionStep.newBuilder()
                                .setStatus(Status.SUCCESS)
                                .setCompletionTime(localDateTime.toInstant(ZoneOffset.UTC).toEpochMilli())
                                .build()))
                .setLivenessState(livenessState)
                .build();
    }

    private TopologyEntityDTO createTopologyEntity(long entityId, int entityType, long providerId,
                                                   boolean powerState, Map<Integer, Double> commodityUsage) {

        entityIdToProviderId.put(entityId, providerId);
        TopologyEntityDTO.Builder te = TopologyEntityDTO.newBuilder()
                .setOid(entityId)
                .setEntityType(entityType)
                .setEntityState(powerState
                        ? TopologyDTO.EntityState.POWERED_ON
                        : TopologyDTO.EntityState.POWERED_OFF);
        te.addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .setProviderId(providerId)
                .setProviderEntityType(ENTITY_TYPE_TO_PROVIDER_TYPE.get(entityType)));
        commodityUsage.forEach((commodityType, amount) ->
                te.addCommoditySoldList(CommoditySoldDTO.newBuilder()
                        .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                                .setType(commodityType))
                        .setCapacity(amount)));
        return te.build();
    }

    @Nonnull
    private Map<Long, TopologyEntityDTO> createTopologyMap() {
        Map<Long, TopologyEntityDTO> entityMap = new HashMap<>();
        entityMap.put(1L, createTopologyEntity(1L, EntityType.VIRTUAL_MACHINE_VALUE, 2001L, true, ImmutableMap
                .of()));
        entityMap.put(2L, createTopologyEntity(2L, EntityType.VIRTUAL_MACHINE_VALUE, 1002L, false, ImmutableMap.of()));
        entityMap.put(3L, createTopologyEntity(3L, EntityType.VIRTUAL_MACHINE_VALUE, 1003L, true, ImmutableMap.of()));
        entityMap.put(4L, createTopologyEntity(4L, EntityType.VIRTUAL_VOLUME_VALUE, 2004L, true,
                ImmutableMap.of(
                        CommodityType.STORAGE_AMOUNT_VALUE, 100d,
                        CommodityType.IO_THROUGHPUT_VALUE, 200d,
                        CommodityType.STORAGE_ACCESS_VALUE, 300d)));
        entityMap.put(5L, createTopologyEntity(5L, EntityType.VIRTUAL_VOLUME_VALUE, 1005L, true,
                ImmutableMap.of(
                        CommodityType.STORAGE_AMOUNT_VALUE, 100d,
                        CommodityType.STORAGE_ACCESS_VALUE, 300d)));
        entityMap.put(6L, createTopologyEntity(6L, EntityType.VIRTUAL_MACHINE_VALUE, 1006L, true, ImmutableMap.of()));
        entityMap.put(7L, createTopologyEntity(7L, EntityType.DATABASE_VALUE, 1007L, true,
                ImmutableMap.of(
                        CommodityType.STORAGE_AMOUNT_VALUE, 100d)));
        entityMap.put(8L, createDBSEntity(8L, EntityType.DATABASE_SERVER_VALUE, 1008L, true,
                ImmutableMap.of(
                        CommodityType.STORAGE_AMOUNT_VALUE, 100d), DeploymentType.SINGLE_AZ));
        return entityMap;
    }

    private Optional<TopologyEntityDTO> createProvider(Object o, int entityType) {
            long entityId = entityIdToProviderId.get(Long.parseLong(o.toString()));
            return Optional.of(createTopologyEntity(entityId, entityType, entityId, true, ImmutableMap.of()));
    }

    private CloudTopology createCloudTopology(Map<Long, TopologyEntityDTO> entityMap) {
        CloudTopology ct = mock(TopologyEntityCloudTopology.class);
        when(ct.getEntity(anyLong())).thenAnswer(invocationOnMock ->
                Optional.ofNullable(entityMap.get(invocationOnMock.getArguments()[0])));
        when(ct.getComputeTier(anyLong())).thenAnswer(invocationOnMock ->
                createProvider(invocationOnMock.getArguments()[0], EntityType.VIRTUAL_MACHINE_VALUE));
        when(ct.getStorageTier(anyLong())).thenAnswer(invocationOnMock ->
                createProvider(invocationOnMock.getArguments()[0], EntityType.VIRTUAL_VOLUME_VALUE));
        when(ct.getDatabaseTier(anyLong())).thenAnswer(invocationOnMock ->
                createProvider(invocationOnMock.getArguments()[0], EntityType.DATABASE_VALUE));
        when(ct.getDatabaseServerTier(anyLong())).thenAnswer(invocationOnMock ->
                createProvider(invocationOnMock.getArguments()[0], EntityType.DATABASE_SERVER_VALUE));
        return ct;
    }

    private TopologyEntityDTO createDBSEntity(long entityId, int entityType, long providerId,
                                              boolean powerState, Map<Integer, Double> commodityUsage, DeploymentType deploymentType) {
        TopologyEntityDTO dbs = createTopologyEntity(entityId, entityType, providerId, powerState, commodityUsage);
        return dbs.toBuilder().setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                .setDatabase(DatabaseInfo.newBuilder()
                        .setDeploymentType(deploymentType).build()).build()).build();
    }
}
