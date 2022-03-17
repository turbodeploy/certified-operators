package com.vmturbo.cost.component.savings.tem;

import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.cloud.common.topology.CloudTopology;
import com.vmturbo.cloud.common.topology.TopologyEntityCloudTopology;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.DatabaseInfo;
import com.vmturbo.cost.component.savings.bottomup.MonitoredEntity;
import com.vmturbo.cost.component.savings.bottomup.SavingsEvent;
import com.vmturbo.cost.component.savings.bottomup.TopologyEvent;
import com.vmturbo.cost.component.savings.tem.TopologyEventsMonitor.ChangeResult;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DeploymentType;

/**
 * Tests for the Topology Events Monitor.
 */
public class TopologyEventsMonitorTest {
    private static final Map<Integer, Integer> ENTITY_TYPE_TO_PROVIDER_TYPE = ImmutableMap.of(
            EntityType.VIRTUAL_MACHINE_VALUE, EntityType.COMPUTE_TIER_VALUE,
            EntityType.VIRTUAL_VOLUME_VALUE, EntityType.STORAGE_TIER_VALUE,
            EntityType.DATABASE_VALUE, EntityType.DATABASE_TIER_VALUE,
            EntityType.DATABASE_SERVER_VALUE, EntityType.DATABASE_SERVER_TIER_VALUE
    );
    /**
     * Test monitored state database.
     */
    private Map<Long, MonitoredEntity> monitoredEntityMap;
    /**
     * Maps entity ID to provider ID.
     */
    private Map<Long, Long> entityIdToProviderId = new HashMap<Long, Long>();
    /**
     * Standard TEM (monitor VMs and VVs).
     */
    private final TopologyEventsMonitor tem = new TopologyEventsMonitor();

    /**
     * Test setup.
     *
     * @throws Exception if something went wrong.
     */
    @Before
    public void setUp() throws Exception {
        // Create some EntityState
        monitoredEntityMap = createEntityState();
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

    private TestMonitoredEntity createMonitoredEntity(long entityId, int entityType,
            boolean powerState, Map<Integer, Double> commodityUsage) {
        return new TestMonitoredEntity(entityId, entityType, entityId + 1000L, powerState, commodityUsage);
    }

    /**
     * Create test entity states.
     * - VM powered on  OID=1
     * - VM powered off OID=2
     * - VM case:    OID=3
     * - VV case:    OID=4 8=100 39=200 64=300
     * - VV case:    OID=5 8=100 64=300
     * - DBS case:   OID=6
     *
     * @return map from entity OID -> entity state
     */
    private Map<Long, MonitoredEntity> createEntityState() {
        Map<Long, MonitoredEntity> entityState = new HashMap<>();
        entityState.put(1L, createMonitoredEntity(1L, EntityType.VIRTUAL_MACHINE_VALUE, true, ImmutableMap.of()));
        entityState.put(2L, createMonitoredEntity(2L, EntityType.VIRTUAL_MACHINE_VALUE, false, ImmutableMap.of()));
        entityState.put(3L, createMonitoredEntity(3L, EntityType.VIRTUAL_MACHINE_VALUE, true, ImmutableMap.of()));
        entityState.put(4L, createMonitoredEntity(4L, EntityType.VIRTUAL_VOLUME_VALUE, true, ImmutableMap.of(
                CommodityType.STORAGE_AMOUNT.getNumber(), 100d,
                CommodityType.IO_THROUGHPUT.getNumber(), 200d,
                CommodityType.STORAGE_ACCESS.getNumber(), 300d)));
        entityState.put(5L, createMonitoredEntity(5L, EntityType.VIRTUAL_VOLUME_VALUE, true, ImmutableMap.of(
                CommodityType.STORAGE_AMOUNT.getNumber(), 100d,
                CommodityType.STORAGE_ACCESS.getNumber(), 300d)));
        entityState.put(6L, createMonitoredEntity(6L, EntityType.VIRTUAL_MACHINE_VALUE, true, ImmutableMap.of()));
        entityState.put(7L, createMonitoredEntity(7L, EntityType.DATABASE_VALUE, true, ImmutableMap.of(
                CommodityType.STORAGE_AMOUNT.getNumber(), 100d)));
        entityState.put(8L, createMonitoredEntity(8L, EntityType.DATABASE_SERVER_VALUE, true, ImmutableMap.of(
                CommodityType.STORAGE_AMOUNT.getNumber(), 100d)));
        return entityState;
    }

    @Nonnull
    private Map<Long, TopologyEntityDTO> createTopologyMap() {
        Map<Long, TopologyEntityDTO> entityMap = new HashMap<>();
        entityMap.put(1L, createTopologyEntity(1L, EntityType.VIRTUAL_MACHINE_VALUE, 1001L, true, ImmutableMap
                .of()));
        entityMap.put(2L, createTopologyEntity(2L, EntityType.VIRTUAL_MACHINE_VALUE, 1002L, false, ImmutableMap.of()));
        entityMap.put(3L, createTopologyEntity(3L, EntityType.VIRTUAL_MACHINE_VALUE, 1003L, true, ImmutableMap.of()));
        entityMap.put(4L, createTopologyEntity(4L, EntityType.VIRTUAL_VOLUME_VALUE, 1004L, true,
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

    private TopologyEntityDTO createDBSEntity(long entityId, int entityType, long providerId,
            boolean powerState, Map<Integer, Double> commodityUsage, DeploymentType deploymentType) {
        TopologyEntityDTO dbs = createTopologyEntity(entityId, entityType, providerId, powerState, commodityUsage);
        return dbs.toBuilder().setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                .setDatabase(DatabaseInfo.newBuilder()
                        .setDeploymentType(deploymentType).build()).build()).build();
    }

    /**
     * Discovered topology and current monitored entity state are the same.
     */
    @Test
    public void testNoChanges() {
        Map<Long, TopologyEntityDTO> entityMap = createTopologyMap();

        CloudTopology cloudTopology = createCloudTopology(entityMap);
        for (MonitoredEntity entity : this.monitoredEntityMap.values()) {
            ChangeResult result = tem.generateEvents(entity, cloudTopology, 0L);
            Assert.assertEquals(result, ChangeResult.EMPTY);
        }
    }

    /**
     * Test provider change only.
     */
    @Test
    public void testProviderChangeOnly() {
        Map<Long, TopologyEntityDTO> topologyMap = createTopologyMap();
        // Change provider ID to 9000.
        topologyMap.put(2L, createTopologyEntity(2L, EntityType.VIRTUAL_MACHINE_VALUE, 9000L,
                false, ImmutableMap.of()));

        CloudTopology cloudTopology = createCloudTopology(topologyMap);
        for (MonitoredEntity entity : this.monitoredEntityMap.values()) {
            ChangeResult result = tem.generateEvents(entity, cloudTopology, 0L);
            if (entity.getEntityId() == 2L) {
                Assert.assertTrue(result.stateUpdated);
                Assert.assertEquals(1, result.savingsEvents.size());
                SavingsEvent savingsEvent = result.savingsEvents.get(0);
                Assert.assertTrue(savingsEvent.hasTopologyEvent());
                TopologyEvent topologyEvent = savingsEvent.getTopologyEvent().get();
                Assert.assertTrue(topologyEvent.getProviderInfo().isPresent());
                ProviderInfo providerInfo = topologyEvent.getProviderInfo().get();
                Assert.assertTrue(providerInfo instanceof VirtualMachineProviderInfo);
                VirtualMachineProviderInfo vmProviderInfo = (VirtualMachineProviderInfo)providerInfo;
                Assert.assertEquals(9000L, (long)vmProviderInfo.getProviderOid());
            } else {
                Assert.assertEquals(result, ChangeResult.EMPTY);
            }
        }
    }

    /**
     * Test commodity change only.
     */
    @Test
    public void testCommodityChangeOnly() {
        Map<Long, TopologyEntityDTO> topologyMap = createTopologyMap();
        // Change IO_THROUGHPUT usage
        Map<Integer, Double> commodityUsage = ImmutableMap.of(
                CommodityType.STORAGE_AMOUNT_VALUE, 100d,
                CommodityType.IO_THROUGHPUT_VALUE, 201d,
                CommodityType.STORAGE_ACCESS_VALUE, 300d);
        topologyMap.put(4L, createTopologyEntity(4L, EntityType.VIRTUAL_VOLUME_VALUE,
                1004L, true, commodityUsage));

        CloudTopology cloudTopology = createCloudTopology(topologyMap);
        for (MonitoredEntity entity : this.monitoredEntityMap.values()) {
            ChangeResult result = tem.generateEvents(entity, cloudTopology, 0L);
            if (entity.getEntityId() == 4L) {
                Assert.assertTrue(result.stateUpdated);
                Assert.assertEquals(1, result.savingsEvents.size());
                SavingsEvent savingsEvent = result.savingsEvents.get(0);
                Assert.assertTrue(savingsEvent.hasTopologyEvent());
                TopologyEvent topologyEvent = savingsEvent.getTopologyEvent().get();

                // ??? Is it important that provider ID is not set?
                //Assert.assertFalse(topologyEvent.getProviderOid().isPresent());

                Assert.assertTrue(topologyEvent.getProviderInfo().isPresent());
                ProviderInfo providerInfo = topologyEvent.getProviderInfo().get();
                Assert.assertTrue(providerInfo instanceof VolumeProviderInfo);
                VolumeProviderInfo volumeProviderInfo = (VolumeProviderInfo)providerInfo;
                VolumeProviderInfo expectedProviderInfo = new VolumeProviderInfo(1004L, commodityUsage);
                Assert.assertEquals(expectedProviderInfo, volumeProviderInfo);

            } else {
                Assert.assertEquals(result, ChangeResult.EMPTY);
            }
        }
    }

    /**
     * Test provider change and commodity change.
     */
    @Test
    public void testProviderAndCommodityChange() {
        Map<Long, TopologyEntityDTO> topologyMap = createTopologyMap();
        // Change IO_THROUGHPUT usage and the provider.
        Map<Integer, Double> commodityUsage = ImmutableMap.of(
                CommodityType.STORAGE_AMOUNT_VALUE, 100d,
                CommodityType.IO_THROUGHPUT_VALUE, 201d,
                CommodityType.STORAGE_ACCESS_VALUE, 300d);
        topologyMap.put(4L, createTopologyEntity(4L, EntityType.VIRTUAL_VOLUME_VALUE,
                9004L, true, commodityUsage));

        CloudTopology cloudTopology = createCloudTopology(topologyMap);
        for (MonitoredEntity entity : this.monitoredEntityMap.values()) {
            ChangeResult result = tem.generateEvents(entity, cloudTopology, 0L);
            if (entity.getEntityId() == 4L) {
                Assert.assertTrue(result.stateUpdated);
                Assert.assertEquals(1, result.savingsEvents.size());
                SavingsEvent savingsEvent = result.savingsEvents.get(0);
                Assert.assertTrue(savingsEvent.hasTopologyEvent());
                TopologyEvent topologyEvent = savingsEvent.getTopologyEvent().get();

                Assert.assertTrue(topologyEvent.getProviderInfo().isPresent());
                ProviderInfo providerInfo = topologyEvent.getProviderInfo().get();
                Assert.assertTrue(providerInfo instanceof VolumeProviderInfo);
                VolumeProviderInfo volumeProviderInfo = (VolumeProviderInfo)providerInfo;
                VolumeProviderInfo expectedProviderInfo = new VolumeProviderInfo(9004L, commodityUsage);
                Assert.assertEquals(expectedProviderInfo, volumeProviderInfo);

            } else {
                Assert.assertEquals(result, ChangeResult.EMPTY);
            }
        }
    }

    /**
     * Test provider changes for DB.
     */
    @Test
    public void testDBProviderAndCommodityChange() {
        Map<Long, TopologyEntityDTO> topologyMap = createTopologyMap();
        // Change Storage amount and the provider.
        Map<Integer, Double> commodityUsage = ImmutableMap.of(
                CommodityType.STORAGE_AMOUNT_VALUE, 200d);
        topologyMap.put(7L, createTopologyEntity(7L, EntityType.DATABASE_VALUE,
                9007L, true, commodityUsage));

        CloudTopology cloudTopology = createCloudTopology(topologyMap);
        for (MonitoredEntity entity : this.monitoredEntityMap.values()) {
            ChangeResult result = tem.generateEvents(entity, cloudTopology, 0L);
            if (entity.getEntityId() == 7L) {
                Assert.assertTrue(result.stateUpdated);
                Assert.assertEquals(1, result.savingsEvents.size());
                SavingsEvent savingsEvent = result.savingsEvents.get(0);
                Assert.assertTrue(savingsEvent.hasTopologyEvent());
                TopologyEvent topologyEvent = savingsEvent.getTopologyEvent().get();

                Assert.assertTrue(topologyEvent.getProviderInfo().isPresent());
                ProviderInfo providerInfo = topologyEvent.getProviderInfo().get();
                Assert.assertTrue(providerInfo instanceof DatabaseProviderInfo);
                DatabaseProviderInfo dbProviderInfo = (DatabaseProviderInfo)providerInfo;
                DatabaseProviderInfo expectedProviderInfo = new DatabaseProviderInfo(9007L, commodityUsage);
                Assert.assertEquals(expectedProviderInfo, dbProviderInfo);

            } else {
                Assert.assertEquals(result, ChangeResult.EMPTY);
            }
        }
    }

    /**
     * Test provider changes for DBS.
     */
    @Test
    public void testDBSProviderAndCommodityChange() {
        Map<Long, TopologyEntityDTO> topologyMap = createTopologyMap();
        // Change storage amount and the provider.
        Map<Integer, Double> commodityUsage = ImmutableMap.of(
                CommodityType.STORAGE_AMOUNT_VALUE, 200d);
        topologyMap.put(8L, createDBSEntity(8L, EntityType.DATABASE_SERVER_VALUE,
                9008L, true, commodityUsage, DeploymentType.MULTI_AZ));

        CloudTopology cloudTopology = createCloudTopology(topologyMap);
        for (MonitoredEntity entity : this.monitoredEntityMap.values()) {
            ChangeResult result = tem.generateEvents(entity, cloudTopology, 0L);
            if (entity.getEntityId() == 8L) {
                Assert.assertTrue(result.stateUpdated);
                Assert.assertEquals(1, result.savingsEvents.size());
                SavingsEvent savingsEvent = result.savingsEvents.get(0);
                Assert.assertTrue(savingsEvent.hasTopologyEvent());
                TopologyEvent topologyEvent = savingsEvent.getTopologyEvent().get();

                Assert.assertTrue(topologyEvent.getProviderInfo().isPresent());
                ProviderInfo providerInfo = topologyEvent.getProviderInfo().get();
                Assert.assertTrue(providerInfo instanceof DatabaseServerProviderInfo);
                DatabaseServerProviderInfo dbsProviderInfo = (DatabaseServerProviderInfo)providerInfo;
                DatabaseServerProviderInfo expectedProviderInfo = new DatabaseServerProviderInfo(9008L, commodityUsage, DeploymentType.MULTI_AZ);
                Assert.assertEquals(expectedProviderInfo, dbsProviderInfo);

            } else {
                Assert.assertEquals(result, ChangeResult.EMPTY);
            }
        }
    }

    /**
     * Test untracked commodity changed.
     */
    @Test
    public void testUnmonitoredCommodityChange() {
        Map<Long, TopologyEntityDTO> topologyMap = createTopologyMap();
        topologyMap.put(4L, createTopologyEntity(4L, EntityType.VIRTUAL_VOLUME_VALUE, 1004L, true,
                ImmutableMap.of(
                        CommodityType.STORAGE_AMOUNT_VALUE, 100d,
                        CommodityType.IO_THROUGHPUT_VALUE, 200d,
                        CommodityType.STORAGE_ACCESS_VALUE, 300d,
                        CommodityType.DB_MEM_VALUE, 4096d)));

        CloudTopology cloudTopology = createCloudTopology(topologyMap);
        for (MonitoredEntity entity : this.monitoredEntityMap.values()) {
            ChangeResult result = tem.generateEvents(entity, cloudTopology, 0L);
            Assert.assertEquals(result, ChangeResult.EMPTY);
        }
    }

    /**
     * Test VirtualMachineProviderInfo.
     */
    @Test
    public void testVMProviderInfo() {
        Map<Long, TopologyEntityDTO> entityMap = createTopologyMap();
        CloudTopology<TopologyEntityDTO> cloudTopology = createCloudTopology(entityMap);
        TopologyEntityDTO discoveredVM = createTopologyEntity(100L,
                EntityType.VIRTUAL_MACHINE_VALUE, 1000L, true, new HashMap<>());
        ProviderInfo providerInfo = ProviderInfoFactory.getDiscoveredProviderInfo(cloudTopology,
                discoveredVM);
        Assert.assertNotNull(providerInfo);
        Assert.assertEquals(EntityType.VIRTUAL_MACHINE_VALUE, providerInfo.getEntityType());
        Assert.assertTrue(providerInfo instanceof VirtualMachineProviderInfo);
        VirtualMachineProviderInfo vmProviderInfo = (VirtualMachineProviderInfo)providerInfo;
        Assert.assertEquals(entityIdToProviderId.get(discoveredVM.getOid()), vmProviderInfo.getProviderOid());
        Assert.assertEquals(new HashMap<>(), vmProviderInfo.getCommodityCapacities());
    }

    /**
     * Test VolumeProviderInfo.
     */
    @Test
    public void testVolumeProviderInfo() {
        Map<Long, TopologyEntityDTO> entityMap = createTopologyMap();
        CloudTopology<TopologyEntityDTO> cloudTopology = createCloudTopology(entityMap);
        Map<Integer, Double> commodityMap = ImmutableMap.of(
                CommodityType.STORAGE_AMOUNT_VALUE, 100d,
                CommodityType.IO_THROUGHPUT_VALUE, 200d,
                CommodityType.STORAGE_ACCESS_VALUE, 300d);
        TopologyEntityDTO discoveredVol = createTopologyEntity(100L,
                EntityType.VIRTUAL_VOLUME_VALUE, 1000L, true, commodityMap);
        ProviderInfo providerInfo = ProviderInfoFactory.getDiscoveredProviderInfo(cloudTopology,
                discoveredVol);
        Assert.assertNotNull(providerInfo);
        Assert.assertEquals(EntityType.VIRTUAL_VOLUME_VALUE, providerInfo.getEntityType());
        Assert.assertTrue(providerInfo instanceof VolumeProviderInfo);
        VolumeProviderInfo volumeProviderInfo = (VolumeProviderInfo)providerInfo;
        Assert.assertEquals(entityIdToProviderId.get(discoveredVol.getOid()), volumeProviderInfo.getProviderOid());
        Assert.assertEquals(commodityMap, volumeProviderInfo.getCommodityCapacities());
    }

    /**
     * Test DatabaseProviderInfo.
     */
    @Test
    public void testDatabaseProviderInfo() {
        Map<Long, TopologyEntityDTO> entityMap = createTopologyMap();
        CloudTopology<TopologyEntityDTO> cloudTopology = createCloudTopology(entityMap);
        Map<Integer, Double> commodityMap = ImmutableMap.of(
                CommodityType.STORAGE_AMOUNT_VALUE, 100d);
        TopologyEntityDTO discoveredDB = createTopologyEntity(100L,
                EntityType.DATABASE_VALUE, 1000L, true, commodityMap);
        ProviderInfo providerInfo = ProviderInfoFactory.getDiscoveredProviderInfo(cloudTopology,
                discoveredDB);
        Assert.assertNotNull(providerInfo);
        Assert.assertEquals(EntityType.DATABASE_VALUE, providerInfo.getEntityType());
        Assert.assertTrue(providerInfo instanceof DatabaseProviderInfo);
        DatabaseProviderInfo dbProviderInfo = (DatabaseProviderInfo)providerInfo;
        Assert.assertEquals(entityIdToProviderId.get(discoveredDB.getOid()), dbProviderInfo.getProviderOid());
        Assert.assertEquals(commodityMap, dbProviderInfo.getCommodityCapacities());
    }

    /**
     * Test DatabaseServerProviderInfo.
     */
    @Test
    public void testDatabaseServerProviderInfo() {
        Map<Integer, Double> commodityMap = ImmutableMap.of(
                CommodityType.STORAGE_AMOUNT_VALUE, 100d);
        TopologyEntityDTO discoveredDBS = createDBSEntity(100L,
                EntityType.DATABASE_SERVER_VALUE, 1000L, true, commodityMap,
                DeploymentType.SINGLE_AZ);
        Map<Long, TopologyEntityDTO> entityMap = createTopologyMap();
        entityMap.put(100L, discoveredDBS);
        CloudTopology<TopologyEntityDTO> cloudTopology = createCloudTopology(entityMap);
        ProviderInfo providerInfo = ProviderInfoFactory.getDiscoveredProviderInfo(cloudTopology,
                discoveredDBS);
        Assert.assertNotNull(providerInfo);
        Assert.assertEquals(EntityType.DATABASE_SERVER_VALUE, providerInfo.getEntityType());
        Assert.assertTrue(providerInfo instanceof DatabaseServerProviderInfo);
        DatabaseServerProviderInfo dbsProviderInfo = (DatabaseServerProviderInfo)providerInfo;
        Assert.assertEquals(entityIdToProviderId.get(discoveredDBS.getOid()), dbsProviderInfo.getProviderOid());
        Assert.assertEquals(commodityMap, dbsProviderInfo.getCommodityCapacities());
        Assert.assertEquals(DeploymentType.SINGLE_AZ, dbsProviderInfo.getDeploymentType());
    }

    /**
     * Monitored entity for testing purposes.
     */
    class TestMonitoredEntity implements MonitoredEntity {
        private final long entityId;
        private boolean powerState;
        private long lastPowerOffTransition;
        private ProviderInfo providerInfo;

        TestMonitoredEntity(long entityId, int entityType, long providerId, boolean powerState,
                Map<Integer, Double> commodityUsage) {
            this.entityId = entityId;
            this.powerState = powerState;
            this.lastPowerOffTransition = 0L;
            switch (entityType) {
                case EntityType.VIRTUAL_MACHINE_VALUE:
                    this.providerInfo = new VirtualMachineProviderInfo(providerId);
                    break;
                case EntityType.VIRTUAL_VOLUME_VALUE:
                    this.providerInfo = new VolumeProviderInfo(providerId, commodityUsage);
                    break;
                case EntityType.DATABASE_VALUE:
                    this.providerInfo = new DatabaseProviderInfo(providerId, commodityUsage);
                    break;
                case EntityType.DATABASE_SERVER_VALUE:
                    this.providerInfo = new DatabaseServerProviderInfo(providerId, commodityUsage, DeploymentType.SINGLE_AZ);
                    break;
            }
        }

        /**
         * Get the entity ID.
         *
         * @return the entity ID.
         */
        @Override
        public long getEntityId() {
            return entityId;
        }

        /**
         * Get the timestamp when the entity was first detected missing.
         *
         * @return timestamp in milliseconds.
         */
        @Override
        public Long getLastPowerOffTransition() {
            return lastPowerOffTransition;
        }

        /**
         * Set the timestamp when the entity was first detected missing.
         *
         * @param timestamp timestamp in milliseconds.
         */
        @Override
        public void setLastPowerOffTransition(Long timestamp) {
            this.lastPowerOffTransition = timestamp;
        }

        @Override
        public ProviderInfo getProviderInfo() {
            return providerInfo;
        }

        @Override
        public void setProviderInfo(ProviderInfo providerInfo) {
            this.providerInfo = providerInfo;
        }
    }
}
