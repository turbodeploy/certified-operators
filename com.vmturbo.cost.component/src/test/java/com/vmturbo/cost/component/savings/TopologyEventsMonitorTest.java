package com.vmturbo.cost.component.savings;

import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopology;
import com.vmturbo.cost.component.savings.EntityEventsJournal.SavingsEvent;
import com.vmturbo.cost.component.savings.TopologyEventsMonitor.ChangeResult;
import com.vmturbo.cost.component.savings.TopologyEventsMonitor.Config;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Tests for the Topology Events Monitor.
 */
public class TopologyEventsMonitorTest {
    private static final Map<Integer, Integer> ENTITY_TYPE_TO_PROVIDER_TYPE = ImmutableMap.of(
            EntityType.VIRTUAL_MACHINE_VALUE, EntityType.COMPUTE_TIER_VALUE,
            EntityType.VIRTUAL_VOLUME_VALUE, EntityType.STORAGE_TIER_VALUE
    );
    /**
     * Test monitored state database.
     */
    private Map<Long, MonitoredEntity> monitoredEntityMap;
    /**
     * Maps entity ID to provider ID.
     */
    private Map<Long, Long> entityIdToProviderId = new HashMap<Long, Long>();
    // Monitor configurations.
    private static final Config VM_CONFIG = new Config(true, ImmutableSet.of());
    private static final Config VV_CONFIG = new Config(true, ImmutableSet.of(
            CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE,
            CommodityDTO.CommodityType.STORAGE_ACCESS_VALUE,
            CommodityDTO.CommodityType.IO_THROUGHPUT_VALUE));
    /**
     * Standard TEM (monitor VMs and VVs).
     */
    private final TopologyEventsMonitor tem = new TopologyEventsMonitor(ImmutableMap.of(
            EntityType.VIRTUAL_MACHINE_VALUE, VM_CONFIG,
            EntityType.VIRTUAL_VOLUME_VALUE, VV_CONFIG));
    /**
     * VM-only TEM.
     */
    private final TopologyEventsMonitor vmTem =
            new TopologyEventsMonitor(ImmutableMap.of(EntityType.VIRTUAL_MACHINE_VALUE, VM_CONFIG));

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

    private TestMonitoredEntity createMonitoredEntity(long entityId,
            boolean powerState, Map<Integer, Double> commodityUsage) {
        return new TestMonitoredEntity(entityId, entityId + 1000L, powerState, commodityUsage);
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
        entityState.put(1L, createMonitoredEntity(1L, true, ImmutableMap.of()));
        entityState.put(2L, createMonitoredEntity(2L, false, ImmutableMap.of()));
        entityState.put(3L, createMonitoredEntity(3L, true, ImmutableMap.of()));
        entityState.put(4L, createMonitoredEntity(4L, true, ImmutableMap.of(
                CommodityType.STORAGE_AMOUNT.getNumber(), 100d,
                CommodityType.IO_THROUGHPUT.getNumber(), 200d,
                CommodityType.STORAGE_ACCESS.getNumber(), 300d)));
        entityState.put(5L, createMonitoredEntity(5L, true, ImmutableMap.of(
                CommodityType.STORAGE_AMOUNT.getNumber(), 100d,
                CommodityType.STORAGE_ACCESS.getNumber(), 300d)));
        entityState.put(6L, createMonitoredEntity(6L, true, ImmutableMap.of()));
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
        return entityMap;
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
                Assert.assertTrue(topologyEvent.getProviderOid().isPresent());
                Assert.assertEquals(9000L, (long)topologyEvent.getProviderOid().get());
                Assert.assertTrue(topologyEvent.getCommodityUsage().isEmpty());
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
                Assert.assertFalse(topologyEvent.getProviderOid().isPresent());
                Assert.assertEquals(commodityUsage, topologyEvent.getCommodityUsage());
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
                Assert.assertTrue(topologyEvent.getProviderOid().isPresent());
                Assert.assertEquals(commodityUsage, topologyEvent.getCommodityUsage());
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
     * Monitored entity for testing purposes.
     */
    class TestMonitoredEntity implements MonitoredEntity {
        private final long entityId;
        private long providerId;
        private boolean powerState;
        private long lastPowerOffTransition;
        private Map<Integer, Double> commodityUsage;

        TestMonitoredEntity(long entityId, long providerId, boolean powerState,
                Map<Integer, Double> commodityUsage) {
            this.entityId = entityId;
            this.providerId = providerId;
            this.powerState = powerState;
            this.lastPowerOffTransition = 0L;
            this.commodityUsage = commodityUsage;
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

        /**
         * Get provider ID.
         *
         * @return provider ID.
         */
        @Override
        public Long getProviderId() {
            return providerId;
        }

        /**
         * Set provider ID.
         *
         * @param providerId provider ID.
         */
        @Override
        public void setProviderId(Long providerId) {
            this.providerId = providerId;
        }

        /**
         * Get commodity usage for tracked commodity types.
         *
         * @return map from commodity type to usage.  If there are no tracked commodities, an
         *         empty map is returned.
         */
        @NotNull
        @Override
        public Map<Integer, Double> getCommodityUsage() {
            return commodityUsage;
        }

        /**
         * Set commodity usage for this entity.
         *
         * @param commodityUsage map of commodity type to usage for that commodity.
         */
        @Override
        public void setCommodityUsage(@NotNull Map<Integer, Double> commodityUsage) {
            this.commodityUsage = Objects.requireNonNull(commodityUsage);
        }
    }
}
