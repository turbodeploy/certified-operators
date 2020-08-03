package com.vmturbo.topology.processor.group.discovery;

import static com.vmturbo.topology.processor.topology.TopologyEntityUtils.topologyEntity;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.ConstraintInfo;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.ConstraintType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.MembersList;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.entity.Entity;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.group.discovery.DiscoveredClusterConstraintCache.DiscoveredClusterConstraint;
import com.vmturbo.topology.processor.topology.TopologyEntityTopologyGraphCreator;

public class DiscoveredClusterConstraintCacheTest {

    private final EntityStore entityStore = Mockito.mock(EntityStore.class);

    private final DiscoveredClusterConstraintCache discoveredClusterConstraintCache =
            new DiscoveredClusterConstraintCache(entityStore);

    private final Map<String, Long> targetEntityIdMap = ImmutableMap.of("VM-A", 11L,
            "PM-A", 22L);

    private final Entity vmEntity = new Entity(11L, EntityType.VIRTUAL_MACHINE);

    private final Entity pmEntity = new Entity(22L, EntityType.PHYSICAL_MACHINE);

    final long targetId = 123L;

    GroupDTO vmGroup = GroupDTO.newBuilder()
            .setEntityType(EntityType.VIRTUAL_MACHINE)
            .setDisplayName("test-VM")
            .setConstraintInfo(ConstraintInfo.newBuilder()
                    .setConstraintType(ConstraintType.CLUSTER)
                    .setConstraintId("test-id")
                    .setConstraintName("test-name")
                    .setIsBuyer(true)
                    .setForExcludedConsumers(true))
            .setMemberList(MembersList.newBuilder()
                    .addMember("VM-A"))
            .build();

    GroupDTO pmGroup = GroupDTO.newBuilder()
            .setEntityType(EntityType.PHYSICAL_MACHINE)
            .setDisplayName("test-PM")
            .setConstraintInfo(ConstraintInfo.newBuilder()
                    .setConstraintType(ConstraintType.CLUSTER)
                    .setConstraintId("test-id")
                    .setConstraintName("test-name"))
            .setMemberList(MembersList.newBuilder()
                    .addMember("PM-A"))
            .build();

    private final TopologyEntity.Builder vmFirstBuilder = TopologyEntity.newBuilder(
            TopologyEntityDTO.newBuilder()
                    .setOid(11L)
                    .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                    .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                            .setProviderEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                            .setProviderId(22L)));

    private final TopologyEntity.Builder vmSecondBuilder = TopologyEntity.newBuilder(
            TopologyEntityDTO.newBuilder()
                    .setOid(12L)
                    .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                    .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                            .setProviderEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                            .setProviderId(22L)));

    private final TopologyEntity.Builder pmWithCluster = TopologyEntity.newBuilder(
            TopologyEntityDTO.newBuilder()
                    .setOid(22L)
                    .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                    .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                            .setCommodityType(CommodityType.newBuilder()
                                    .setType(CommodityDTO.CommodityType.CLUSTER_VALUE))));

    private TopologyGraph<TopologyEntity> topologyGraph;

    @Before
    public void setup() {
        Mockito.when(entityStore.getTargetEntityIdMap(targetId)).thenReturn(Optional.of(targetEntityIdMap));
        Mockito.when(entityStore.getEntity(11L)).thenReturn(Optional.of(vmEntity));
        Mockito.when(entityStore.getEntity(22L)).thenReturn(Optional.of(pmEntity));
    }

    @Test
    public void testStoreDiscoveredClusterConstraint() {
        discoveredClusterConstraintCache.storeDiscoveredClusterConstraint(targetId,
                Lists.newArrayList(vmGroup, pmGroup));
        final List<DiscoveredClusterConstraint> results =
                discoveredClusterConstraintCache.getClusterConstraintByTarget(targetId).get();
        assertEquals(1L, results.size());
        final DiscoveredClusterConstraint discoveredClusterConstraint = results.get(0);
        assertEquals(Sets.newHashSet(22L), discoveredClusterConstraint.getProviderIds());
        assertEquals(Sets.newHashSet(11L), discoveredClusterConstraint.getExcludedConsumerIds());
    }

    @Test
    public void testApplyClusterConstraint() {
        final Map<Long, TopologyEntity.Builder> topologyMap = new HashMap<>();
        topologyMap.put(22L, topologyEntity(22L, EntityType.PHYSICAL_MACHINE));
        topologyMap.put(23L, topologyEntity(23L, EntityType.PHYSICAL_MACHINE));
        topologyMap.put(11L, vmFirstBuilder);
        topologyMap.put(12L, vmSecondBuilder);

        topologyGraph = TopologyEntityTopologyGraphCreator.newGraph(topologyMap);
        discoveredClusterConstraintCache.storeDiscoveredClusterConstraint(targetId,
                Lists.newArrayList(vmGroup, pmGroup));
        discoveredClusterConstraintCache.applyClusterCommodity(topologyGraph);
        assertTrue(hasProviderClusterConstraint(topologyGraph.getEntity(22L).get()));
        assertFalse(hasConsumerClusterConstraint(topologyGraph.getEntity(11L).get(),
                EntityType.PHYSICAL_MACHINE_VALUE));
        assertTrue(hasConsumerClusterConstraint(topologyGraph.getEntity(12L).get(),
                EntityType.PHYSICAL_MACHINE_VALUE));
    }

    @Test
    public void testProvidersAlreadyContainsClusterConstraint() {
        final Map<Long, TopologyEntity.Builder> topologyMap = new HashMap<>();
        topologyMap.put(22L, pmWithCluster);
        topologyMap.put(23L, topologyEntity(23L, EntityType.PHYSICAL_MACHINE));
        topologyMap.put(11L, vmFirstBuilder);
        topologyMap.put(12L, vmSecondBuilder);

        topologyGraph = TopologyEntityTopologyGraphCreator.newGraph(topologyMap);
        discoveredClusterConstraintCache.storeDiscoveredClusterConstraint(targetId,
                Lists.newArrayList(vmGroup, pmGroup));
        discoveredClusterConstraintCache.applyClusterCommodity(topologyGraph);
        assertFalse(hasConsumerClusterConstraint(topologyGraph.getEntity(11L).get(),
                EntityType.PHYSICAL_MACHINE_VALUE));
        assertTrue(hasConsumerClusterConstraint(topologyGraph.getEntity(12L).get(),
                EntityType.PHYSICAL_MACHINE_VALUE));
    }

    private boolean hasProviderClusterConstraint(@Nonnull final TopologyEntity topologyEntity) {
        return topologyEntity.getTopologyEntityDtoBuilder().getCommoditySoldListList().stream()
                .anyMatch(commoditySoldDTO -> commoditySoldDTO.getCommodityType().getType() ==
                        CommonDTO.CommodityDTO.CommodityType.CLUSTER_VALUE);
    }

    private boolean hasConsumerClusterConstraint(@Nonnull final TopologyEntity topologyEntity,
                                                 final int providerEntityType) {
        return topologyEntity.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList().stream()
                .anyMatch(commoditiesBoughtFromProvider ->
                        commoditiesBoughtFromProvider.getProviderEntityType() == providerEntityType
                        && commoditiesBoughtFromProvider.getCommodityBoughtList().stream()
                                .anyMatch(commodityBoughtDTO ->
                                        commodityBoughtDTO.getCommodityType().getType() ==
                                                CommonDTO.CommodityDTO.CommodityType.CLUSTER_VALUE));
    }
}
