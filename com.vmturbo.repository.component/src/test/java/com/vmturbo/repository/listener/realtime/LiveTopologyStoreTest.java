package com.vmturbo.repository.listener.realtime;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntitiesWithNewState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.UICommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.repository.listener.realtime.SourceRealtimeTopology.SourceRealtimeTopologyBuilder;
import com.vmturbo.topology.graph.supplychain.GlobalSupplyChainCalculator;

/**
 * Test for the {@link LiveTopologyStore} class.
 */
public class LiveTopologyStoreTest {
    /*
     *  Topology to test on:
     *  VM
     *   |
     *  PM   PM
     *   \  /
     *    ST
     */

    private final TopologyEntityDTO storage = TopologyEntityDTO.newBuilder()
        .setOid(1)
        .setTypeSpecificInfo(TypeSpecificInfo.getDefaultInstance())
        .setEntityType(ApiEntityType.STORAGE.typeNumber())
        .setEntityState(EntityState.POWERED_ON)
        .setDisplayName("storage")
        .build();

    private final TopologyEntityDTO host1 = TopologyEntityDTO.newBuilder()
        .setOid(2)
        .setTypeSpecificInfo(TypeSpecificInfo.getDefaultInstance())
        .setEntityType(ApiEntityType.PHYSICAL_MACHINE.typeNumber())
        .setEntityState(EntityState.POWERED_ON)
        .setDisplayName("pm")
        .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
            .setProviderEntityType(ApiEntityType.STORAGE.typeNumber())
            .setProviderId(storage.getOid())
            .addCommodityBought(CommodityBoughtDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                    .setType(UICommodityType.STORAGE_AMOUNT.typeNumber()))))
        .build();

    private final TopologyEntityDTO host2 = TopologyEntityDTO.newBuilder()
        .setOid(3)
        .setTypeSpecificInfo(TypeSpecificInfo.getDefaultInstance())
        .setEntityType(ApiEntityType.PHYSICAL_MACHINE.typeNumber())
        .setEntityState(EntityState.POWERED_ON)
        .setDisplayName("pm")
        .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
            .setProviderEntityType(ApiEntityType.STORAGE.typeNumber())
            .setProviderId(storage.getOid())
            .addCommodityBought(CommodityBoughtDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                    .setType(UICommodityType.STORAGE_AMOUNT.typeNumber()))))
        .build();

    private final LiveTopologyStore liveTopologyStore =
            new LiveTopologyStore(new GlobalSupplyChainCalculator());

    @Test
    public void testSourceTopology() {
        TopologyInfo tInfo = TopologyInfo.newBuilder()
            .setTopologyId(7)
            .build();
        final SourceRealtimeTopologyBuilder bldr = liveTopologyStore.newRealtimeTopology(tInfo);
        bldr.addEntities(Collections.singleton(storage));
        bldr.addEntities(Arrays.asList(host1, host2));
        bldr.finish();
        final SourceRealtimeTopology topo = liveTopologyStore.getSourceTopology().get();
        List<TopologyEntityDTO> entities = topo.entityGraph().entities()
            .map(RepoGraphEntity::getTopologyEntity)
            .collect(Collectors.toList());
        assertThat(entities, containsInAnyOrder(storage, host1, host2));
    }

    /**
     * Tests updating a entity state in the realtime topology.
     */
    @Test
    public void testUpdateEntityWithNewState() {
        TopologyInfo tInfo = TopologyInfo.newBuilder()
            .setTopologyId(7)
            .setCreationTime(0)
            .build();
        final SourceRealtimeTopologyBuilder bldr = liveTopologyStore.newRealtimeTopology(tInfo);
        bldr.addEntities(Arrays.asList(storage, host1, host2));
        bldr.finish();
        TopologyEntityDTO updatedHost = TopologyEntityDTO.newBuilder()
            .setOid(host1.getOid()).setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
            .setEntityState(EntityState.MAINTENANCE).build();
        liveTopologyStore.updateEntityWithNewState(updatedHost);
        final SourceRealtimeTopology topo = liveTopologyStore.getSourceTopology().get();
        Assert.assertEquals(EntityState.MAINTENANCE,
            topo.entityGraph().getEntity(host1.getOid()).get().getEntityState());
        Assert.assertEquals(EntityState.MAINTENANCE,
            topo.entityGraph().getEntity(host1.getOid()).get().getTopologyEntity().getEntityState());
    }

    /**
     * Tests that entity states cached in entitiesWithUpdatedState get applied correctly to
     * incoming topologies, and don't get applied to topologies that are more recent than the
     * state change.
     */
    @Test
    public void testUpdateEntityWithNewStateCache() {
        final TopologyEntityDTO host1PoweredOn = createHostWithState(2, EntityState.POWERED_ON);

        final TopologyEntityDTO host1InMaintenance = createHostWithState(2,
            EntityState.MAINTENANCE);

        EntitiesWithNewState poweredOnStateChange = EntitiesWithNewState
            .newBuilder().addTopologyEntity(host1PoweredOn)
            .setStateChangeId(0)
            .build();

        EntitiesWithNewState maintenanceStateChange = EntitiesWithNewState
            .newBuilder().addTopologyEntity(host1InMaintenance)
            .setStateChangeId(1)
            .build();

        liveTopologyStore.setEntityWithUpdatedState(poweredOnStateChange);
        liveTopologyStore.setEntityWithUpdatedState(maintenanceStateChange);

        TopologyInfo tInfo = TopologyInfo.newBuilder()
            .setTopologyId(0)
            .setCreationTime(0)
            .build();
        final SourceRealtimeTopologyBuilder bldr = liveTopologyStore.newRealtimeTopology(tInfo);
        bldr.addEntities(Arrays.asList(storage, host1, host2));
        bldr.finish();
        final SourceRealtimeTopology topo = liveTopologyStore.getSourceTopology().get();

        // Check the topology is updated with the host state in the entityWithNewState cache
        Assert.assertEquals(EntityState.MAINTENANCE,
            topo.entityGraph().getEntity(host1.getOid()).get().getEntityState());
        Assert.assertEquals(EntityState.MAINTENANCE,
            topo.entityGraph().getEntity(host1.getOid()).get().getTopologyEntity().getEntityState());

        // New topology, with a more updated time stamp then the host state change, should not
        // get affected by the entityWithNewStateCache
        final SourceRealtimeTopologyBuilder updatedBldr = liveTopologyStore
            .newRealtimeTopology(tInfo.toBuilder().setTopologyId(2).build());
        updatedBldr.addEntities(Arrays.asList(storage, host1, host2));
        updatedBldr.finish();
        final SourceRealtimeTopology updatedTopo = liveTopologyStore.getSourceTopology().get();
        Assert.assertEquals(EntityState.POWERED_ON,
            updatedTopo.entityGraph().getEntity(host1.getOid()).get().getEntityState());
        Assert.assertEquals(EntityState.POWERED_ON,
            updatedTopo.entityGraph().getEntity(host1.getOid()).get().getTopologyEntity().getEntityState());
    }

    private TopologyEntityDTO createHostWithState(long id, EntityState entityState) {
        return TopologyEntityDTO.newBuilder()
            .setOid(id)
            .setEntityType(ApiEntityType.PHYSICAL_MACHINE.typeNumber())
            .setEntityState(entityState)
            .setDisplayName("pm")
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .setProviderEntityType(ApiEntityType.STORAGE.typeNumber())
                .setProviderId(storage.getOid())
                .addCommodityBought(CommodityBoughtDTO.newBuilder()
                    .setCommodityType(CommodityType.newBuilder()
                        .setType(UICommodityType.STORAGE_AMOUNT.typeNumber()))))
            .build();
    }
}
