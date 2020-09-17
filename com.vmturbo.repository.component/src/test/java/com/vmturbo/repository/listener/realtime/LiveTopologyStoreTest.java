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
import com.vmturbo.topology.graph.search.SearchResolver;
import com.vmturbo.topology.graph.search.filter.TopologyFilterFactory;
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

    private final SearchResolver<RepoGraphEntity>
            searchResolver = new SearchResolver<>(new TopologyFilterFactory<RepoGraphEntity>());

    private final LiveTopologyStore liveTopologyStore =
            new LiveTopologyStore(new GlobalSupplyChainCalculator(), searchResolver);

    @Test
    public void testSourceTopology() {
        TopologyInfo tInfo = TopologyInfo.newBuilder()
            .setTopologyId(7)
            .build();
        final SourceRealtimeTopologyBuilder bldr = liveTopologyStore.newRealtimeSourceTopology(tInfo);
        bldr.addEntities(Collections.singleton(storage));
        bldr.addEntities(Arrays.asList(host1, host2));
        bldr.finish();
        final SourceRealtimeTopology topo = liveTopologyStore.getSourceTopology().get();
        List<TopologyEntityDTO> entities = topo.entityGraph().entities()
            .map(RepoGraphEntity::getTopologyEntity)
            .collect(Collectors.toList());
        assertThat(entities, containsInAnyOrder(storage, host1, host2));
    }
}
