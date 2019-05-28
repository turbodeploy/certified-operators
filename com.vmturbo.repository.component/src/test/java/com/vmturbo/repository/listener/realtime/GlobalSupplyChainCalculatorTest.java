package com.vmturbo.repository.listener.realtime;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import java.util.Arrays;
import java.util.Map;

import org.junit.Test;

import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.UICommodityType;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.graph.TopologyGraphCreator;

public class GlobalSupplyChainCalculatorTest {

    /**
     *  VM
     *   |
     *  PM   PM
     *   \  /
     *    ST
     */

    private final TopologyEntityDTO storage = TopologyEntityDTO.newBuilder()
        .setOid(1)
        .setEntityType(UIEntityType.STORAGE.typeNumber())
        .setDisplayName("storage")
        .build();

    private final TopologyEntityDTO host1 = TopologyEntityDTO.newBuilder()
        .setOid(2)
        .setEntityType(UIEntityType.PHYSICAL_MACHINE.typeNumber())
        .setDisplayName("pm")
        .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
            .setProviderEntityType(UIEntityType.STORAGE.typeNumber())
            .setProviderId(storage.getOid())
            .addCommodityBought(CommodityBoughtDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                    .setType(UICommodityType.STORAGE_AMOUNT.typeNumber()))))
        .build();

    private final TopologyEntityDTO host2 = TopologyEntityDTO.newBuilder()
        .setOid(3)
        .setEntityType(UIEntityType.PHYSICAL_MACHINE.typeNumber())
        .setDisplayName("pm")
        .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
            .setProviderEntityType(UIEntityType.STORAGE.typeNumber())
            .setProviderId(storage.getOid())
            .addCommodityBought(CommodityBoughtDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                    .setType(UICommodityType.STORAGE_AMOUNT.typeNumber()))))
        .build();

    private final TopologyEntityDTO vm = TopologyEntityDTO.newBuilder()
        .setOid(4)
        .setEntityType(UIEntityType.VIRTUAL_MACHINE.typeNumber())
        .setDisplayName("vm")
        .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
            .setProviderEntityType(UIEntityType.PHYSICAL_MACHINE.typeNumber())
            .setProviderId(host1.getOid())
            .addCommodityBought(CommodityBoughtDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                    .setType(UICommodityType.MEM.typeNumber()))))
        .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
            .setProviderEntityType(UIEntityType.STORAGE.typeNumber())
            .setProviderId(storage.getOid())
            .addCommodityBought(CommodityBoughtDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                    .setType(UICommodityType.STORAGE_AMOUNT.typeNumber()))))
        .build();

    private final GlobalSupplyChainCalculator supplyChainCalculatorFactory =
        GlobalSupplyChainCalculator.newFactory().newCalculator();

    @Test
    public void testCalculateSupplyChain() {
        final TopologyGraph<RepoGraphEntity> graph = new TopologyGraphCreator<RepoGraphEntity.Builder, RepoGraphEntity>()
            .addEntities(Arrays.asList(
                RepoGraphEntity.newBuilder(vm),
                RepoGraphEntity.newBuilder(host1),
                RepoGraphEntity.newBuilder(host2),
                RepoGraphEntity.newBuilder(storage)))
            .build();

        final Map<UIEntityType, SupplyChainNode> nodesByType =
            supplyChainCalculatorFactory.computeGlobalSupplyChain(graph);
        assertThat(nodesByType.keySet(),
            containsInAnyOrder(UIEntityType.VIRTUAL_MACHINE, UIEntityType.PHYSICAL_MACHINE, UIEntityType.STORAGE));
        assertThat(nodesByType.get(UIEntityType.VIRTUAL_MACHINE).getConnectedProviderTypesList(),
            containsInAnyOrder(UIEntityType.PHYSICAL_MACHINE.apiStr(), UIEntityType.STORAGE.apiStr()));
        assertThat(RepositoryDTOUtil.getAllMemberOids(nodesByType.get(UIEntityType.VIRTUAL_MACHINE)),
            containsInAnyOrder(vm.getOid()));

        assertThat(nodesByType.get(UIEntityType.PHYSICAL_MACHINE).getConnectedProviderTypesList(),
            containsInAnyOrder(UIEntityType.STORAGE.apiStr()));
        assertThat(nodesByType.get(UIEntityType.PHYSICAL_MACHINE).getConnectedConsumerTypesList(),
            containsInAnyOrder(UIEntityType.VIRTUAL_MACHINE.apiStr()));
        assertThat(RepositoryDTOUtil.getAllMemberOids(nodesByType.get(UIEntityType.PHYSICAL_MACHINE)),
            containsInAnyOrder(host1.getOid(), host2.getOid()));

        assertThat(nodesByType.get(UIEntityType.STORAGE).getConnectedConsumerTypesList(),
            containsInAnyOrder(UIEntityType.PHYSICAL_MACHINE.apiStr(), UIEntityType.VIRTUAL_MACHINE.apiStr()));
        assertThat(RepositoryDTOUtil.getAllMemberOids(nodesByType.get(UIEntityType.STORAGE)),
            containsInAnyOrder(storage.getOid()));
    }

}