package com.vmturbo.repository.listener.realtime;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import java.util.Arrays;
import java.util.Map;

import org.junit.Test;

import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.UICommodityType;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.common.protobuf.topology.UIEnvironmentType;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.graph.TopologyGraphCreator;
import com.vmturbo.topology.graph.supplychain.SupplyChainResolver;

public class GlobalSupplyChainCalculatorTest {

    /**
     *  VM           Cloud VM
     *   |              |
     *  PM   PM      Compute Tier
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

    private final TopologyEntityDTO computeTier = TopologyEntityDTO.newBuilder()
        .setOid(5)
        .setEntityType(UIEntityType.COMPUTE_TIER.typeNumber())
        .setDisplayName("compute tier")
        .setEnvironmentType(EnvironmentType.CLOUD)
        .build();

    private final TopologyEntityDTO cloudVm = TopologyEntityDTO.newBuilder()
        .setOid(6)
        .setEntityType(UIEntityType.VIRTUAL_MACHINE.typeNumber())
        .setDisplayName("cloud vm")
        .setEnvironmentType(EnvironmentType.CLOUD)
        .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
            .setProviderEntityType(UIEntityType.COMPUTE_TIER.typeNumber())
            .setProviderId(computeTier.getOid())
            .addCommodityBought(CommodityBoughtDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                    .setType(UICommodityType.MEM.typeNumber()))))
        .build();

    private final GlobalSupplyChainCalculator supplyChainCalculatorFactory =
        GlobalSupplyChainCalculator.newFactory().newCalculator();

    private final TopologyGraph<RepoGraphEntity> graph = new TopologyGraphCreator<RepoGraphEntity.Builder, RepoGraphEntity>()
        .addEntities(Arrays.asList(
            RepoGraphEntity.newBuilder(vm),
            RepoGraphEntity.newBuilder(host1),
            RepoGraphEntity.newBuilder(host2),
            RepoGraphEntity.newBuilder(storage),
            RepoGraphEntity.newBuilder(computeTier),
            RepoGraphEntity.newBuilder(cloudVm)))
        .build();

    private final SupplyChainResolver<RepoGraphEntity> supplyChainResolver = new SupplyChainResolver<>();

    @Test
    public void testCalculateOnPremSupplyChain() {
        final Map<UIEntityType, SupplyChainNode> nodesByType =
            supplyChainCalculatorFactory.computeGlobalSupplyChain(graph, UIEnvironmentType.ON_PREM, supplyChainResolver);

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

    @Test
    public void testCalculateCloudSupplyChain() {
        final Map<UIEntityType, SupplyChainNode> nodesByType =
            supplyChainCalculatorFactory.computeGlobalSupplyChain(graph, UIEnvironmentType.CLOUD, supplyChainResolver);

        assertThat(nodesByType.keySet(),
            containsInAnyOrder(UIEntityType.VIRTUAL_MACHINE));
        // We actually ignore the compute tiers.
        assertThat(RepositoryDTOUtil.getAllMemberOids(nodesByType.get(UIEntityType.VIRTUAL_MACHINE)),
            containsInAnyOrder(cloudVm.getOid()));
    }

}