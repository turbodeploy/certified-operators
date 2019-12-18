package com.vmturbo.topology.graph.supplychain;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Test;

import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.common.protobuf.topology.UIEnvironmentType;
import com.vmturbo.topology.graph.TestGraphEntity;
import com.vmturbo.topology.graph.TopologyGraph;

/**
 * Tests the generation of global supply chains.
 */
public class GlobalSupplyChainCalculatorTest {
    private final GlobalSupplyChainCalculator globalSupplyChainCalculator =
            new GlobalSupplyChainCalculator();

    /*
     *  Topology on which tests are made:
     *
     *  VM           Cloud VM      Region aggregates VM and Compute Tier
     *   |              |
     *  PM   PM      Compute Tier
     *   \  /
     *    ST
     */

    private static final long STORAGE_ID = 1;
    private static final long HOST1_ID = 2;
    private static final long HOST2_ID = 3;
    private static final long VM_ID = 4;
    private static final long COMPUTE_TIER_ID = 5;
    private static final long CLOUD_VM_ID = 6;
    private static final long REGION_ID = 7;

    private final TestGraphEntity.Builder storage =
            TestGraphEntity.newBuilder(STORAGE_ID, UIEntityType.STORAGE).setName("storage");

    private final TestGraphEntity.Builder host1 =
            TestGraphEntity.newBuilder(HOST1_ID, UIEntityType.PHYSICAL_MACHINE)
                    .setName("pm")
                    .addProviderId(STORAGE_ID);

    private final TestGraphEntity.Builder host2 =
            TestGraphEntity.newBuilder(HOST2_ID, UIEntityType.PHYSICAL_MACHINE)
                    .setName("pm1")
                    .addProviderId(STORAGE_ID);

    private final TestGraphEntity.Builder vm =
            TestGraphEntity.newBuilder(VM_ID, UIEntityType.VIRTUAL_MACHINE)
                   .setName("vm")
                   .addProviderId(HOST1_ID)
                   .addProviderId(STORAGE_ID);

    private final TestGraphEntity.Builder computeTier =
            TestGraphEntity.newBuilder(COMPUTE_TIER_ID, UIEntityType.COMPUTE_TIER)
                    .setName("compute tier")
                    .addConnectedEntity(REGION_ID, ConnectionType.AGGREGATED_BY_CONNECTION)
                    .setEnvironmentType(EnvironmentType.CLOUD);

    private final TestGraphEntity.Builder cloudVm =
            TestGraphEntity.newBuilder(CLOUD_VM_ID, UIEntityType.VIRTUAL_MACHINE)
                    .setName("cloud vm")
                    .setEnvironmentType(EnvironmentType.CLOUD)
                    .addProviderId(COMPUTE_TIER_ID)
                    .addConnectedEntity(REGION_ID, ConnectionType.AGGREGATED_BY_CONNECTION);

    private final TestGraphEntity.Builder region =
            TestGraphEntity.newBuilder(REGION_ID, UIEntityType.REGION)
                    .setName("region")
                    .setEnvironmentType(EnvironmentType.CLOUD);

    private final Map<Long, TestGraphEntity.Builder> graph =
            Stream.of(storage, host1, host2, vm, computeTier, cloudVm, region)
                    .collect(Collectors.toMap(TestGraphEntity.Builder::getOid, Function.identity()));

    private final TopologyGraph<TestGraphEntity> topology = TestGraphEntity.newGraph(graph);

    /**
     * Test the proper calculation of the on-prem global supply chain.
     */
    @Test
    public void testCalculateOnPremSupplyChain() {
        final Map<UIEntityType, SupplyChainNode> nodesByType =
            globalSupplyChainCalculator.getSupplyChainNodes(topology, UIEnvironmentType.ON_PREM);

        assertThat(nodesByType.keySet(), containsInAnyOrder(UIEntityType.VIRTUAL_MACHINE,
                                                            UIEntityType.PHYSICAL_MACHINE,
                                                            UIEntityType.STORAGE));

        assertThat(nodesByType.get(UIEntityType.VIRTUAL_MACHINE).getConnectedConsumerTypesList(),
                   containsInAnyOrder());
        assertThat(nodesByType.get(UIEntityType.VIRTUAL_MACHINE).getConnectedProviderTypesList(),
                   containsInAnyOrder(UIEntityType.PHYSICAL_MACHINE.apiStr(),
                                      UIEntityType.STORAGE.apiStr()));
        assertThat(RepositoryDTOUtil.getAllMemberOids(nodesByType.get(UIEntityType.VIRTUAL_MACHINE)),
                   containsInAnyOrder(VM_ID));

        assertThat(nodesByType.get(UIEntityType.PHYSICAL_MACHINE).getConnectedProviderTypesList(),
                   containsInAnyOrder(UIEntityType.STORAGE.apiStr()));
        assertThat(nodesByType.get(UIEntityType.PHYSICAL_MACHINE).getConnectedConsumerTypesList(),
                   containsInAnyOrder(UIEntityType.VIRTUAL_MACHINE.apiStr()));
        assertThat(RepositoryDTOUtil.getAllMemberOids(nodesByType.get(UIEntityType.PHYSICAL_MACHINE)),
                   containsInAnyOrder(HOST1_ID, HOST2_ID));

        assertThat(nodesByType.get(UIEntityType.STORAGE).getConnectedConsumerTypesList(),
                   containsInAnyOrder(UIEntityType.PHYSICAL_MACHINE.apiStr(),
                                      UIEntityType.VIRTUAL_MACHINE.apiStr()));
        assertThat(nodesByType.get(UIEntityType.STORAGE).getConnectedProviderTypesList(),
                   containsInAnyOrder());
        assertThat(RepositoryDTOUtil.getAllMemberOids(nodesByType.get(UIEntityType.STORAGE)),
                   containsInAnyOrder(STORAGE_ID));
    }

    /**
     * Test the proper calculation of the cloud global supply chain.
     */
    @Test
    public void testCalculateCloudSupplyChain() {
        final Map<UIEntityType, SupplyChainNode> nodesByType =
                globalSupplyChainCalculator.getSupplyChainNodes(topology, UIEnvironmentType.CLOUD);

        // Note that tiers are ignored
        assertThat(nodesByType.keySet(),
                   containsInAnyOrder(UIEntityType.VIRTUAL_MACHINE, UIEntityType.REGION));

        // Region appears as a "consumer" in the chain ("consumers" are all inbound edges)
        assertThat(nodesByType.get(UIEntityType.VIRTUAL_MACHINE).getConnectedProviderTypesList(),
                   containsInAnyOrder(UIEntityType.REGION.apiStr()));
        assertThat(nodesByType.get(UIEntityType.VIRTUAL_MACHINE).getConnectedConsumerTypesList(),
                   containsInAnyOrder());
        assertThat(RepositoryDTOUtil.getAllMemberOids(nodesByType.get(UIEntityType.VIRTUAL_MACHINE)),
                   containsInAnyOrder(CLOUD_VM_ID));

        // VM appears as a "provider" in the chain ("providers" are all outbound edges)
        assertThat(nodesByType.get(UIEntityType.REGION).getConnectedProviderTypesList(),
                   containsInAnyOrder());
        assertThat(nodesByType.get(UIEntityType.REGION).getConnectedConsumerTypesList(),
                   containsInAnyOrder(UIEntityType.VIRTUAL_MACHINE.apiStr()));
        assertThat(RepositoryDTOUtil.getAllMemberOids(nodesByType.get(UIEntityType.REGION)),
                   containsInAnyOrder(REGION_ID));
    }
}