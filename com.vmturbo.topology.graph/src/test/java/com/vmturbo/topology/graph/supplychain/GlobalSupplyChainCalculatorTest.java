package com.vmturbo.topology.graph.supplychain;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.junit.Test;

import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.UIEntityType;
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
     *
     *    For testing purposes, the environment of the cloud VM appears as "hybrid".
     */

    private static final long STORAGE_ID = 1;
    private static final long HOST1_ID = 2;
    private static final long HOST2_ID = 3;
    private static final long VM_ID = 4;
    private static final long COMPUTE_TIER_ID = 5;
    private static final long CLOUD_VM_ID = 6;
    private static final long REGION_ID = 7;

    private final TestGraphEntity.Builder storage =
            TestGraphEntity.newBuilder(STORAGE_ID, UIEntityType.STORAGE)
                    .setEnvironmentType(EnvironmentType.ON_PREM)
                    .setName("storage");

    private final TestGraphEntity.Builder host1 =
            TestGraphEntity.newBuilder(HOST1_ID, UIEntityType.PHYSICAL_MACHINE)
                    .setName("pm")
                    .setEnvironmentType(EnvironmentType.ON_PREM)
                    .addProviderId(STORAGE_ID);

    private final TestGraphEntity.Builder host2 =
            TestGraphEntity.newBuilder(HOST2_ID, UIEntityType.PHYSICAL_MACHINE)
                    .setName("pm1")
                    .setEnvironmentType(EnvironmentType.ON_PREM)
                    .addProviderId(STORAGE_ID);

    private final TestGraphEntity.Builder vm =
            TestGraphEntity.newBuilder(VM_ID, UIEntityType.VIRTUAL_MACHINE)
                   .setName("vm")
                   .setEnvironmentType(EnvironmentType.ON_PREM)
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
                    .setEnvironmentType(EnvironmentType.HYBRID)
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
            globalSupplyChainCalculator.getSupplyChainNodes(topology, EnvironmentType.ON_PREM,
                    GlobalSupplyChainCalculator.DEFAULT_ENTITY_TYPE_FILTER);

        assertThat(nodesByType.keySet(), containsInAnyOrder(UIEntityType.VIRTUAL_MACHINE,
                                                            UIEntityType.PHYSICAL_MACHINE,
                                                            UIEntityType.STORAGE));

        assertThat(nodesByType.get(UIEntityType.VIRTUAL_MACHINE).getConnectedConsumerTypesList(),
                   containsInAnyOrder());
        assertThat(nodesByType.get(UIEntityType.VIRTUAL_MACHINE).getConnectedProviderTypesList(),
                   containsInAnyOrder(UIEntityType.PHYSICAL_MACHINE.apiStr(),
                                      UIEntityType.STORAGE.apiStr(),
                                      UIEntityType.REGION.apiStr())); // appears here because the cloud VM
                                                                      // is included in the supply chain
                                                                      // as a hybrid entity
        assertThat(RepositoryDTOUtil.getAllMemberOids(nodesByType.get(UIEntityType.VIRTUAL_MACHINE)),
                   containsInAnyOrder(VM_ID, CLOUD_VM_ID)); // note that hybrid entity is also included

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
                globalSupplyChainCalculator.getSupplyChainNodes(topology, EnvironmentType.CLOUD,
                        GlobalSupplyChainCalculator.DEFAULT_ENTITY_TYPE_FILTER);

        // Note that tiers are ignored
        assertThat(nodesByType.keySet(),
                   containsInAnyOrder(UIEntityType.VIRTUAL_MACHINE, UIEntityType.REGION));

        // Region appears as a "consumer" in the chain ("consumers" are all inbound edges)
        assertThat(nodesByType.get(UIEntityType.VIRTUAL_MACHINE).getConnectedProviderTypesList(),
                   containsInAnyOrder(UIEntityType.REGION.apiStr()));
        assertThat(nodesByType.get(UIEntityType.VIRTUAL_MACHINE).getConnectedConsumerTypesList(),
                   containsInAnyOrder());
        assertThat(RepositoryDTOUtil.getAllMemberOids(nodesByType.get(UIEntityType.VIRTUAL_MACHINE)),
                   containsInAnyOrder(CLOUD_VM_ID)); // hybrid entity is also included

        // VM appears as a "provider" in the chain ("providers" are all outbound edges)
        assertThat(nodesByType.get(UIEntityType.REGION).getConnectedProviderTypesList(),
                   containsInAnyOrder());
        assertThat(nodesByType.get(UIEntityType.REGION).getConnectedConsumerTypesList(),
                   containsInAnyOrder(UIEntityType.VIRTUAL_MACHINE.apiStr()));
        assertThat(RepositoryDTOUtil.getAllMemberOids(nodesByType.get(UIEntityType.REGION)),
                   containsInAnyOrder(REGION_ID));
    }

    /**
     * Test proper calculation of hybrid global supply chain
     */
    @Test
    public void testCalculateHybridSupplyChain() {
        assertFullTopology(globalSupplyChainCalculator.getSupplyChainNodes(
                                topology,
                                EnvironmentType.HYBRID,
                                GlobalSupplyChainCalculator.DEFAULT_ENTITY_TYPE_FILTER));
    }

    /**
     * Test proper calculation of global supply chain with no environment filtering.
     */
    @Test
    public void testCalculateNoEnvFilteringSupplyChain() {
        assertFullTopology(globalSupplyChainCalculator.getSupplyChainNodes(
                                topology,
                                entity -> true,
                                GlobalSupplyChainCalculator.DEFAULT_ENTITY_TYPE_FILTER));
    }

    /**
     * Verify that "non-displayed" entity type nodes will be returned,
     * if you request the supply chain with no entity type filtering.
     */
    @Test
    public void testCalculateCloudSupplyChainNoFilterForDisplay() {
        final Map<UIEntityType, SupplyChainNode> nodesByType =
                globalSupplyChainCalculator.getSupplyChainNodes(topology, EnvironmentType.CLOUD, type -> false);

        // Verify that the compute tiers are included in this response
        assertThat(nodesByType.keySet(),
                containsInAnyOrder(UIEntityType.VIRTUAL_MACHINE, UIEntityType.COMPUTE_TIER, UIEntityType.REGION));
        assertThat(RepositoryDTOUtil.getAllMemberOids(nodesByType.get(UIEntityType.VIRTUAL_MACHINE)),
                containsInAnyOrder(cloudVm.getOid()));
        assertThat(RepositoryDTOUtil.getAllMemberOids(nodesByType.get(UIEntityType.COMPUTE_TIER)),
                containsInAnyOrder(computeTier.getOid()));
    }

    private void assertFullTopology(@Nonnull Map<UIEntityType, SupplyChainNode> nodesByType) {
        assertThat(nodesByType.keySet(), containsInAnyOrder(UIEntityType.VIRTUAL_MACHINE,
                                                            UIEntityType.PHYSICAL_MACHINE,
                                                            UIEntityType.STORAGE,
                                                            UIEntityType.REGION));

        assertThat(nodesByType.get(UIEntityType.VIRTUAL_MACHINE).getConnectedConsumerTypesList(),
                   containsInAnyOrder());
        assertThat(nodesByType.get(UIEntityType.VIRTUAL_MACHINE).getConnectedProviderTypesList(),
                   containsInAnyOrder(UIEntityType.PHYSICAL_MACHINE.apiStr(),
                                      UIEntityType.STORAGE.apiStr(),
                                      UIEntityType.REGION.apiStr()));
        assertThat(RepositoryDTOUtil.getAllMemberOids(nodesByType.get(UIEntityType.VIRTUAL_MACHINE)),
                   containsInAnyOrder(VM_ID, CLOUD_VM_ID));

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
        assertThat(nodesByType.get(UIEntityType.REGION).getConnectedProviderTypesList(),
                   containsInAnyOrder());
        assertThat(nodesByType.get(UIEntityType.REGION).getConnectedConsumerTypesList(),
                   containsInAnyOrder(UIEntityType.VIRTUAL_MACHINE.apiStr()));
        assertThat(RepositoryDTOUtil.getAllMemberOids(nodesByType.get(UIEntityType.REGION)),
                   containsInAnyOrder(REGION_ID));
    }
}