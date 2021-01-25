package com.vmturbo.topology.graph.supplychain;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.topology.graph.TestGraphEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.graph.supplychain.SupplyChainCalculator.Frontier;
import com.vmturbo.topology.graph.supplychain.SupplyChainCalculator.TraversalMode;
import com.vmturbo.topology.graph.supplychain.SupplyChainCalculator.TraversalState;

/**
 * Tests the functionality of the generation of scoped supply chains.
 */
public class SupplyChainCalculatorTest {
    /**
     * Constants for the orphan volume-related checks
     */
    private static final long VOL_ID = 1L;
    private static final long AZ_ID = 2L;
    private static final long REG_ID = 3L;

    /**
     * Constants for on-prem checks.
     */
    private static final long PM_ID = 1L;
    private static final long ST_ID = 2L;
    private static final long DC_ID = 3L;
    private static final long PM_ID2 = 4L;
    private static final long ST_ID2 = 5L;
    private static final long VM_ID = 6L;

    /**
     * Constants for cloud-native checks.
     */
    private static final long CONTAINER_CLUSTER_ID = 600L;
    private static final long NAMESPACE_DEMO_APP_ID = 610L;
    private static final long NAMESPACE_ROBOT_SHOP_ID = 611L;
    private static final long WORKLOAD_CONTROLLER_CART_ID = 620L;
    private static final long WORKLOAD_CONTROLLER_REDIS_ID = 621L;
    private static final long CONTAINER_POD_CART1_ID = 630L;
    private static final long CONTAINER_POD_CART2_ID = 631L;
    private static final long CONTAINER_POD_REDIS1_ID = 632L;
    private static final long CONTAINER_SPEC_CART_ID = 640L;
    private static final long CONTAINER_SPEC_ISTIO_PROXY_ID = 641L;
    private static final long CONTAINER_SPEC_REDIS_ID = 642L;
    private static final long CONTAINER_CART1_ID = 650L;
    private static final long CONTAINER_CART2_ID = 651L;
    private static final long CONTAINER_ISTIO_PROXY1_ID = 652L;
    private static final long CONTAINER_ISTIO_PROXY2_ID = 653L;
    private static final long CONTAINER_REDIS1_ID = 654L;
    private static final long ANOTHER_VM_IN_CLUSTER_ID = 660L;

    /**
     * Constants and variables for the main cloud checks.
     */
    private static final long REGION_ID = 1L;
    private static final long ZONE_ID = 2L;
    private static final long VM_1_ID = 3L;
    private static final long VM_2_ID = 4L;
    private static final long VOLUME_ID = 5L;
    private static final long ACCOUNT_ID = 6L;
    private static final long APP_ID = 7L;
    private static final long VMSPEC_ID = 25L;
    private TopologyGraph<TestGraphEntity> cloudTopology;
    private SupplyChainNode region;
    private SupplyChainNode zone;
    private SupplyChainNode vm;
    private SupplyChainNode volume;
    private SupplyChainNode account;
    private SupplyChainNode app;
    private SupplyChainNode vmspec;

    /**
     * Constants for VDI topology creation.
     */
    private static final long VDI_TOPO_PM1ID = 1L;
    private static final long VDI_TOPO_PM2ID = 2L;
    private static final long VDI_TOPO_VDC1ID = 11L;
    private static final long VDI_TOPO_DP1ID = 12L;
    private static final long VDI_TOPO_VM1ID = 21L;
    private static final long VDI_TOPO_BU1ID = 22L;
    private static final long VDI_TOPO_VPODID = 333L;

    /**
     * Constants and variables for checks related to topologies with VDCs.
     */
    private static final long VDC_TOPO_PM1ID = 1L;
    private static final long VDC_TOPO_PM2ID = 2L;
    private static final long VDC_TOPO_PM3ID = 3L;
    private static final long VDC_TOPO_VDC1ID = 11L;
    private static final long VDC_TOPO_VDC2ID = 12L;
    private static final long VDC_TOPO_VDC3ID = 13L;
    private static final long VDC_TOPO_VDC4ID = 14L;
    private static final long VDC_TOPO_VM1ID = 21L;
    private static final long VDC_TOPO_VM2ID = 22L;
    private static final long VDC_TOPO_VM3ID = 23L;
    private static final long VDC_TOPO_VM4ID = 24L;
    private TopologyGraph<TestGraphEntity> vdcTopology;
    private SupplyChainNode vdcTopoPm;
    private SupplyChainNode vdcTopoVdc;
    private SupplyChainNode vdcTopoVm;

    private static final long POD_2_ID = 2;
    private static final long VDC_CONTAINERS_TOPO_VM_2_ID = 22;

    /**
     * Simple topology.
     */
    @Test
    public void testMultipleEntitiesConnectedToSameProvider() {
        /*
         * One diskarray underlying two storages.
         *   1
         *  / \
         * 22 33
         *  \ /
         *  444
         */
        final TopologyGraph<TestGraphEntity> graph =
            TestGraphEntity.newGraph(
                TestGraphEntity.newBuilder(444L, ApiEntityType.DISKARRAY),
                TestGraphEntity.newBuilder(22L, ApiEntityType.STORAGE)
                    .addProviderId(444L),
                TestGraphEntity.newBuilder(33L, ApiEntityType.STORAGE)
                    .addProviderId(444L),
                TestGraphEntity.newBuilder(1L, ApiEntityType.VIRTUAL_MACHINE)
                    .addProviderId(22L)
                    .addProviderId(33L));

        final Map<Integer, SupplyChainNode> supplychain = getSupplyChain(graph, 1L);

        assertThat(supplychain.get(ApiEntityType.VIRTUAL_MACHINE.typeNumber())
                              .getConnectedProviderTypesList(),
                   containsInAnyOrder(ApiEntityType.STORAGE.typeNumber()));
        assertThat(RepositoryDTOUtil.getMemberCount(
                        supplychain.get(ApiEntityType.VIRTUAL_MACHINE.typeNumber())),
                   is(1));

        assertThat(supplychain.get(ApiEntityType.STORAGE.typeNumber())
                              .getConnectedProviderTypesList(),
                   containsInAnyOrder(ApiEntityType.DISKARRAY.typeNumber()));
        assertThat(supplychain.get(ApiEntityType.STORAGE.typeNumber())
                              .getConnectedConsumerTypesList(),
                   containsInAnyOrder(ApiEntityType.VIRTUAL_MACHINE.typeNumber()));
        assertThat(RepositoryDTOUtil.getMemberCount(
                        supplychain.get(ApiEntityType.STORAGE.typeNumber())),
                   is(2));

        assertThat(supplychain.get(ApiEntityType.DISKARRAY.typeNumber())
                              .getConnectedConsumerTypesList(),
                   containsInAnyOrder(ApiEntityType.STORAGE.typeNumber()));
        assertThat(RepositoryDTOUtil.getMemberCount(
                        supplychain.get(ApiEntityType.DISKARRAY.typeNumber())),
                   is(1));
    }

    /**
     * Topology with an unconnected entity.
     */
    @Test
    public void testUnconnectedEntitySupplyChain() {
        /*
         * 33 should have no connected types.
         *   1
         *  /
         * 22 33
         *  \
         *  444
         */
       final TopologyGraph<TestGraphEntity> graph =
           TestGraphEntity.newGraph(
                TestGraphEntity.newBuilder(444L, ApiEntityType.DISKARRAY),
                TestGraphEntity.newBuilder(22L, ApiEntityType.STORAGE)
                    .addProviderId(444L),
                TestGraphEntity.newBuilder(33L, ApiEntityType.STORAGE),
                TestGraphEntity.newBuilder(1L, ApiEntityType.VIRTUAL_MACHINE)
                    .addProviderId(22L));

        final Map<Integer, SupplyChainNode> supplychain = getSupplyChain(graph, 33L);

        assertThat(supplychain.size(), is(1));
        assertThat(RepositoryDTOUtil.getMemberCount(supplychain.get(ApiEntityType.STORAGE.typeNumber())),
                   is(1));
        assertTrue(supplychain.get(ApiEntityType.STORAGE.typeNumber())
                        .getConnectedProviderTypesList().isEmpty());
        assertTrue(supplychain.get(ApiEntityType.STORAGE.typeNumber())
                        .getConnectedConsumerTypesList().isEmpty());
    }

    /**
     * This tests the situation, in which an entity type
     * can be found in two (or more) different depths.
     *
     * <p>Let A, B, and C represent entity types.
     * Let A1, A2 be entities of type A, B1 entity of type B,
     * and C1 entity of type C. Let A1 consume from B1 and A2 from C2.
     * Let C1 be the seed.</p>
     *
     * <p>The "indirection" rule of the old supply chain algorithm
     * would omit the "indirect" consumer A1. This unit test ensures that
     * this all entities are included in the supply chain.</p>
     */
    @Test
    public void testOneEntityTypeInMultipleDepths() {
        /*                 A (2 entities; one consumes from C and one from B)
         *               / |
         *   (1 entity) B  |
         *               \ |
         *                 C (1 entity) <-- starting point
         */
       final TopologyGraph<TestGraphEntity> graph =
           TestGraphEntity.newGraph(
                TestGraphEntity.newBuilder(444L, ApiEntityType.DISKARRAY),
                TestGraphEntity.newBuilder(33L, ApiEntityType.STORAGE)
                        .addProviderId(444L),
                // Buying from storage.
                TestGraphEntity.newBuilder(2L, ApiEntityType.VIRTUAL_MACHINE)
                        .addProviderId(33L),
                // Buying from the DA directly.
                TestGraphEntity.newBuilder(1L, ApiEntityType.VIRTUAL_MACHINE)
                        .addProviderId(444L));

        final Map<Integer, SupplyChainNode> supplychain = getSupplyChain(graph, 444L);

        assertThat(supplychain.keySet(),
                   containsInAnyOrder(ApiEntityType.VIRTUAL_MACHINE.typeNumber(),
                                      ApiEntityType.STORAGE.typeNumber(),
                                      ApiEntityType.DISKARRAY.typeNumber()));

        final SupplyChainNode vmNode = supplychain.get(ApiEntityType.VIRTUAL_MACHINE.typeNumber());
        assertThat(RepositoryDTOUtil.getMemberCount(vmNode), is(2));
        assertThat(vmNode.getConnectedProviderTypesList(),
                   containsInAnyOrder(ApiEntityType.DISKARRAY.typeNumber(), ApiEntityType.STORAGE.typeNumber()));

        final SupplyChainNode stNode = supplychain.get(ApiEntityType.STORAGE.typeNumber());
        assertThat(RepositoryDTOUtil.getMemberCount(stNode), is(1));
        // No VM provider, because the VM that's buying from the storage directly is not in
        // the DA's supply chain.
        assertThat(stNode.getConnectedProviderTypesList(),
                   containsInAnyOrder(ApiEntityType.DISKARRAY.typeNumber()));

        final SupplyChainNode daNode = supplychain.get(ApiEntityType.DISKARRAY.typeNumber());
        assertThat(RepositoryDTOUtil.getMemberCount(daNode), is(1));
        assertThat(daNode.getConnectedConsumerTypesList(),
                   containsInAnyOrder(ApiEntityType.VIRTUAL_MACHINE.typeNumber(),
                                      ApiEntityType.STORAGE.typeNumber()));
    }

    /**
     * Tests special rule for relationship between PMs and storage.
     */
   @Test
   public void testSpecialPMStorageRule() {
        /*
         *  Topology:
         *  VM1     VM2
         *   |\     /|
         *   | \   / |
         *   |  \ /  |
         *   |  PM   |
         *   |  / \  |
         *   | /   \ |
         *   ST1   ST2
         *
         *   Thanks to the special rule between PM/ST,
         *   scoping on VM1 will not bring in ST2
         *   scoping on PM will bring everything
         */
       final long vm1Id = 1;
       final long vm2Id = 2;
       final long pmId = 11;
       final long st1Id = 21;
       final long st2Id = 22;
       final TopologyGraph<TestGraphEntity> graph =
           TestGraphEntity.newGraph(
               TestGraphEntity.newBuilder(vm1Id, ApiEntityType.VIRTUAL_MACHINE)
                   .addProviderId(pmId)
                   .addProviderId(st1Id),
               TestGraphEntity.newBuilder(vm2Id, ApiEntityType.VIRTUAL_MACHINE)
                   .addProviderId(pmId)
                   .addProviderId(st2Id),
               TestGraphEntity.newBuilder(pmId, ApiEntityType.PHYSICAL_MACHINE)
                   .addProviderId(st1Id)
                   .addProviderId(st2Id),
               TestGraphEntity.newBuilder(st1Id, ApiEntityType.STORAGE),
               TestGraphEntity.newBuilder(st2Id, ApiEntityType.STORAGE));

       final Map<Integer, SupplyChainNode> supplychainFromVM1 = getSupplyChain(graph, vm1Id);
       final Map<Integer, SupplyChainNode> supplychainFromPM = getSupplyChain(graph, pmId);

       commonForSpecialPMStorageRule(supplychainFromVM1);
       commonForSpecialPMStorageRule(supplychainFromPM);

       final SupplyChainNode vmNodeFromChainScopedOnVM1 =
               supplychainFromVM1.get(ApiEntityType.VIRTUAL_MACHINE.typeNumber());
       final SupplyChainNode pmNodeFromChainScopedOnVM1 =
               supplychainFromVM1.get(ApiEntityType.PHYSICAL_MACHINE.typeNumber());
       final SupplyChainNode stNodeFromChainScopedOnVM1 =
               supplychainFromVM1.get(ApiEntityType.STORAGE.typeNumber());
       assertThat(getAllNodeIds(vmNodeFromChainScopedOnVM1),
                  containsInAnyOrder(vm1Id));
       assertThat(getAllNodeIds(pmNodeFromChainScopedOnVM1),
                  containsInAnyOrder(pmId));
       assertThat(getAllNodeIds(stNodeFromChainScopedOnVM1),
                  containsInAnyOrder(st1Id));

       final SupplyChainNode vmNodeFromChainScopedOnPM =
               supplychainFromPM.get(ApiEntityType.VIRTUAL_MACHINE.typeNumber());
       final SupplyChainNode pmNodeFromChainScopedOnPM =
               supplychainFromPM.get(ApiEntityType.PHYSICAL_MACHINE.typeNumber());
       final SupplyChainNode stNodeFromChainScopedOnPM =
               supplychainFromPM.get(ApiEntityType.STORAGE.typeNumber());
       assertThat(getAllNodeIds(vmNodeFromChainScopedOnPM),
                  containsInAnyOrder(vm2Id, vm1Id));
       assertThat(getAllNodeIds(pmNodeFromChainScopedOnPM),
                  containsInAnyOrder(pmId));
       assertThat(getAllNodeIds(stNodeFromChainScopedOnPM),
                  containsInAnyOrder(st2Id, st1Id));
   }

   private void commonForSpecialPMStorageRule(
           @Nonnull Map<Integer, SupplyChainNode> supplychain) {
       assertThat(supplychain.keySet(),
                  containsInAnyOrder(ApiEntityType.VIRTUAL_MACHINE.typeNumber(),
                                     ApiEntityType.STORAGE.typeNumber(),
                                     ApiEntityType.PHYSICAL_MACHINE.typeNumber()));

       assertThat(supplychain.get(ApiEntityType.VIRTUAL_MACHINE.typeNumber()).getConnectedProviderTypesList(),
                  containsInAnyOrder(ApiEntityType.STORAGE.typeNumber(),
                                     ApiEntityType.PHYSICAL_MACHINE.typeNumber()));
       assertThat(supplychain.get(ApiEntityType.VIRTUAL_MACHINE.typeNumber()).getConnectedConsumerTypesList(),
                  containsInAnyOrder());

       assertThat(supplychain.get(ApiEntityType.PHYSICAL_MACHINE.typeNumber())
                       .getConnectedProviderTypesList(),
                  containsInAnyOrder(ApiEntityType.STORAGE.typeNumber()));
       assertThat(supplychain.get(ApiEntityType.PHYSICAL_MACHINE.typeNumber())
                       .getConnectedConsumerTypesList(),
                  containsInAnyOrder(ApiEntityType.VIRTUAL_MACHINE.typeNumber()));

       assertThat(supplychain.get(ApiEntityType.STORAGE.typeNumber()).getConnectedConsumerTypesList(),
                  containsInAnyOrder(ApiEntityType.VIRTUAL_MACHINE.typeNumber(),
                                     ApiEntityType.PHYSICAL_MACHINE.typeNumber()));
       assertThat(supplychain.get(ApiEntityType.STORAGE.typeNumber()).getConnectedProviderTypesList(),
                  containsInAnyOrder());
   }

    /**
     * Tests that scoping on storage includes that related DC in the result
     * and does not include any other storage in the result.  Also tests
     * the reverse: scoping on DC includes related storage in the result.
     */
   @Test
   public void testDCAndStorageRelationship() {
       /*
        * Topology:
        *      PM
        *    / \ \
        *  ST1 DC ST2
        */
        final TopologyGraph<TestGraphEntity> graph =
            TestGraphEntity.newGraph(
                TestGraphEntity.newBuilder(PM_ID, ApiEntityType.PHYSICAL_MACHINE)
                    .addProviderId(ST_ID)
                    .addProviderId(DC_ID)
                    .addProviderId(ST_ID2),
               TestGraphEntity.newBuilder(ST_ID, ApiEntityType.STORAGE),
               TestGraphEntity.newBuilder(ST_ID2, ApiEntityType.STORAGE),
               TestGraphEntity.newBuilder(DC_ID, ApiEntityType.DATACENTER));

       final Map<Integer, SupplyChainNode> supplychainFromST = getSupplyChain(graph, ST_ID);
       final Map<Integer, SupplyChainNode> supplychainFromDC = getSupplyChain(graph, DC_ID);

       commonForDCAndStorageRelationship(supplychainFromDC);
       commonForDCAndStorageRelationship(supplychainFromST);

       assertThat(getAllNodeIds(supplychainFromDC.get(ApiEntityType.STORAGE.typeNumber())),
                  containsInAnyOrder(ST_ID, ST_ID2));
       assertThat(getAllNodeIds(supplychainFromST.get(ApiEntityType.STORAGE.typeNumber())),
                  containsInAnyOrder(ST_ID));
   }

    /**
     * Tests that storage does not bring irrelevant DCs, when shared.
     */
    @Test
    public void testSharedStorage() {
        /*
         * Topology:
         *   VM
         *    |
         *   PM1 PM2
         *    | \/ |
         *  DC1 ST DC2
         * Scoping on VM should not bring DC2
         */
        final long pm1Id = 1L;
        final long pm2Id = 2L;
        final long stId = 11L;
        final long dc1Id = 101L;
        final long dc2Id = 102L;
        final long vmId = 1001L;
        final TopologyGraph<TestGraphEntity> graph =
                TestGraphEntity.newGraph(
                        TestGraphEntity.newBuilder(pm1Id, ApiEntityType.PHYSICAL_MACHINE)
                                .addProviderId(stId)
                                .addProviderId(dc1Id),
                        TestGraphEntity.newBuilder(pm2Id, ApiEntityType.PHYSICAL_MACHINE)
                                .addProviderId(stId)
                                .addProviderId(dc2Id),
                        TestGraphEntity.newBuilder(stId, ApiEntityType.STORAGE),
                        TestGraphEntity.newBuilder(dc1Id, ApiEntityType.DATACENTER),
                        TestGraphEntity.newBuilder(dc2Id, ApiEntityType.DATACENTER),
                        TestGraphEntity.newBuilder(vmId, ApiEntityType.VIRTUAL_MACHINE)
                                .addProviderId(pm1Id));

        final Map<Integer, SupplyChainNode> supplyChain = getSupplyChain(graph, vmId);

        assertThat(getAllNodeIds(supplyChain.get(ApiEntityType.DATACENTER.typeNumber())),
                                 containsInAnyOrder(dc1Id));
    }

    /**
     * Tests that scoping on a storage does not bring VMs
     * that are not associated with that storage.
     */
    @Test
    public void testStorageWithoutVm() {
        /*
         * Topology:
         *   VM--
         *   |  |
         *  PM1 | PM2
         *  /  \| /
         * ST1 ST2
         * Scoping on ST1 should not bring VM
         * Scoping on VM should not bring PM2
         */
        final TopologyGraph<TestGraphEntity> graph =
            TestGraphEntity.newGraph(TestGraphEntity.newBuilder(PM_ID, ApiEntityType.PHYSICAL_MACHINE)
                                            .addProviderId(ST_ID)
                                            .addProviderId(ST_ID2),
                                     TestGraphEntity.newBuilder(PM_ID2, ApiEntityType.PHYSICAL_MACHINE)
                                            .addProviderId(ST_ID2),
                                     TestGraphEntity.newBuilder(ST_ID, ApiEntityType.STORAGE),
                                     TestGraphEntity.newBuilder(ST_ID2, ApiEntityType.STORAGE),
                                     TestGraphEntity.newBuilder(VM_ID, ApiEntityType.VIRTUAL_MACHINE)
                                            .addProviderId(PM_ID)
                                            .addProviderId(ST_ID2));

        // scoping on ST1
        final Map<Integer, SupplyChainNode> supplychain = getSupplyChain(graph, ST_ID);
        assertThat(supplychain.keySet(), containsInAnyOrder(ApiEntityType.PHYSICAL_MACHINE.typeNumber(),
                                                            ApiEntityType.STORAGE.typeNumber()));
        assertEquals(Collections.singleton(PM_ID),
                     getAllNodeIds(supplychain.get(ApiEntityType.PHYSICAL_MACHINE.typeNumber())));
        assertEquals(Collections.singleton(ST_ID),
                     getAllNodeIds(supplychain.get(ApiEntityType.STORAGE.typeNumber())));

        // scoping on VM
        final Map<Integer, SupplyChainNode> supplychain2 = getSupplyChain(graph, VM_ID);
        assertEquals(Collections.singleton(PM_ID),
                     getAllNodeIds(supplychain2.get(ApiEntityType.PHYSICAL_MACHINE.typeNumber())));
        assertEquals(Collections.singleton(ST_ID2),
                     getAllNodeIds(supplychain2.get(ApiEntityType.STORAGE.typeNumber())));
    }

    /**
     * Tests vSAN topologies, in which hosts may be providers to storage.
     * Scoping on a VM should not bring those hosts to the supply chain.
     */
    @Test
    public void testVsanTopology() {
        /*
         * Topology:
         *     VM
         *    /  \
         *  ST   PM1
         *  |
         *  PM2
         *  Scoping on VM should not include PM2 in the scope
         *  Scoping on PM2 should not include VM but should include ST
         */
        final TopologyGraph<TestGraphEntity> graph =
                TestGraphEntity.newGraph(TestGraphEntity.newBuilder(PM_ID, ApiEntityType.PHYSICAL_MACHINE),
                                         TestGraphEntity.newBuilder(PM_ID2, ApiEntityType.PHYSICAL_MACHINE),
                                         TestGraphEntity.newBuilder(ST_ID, ApiEntityType.STORAGE)
                                            .addProviderId(PM_ID2),
                                         TestGraphEntity.newBuilder(VM_ID, ApiEntityType.VIRTUAL_MACHINE)
                                            .addProviderId(PM_ID)
                                            .addProviderId(ST_ID));
        final Map<Integer, SupplyChainNode> supplychain = getSupplyChain(graph, VM_ID);
        final SupplyChainNode stNode = supplychain.get(ApiEntityType.STORAGE.typeNumber());
        final SupplyChainNode pmNode = supplychain.get(ApiEntityType.PHYSICAL_MACHINE.typeNumber());

        assertEquals(Collections.singleton(PM_ID), getAllNodeIds(pmNode));
        assertEquals(Collections.singleton(ST_ID), getAllNodeIds(stNode));

        final Map<Integer, SupplyChainNode> supplychainFromPm2 = getSupplyChain(graph, PM_ID2);
        final SupplyChainNode stNode2 = supplychainFromPm2.get(ApiEntityType.STORAGE.typeNumber());
        final SupplyChainNode pmNode2 = supplychainFromPm2.get(ApiEntityType.PHYSICAL_MACHINE.typeNumber());

        assertEquals(Collections.singleton(PM_ID2), getAllNodeIds(pmNode2));
        assertEquals(Collections.singleton(ST_ID), getAllNodeIds(stNode2));
        assertFalse(supplychainFromPm2.containsKey(ApiEntityType.VIRTUAL_MACHINE.typeNumber()));
    }
    @Test
    public void testVMSpecSeed() {
        createCloudTopologyWithScalingGroup();
        final Map<Integer, SupplyChainNode> supplyChain = getSupplyChain(cloudTopology, VMSPEC_ID);
        populateCloudEntityFields(supplyChain);
        assertFullCloudSupplyChain(supplyChain, false, true);
    }

    @Test
    public void testControlledEntitySeed() {
        createCloudTopologyWithScalingGroup();
        final Map<Integer, SupplyChainNode> supplyChain = getSupplyChain(cloudTopology, VM_1_ID);
        populateCloudEntityFields(supplyChain);
        assertFullCloudSupplyChain(supplyChain, true, true);
    }

    /**
     * Test supply chains for cloud native topology starting from each container type.  Traversal
     * coverage includes VMs, Hosts and volumes connected through our related pods.
     */
    @Test
    public void testContainerTopology() {
        /*
         *      Container -- ContainerSpec
         *          |             |
         *    ----Pod ------ WorkloadController
         *   /      |             |
         *   |   /--VM--\    Namespace
         *   |   |    \  \        |
         *   |   PM    |  \- ContainerCluster
         *   VV        ST
         *
         * Actual topology
         * - A cluster has two namespaces: demoapp and robot-shop
         * - The robot-shop namespace has two workload controllers: cart and redis
         * - The cart workload controller has two pods: 1 & 2
         * - The cart pod has two containers: cart and istio-proxy
         * - The redis pod has a single container: redis
         *
         * Onto the connectivity with the infra topology; we reuse some already defined for
         * other tests ("VM_ID", "PM_ID", "VOLUME_ID", "ST_ID") and an additional VM
         * ("ANOTHER_VM_IN_CLUSTER_ID"):
         * - The cluster has two VMs: "VM_ID" and "ANOTHER_VM_IN_CLUSTER_ID".
         * - Both cart pod 1 and the redis pod run on "VM_ID"; cart pod 2 runs on
         *   "ANOTHER_VM_IN_CLUSTER_ID".
         * - Redis pod is attached to virtual volume "VOLUME_ID"; both cart pods are not.
         * - "VM_ID" sits on "PM_ID" and also consumes from "ST_ID".
         * - How "ANOTHER_VM_IN_CLUSTER_ID" is related to other infra pieces is omitted.
         */
        final TopologyGraph<TestGraphEntity> graph = TestGraphEntity.newGraph(
                TestGraphEntity.newBuilder(PM_ID, ApiEntityType.PHYSICAL_MACHINE),
                TestGraphEntity.newBuilder(ST_ID, ApiEntityType.STORAGE),
                TestGraphEntity.newBuilder(VOLUME_ID, ApiEntityType.VIRTUAL_VOLUME),
                TestGraphEntity.newBuilder(VM_ID, ApiEntityType.VIRTUAL_MACHINE)
                    .addProviderId(ST_ID)
                    .addProviderId(PM_ID)
                    .addConnectedEntity(CONTAINER_CLUSTER_ID, ConnectionType.AGGREGATED_BY_CONNECTION),
                TestGraphEntity.newBuilder(ANOTHER_VM_IN_CLUSTER_ID, ApiEntityType.VIRTUAL_MACHINE)
                    .addConnectedEntity(CONTAINER_CLUSTER_ID, ConnectionType.AGGREGATED_BY_CONNECTION),
                TestGraphEntity.newBuilder(CONTAINER_CLUSTER_ID, ApiEntityType.CONTAINER_PLATFORM_CLUSTER),
                TestGraphEntity.newBuilder(NAMESPACE_DEMO_APP_ID, ApiEntityType.NAMESPACE)
                    .addProviderId(CONTAINER_CLUSTER_ID)
                    .addConnectedEntity(CONTAINER_CLUSTER_ID, ConnectionType.AGGREGATED_BY_CONNECTION),
                TestGraphEntity.newBuilder(NAMESPACE_ROBOT_SHOP_ID, ApiEntityType.NAMESPACE)
                    .addProviderId(CONTAINER_CLUSTER_ID)
                    .addConnectedEntity(CONTAINER_CLUSTER_ID, ConnectionType.AGGREGATED_BY_CONNECTION),
                TestGraphEntity.newBuilder(WORKLOAD_CONTROLLER_CART_ID, ApiEntityType.WORKLOAD_CONTROLLER)
                    .addProviderId(NAMESPACE_ROBOT_SHOP_ID)
                    .addConnectedEntity(NAMESPACE_ROBOT_SHOP_ID, ConnectionType.AGGREGATED_BY_CONNECTION),
                TestGraphEntity.newBuilder(WORKLOAD_CONTROLLER_REDIS_ID, ApiEntityType.WORKLOAD_CONTROLLER)
                    .addProviderId(NAMESPACE_ROBOT_SHOP_ID)
                    .addConnectedEntity(NAMESPACE_ROBOT_SHOP_ID, ConnectionType.AGGREGATED_BY_CONNECTION),
                TestGraphEntity.newBuilder(CONTAINER_POD_CART1_ID, ApiEntityType.CONTAINER_POD)
                    .addProviderId(VM_ID)
                    .addProviderId(WORKLOAD_CONTROLLER_CART_ID)
                    .addConnectedEntity(WORKLOAD_CONTROLLER_CART_ID, ConnectionType.AGGREGATED_BY_CONNECTION),
                TestGraphEntity.newBuilder(CONTAINER_POD_CART2_ID, ApiEntityType.CONTAINER_POD)
                    .addProviderId(ANOTHER_VM_IN_CLUSTER_ID)
                    .addProviderId(WORKLOAD_CONTROLLER_CART_ID)
                    .addConnectedEntity(WORKLOAD_CONTROLLER_CART_ID, ConnectionType.AGGREGATED_BY_CONNECTION),
                TestGraphEntity.newBuilder(CONTAINER_POD_REDIS1_ID, ApiEntityType.CONTAINER_POD)
                    .addProviderId(VM_ID)
                    .addProviderId(VOLUME_ID)
                    .addProviderId(WORKLOAD_CONTROLLER_REDIS_ID)
                    .addConnectedEntity(WORKLOAD_CONTROLLER_REDIS_ID, ConnectionType.AGGREGATED_BY_CONNECTION),
                TestGraphEntity.newBuilder(CONTAINER_CART1_ID, ApiEntityType.CONTAINER)
                    .addProviderId(CONTAINER_POD_CART1_ID)
                    .addConnectedEntity(CONTAINER_SPEC_CART_ID, ConnectionType.AGGREGATED_BY_CONNECTION),
                TestGraphEntity.newBuilder(CONTAINER_CART2_ID, ApiEntityType.CONTAINER)
                    .addProviderId(CONTAINER_POD_CART2_ID)
                    .addConnectedEntity(CONTAINER_SPEC_CART_ID, ConnectionType.AGGREGATED_BY_CONNECTION),
                TestGraphEntity.newBuilder(CONTAINER_ISTIO_PROXY1_ID, ApiEntityType.CONTAINER)
                    .addProviderId(CONTAINER_POD_CART1_ID)
                    .addConnectedEntity(CONTAINER_SPEC_ISTIO_PROXY_ID, ConnectionType.AGGREGATED_BY_CONNECTION),
                TestGraphEntity.newBuilder(CONTAINER_ISTIO_PROXY2_ID, ApiEntityType.CONTAINER)
                    .addProviderId(CONTAINER_POD_CART2_ID)
                    .addConnectedEntity(CONTAINER_SPEC_ISTIO_PROXY_ID, ConnectionType.AGGREGATED_BY_CONNECTION),
                TestGraphEntity.newBuilder(CONTAINER_CART1_ID, ApiEntityType.CONTAINER)
                    .addProviderId(CONTAINER_POD_CART1_ID)
                    .addConnectedEntity(CONTAINER_SPEC_CART_ID, ConnectionType.AGGREGATED_BY_CONNECTION),
                TestGraphEntity.newBuilder(CONTAINER_CART2_ID, ApiEntityType.CONTAINER)
                    .addProviderId(CONTAINER_POD_CART2_ID)
                    .addConnectedEntity(CONTAINER_SPEC_CART_ID, ConnectionType.AGGREGATED_BY_CONNECTION),
                TestGraphEntity.newBuilder(CONTAINER_ISTIO_PROXY1_ID, ApiEntityType.CONTAINER)
                    .addProviderId(CONTAINER_POD_CART1_ID)
                    .addConnectedEntity(CONTAINER_SPEC_ISTIO_PROXY_ID, ConnectionType.AGGREGATED_BY_CONNECTION),
                TestGraphEntity.newBuilder(CONTAINER_ISTIO_PROXY2_ID, ApiEntityType.CONTAINER)
                    .addProviderId(CONTAINER_POD_CART2_ID)
                    .addConnectedEntity(CONTAINER_SPEC_ISTIO_PROXY_ID, ConnectionType.AGGREGATED_BY_CONNECTION),
                TestGraphEntity.newBuilder(CONTAINER_REDIS1_ID, ApiEntityType.CONTAINER)
                    .addProviderId(CONTAINER_POD_REDIS1_ID)
                    .addConnectedEntity(CONTAINER_SPEC_REDIS_ID, ConnectionType.AGGREGATED_BY_CONNECTION),
                TestGraphEntity.newBuilder(CONTAINER_SPEC_CART_ID, ApiEntityType.CONTAINER_SPEC)
                    .addConnectedEntity(WORKLOAD_CONTROLLER_CART_ID, ConnectionType.CONTROLLED_BY_CONNECTION),
                TestGraphEntity.newBuilder(CONTAINER_SPEC_ISTIO_PROXY_ID, ApiEntityType.CONTAINER_SPEC)
                    .addConnectedEntity(WORKLOAD_CONTROLLER_CART_ID, ConnectionType.CONTROLLED_BY_CONNECTION),
                TestGraphEntity.newBuilder(CONTAINER_SPEC_REDIS_ID, ApiEntityType.CONTAINER_SPEC)
                    .addConnectedEntity(WORKLOAD_CONTROLLER_REDIS_ID, ConnectionType.CONTROLLED_BY_CONNECTION)
            );

        // Cart scenarios (without volume)
        // Container cart1 as seed
        final Collection<Long> cart1ContainerChain = ImmutableSet.of(CONTAINER_CLUSTER_ID,
                NAMESPACE_ROBOT_SHOP_ID, WORKLOAD_CONTROLLER_CART_ID, CONTAINER_SPEC_CART_ID,
                CONTAINER_CART1_ID, CONTAINER_POD_CART1_ID, VM_ID, PM_ID, ST_ID);
        assertEquals(cart1ContainerChain, getAllNodeIds(getSupplyChain(graph, CONTAINER_CART1_ID)));
        // ContainerSpec cart as seed
        final Collection<Long> cartSpecChain = ImmutableSet.<Long>builder()
                .addAll(cart1ContainerChain).add(CONTAINER_CART2_ID).add(CONTAINER_POD_CART2_ID)
                .add(ANOTHER_VM_IN_CLUSTER_ID).build();
        assertEquals(cartSpecChain, getAllNodeIds(getSupplyChain(graph, CONTAINER_SPEC_CART_ID)));
        // ContainerPod cart1 as seed
        final Collection<Long> cart1PodChain = ImmutableSet.<Long>builder()
                .addAll(cart1ContainerChain).add(CONTAINER_ISTIO_PROXY1_ID)
                .add(CONTAINER_SPEC_ISTIO_PROXY_ID).build();
        assertEquals(cart1PodChain, getAllNodeIds(getSupplyChain(graph, CONTAINER_POD_CART1_ID)));
        // WorkloadController cart as seed
        final Collection<Long> cartControllerChain = ImmutableSet.<Long>builder()
                .addAll(cartSpecChain).add(CONTAINER_ISTIO_PROXY1_ID).add(CONTAINER_ISTIO_PROXY2_ID)
                .add(CONTAINER_SPEC_ISTIO_PROXY_ID).build();
        assertEquals(cartControllerChain, getAllNodeIds(getSupplyChain(graph, WORKLOAD_CONTROLLER_CART_ID)));

        // Redis scenarios (with volume)
        // Container redis as seed
        final Collection<Long> redis1ContainerChain = ImmutableSet.of(CONTAINER_CLUSTER_ID,
                NAMESPACE_ROBOT_SHOP_ID, WORKLOAD_CONTROLLER_REDIS_ID, CONTAINER_SPEC_REDIS_ID,
                CONTAINER_REDIS1_ID, CONTAINER_POD_REDIS1_ID, VM_ID, PM_ID, ST_ID, VOLUME_ID);
        assertEquals(redis1ContainerChain, getAllNodeIds(getSupplyChain(graph, CONTAINER_REDIS1_ID)));
        // ContainerSpec redis as seed
        final Collection<Long> redisSpecChain = redis1ContainerChain;
        assertEquals(redisSpecChain, getAllNodeIds(getSupplyChain(graph, CONTAINER_SPEC_REDIS_ID)));
        // ContainerPod redis as seed
        final Collection<Long> redis1PodChain = redis1ContainerChain;
        assertEquals(redis1PodChain, getAllNodeIds(getSupplyChain(graph, CONTAINER_POD_REDIS1_ID)));
        // WorkloadController redis as seed
        final Collection<Long> redisControllerChain = redis1ContainerChain;
        assertEquals(redisControllerChain, getAllNodeIds(getSupplyChain(graph, WORKLOAD_CONTROLLER_REDIS_ID)));

        // Robot-shop namespace
        final Collection<Long> robotShopNamespaceChain = ImmutableSet.<Long>builder()
                .addAll(cartControllerChain).addAll(redisControllerChain).build();
        assertEquals(robotShopNamespaceChain, getAllNodeIds(getSupplyChain(graph, NAMESPACE_ROBOT_SHOP_ID)));

        // Demo app namespace
        final Collection<Long> demoAppNamespaceChain = ImmutableSet.of(NAMESPACE_DEMO_APP_ID, CONTAINER_CLUSTER_ID);
        assertEquals(demoAppNamespaceChain, getAllNodeIds(getSupplyChain(graph, NAMESPACE_DEMO_APP_ID)));

        // ContainerCluster as seed
        final Collection<Long> clusterChain = ImmutableSet.<Long>builder()
                .addAll(robotShopNamespaceChain).addAll(demoAppNamespaceChain).build();
        assertEquals(clusterChain, getAllNodeIds(getSupplyChain(graph, CONTAINER_CLUSTER_ID)));
    }

    /**
     * Test cloud native topology starting from VMs to include volumes in
     * an unstitched env. When unstitched the volumes are reported only as
     * pods providers.
     */
    @Test
    public void testVMToVolumesUnstitched() {
        /*
         *  *  Topology:
         *
         *      Pod1    Pod2
         *    /   \   /   \
         *   /     \ /     \
         * Vol1    VM1    Vol2
         *
         */

        final long pod1Id = 1;
        final long pod2Id = 2;
        final long vm1Id = 11;
        final long vol1Id = 21;
        final long vol2Id = 22;


        final TopologyGraph<TestGraphEntity> graph =
                TestGraphEntity.newGraph(
                        TestGraphEntity.newBuilder(pod1Id, ApiEntityType.CONTAINER_POD)
                                .addProviderId(vm1Id)
                                .addProviderId(vol1Id),
                        TestGraphEntity.newBuilder(pod2Id, ApiEntityType.CONTAINER_POD)
                                .addProviderId(vm1Id)
                                .addProviderId(vol2Id),
                        TestGraphEntity.newBuilder(vm1Id, ApiEntityType.VIRTUAL_MACHINE),
                        TestGraphEntity.newBuilder(vol1Id, ApiEntityType.VIRTUAL_VOLUME),
                        TestGraphEntity.newBuilder(vol2Id, ApiEntityType.VIRTUAL_VOLUME));

        // 1.
        Map<Integer, SupplyChainNode> supplychain = getSupplyChain(graph, vm1Id);

        assertThat(getAllNodeIds(supplychain.get(ApiEntityType.VIRTUAL_VOLUME
                .typeNumber())), containsInAnyOrder(vol1Id, vol2Id));

    }

    /**
     * Test cloud native topology starting from Pods which consume volumes.
     * Also test the traversal from volumes which provide for pods going up
     * the supply chain.
     */
    @Test
    public void testVolumeToPods() {
        /*
         *  *  Topology:
         *      Pod3
         *       |
         *  Pod1 |  Pod2
         *   | \ | /   \
         *   |  \|/     \
         *   |  VM1______Vol2
         *   |  /|  \
         *   | / |\  \
         *   |/  | \  \
         * Vol1  |  \  \
         *       | Vol3 \
         *       |    \  \
         *       PM1   \  |
         *              Zone1
         *
         * Vol1 is provider to both Pod1 and VM1 and not Pod2
         * Vol2 is provider to both Pod3 and VM1 and not Pod1.
         * Vol3 is provider to only VM1.
         * Pod3 has no volume directly attached.
         * Assumption: If a volume is attached to a pod, its also
         * always attached to the pods provider VM.
         */
        final long pod1Id = 1;
        final long pod2Id = 2;
        final long pod3Id = 3;
        final long vm1Id = 11;
        final long vol1Id = 21;
        final long vol2Id = 22;
        final long vol3Id = 23;
        final long pm1Id = 31;
        final long zone1Id = 41;

        final TopologyGraph<TestGraphEntity> graph =
                TestGraphEntity.newGraph(
                        TestGraphEntity.newBuilder(pod1Id, ApiEntityType.CONTAINER_POD)
                                .addProviderId(vm1Id)
                                .addProviderId(vol1Id),
                        TestGraphEntity.newBuilder(pod2Id, ApiEntityType.CONTAINER_POD)
                                .addProviderId(vm1Id)
                                .addProviderId(vol2Id),
                        TestGraphEntity.newBuilder(pod3Id, ApiEntityType.CONTAINER_POD)
                                .addProviderId(vm1Id),
                        TestGraphEntity.newBuilder(vm1Id, ApiEntityType.VIRTUAL_MACHINE)
                                .addProviderId(pm1Id)
                                .addProviderId(vol1Id)
                                .addProviderId(vol2Id)
                                .addProviderId(vol3Id)
                                .addProviderId(zone1Id),
                        TestGraphEntity.newBuilder(pm1Id, ApiEntityType.PHYSICAL_MACHINE),
                        TestGraphEntity.newBuilder(vol1Id, ApiEntityType.VIRTUAL_VOLUME),
                        TestGraphEntity.newBuilder(vol2Id, ApiEntityType.VIRTUAL_VOLUME),
                        TestGraphEntity.newBuilder(vol3Id, ApiEntityType.VIRTUAL_VOLUME)
                                .addProviderId(zone1Id),
                        TestGraphEntity.newBuilder(zone1Id, ApiEntityType.AVAILABILITY_ZONE));

        // 1.
        Map<Integer, SupplyChainNode> supplychain = getSupplyChain(graph, pod1Id);
        // 1a. Ensure nodes are reachable traversing down from pod as seed
        assertEquals(Collections.singleton(vm1Id), getAllNodeIds(supplychain
                .get(ApiEntityType.VIRTUAL_MACHINE.typeNumber())));
        assertEquals(Collections.singleton(pm1Id), getAllNodeIds(supplychain
                .get(ApiEntityType.PHYSICAL_MACHINE.typeNumber())));
        assertEquals(Collections.singleton(zone1Id), getAllNodeIds(supplychain
                .get(ApiEntityType.AVAILABILITY_ZONE.typeNumber())));
        // Pod1 as seed
        // 1b. selects vol1, its direct provider
        // 1c. selects vol3 i.e. its vms direct provider
        // 1d. and does not select vol2 (pod 2's provider)
        assertThat(getAllNodeIds(supplychain.get(ApiEntityType.VIRTUAL_VOLUME
                .typeNumber())), containsInAnyOrder(vol1Id, vol3Id));

        // 2.
        supplychain = getSupplyChain(graph, pod3Id);
        // Pod3 as seed (which does not use any volumes directly)
        // 2a. selects only vol3, i.e. the provider vms direct provider
        assertThat(getAllNodeIds(supplychain.get(ApiEntityType.VIRTUAL_VOLUME
                .typeNumber())), containsInAnyOrder(vol3Id));

        // 3.
        supplychain = getSupplyChain(graph, vol1Id);
        // Vol1 as seed, which is connected to pod1 apart from vm1
        // 3a. selects only the pods (pod1), which use this volume directly
        assertThat(getAllNodeIds(supplychain.get(ApiEntityType.CONTAINER_POD
                .typeNumber())), containsInAnyOrder(pod1Id));

        // 4.
        supplychain = getSupplyChain(graph, vol3Id);
        // Vol3 as seed, which is connected only to vm1
        // 4a. selects all pods
        assertThat(getAllNodeIds(supplychain.get(ApiEntityType.CONTAINER_POD
                .typeNumber())), containsInAnyOrder(pod1Id, pod2Id, pod3Id));
    }

    /**
     * Tests the behavior of arrows. If two entity types are joined
     * in the full topology, but not in the generated scoped supply
     * chain, then there should be no arrow between them in the
     * generated supply chain.
     */
    @Test
    public void testArrows() {
        /*
         * Topology, same as in the vSAN test:
         *     VM
         *    /  \
         *  ST   PM1
         *  |
         *  PM2
         *  Scoping on VM: Since PM2 is not included, there should be
         *  no arrow from the PMs node to the Storage node in the result.
         */
        final TopologyGraph<TestGraphEntity> graph =
                TestGraphEntity.newGraph(TestGraphEntity.newBuilder(PM_ID, ApiEntityType.PHYSICAL_MACHINE),
                        TestGraphEntity.newBuilder(PM_ID2, ApiEntityType.PHYSICAL_MACHINE),
                        TestGraphEntity.newBuilder(ST_ID, ApiEntityType.STORAGE)
                                .addProviderId(PM_ID2),
                        TestGraphEntity.newBuilder(VM_ID, ApiEntityType.VIRTUAL_MACHINE)
                                .addProviderId(PM_ID)
                                .addProviderId(ST_ID));
        final Map<Integer, SupplyChainNode> supplychain = getSupplyChain(graph, VM_ID);
        final SupplyChainNode stNode = supplychain.get(ApiEntityType.STORAGE.typeNumber());
        final SupplyChainNode pmNode = supplychain.get(ApiEntityType.PHYSICAL_MACHINE.typeNumber());

        assertEquals(Collections.singletonList(ApiEntityType.VIRTUAL_MACHINE.typeNumber()),
                pmNode.getConnectedConsumerTypesList());
        assertEquals(Collections.emptyList(), stNode.getConnectedProviderTypesList());
    }

   private void commonForDCAndStorageRelationship(@Nonnull Map<Integer, SupplyChainNode> supplychain) {
       assertThat(supplychain.keySet(),
                  containsInAnyOrder(ApiEntityType.DATACENTER.typeNumber(),
                                     ApiEntityType.STORAGE.typeNumber(),
                                     ApiEntityType.PHYSICAL_MACHINE.typeNumber()));

       final SupplyChainNode pmNode = supplychain.get(ApiEntityType.PHYSICAL_MACHINE.typeNumber());
       final SupplyChainNode stNode = supplychain.get(ApiEntityType.STORAGE.typeNumber());
       final SupplyChainNode dcNode = supplychain.get(ApiEntityType.DATACENTER.typeNumber());

       assertThat(pmNode.getConnectedProviderTypesList(),
                  containsInAnyOrder(ApiEntityType.DATACENTER.typeNumber(),
                                     ApiEntityType.STORAGE.typeNumber()));
       assertThat(pmNode.getConnectedConsumerTypesList(), containsInAnyOrder());

       assertThat(dcNode.getConnectedConsumerTypesList(),
                  containsInAnyOrder(ApiEntityType.PHYSICAL_MACHINE.typeNumber()));
       assertThat(dcNode.getConnectedProviderTypesList(), containsInAnyOrder());

       assertThat(stNode.getConnectedConsumerTypesList(),
                  containsInAnyOrder(ApiEntityType.PHYSICAL_MACHINE.typeNumber()));
       assertThat(stNode.getConnectedProviderTypesList(), containsInAnyOrder());

       assertThat(getAllNodeIds(pmNode), containsInAnyOrder(PM_ID));
       assertThat(getAllNodeIds(dcNode), containsInAnyOrder(DC_ID));
   }

    /**
     * Test the cloud topology, scoping on region.
     */
   @Test
   public void testRegionScope() {
       createCloudTopology();
       final Map<Integer, SupplyChainNode> supplyChain = getSupplyChain(cloudTopology, REGION_ID);
       populateCloudEntityFields(supplyChain);
       assertFullCloudSupplyChain(supplyChain, false, false);
   }

    /**
     * Test the cloud topology, scoping on zone.
     */
    @Test
    public void testZoneScope() {
        createCloudTopology();
        final Map<Integer, SupplyChainNode> supplyChain = getSupplyChain(cloudTopology, ZONE_ID);
        populateCloudEntityFields(supplyChain);
        assertFullCloudSupplyChain(supplyChain, false, false);
    }

    /**
     * Test the cloud topology, scoping on VM1.
     */
    @Test
    public void testVM1Scope() {
        createCloudTopology();
        final Map<Integer, SupplyChainNode> supplyChain = getSupplyChain(cloudTopology, VM_1_ID);
        populateCloudEntityFields(supplyChain);
        assertFullCloudSupplyChain(supplyChain, true, false);
    }

    /**
     * Test the cloud topology, scoping on account.
     */
    @Test
    public void testAccountScope() {
        createCloudTopology();
        final Map<Integer, SupplyChainNode> supplyChain = getSupplyChain(cloudTopology, ACCOUNT_ID);
        populateCloudEntityFields(supplyChain);
        assertFullCloudSupplyChain(supplyChain, true, false);
    }

    /**
     * Test the cloud topology, scoping on volume.
     */
    @Test
    public void testVolumeScope() {
        createCloudTopology();
        final Map<Integer, SupplyChainNode> supplyChain = getSupplyChain(cloudTopology, VOLUME_ID);
        populateCloudEntityFields(supplyChain);
        assertFullCloudSupplyChain(supplyChain, true, false);
    }

    /**
     * Test the cloud topology, scoping on the application.
     */
    @Test
    public void testAppScope() {
        createCloudTopology();
        final Map<Integer, SupplyChainNode> supplyChain = getSupplyChain(cloudTopology, APP_ID);
        populateCloudEntityFields(supplyChain);
        assertFullCloudSupplyChain(supplyChain, true, false);
    }

    /**
     * Test the cloud topology, scoping on VM1.
     */
    @Test
    public void testVM2Scope() {
        createCloudTopology();
        final Map<Integer, SupplyChainNode> supplyChain = getSupplyChain(cloudTopology, VM_2_ID);
        populateCloudEntityFields(supplyChain);

        assertThat(supplyChain.keySet(),
                   containsInAnyOrder(ApiEntityType.REGION.typeNumber(),
                                      ApiEntityType.AVAILABILITY_ZONE.typeNumber(),
                                      ApiEntityType.VIRTUAL_MACHINE.typeNumber()));

        assertThat(getAllNodeIds(region), containsInAnyOrder(REGION_ID));
        assertThat(getAllNodeIds(zone), containsInAnyOrder(ZONE_ID));
        assertThat(getAllNodeIds(vm), containsInAnyOrder(VM_2_ID));

        assertThat(region.getConnectedConsumerTypesList(),
                   containsInAnyOrder(ApiEntityType.AVAILABILITY_ZONE.typeNumber()));
        assertThat(region.getConnectedProviderTypesList(), containsInAnyOrder());

        assertThat(zone.getConnectedConsumerTypesList(),
                   containsInAnyOrder(ApiEntityType.VIRTUAL_MACHINE.typeNumber()));
        assertThat(zone.getConnectedProviderTypesList(), containsInAnyOrder(ApiEntityType.REGION.typeNumber()));

        assertThat(vm.getConnectedConsumerTypesList(), containsInAnyOrder());
        assertThat(vm.getConnectedProviderTypesList(),
                   containsInAnyOrder(ApiEntityType.AVAILABILITY_ZONE.typeNumber()));
    }

   /*
    * Topology
    * ACCOUNT -owns-> APP, VM1, VOLUME
    * APP -consumes-> VM1
    * VM1 -connects to-> VOLUME
    * REGION -owns-> ZONE
    * ZONE -aggregates-> VM1, VM2, VOLUME
    * VM1, VM2 -consumes-> ZONE
    */
   private void createCloudTopology() {
       // entities
       final TestGraphEntity.Builder volumeBuilder =
               TestGraphEntity.newBuilder(VOLUME_ID, ApiEntityType.VIRTUAL_VOLUME);
       final TestGraphEntity.Builder vm1Builder =
               TestGraphEntity.newBuilder(VM_1_ID, ApiEntityType.VIRTUAL_MACHINE);
       final TestGraphEntity.Builder vm2Builder =
               TestGraphEntity.newBuilder(VM_2_ID, ApiEntityType.VIRTUAL_MACHINE);
       final TestGraphEntity.Builder appBuilder =
               TestGraphEntity.newBuilder(APP_ID, ApiEntityType.APPLICATION);
       final TestGraphEntity.Builder zoneBuilder =
               TestGraphEntity.newBuilder(ZONE_ID, ApiEntityType.AVAILABILITY_ZONE);
       final TestGraphEntity.Builder regionBuilder =
               TestGraphEntity.newBuilder(REGION_ID, ApiEntityType.REGION);
       final TestGraphEntity.Builder accountBuilder =
               TestGraphEntity.newBuilder(ACCOUNT_ID, ApiEntityType.BUSINESS_ACCOUNT);

       // ACCOUNT -owns-> APP, VM1, VOLUME
       accountBuilder.addConnectedEntity(VM_1_ID, ConnectionType.OWNS_CONNECTION);
       accountBuilder.addConnectedEntity(VOLUME_ID, ConnectionType.OWNS_CONNECTION);
       accountBuilder.addConnectedEntity(APP_ID, ConnectionType.OWNS_CONNECTION);

       // APP -consumes-> VM1
       appBuilder.addProviderId(VM_1_ID);

       // VM1 -connects to-> VOLUME
       vm1Builder.addConnectedEntity(VOLUME_ID, ConnectionType.NORMAL_CONNECTION);

       // REGION -owns-> ZONE
       regionBuilder.addConnectedEntity(ZONE_ID, ConnectionType.OWNS_CONNECTION);

       // ZONE -aggregates-> VM1, VM2, VOLUME
       vm1Builder.addConnectedEntity(ZONE_ID, ConnectionType.AGGREGATED_BY_CONNECTION);
       vm2Builder.addConnectedEntity(ZONE_ID, ConnectionType.AGGREGATED_BY_CONNECTION);
       volumeBuilder.addConnectedEntity(ZONE_ID, ConnectionType.AGGREGATED_BY_CONNECTION);

       // VM1, VM2 -consumes-> ZONE
       vm1Builder.addProviderId(ZONE_ID);
       vm2Builder.addProviderId(ZONE_ID);

       cloudTopology =
           TestGraphEntity.newGraph(vm1Builder, vm2Builder, volumeBuilder, appBuilder,
                                    zoneBuilder, regionBuilder, accountBuilder);
   }

    /*
     * Topology
     * ACCOUNT -owns-> APP, VM1, VOLUME
     * APP -consumes-> VM1
     * VM1 -connects to-> VOLUME
     * REGION -owns-> ZONE
     * ZONE -aggregates-> VM1, VM2, VOLUME
     * VM1, VM2 -consumes-> ZONE
     * VMSpec -controls-> VM1, VM2
     */
    private void createCloudTopologyWithScalingGroup() {
        // entities
        final TestGraphEntity.Builder volumeBuilder =
            TestGraphEntity.newBuilder(VOLUME_ID, ApiEntityType.VIRTUAL_VOLUME);
        final TestGraphEntity.Builder vm1Builder =
            TestGraphEntity.newBuilder(VM_1_ID, ApiEntityType.VIRTUAL_MACHINE);
        final TestGraphEntity.Builder vm2Builder =
            TestGraphEntity.newBuilder(VM_2_ID, ApiEntityType.VIRTUAL_MACHINE);
        final TestGraphEntity.Builder appBuilder =
            TestGraphEntity.newBuilder(APP_ID, ApiEntityType.APPLICATION);
        final TestGraphEntity.Builder zoneBuilder =
            TestGraphEntity.newBuilder(ZONE_ID, ApiEntityType.AVAILABILITY_ZONE);
        final TestGraphEntity.Builder regionBuilder =
            TestGraphEntity.newBuilder(REGION_ID, ApiEntityType.REGION);
        final TestGraphEntity.Builder accountBuilder =
            TestGraphEntity.newBuilder(ACCOUNT_ID, ApiEntityType.BUSINESS_ACCOUNT);
        final TestGraphEntity.Builder vmspecBuilder =
            TestGraphEntity.newBuilder(VMSPEC_ID, ApiEntityType.VM_SPEC);
        // ACCOUNT -owns-> APP, VM1, VOLUME
        accountBuilder.addConnectedEntity(VM_1_ID, ConnectionType.OWNS_CONNECTION);
        accountBuilder.addConnectedEntity(VOLUME_ID, ConnectionType.OWNS_CONNECTION);
        accountBuilder.addConnectedEntity(APP_ID, ConnectionType.OWNS_CONNECTION);

        // APP -consumes-> VM1
        appBuilder.addProviderId(VM_1_ID);

        // VM1 -connects to-> VOLUME
        vm1Builder.addConnectedEntity(VOLUME_ID, ConnectionType.NORMAL_CONNECTION);

        // REGION -owns-> ZONE
        regionBuilder.addConnectedEntity(ZONE_ID, ConnectionType.OWNS_CONNECTION);

        // ZONE -aggregates-> VM1, VM2, VOLUME
        vm1Builder.addConnectedEntity(ZONE_ID, ConnectionType.AGGREGATED_BY_CONNECTION);
        vm2Builder.addConnectedEntity(ZONE_ID, ConnectionType.AGGREGATED_BY_CONNECTION);
        volumeBuilder.addConnectedEntity(ZONE_ID, ConnectionType.AGGREGATED_BY_CONNECTION);

        // VM1, VM2 -consumes-> ZONE
        vm1Builder.addProviderId(ZONE_ID);
        vm2Builder.addProviderId(ZONE_ID);

        //VM1, VM2 -controlledBy-> VMSpec
        vm1Builder.addConnectedEntity(VMSPEC_ID, ConnectionType.CONTROLLED_BY_CONNECTION);
        vm2Builder.addConnectedEntity(VMSPEC_ID, ConnectionType.CONTROLLED_BY_CONNECTION);

        cloudTopology =
            TestGraphEntity.newGraph(vm1Builder, vm2Builder, volumeBuilder, appBuilder,
                zoneBuilder, regionBuilder, accountBuilder, vmspecBuilder);
    }

   private void assertFullCloudSupplyChain(
           @Nonnull Map<Integer, SupplyChainNode> supplyChain, boolean excludeVM2, boolean containsControls) {
        if (containsControls) {
            assertThat(supplyChain.keySet(),
                containsInAnyOrder(ApiEntityType.REGION.typeNumber(),
                    ApiEntityType.AVAILABILITY_ZONE.typeNumber(),
                    ApiEntityType.VM_SPEC.typeNumber(),
                    ApiEntityType.VIRTUAL_MACHINE.typeNumber(),
                    ApiEntityType.VIRTUAL_VOLUME.typeNumber(),
                    ApiEntityType.BUSINESS_ACCOUNT.typeNumber(),
                    ApiEntityType.APPLICATION.typeNumber()));
            assertThat(vm.getConnectedProviderTypesList(),
                containsInAnyOrder(ApiEntityType.VIRTUAL_VOLUME.typeNumber(),
                    ApiEntityType.AVAILABILITY_ZONE.typeNumber(),
                    ApiEntityType.VM_SPEC.typeNumber(),
                    ApiEntityType.BUSINESS_ACCOUNT.typeNumber()));

        } else {
            assertThat(supplyChain.keySet(),
                containsInAnyOrder(ApiEntityType.REGION.typeNumber(),
                    ApiEntityType.AVAILABILITY_ZONE.typeNumber(),
                    ApiEntityType.VIRTUAL_MACHINE.typeNumber(),
                    ApiEntityType.VIRTUAL_VOLUME.typeNumber(),
                    ApiEntityType.BUSINESS_ACCOUNT.typeNumber(),
                    ApiEntityType.APPLICATION.typeNumber()));
            assertThat(vm.getConnectedProviderTypesList(),
                containsInAnyOrder(ApiEntityType.VIRTUAL_VOLUME.typeNumber(),
                    ApiEntityType.AVAILABILITY_ZONE.typeNumber(),
                    ApiEntityType.BUSINESS_ACCOUNT.typeNumber()));
        }

       assertThat(getAllNodeIds(region), containsInAnyOrder(REGION_ID));
       assertThat(getAllNodeIds(zone), containsInAnyOrder(ZONE_ID));
       if (excludeVM2) {
           assertThat(getAllNodeIds(vm), containsInAnyOrder(VM_1_ID));
       } else {
           assertThat(getAllNodeIds(vm), containsInAnyOrder(VM_1_ID, VM_2_ID));
       }
       assertThat(getAllNodeIds(volume), containsInAnyOrder(VOLUME_ID));
       assertThat(getAllNodeIds(account), containsInAnyOrder(ACCOUNT_ID));
       assertThat(getAllNodeIds(app), containsInAnyOrder(APP_ID));

       assertThat(region.getConnectedConsumerTypesList(),
                  containsInAnyOrder(ApiEntityType.AVAILABILITY_ZONE.typeNumber()));
       assertThat(region.getConnectedProviderTypesList(), containsInAnyOrder());

       assertThat(zone.getConnectedConsumerTypesList(),
                  containsInAnyOrder(ApiEntityType.VIRTUAL_MACHINE.typeNumber(),
                                     ApiEntityType.VIRTUAL_VOLUME.typeNumber()));
       assertThat(zone.getConnectedProviderTypesList(),
                  containsInAnyOrder(ApiEntityType.REGION.typeNumber()));

       assertThat(vm.getConnectedConsumerTypesList(),
                  containsInAnyOrder(ApiEntityType.APPLICATION.typeNumber()));

       assertThat(volume.getConnectedConsumerTypesList(),
                  containsInAnyOrder(ApiEntityType.VIRTUAL_MACHINE.typeNumber()));
       assertThat(volume.getConnectedProviderTypesList(),
                  containsInAnyOrder(ApiEntityType.AVAILABILITY_ZONE.typeNumber(),
                                     ApiEntityType.BUSINESS_ACCOUNT.typeNumber()));

       assertThat(account.getConnectedProviderTypesList(), containsInAnyOrder());
       assertThat(account.getConnectedConsumerTypesList(),
                  containsInAnyOrder(ApiEntityType.VIRTUAL_MACHINE.typeNumber(),
                                     ApiEntityType.VIRTUAL_VOLUME.typeNumber(),
                                     ApiEntityType.APPLICATION.typeNumber()));

       assertThat(app.getConnectedConsumerTypesList(), containsInAnyOrder());
       assertThat(app.getConnectedProviderTypesList(),
                  containsInAnyOrder(ApiEntityType.VIRTUAL_MACHINE.typeNumber(),
                                     ApiEntityType.BUSINESS_ACCOUNT.typeNumber()));
   }

   private void populateCloudEntityFields(@Nonnull Map<Integer, SupplyChainNode> graph) {
       region = graph.get(ApiEntityType.REGION.typeNumber());
       zone = graph.get(ApiEntityType.AVAILABILITY_ZONE.typeNumber());
       vm = graph.get(ApiEntityType.VIRTUAL_MACHINE.typeNumber());
       volume = graph.get(ApiEntityType.VIRTUAL_VOLUME.typeNumber());
       account = graph.get(ApiEntityType.BUSINESS_ACCOUNT.typeNumber());
       app = graph.get(ApiEntityType.APPLICATION.typeNumber());
       vmspec = graph.get(ApiEntityType.VM_SPEC.typeNumber());
   }

    private Collection<Long> getAllNodeIds(@Nonnull SupplyChainNode supplyChainNode) {
        return supplyChainNode.getMembersByStateMap().values().stream()
                .flatMap(m -> m.getMemberOidsList().stream())
                .collect(Collectors.toSet());
    }

    private Collection<Long> getAllNodeIds(@Nonnull Map<Integer, SupplyChainNode> supplyChain) {
        return supplyChain.values().stream()
                .flatMap(node -> getAllNodeIds(node).stream())
                .collect(Collectors.toSet());
    }

    /**
     * Tests the case where nodes in the seed do not satisfy the entity predicate.
     * The resulting supply chain should be empty.
     */
    @Test
    public void testStartingPointNoMatchPredicate() {
        final TopologyGraph<TestGraphEntity> graph =
            TestGraphEntity.newGraph(
                TestGraphEntity.newBuilder(444L, ApiEntityType.PHYSICAL_MACHINE),
                TestGraphEntity.newBuilder(33L, ApiEntityType.VIRTUAL_MACHINE) // <-- starting point
                    .addProviderId(444L));

        final Map<Integer, SupplyChainNode> supplychain = getSupplyChain(graph, 444L,
                                                                         e -> e.getOid() != 444L);

        assertTrue(supplychain.isEmpty());
    }

    /**
     * Test that the entity filter completely stops the traversal.
     */
    @Test
    public void testIgnoreNoMatchPredicateSubtree() {
        /*
         * App <-- starting point
         * |
         * VM <-- doesn't match predicate
         * |
         * PM <-- should not be in the result
         */
        final TopologyGraph<TestGraphEntity> graph =
            TestGraphEntity.newGraph(
                TestGraphEntity.newBuilder(444L, ApiEntityType.PHYSICAL_MACHINE),
                TestGraphEntity.newBuilder(33L, ApiEntityType.VIRTUAL_MACHINE)
                    .addProviderId(444L),
                TestGraphEntity.newBuilder(1L, ApiEntityType.APPLICATION) // <-- starting point
                    .addProviderId(33L));

        final Map<Integer, SupplyChainNode> supplychain = getSupplyChain(graph, 1L,
                                                                         e -> e.getOid() != 33L);

        // Only the VM is in the supply chain. The PM didn't match the predicate, and the ST is
        // only accessible from the PM.
        assertThat(supplychain.keySet(), containsInAnyOrder(ApiEntityType.APPLICATION.typeNumber()));
    }

    /**
     * Test multiple starting points: result should be the union of
     * individual results.
     */
    @Test
    public void testMultiStartingPoints() {
        /*
         * VM1  VM2 <-- starting points
         *  |    |
         * PM1  PM2
         *
         * The supply chain should contain all entities.
         */
       final TopologyGraph<TestGraphEntity> graph =
           TestGraphEntity.newGraph(
                TestGraphEntity.newBuilder(33L, ApiEntityType.PHYSICAL_MACHINE),
                TestGraphEntity.newBuilder(22L, ApiEntityType.PHYSICAL_MACHINE),
                TestGraphEntity.newBuilder(2L, ApiEntityType.VIRTUAL_MACHINE) // <-- starting point
                    .addProviderId(33L),
                TestGraphEntity.newBuilder(1L, ApiEntityType.VIRTUAL_MACHINE) // <-- starting point
                    .addProviderId(22L));

        final Map<Integer, SupplyChainNode> supplychain = getSupplyChain(graph, ImmutableSet.of(1L, 2L));

        assertThat(supplychain.keySet(),
                   containsInAnyOrder(ApiEntityType.VIRTUAL_MACHINE.typeNumber(),
                                      ApiEntityType.PHYSICAL_MACHINE.typeNumber()));
        assertThat(RepositoryDTOUtil.getMemberCount(supplychain.get(ApiEntityType.VIRTUAL_MACHINE.typeNumber())),
                                                    is(2));
        assertThat(RepositoryDTOUtil.getMemberCount(supplychain.get(ApiEntityType.PHYSICAL_MACHINE.typeNumber())),
                                                    is(2));

        assertThat(supplychain.get(ApiEntityType.PHYSICAL_MACHINE.typeNumber()).getConnectedConsumerTypesList(),
                                   containsInAnyOrder(ApiEntityType.VIRTUAL_MACHINE.typeNumber()));
        assertThat(supplychain.get(ApiEntityType.VIRTUAL_MACHINE.typeNumber()).getConnectedProviderTypesList(),
                                   containsInAnyOrder(ApiEntityType.PHYSICAL_MACHINE.typeNumber()));
    }

    /**
     * Test starting points with multiple entity types:
     * result should be the union of individual results.
     */
    @Test
    public void testMultiTypeStartingPoints() {
        /*
         *                    VM1  VM2 <-- starting point
         *                     |    |
         * starting point --> PM1  PM2
         *
         * The supply chain should contain all entities.
         */
        final TopologyGraph<TestGraphEntity> graph =
            TestGraphEntity.newGraph(
                TestGraphEntity.newBuilder(33L, ApiEntityType.PHYSICAL_MACHINE),
                TestGraphEntity.newBuilder(22L, ApiEntityType.PHYSICAL_MACHINE), // <-- starting point
                TestGraphEntity.newBuilder(2L, ApiEntityType.VIRTUAL_MACHINE) // <-- starting point
                        .addProviderId(33L),
                TestGraphEntity.newBuilder(1L, ApiEntityType.VIRTUAL_MACHINE)
                        .addProviderId(22L));

        final Map<Integer, SupplyChainNode> supplychain = getSupplyChain(graph, ImmutableSet.of(1L, 33L));

        assertThat(supplychain.keySet(),
                   containsInAnyOrder(ApiEntityType.VIRTUAL_MACHINE.typeNumber(),
                                      ApiEntityType.PHYSICAL_MACHINE.typeNumber()));
        assertThat(RepositoryDTOUtil.getMemberCount(supplychain.get(ApiEntityType.VIRTUAL_MACHINE.typeNumber())),
                   is(2));
        assertThat(RepositoryDTOUtil.getMemberCount(supplychain.get(ApiEntityType.PHYSICAL_MACHINE.typeNumber())),
                   is(2));

        assertThat(supplychain.get(ApiEntityType.PHYSICAL_MACHINE.typeNumber()).getConnectedConsumerTypesList(),
                   containsInAnyOrder(ApiEntityType.VIRTUAL_MACHINE.typeNumber()));
        assertThat(supplychain.get(ApiEntityType.VIRTUAL_MACHINE.typeNumber()).getConnectedProviderTypesList(),
                   containsInAnyOrder(ApiEntityType.PHYSICAL_MACHINE.typeNumber()));
    }

    /**
     * Tests that scoping on a PM includes the related DC
     * but not other PMs in the result.
     */
    @Test
    public void testDCAndPMsRelationship() {
       /*
        * Topology:
        *    PM1 PM2
        *     \  /
        *      DC
        *  scoping on PM1 should not return PM2
        */
        final TopologyGraph<TestGraphEntity> graph =
                TestGraphEntity.newGraph(
                        TestGraphEntity.newBuilder(PM_ID, ApiEntityType.PHYSICAL_MACHINE)
                                .addProviderId(DC_ID),
                        TestGraphEntity.newBuilder(PM_ID2, ApiEntityType.PHYSICAL_MACHINE)
                                .addProviderId(DC_ID),
                        TestGraphEntity.newBuilder(DC_ID, ApiEntityType.DATACENTER));

        final Map<Integer, SupplyChainNode> supplychain = getSupplyChain(graph, PM_ID);
        final SupplyChainNode pmNode = supplychain.get(ApiEntityType.PHYSICAL_MACHINE.typeNumber());
        final SupplyChainNode dcNode = supplychain.get(ApiEntityType.DATACENTER.typeNumber());

        Assert.assertEquals(Collections.singletonList(ApiEntityType.DATACENTER.typeNumber()),
                pmNode.getConnectedProviderTypesList());
        assertTrue(pmNode.getConnectedConsumerTypesList().isEmpty());
        Assert.assertEquals(Collections.singletonList(ApiEntityType.PHYSICAL_MACHINE.typeNumber()),
                dcNode.getConnectedConsumerTypesList());
        assertTrue(dcNode.getConnectedProviderTypesList().isEmpty());

        Assert.assertEquals(Collections.singleton(PM_ID), getAllNodeIds(pmNode));
        Assert.assertEquals(Collections.singleton(DC_ID), getAllNodeIds(dcNode));
    }

    /**
     * Tests that VMs with multiple volumes do not bring irrelevant storage.
     */
    @Test
    public void testVmWithMultipleVolumes() {
       /*
        * Topology:
        *       VM
        *      / \
        *   Vol1 Vol2
        *    |    |
        *    ST1 ST2
        *  scoping on ST1 should not return ST2
        */
       final long vmId = 1L;
       final long vol1Id = 11L;
       final long vol2Id = 12L;
       final long st1Id = 21L;
       final long st2Id = 22L;

       final TopologyGraph<TestGraphEntity> graph =
               TestGraphEntity.newGraph(TestGraphEntity.newBuilder(vmId, ApiEntityType.VIRTUAL_MACHINE)
                                            .addProviderId(st1Id)
                                            .addProviderId(st2Id)
                                            .addConnectedEntity(vol1Id, ConnectionType.NORMAL_CONNECTION)
                                            .addConnectedEntity(vol2Id, ConnectionType.NORMAL_CONNECTION),
                                        TestGraphEntity.newBuilder(vol1Id, ApiEntityType.VIRTUAL_VOLUME)
                                            .addConnectedEntity(st1Id, ConnectionType.NORMAL_CONNECTION),
                                        TestGraphEntity.newBuilder(vol2Id, ApiEntityType.VIRTUAL_VOLUME)
                                            .addConnectedEntity(st2Id, ConnectionType.NORMAL_CONNECTION),
                                        TestGraphEntity.newBuilder(st1Id, ApiEntityType.STORAGE),
                                        TestGraphEntity.newBuilder(st2Id, ApiEntityType.STORAGE));
        assertEquals(Collections.singleton(st1Id),
                     getAllNodeIds(getSupplyChain(graph, st1Id).get(ApiEntityType.STORAGE.typeNumber())));
    }

    /**
     * Test behavior when the seed is a volume without a VM.
     * The connected storage and anything "underneath it" in the
     * supply chain should be included.
     */
    @Test
    public void testOrphanVolume() {
        /*
         * Topology:
         *   Volume
         *     | connected to
         *   Storage
         *     | consumes from
         *   DiskArray
         */
        final long volId = 1L;
        final long stId = 2L;
        final long daId = 3L;
        final TopologyGraph<TestGraphEntity> graph =
            TestGraphEntity.newGraph(TestGraphEntity.newBuilder(daId, ApiEntityType.DISKARRAY),
                                     TestGraphEntity.newBuilder(stId, ApiEntityType.STORAGE)
                                        .addProviderId(daId),
                                     TestGraphEntity.newBuilder(volId, ApiEntityType.VIRTUAL_VOLUME)
                                        .addConnectedEntity(stId, ConnectionType.NORMAL_CONNECTION));

        final Map<Integer, SupplyChainNode> supplychain = getSupplyChain(graph, volId);

        assertThat(supplychain.get(ApiEntityType.VIRTUAL_VOLUME.typeNumber())
                        .getConnectedProviderTypesList(),
                   containsInAnyOrder(ApiEntityType.STORAGE.typeNumber()));
        assertThat(RepositoryDTOUtil.getMemberCount(
                        supplychain.get(ApiEntityType.VIRTUAL_VOLUME.typeNumber())),
                   is(1));
        assertThat(supplychain.get(ApiEntityType.STORAGE.typeNumber())
                        .getConnectedProviderTypesList(),
                   containsInAnyOrder(ApiEntityType.DISKARRAY.typeNumber()));
        assertThat(supplychain.get(ApiEntityType.STORAGE.typeNumber())
                        .getConnectedConsumerTypesList(),
                   containsInAnyOrder(ApiEntityType.VIRTUAL_VOLUME.typeNumber()));
        assertThat(RepositoryDTOUtil.getMemberCount(
                        supplychain.get(ApiEntityType.STORAGE.typeNumber())),
                   is(1));
        assertThat(supplychain.get(ApiEntityType.DISKARRAY.typeNumber())
                        .getConnectedConsumerTypesList(),
                   containsInAnyOrder(ApiEntityType.STORAGE.typeNumber()));
        assertThat(RepositoryDTOUtil.getMemberCount(
                        supplychain.get(ApiEntityType.DISKARRAY.typeNumber())),
                   is(1));
    }

    /**
     * Tests that the ownership between two accounts is not traversed.
     */
    @Test
    public void testAccountRule() {
        /*
         * Acc1 -owns-> Acc2
         *   |owns        | owns
         *  VM1          VM2    <-aggregates both- Reg
         *
         *  Scoping on Acc1 should not bring VM2.
         */
        final long acc1Id = 1L;
        final long acc2Id = 2L;
        final long vm1Id = 11L;
        final long vm2Id = 22L;
        final long regId = 333L;
        final TopologyGraph<TestGraphEntity> graph =
            TestGraphEntity.newGraph(
                TestGraphEntity.newBuilder(acc1Id, ApiEntityType.BUSINESS_ACCOUNT)
                    .addConnectedEntity(acc2Id, ConnectionType.OWNS_CONNECTION)
                    .addConnectedEntity(vm1Id, ConnectionType.OWNS_CONNECTION),
                TestGraphEntity.newBuilder(acc2Id, ApiEntityType.BUSINESS_ACCOUNT)
                    .addConnectedEntity(vm2Id, ConnectionType.OWNS_CONNECTION),
                TestGraphEntity.newBuilder(vm1Id, ApiEntityType.VIRTUAL_MACHINE)
                    .addConnectedEntity(regId, ConnectionType.AGGREGATED_BY_CONNECTION),
                TestGraphEntity.newBuilder(vm2Id, ApiEntityType.VIRTUAL_MACHINE)
                    .addConnectedEntity(regId, ConnectionType.AGGREGATED_BY_CONNECTION),
                TestGraphEntity.newBuilder(regId, ApiEntityType.REGION));

        final Map<Integer, SupplyChainNode> supplychain = getSupplyChain(graph, acc1Id);
        assertThat(RepositoryDTOUtil.getMemberCount(
                        supplychain.get(ApiEntityType.BUSINESS_ACCOUNT.typeNumber())),
                   is(1));
        assertEquals(acc1Id, supplychain.get(ApiEntityType.BUSINESS_ACCOUNT.typeNumber())
                                    .getMembersByStateMap().values().iterator().next()
                                    .getMemberOids(0));
        assertThat(RepositoryDTOUtil.getMemberCount(
                        supplychain.get(ApiEntityType.VIRTUAL_MACHINE.typeNumber())),
                   is(1));
        assertEquals(vm1Id, supplychain.get(ApiEntityType.VIRTUAL_MACHINE.typeNumber())
                                    .getMembersByStateMap().values().iterator().next()
                                    .getMemberOids(0));
        assertThat(RepositoryDTOUtil.getMemberCount(
                        supplychain.get(ApiEntityType.REGION.typeNumber())),
                   is(1));
    }

    /**
     * Test scoping on a volume that is not attached to a VM,
     * but contained in a zone.
     */
    @Test
    public void testAwsOrphanVolume() {
        checkAWSTopologyWithOrphanVolume(getSupplyChain(makeAWSTopologyWithOrphanVolume(), VOL_ID));
    }

    /**
     * Test scoping on a zone in the presence of a volume
     * that is not attached to a VM.
     */
    @Test
    public void testAwsOrphanVolumeScopeOnZone() {
        checkAWSTopologyWithOrphanVolume(getSupplyChain(makeAWSTopologyWithOrphanVolume(), AZ_ID));
    }

    /**
     * Test scoping on a region in the presence of a volume
     * that is not attached to a VM and an availability zone.
     */
    @Test
    public void testAwsOrphanVolumeScopeOnRegion() {
        checkAWSTopologyWithOrphanVolume(getSupplyChain(makeAWSTopologyWithOrphanVolume(), REGION_ID));
    }

    /**
     * Test scoping on a volume that is not attached to a VM,
     * but contained in a region.
     */
    @Test
    public void testAzureOrphanVolume() {
        checkAzureTopologyWithOrphanVolume(getSupplyChain(makeAzureTopologyWithOrphanVolume(), VOL_ID));
    }

    /**
     * Test scoping on a region in the presence of a volume
     * that is not attached to a VM.
     */
    @Test
    public void testAzureOrphanVolumeScopeOnRegion() {
        checkAzureTopologyWithOrphanVolume(getSupplyChain(makeAzureTopologyWithOrphanVolume(), REGION_ID));
    }

    /*
     * Topology:
     *   Volume
     *     | aggregated by
     *   Zone
     *     | owned by
     *   Region
     */
    private TopologyGraph<TestGraphEntity> makeAWSTopologyWithOrphanVolume() {
        return TestGraphEntity.newGraph(
                TestGraphEntity.newBuilder(REG_ID, ApiEntityType.REGION)
                        .addConnectedEntity(AZ_ID, ConnectionType.OWNS_CONNECTION),
                TestGraphEntity.newBuilder(AZ_ID, ApiEntityType.AVAILABILITY_ZONE),
                TestGraphEntity.newBuilder(VOL_ID, ApiEntityType.VIRTUAL_VOLUME)
                        .addConnectedEntity(AZ_ID, ConnectionType.AGGREGATED_BY_CONNECTION));
    }

    /*
     * Topology:
     *   Volume
     *     | aggregated by
     *   Region
     */
    private TopologyGraph<TestGraphEntity> makeAzureTopologyWithOrphanVolume() {
        return TestGraphEntity.newGraph(
                TestGraphEntity.newBuilder(REG_ID, ApiEntityType.REGION),
                TestGraphEntity.newBuilder(VOL_ID, ApiEntityType.VIRTUAL_VOLUME)
                        .addConnectedEntity(REG_ID, ConnectionType.AGGREGATED_BY_CONNECTION));
    }

    private void checkAWSTopologyWithOrphanVolume(Map<Integer, SupplyChainNode> supplychain) {
        assertThat(supplychain.get(ApiEntityType.VIRTUAL_VOLUME.typeNumber())
                        .getConnectedProviderTypesList(),
                containsInAnyOrder(ApiEntityType.AVAILABILITY_ZONE.typeNumber()));
        assertThat(RepositoryDTOUtil.getMemberCount(
                supplychain.get(ApiEntityType.VIRTUAL_VOLUME.typeNumber())),
                is(1));
        assertThat(supplychain.get(ApiEntityType.AVAILABILITY_ZONE.typeNumber())
                        .getConnectedConsumerTypesList(),
                containsInAnyOrder(ApiEntityType.VIRTUAL_VOLUME.typeNumber()));
        assertThat(supplychain.get(ApiEntityType.AVAILABILITY_ZONE.typeNumber())
                        .getConnectedProviderTypesList(),
                containsInAnyOrder(ApiEntityType.REGION.typeNumber()));
        assertThat(RepositoryDTOUtil.getMemberCount(
                supplychain.get(ApiEntityType.AVAILABILITY_ZONE.typeNumber())),
                is(1));
        assertThat(supplychain.get(ApiEntityType.REGION.typeNumber())
                        .getConnectedConsumerTypesList(),
                containsInAnyOrder(ApiEntityType.AVAILABILITY_ZONE.typeNumber()));
        assertThat(RepositoryDTOUtil.getMemberCount(
                supplychain.get(ApiEntityType.REGION.typeNumber())),
                is(1));
    }

    private void checkAzureTopologyWithOrphanVolume(Map<Integer, SupplyChainNode> supplychain) {
        assertThat(supplychain.get(ApiEntityType.VIRTUAL_VOLUME.typeNumber())
                        .getConnectedProviderTypesList(),
                containsInAnyOrder(ApiEntityType.REGION.typeNumber()));
        assertThat(RepositoryDTOUtil.getMemberCount(
                supplychain.get(ApiEntityType.VIRTUAL_VOLUME.typeNumber())),
                is(1));
        assertThat(supplychain.get(ApiEntityType.REGION.typeNumber())
                        .getConnectedConsumerTypesList(),
                containsInAnyOrder(ApiEntityType.VIRTUAL_VOLUME.typeNumber()));
        assertThat(RepositoryDTOUtil.getMemberCount(
                supplychain.get(ApiEntityType.REGION.typeNumber())),
                is(1));
    }

    /**
     * In the topology created by {@link #createVDCTopology()}, scope on PM1.
     * Expect: PM1, all VDCs except VDC2, all VMs except VM2.
     */
    @Test
    public void testVdcTopologyScopeOnPm1() {
        createVDCTopology();
        populateVdcTopoEntityFields(getSupplyChain(vdcTopology, VDC_TOPO_PM1ID));
        checkVdcTopoSupplyChainNodes(true);
        assertEquals(Collections.singleton(VDC_TOPO_PM1ID), getAllNodeIds(vdcTopoPm));
        assertThat(getAllNodeIds(vdcTopoVdc),
                   containsInAnyOrder(VDC_TOPO_VDC1ID, VDC_TOPO_VDC3ID, VDC_TOPO_VDC4ID));
        assertThat(getAllNodeIds(vdcTopoVm),
                   containsInAnyOrder(VDC_TOPO_VM1ID, VDC_TOPO_VM3ID, VDC_TOPO_VM4ID));
    }

    /**
     * In the topology created by {@link #createVDCTopology()}, scope on PM2.
     * Expect: PM2, VDC1, VDC2, VM2.
     */
    @Test
    public void testVdcTopologyScopeOnPm2() {
        createVDCTopology();
        populateVdcTopoEntityFields(getSupplyChain(vdcTopology, VDC_TOPO_PM2ID));
        checkVdcTopoSupplyChainNodes(true);
        assertEquals(Collections.singleton(VDC_TOPO_PM2ID), getAllNodeIds(vdcTopoPm));
        assertThat(getAllNodeIds(vdcTopoVdc),
                   containsInAnyOrder(VDC_TOPO_VDC1ID, VDC_TOPO_VDC2ID));
        assertEquals(Collections.singleton(VDC_TOPO_VM2ID), getAllNodeIds(vdcTopoVm));
    }

    /**
     * In the topology created by {@link #createVDCTopology()}, scope on PM3.
     * Expect: PM3, VDC4.
     */
    @Test
    public void testVdcTopologyScopeOnPm3() {
        createVDCTopology();
        populateVdcTopoEntityFields(getSupplyChain(vdcTopology, VDC_TOPO_PM3ID));
        assertEquals(Collections.singleton(VDC_TOPO_PM3ID), getAllNodeIds(vdcTopoPm));
        assertEquals(Collections.singleton(VDC_TOPO_VDC4ID), getAllNodeIds(vdcTopoVdc));

        // check types: VMs do not appear in the supply chain
        assertTrue(vdcTopoPm.getConnectedProviderTypesList().isEmpty());
        assertEquals(Collections.singletonList(ApiEntityType.VIRTUAL_DATACENTER.typeNumber()),
                     vdcTopoPm.getConnectedConsumerTypesList());
        assertEquals(Collections.singletonList(ApiEntityType.PHYSICAL_MACHINE.typeNumber()),
                     vdcTopoVdc.getConnectedProviderTypesList());
        assertTrue(vdcTopoVdc.getConnectedConsumerTypesList().isEmpty());
    }

    /**
     * In the topology created by {@link #createVDCTopology()}, scope on VDC1.
     * Expect: PM1, PM2, all VDCs except VDC4, all VMs except VM4.
     */
    @Test
    public void testVdcTopologyScopeOnVdc1() {
        createVDCTopology();
        populateVdcTopoEntityFields(getSupplyChain(vdcTopology, VDC_TOPO_VDC1ID));
        checkVdcTopoSupplyChainNodes(true);
        assertThat(getAllNodeIds(vdcTopoPm),
                   containsInAnyOrder(VDC_TOPO_PM1ID, VDC_TOPO_PM2ID));
        assertThat(getAllNodeIds(vdcTopoVdc),
                   containsInAnyOrder(VDC_TOPO_VDC1ID, VDC_TOPO_VDC2ID, VDC_TOPO_VDC3ID));
        assertThat(getAllNodeIds(vdcTopoVm),
                   containsInAnyOrder(VDC_TOPO_VM1ID, VDC_TOPO_VM2ID, VDC_TOPO_VM3ID));
    }

    /**
     * In the topology created by {@link #createVDCTopology()}, scope on VDC2.
     * Expect: PM1, PM2, VDC1, VDC2, VM2.
     */
    @Test
    public void testVdcTopologyScopeOnVdc2() {
        createVDCTopology();
        populateVdcTopoEntityFields(getSupplyChain(vdcTopology, VDC_TOPO_VDC2ID));
        checkVdcTopoSupplyChainNodes(true);
        assertThat(getAllNodeIds(vdcTopoPm),
                   containsInAnyOrder(VDC_TOPO_PM1ID, VDC_TOPO_PM2ID));
        assertThat(getAllNodeIds(vdcTopoVdc),
                   containsInAnyOrder(VDC_TOPO_VDC1ID, VDC_TOPO_VDC2ID));
        assertEquals(Collections.singleton(VDC_TOPO_VM2ID), getAllNodeIds(vdcTopoVm));
    }

    /**
     * In the topology created by {@link #createVDCTopology()}, scope on VDC3.
     * Expect: PM1, PM2, VDC1, VDC3, VM3.
     */
    @Test
    public void testVdcTopologyScopeOnVdc3() {
        createVDCTopology();
        populateVdcTopoEntityFields(getSupplyChain(vdcTopology, VDC_TOPO_VDC3ID));
        checkVdcTopoSupplyChainNodes(true);
        assertThat(getAllNodeIds(vdcTopoPm),
                   containsInAnyOrder(VDC_TOPO_PM1ID, VDC_TOPO_PM2ID));
        assertThat(getAllNodeIds(vdcTopoVdc),
                   containsInAnyOrder(VDC_TOPO_VDC1ID, VDC_TOPO_VDC3ID));
        assertEquals(Collections.singleton(VDC_TOPO_VM3ID), getAllNodeIds(vdcTopoVm));
    }

    /**
     * In the topology created by {@link #createVDCTopology()}, scope on VDC4.
     * Expect: PM1, PM3, VDC4, VM4.
     */
    @Test
    public void testVdcTopologyScopeOnVdc4() {
        createVDCTopology();
        populateVdcTopoEntityFields(getSupplyChain(vdcTopology, VDC_TOPO_VDC4ID));
        checkVdcTopoSupplyChainNodes(false);
        assertEquals(Collections.singletonList(ApiEntityType.PHYSICAL_MACHINE.typeNumber()),
                     vdcTopoVdc.getConnectedProviderTypesList());
        assertEquals(Collections.singletonList(ApiEntityType.VIRTUAL_MACHINE.typeNumber()),
                     vdcTopoVdc.getConnectedConsumerTypesList());
        assertThat(getAllNodeIds(vdcTopoPm),
                   containsInAnyOrder(VDC_TOPO_PM1ID, VDC_TOPO_PM3ID));
        assertEquals(Collections.singleton(VDC_TOPO_VDC4ID), getAllNodeIds(vdcTopoVdc));
        assertEquals(Collections.singleton(VDC_TOPO_VM4ID), getAllNodeIds(vdcTopoVm));
    }

    /**
     * In the topology created by {@link #createVDCTopology()}, scope on VM1.
     * Expect: PM1, VDC1, VM1.
     */
    @Test
    public void testVdcTopologyScopeOnVm1() {
        createVDCTopology();
        populateVdcTopoEntityFields(getSupplyChain(vdcTopology, VDC_TOPO_VM1ID));
        checkVdcTopoSupplyChainNodes(false);
        assertEquals(Collections.singletonList(ApiEntityType.PHYSICAL_MACHINE.typeNumber()),
                     vdcTopoVdc.getConnectedProviderTypesList());
        assertThat(vdcTopoVdc.getConnectedConsumerTypesList(),
                   containsInAnyOrder(ApiEntityType.VIRTUAL_MACHINE.typeNumber()));
        assertEquals(Collections.singleton(VDC_TOPO_PM1ID), getAllNodeIds(vdcTopoPm));
        assertEquals(Collections.singleton(VDC_TOPO_VDC1ID), getAllNodeIds(vdcTopoVdc));
        assertEquals(Collections.singleton(VDC_TOPO_VM1ID), getAllNodeIds(vdcTopoVm));
    }

    /**
     * In the topology created by {@link #createVDCTopology()}, scope on VM3.
     * Expect: PM2, VDC2, VM2.
     */
    @Test
    public void testVdcTopologyScopeOnVm2() {
        createVDCTopology();
        populateVdcTopoEntityFields(getSupplyChain(vdcTopology, VDC_TOPO_VM2ID));
        checkVdcTopoSupplyChainNodes(false, false);
        assertEquals(Collections.singletonList(ApiEntityType.VIRTUAL_MACHINE.typeNumber()),
                     vdcTopoVdc.getConnectedConsumerTypesList());
        assertEquals(Collections.emptyList(), vdcTopoVdc.getConnectedProviderTypesList());
        assertEquals(Collections.singleton(VDC_TOPO_PM2ID), getAllNodeIds(vdcTopoPm));
        assertEquals(Collections.singleton(VDC_TOPO_VDC2ID), getAllNodeIds(vdcTopoVdc));
        assertEquals(Collections.singleton(VDC_TOPO_VM2ID), getAllNodeIds(vdcTopoVm));
    }

    /**
     * In the topology created by {@link #createVDCTopology()}, scope on VM3.
     * Expect: PM1, VDC3, VM3.
     */
    @Test
    public void testVdcTopologyScopeOnVm3() {
        createVDCTopology();
        populateVdcTopoEntityFields(getSupplyChain(vdcTopology, VDC_TOPO_VM3ID));
        checkVdcTopoSupplyChainNodes(false, false);
        assertEquals(Collections.singletonList(ApiEntityType.VIRTUAL_MACHINE.typeNumber()),
                     vdcTopoVdc.getConnectedConsumerTypesList());
        assertEquals(Collections.emptyList(), vdcTopoVdc.getConnectedProviderTypesList());
        assertEquals(Collections.singleton(VDC_TOPO_PM1ID), getAllNodeIds(vdcTopoPm));
        assertEquals(Collections.singleton(VDC_TOPO_VDC3ID), getAllNodeIds(vdcTopoVdc));
        assertEquals(Collections.singleton(VDC_TOPO_VM3ID), getAllNodeIds(vdcTopoVm));
    }

    /**
     * In the topology created by {@link #createVDCTopology()}, scope on VM4.
     * Expect: PM1, VDC4, VM4.
     */
    @Test
    public void testVdcTopologyScopeOnVm4() {
        createVDCTopology();
        populateVdcTopoEntityFields(getSupplyChain(vdcTopology, VDC_TOPO_VM4ID));
        checkVdcTopoSupplyChainNodes(false);
        assertEquals(Collections.singletonList(ApiEntityType.PHYSICAL_MACHINE.typeNumber()),
                     vdcTopoVdc.getConnectedProviderTypesList());
        assertEquals(Collections.singletonList(ApiEntityType.VIRTUAL_MACHINE.typeNumber()),
                     vdcTopoVdc.getConnectedConsumerTypesList());
        assertEquals(Collections.singleton(VDC_TOPO_PM1ID), getAllNodeIds(vdcTopoPm));
        assertEquals(Collections.singleton(VDC_TOPO_VDC4ID), getAllNodeIds(vdcTopoVdc));
        assertEquals(Collections.singleton(VDC_TOPO_VM4ID), getAllNodeIds(vdcTopoVm));
    }

    /*
     * Topology:
     *     VM2    VM3
     *      \     /
     * VM1  VDC2 VDC3      VM4
     *   \   \  /          /
     *    ---VDC1       VDC4
     *        / \      /  |
     *    PM2   PM1----  PM3
     *
     * and: PM1 hosts VM1, VM3, VM4
     *      PM2 hosts VM2
     */
    private void createVDCTopology() {
        // entities
        final TestGraphEntity.Builder pm1Builder =
                TestGraphEntity.newBuilder(VDC_TOPO_PM1ID, ApiEntityType.PHYSICAL_MACHINE);
        final TestGraphEntity.Builder pm2Builder =
                TestGraphEntity.newBuilder(VDC_TOPO_PM2ID, ApiEntityType.PHYSICAL_MACHINE);
        final TestGraphEntity.Builder pm3Builder =
                TestGraphEntity.newBuilder(VDC_TOPO_PM3ID, ApiEntityType.PHYSICAL_MACHINE);

        final TestGraphEntity.Builder vdc1Builder =
                TestGraphEntity.newBuilder(VDC_TOPO_VDC1ID, ApiEntityType.VIRTUAL_DATACENTER);
        final TestGraphEntity.Builder vdc2Builder =
                TestGraphEntity.newBuilder(VDC_TOPO_VDC2ID, ApiEntityType.VIRTUAL_DATACENTER);
        final TestGraphEntity.Builder vdc3Builder =
                TestGraphEntity.newBuilder(VDC_TOPO_VDC3ID, ApiEntityType.VIRTUAL_DATACENTER);
        final TestGraphEntity.Builder vdc4Builder =
                TestGraphEntity.newBuilder(VDC_TOPO_VDC4ID, ApiEntityType.VIRTUAL_DATACENTER);

        final TestGraphEntity.Builder vm1Builder =
                TestGraphEntity.newBuilder(VDC_TOPO_VM1ID, ApiEntityType.VIRTUAL_MACHINE);
        final TestGraphEntity.Builder vm2Builder =
                TestGraphEntity.newBuilder(VDC_TOPO_VM2ID, ApiEntityType.VIRTUAL_MACHINE);
        final TestGraphEntity.Builder vm3Builder =
                TestGraphEntity.newBuilder(VDC_TOPO_VM3ID, ApiEntityType.VIRTUAL_MACHINE);
        final TestGraphEntity.Builder vm4Builder =
                TestGraphEntity.newBuilder(VDC_TOPO_VM4ID, ApiEntityType.VIRTUAL_MACHINE);

        // VMs consume from VDCs
        vm1Builder.addProviderId(VDC_TOPO_VDC1ID);
        vm2Builder.addProviderId(VDC_TOPO_VDC2ID);
        vm3Builder.addProviderId(VDC_TOPO_VDC3ID);
        vm4Builder.addProviderId(VDC_TOPO_VDC4ID);

        // VMs consume from PMs
        vm1Builder.addProviderId(VDC_TOPO_PM1ID);
        vm3Builder.addProviderId(VDC_TOPO_PM1ID);
        vm4Builder.addProviderId(VDC_TOPO_PM1ID);
        vm2Builder.addProviderId(VDC_TOPO_PM2ID);

        // VDCs consume from VDCs
        vdc2Builder.addProviderId(VDC_TOPO_VDC1ID);
        vdc3Builder.addProviderId(VDC_TOPO_VDC1ID);

        // VDCs consume from PMs
        vdc1Builder.addProviderId(VDC_TOPO_PM1ID);
        vdc1Builder.addProviderId(VDC_TOPO_PM2ID);
        vdc4Builder.addProviderId(VDC_TOPO_PM1ID);
        vdc4Builder.addProviderId(VDC_TOPO_PM3ID);

        vdcTopology = TestGraphEntity.newGraph(pm1Builder, pm2Builder, pm3Builder,
                vdc1Builder, vdc2Builder, vdc3Builder, vdc4Builder,
                vm1Builder, vm2Builder, vm3Builder, vm4Builder);
    }

    private void populateVdcTopoEntityFields(@Nonnull Map<Integer, SupplyChainNode> graph) {
        vdcTopoPm = graph.get(ApiEntityType.PHYSICAL_MACHINE.typeNumber());
        vdcTopoVdc = graph.get(ApiEntityType.VIRTUAL_DATACENTER.typeNumber());
        vdcTopoVm = graph.get(ApiEntityType.VIRTUAL_MACHINE.typeNumber());
    }

    private void checkVdcTopoSupplyChainNodes(boolean vdcToVdcEdgeExists) {
        checkVdcTopoSupplyChainNodes(vdcToVdcEdgeExists, true);
    }

    private void checkVdcTopoSupplyChainNodes(boolean vdcToVdcEdgeExists, boolean vdcToPmEdgeExists) {
        assertTrue(vdcTopoPm.getConnectedProviderTypesList().isEmpty());

        if (vdcToPmEdgeExists) {
            assertThat(vdcTopoPm.getConnectedConsumerTypesList(),
                       containsInAnyOrder(ApiEntityType.VIRTUAL_MACHINE.typeNumber(),
                                          ApiEntityType.VIRTUAL_DATACENTER.typeNumber()));
        } else {
            assertThat(vdcTopoPm.getConnectedConsumerTypesList(),
                       containsInAnyOrder(ApiEntityType.VIRTUAL_MACHINE.typeNumber()));
        }

        if (vdcToVdcEdgeExists) {
            assertThat(vdcTopoVdc.getConnectedProviderTypesList(),
                       containsInAnyOrder(ApiEntityType.PHYSICAL_MACHINE.typeNumber(),
                                          ApiEntityType.VIRTUAL_DATACENTER.typeNumber()));
            assertThat(vdcTopoVdc.getConnectedConsumerTypesList(),
                       containsInAnyOrder(ApiEntityType.VIRTUAL_MACHINE.typeNumber(),
                                          ApiEntityType.VIRTUAL_DATACENTER.typeNumber()));
        }

        assertThat(vdcTopoVm.getConnectedProviderTypesList(),
                   containsInAnyOrder(ApiEntityType.PHYSICAL_MACHINE.typeNumber(),
                                      ApiEntityType.VIRTUAL_DATACENTER.typeNumber()));
        assertTrue(vdcTopoVm.getConnectedConsumerTypesList().isEmpty());
    }

    /**
     * We should not traverse tier -> region relations, and scoping on a region should not bring in
     * other regions.
     */
    @Test
    public void testTierRule() {
        /*
         *  Topology:
         *   VM1          VM2
         *   | \         / |
         *   |  REG1  REG2 |
         *   |   \     /   |
         *   |    \   /    |
         *     COMPUTE_TIER
         */
        final long vm1Id = 1L;
        final long vm2Id = 2L;
        final long computeTierId = 3L;
        final long region1Id = 4L;
        final long region2Id = 5L;
        final TopologyGraph<TestGraphEntity> graph = TestGraphEntity.newGraph(
                TestGraphEntity.newBuilder(vm1Id, ApiEntityType.VIRTUAL_MACHINE)
                        .addProviderId(computeTierId)
                        .addConnectedEntity(region1Id, ConnectionType.AGGREGATED_BY_CONNECTION),
                TestGraphEntity.newBuilder(vm2Id, ApiEntityType.VIRTUAL_MACHINE)
                        .addProviderId(computeTierId)
                        .addConnectedEntity(region2Id, ConnectionType.AGGREGATED_BY_CONNECTION),
                TestGraphEntity.newBuilder(computeTierId, ApiEntityType.COMPUTE_TIER)
                        .addConnectedEntity(region1Id, ConnectionType.AGGREGATED_BY_CONNECTION)
                        .addConnectedEntity(region2Id, ConnectionType.AGGREGATED_BY_CONNECTION),
                TestGraphEntity.newBuilder(region1Id, ApiEntityType.REGION),
                TestGraphEntity.newBuilder(region2Id, ApiEntityType.REGION));

        // scoping on VM1 will not bring REG2 or VM2
        final Map<Integer, SupplyChainNode> supplyChainVm1 = getSupplyChain(graph, vm1Id);
        assertThat(getAllNodeIds(supplyChainVm1.get(ApiEntityType.REGION.typeNumber())),
                containsInAnyOrder(region1Id));
        assertThat(getAllNodeIds(supplyChainVm1.get(ApiEntityType.VIRTUAL_MACHINE.typeNumber())),
                containsInAnyOrder(vm1Id));

        // scoping on VM2 will not bring REG1 or VM1
        final Map<Integer, SupplyChainNode> supplyChainVm2 = getSupplyChain(graph, vm2Id);
        assertThat(getAllNodeIds(supplyChainVm2.get(ApiEntityType.REGION.typeNumber())),
                containsInAnyOrder(region2Id));
        assertThat(getAllNodeIds(supplyChainVm2.get(ApiEntityType.VIRTUAL_MACHINE.typeNumber())),
                containsInAnyOrder(vm2Id));

        // scoping on REG1 will not bring REG2 or VM2
        final Map<Integer, SupplyChainNode> supplyChainRegion1 = getSupplyChain(graph, region1Id);
        assertThat(getAllNodeIds(supplyChainRegion1.get(ApiEntityType.REGION.typeNumber())),
                containsInAnyOrder(region1Id));
        assertThat(getAllNodeIds(supplyChainRegion1.get(ApiEntityType.VIRTUAL_MACHINE.typeNumber())),
                containsInAnyOrder(vm1Id));

        // scoping on REG2 will not bring REG1 or VM1
        final Map<Integer, SupplyChainNode> supplyChainRegion2 = getSupplyChain(graph, region2Id);
        assertThat(getAllNodeIds(supplyChainRegion2.get(ApiEntityType.REGION.typeNumber())),
                containsInAnyOrder(region2Id));
        assertThat(getAllNodeIds(supplyChainRegion2.get(ApiEntityType.VIRTUAL_MACHINE.typeNumber())),
                containsInAnyOrder(vm2Id));
    }

    /**
     * In the topology created by {@link #createVdiTopology()}, scope on VM1.
     * Expect: PM1, BU1, VM1, DP1, VP, VDC1.
     */
    @Test
    public void testVdiTopologyScopeOnVm1() {
        final TopologyGraph<TestGraphEntity> topology = createVdiTopology();
        final Map<Integer, SupplyChainNode> supplyChain = getSupplyChain(topology, VDI_TOPO_VM1ID);
        assertEquals(6, supplyChain.keySet().size());
        assertEquals(Collections.singleton(VDI_TOPO_PM1ID),
                getAllNodeIds(supplyChain.get(ApiEntityType.PHYSICAL_MACHINE.typeNumber())));
        assertEquals(Collections.singleton(VDI_TOPO_DP1ID),
                getAllNodeIds(supplyChain.get(ApiEntityType.DESKTOP_POOL.typeNumber())));
        assertEquals(Collections.singleton(VDI_TOPO_VM1ID),
                getAllNodeIds(supplyChain.get(ApiEntityType.VIRTUAL_MACHINE.typeNumber())));
        assertEquals(Collections.singleton(VDI_TOPO_BU1ID),
                getAllNodeIds(supplyChain.get(ApiEntityType.BUSINESS_USER.typeNumber())));
        assertEquals(Collections.singleton(VDI_TOPO_VPODID),
                getAllNodeIds(supplyChain.get(ApiEntityType.VIEW_POD.typeNumber())));
        assertEquals(Collections.singleton(VDI_TOPO_VDC1ID),
                getAllNodeIds(supplyChain.get(ApiEntityType.VIRTUAL_DATACENTER.typeNumber())));
    }

    /**
     * In the topology created by {@link #createVdiTopology()}, scope on BU1.
     * Expect: PM1, BU1, VM1, VP, DP1, VDC1.
     */
    @Test
    public void testVdiTopologyScopeOnBu1() {
        final TopologyGraph<TestGraphEntity> topology = createVdiTopology();
        final Map<Integer, SupplyChainNode> supplyChain = getSupplyChain(topology, VDI_TOPO_BU1ID);
        assertEquals(6, supplyChain.keySet().size());
        assertEquals(Collections.singleton(VDI_TOPO_PM1ID),
                getAllNodeIds(supplyChain.get(ApiEntityType.PHYSICAL_MACHINE.typeNumber())));
        assertEquals(Collections.singleton(VDI_TOPO_DP1ID),
                getAllNodeIds(supplyChain.get(ApiEntityType.DESKTOP_POOL.typeNumber())));
        assertEquals(Collections.singleton(VDI_TOPO_VM1ID),
                getAllNodeIds(supplyChain.get(ApiEntityType.VIRTUAL_MACHINE.typeNumber())));
        assertEquals(Collections.singleton(VDI_TOPO_BU1ID),
                getAllNodeIds(supplyChain.get(ApiEntityType.BUSINESS_USER.typeNumber())));
        assertEquals(Collections.singleton(VDI_TOPO_VPODID),
                getAllNodeIds(supplyChain.get(ApiEntityType.VIEW_POD.typeNumber())));
        assertEquals(Collections.singleton(VDI_TOPO_VDC1ID),
                getAllNodeIds(supplyChain.get(ApiEntityType.VIRTUAL_DATACENTER.typeNumber())));
    }

    /**
     * In the topology created by {@link #createVdiTopology()}, scope on Dp1.
     * Expect: PM1, PM2, BU1, VM1, DP1, VP, and VDC1.
     */
    @Test
    public void testVdiTopologyScopeOnDp1() {
        final TopologyGraph<TestGraphEntity> topology = createVdiTopology();
        final Map<Integer, SupplyChainNode> supplyChain = getSupplyChain(topology, VDI_TOPO_DP1ID);
        assertEquals(6, supplyChain.keySet().size());
        assertThat(getAllNodeIds(supplyChain.get(ApiEntityType.PHYSICAL_MACHINE.typeNumber())),
                containsInAnyOrder(VDI_TOPO_PM1ID, VDI_TOPO_PM2ID));
        assertEquals(Collections.singleton(VDI_TOPO_DP1ID),
                getAllNodeIds(supplyChain.get(ApiEntityType.DESKTOP_POOL.typeNumber())));
        assertEquals(Collections.singleton(VDI_TOPO_VM1ID),
                getAllNodeIds(supplyChain.get(ApiEntityType.VIRTUAL_MACHINE.typeNumber())));
        assertEquals(Collections.singleton(VDI_TOPO_BU1ID),
                getAllNodeIds(supplyChain.get(ApiEntityType.BUSINESS_USER.typeNumber())));
        assertEquals(Collections.singleton(VDI_TOPO_VDC1ID),
                getAllNodeIds(supplyChain.get(ApiEntityType.VIRTUAL_DATACENTER.typeNumber())));
        assertEquals(Collections.singleton(VDI_TOPO_VPODID),
                getAllNodeIds(supplyChain.get(ApiEntityType.VIEW_POD.typeNumber())));
    }

    /*
     * Topology:
     *    BU1-------
     *      \      /
     *     VM1----DP1
     *      \     /  \
     *       \ VDC1  VPOD
     *       \  / \
     *        PM1 PM2
     **/
    private TopologyGraph<TestGraphEntity> createVdiTopology() {
        // entities
        final TestGraphEntity.Builder pm1Builder =
                TestGraphEntity.newBuilder(VDI_TOPO_PM1ID, ApiEntityType.PHYSICAL_MACHINE);
        final TestGraphEntity.Builder pm2Builder =
                TestGraphEntity.newBuilder(VDI_TOPO_PM2ID, ApiEntityType.PHYSICAL_MACHINE);

        final TestGraphEntity.Builder vdc1Builder =
                TestGraphEntity.newBuilder(VDI_TOPO_VDC1ID, ApiEntityType.VIRTUAL_DATACENTER);

        final TestGraphEntity.Builder dp1Builder =
                TestGraphEntity.newBuilder(VDI_TOPO_DP1ID, ApiEntityType.DESKTOP_POOL);

        final TestGraphEntity.Builder vpBuilder =
                TestGraphEntity.newBuilder(VDI_TOPO_VPODID, ApiEntityType.VIEW_POD);

        final TestGraphEntity.Builder vm1Builder =
                TestGraphEntity.newBuilder(VDI_TOPO_VM1ID, ApiEntityType.VIRTUAL_MACHINE);

        final TestGraphEntity.Builder bu1Builder =
                TestGraphEntity.newBuilder(VDI_TOPO_BU1ID, ApiEntityType.BUSINESS_USER);

        // VM consumes from PM
        vm1Builder.addProviderId(VDI_TOPO_PM1ID);

        // VM consumes from DP
        vm1Builder.addProviderId(VDI_TOPO_DP1ID);

        // BU consumes from VM
        bu1Builder.addProviderId(VDI_TOPO_VM1ID);

        // BU consumes from DP
        bu1Builder.addProviderId(VDI_TOPO_DP1ID);

        // VDC consumes from PMs
        vdc1Builder.addProviderId(VDI_TOPO_PM1ID);
        vdc1Builder.addProviderId(VDI_TOPO_PM2ID);

        // DP consumes from VDC
        dp1Builder.addProviderId(VDI_TOPO_VDC1ID);
        dp1Builder.addProviderId(VDI_TOPO_VPODID);

        return TestGraphEntity.newGraph(pm1Builder, pm2Builder, vdc1Builder, vm1Builder,
                bu1Builder, dp1Builder, vpBuilder);
    }

    /**
     * When scoping on a single desktop pool,
     * others should not be brought in the supply chain.
     */
    @Test
    public void testScopeOnSingleDP() {
        // Topology
        //   BU1
        //   / \
        // DP1  DP2
        // scoping on DP1 should not bring DP2 to the result
        final long buId = 1L;
        final long dp1Id = 2L;
        final long dp2Id = 3L;
        final TestGraphEntity.Builder buBuilder =
                TestGraphEntity.newBuilder(buId, ApiEntityType.BUSINESS_USER)
                    .addProviderId(dp1Id)
                    .addProviderId(dp2Id);
        final TestGraphEntity.Builder dp1Builder =
                TestGraphEntity.newBuilder(dp1Id, ApiEntityType.DESKTOP_POOL);
        final TestGraphEntity.Builder dp2Builder =
                TestGraphEntity.newBuilder(dp2Id, ApiEntityType.DESKTOP_POOL);

        final TopologyGraph<TestGraphEntity> graph = TestGraphEntity.newGraph(
                                                            buBuilder, dp1Builder, dp2Builder);
        final Map<Integer, SupplyChainNode> supplyChain = getSupplyChain(graph, dp1Id);

        assertEquals(Collections.singleton(dp1Id),
                     getAllNodeIds(supplyChain.get(ApiEntityType.DESKTOP_POOL.typeNumber())));
    }

    /**
     * Checks that {@link com.vmturbo.topology.graph.supplychain.SupplyChainCalculator.Frontier}
     * will skip addition of entity in case entity has been already scheduled for visiting/visited.
     */
    @Test
    public void testDuplicatedOidsInFrontier() {
        final long vm1Id = 1;
        final long vm2Id = 2;
        final long pmId = 11;
        final long st1Id = 21;
        final long st2Id = 22;
        final TopologyGraph<TestGraphEntity> graph =
                        TestGraphEntity.newGraph(
                                        TestGraphEntity.newBuilder(vm1Id, ApiEntityType.VIRTUAL_MACHINE)
                                                        .addProviderId(pmId)
                                                        .addProviderId(st1Id),
                                        TestGraphEntity.newBuilder(vm2Id, ApiEntityType.VIRTUAL_MACHINE)
                                                        .addProviderId(pmId)
                                                        .addProviderId(st2Id),
                                        TestGraphEntity.newBuilder(pmId, ApiEntityType.PHYSICAL_MACHINE)
                                                        .addProviderId(st1Id)
                                                        .addProviderId(st2Id),
                                        TestGraphEntity.newBuilder(st1Id, ApiEntityType.STORAGE),
                                        TestGraphEntity.newBuilder(st2Id, ApiEntityType.STORAGE));
        final Collection<TraversalState> frontier = new Frontier<>(Collections
                        .singleton(new TraversalState(VM_ID, VM_ID, TraversalMode.START, 1)),
                        graph);
        Assert.assertThat(frontier.size(), CoreMatchers.is(1));
        frontier.add(new TraversalState(pmId, st1Id, TraversalMode.CONSUMES, 1));
        Assert.assertThat(frontier.size(), CoreMatchers.is(2));
        frontier.add(new TraversalState(VM_ID, st1Id, TraversalMode.CONSUMES, 1));
        Assert.assertThat(frontier.size(), CoreMatchers.is(2));
    }

    private Map<Integer, SupplyChainNode> getSupplyChain(
            @Nonnull TopologyGraph<TestGraphEntity> topology, long seedId) {
        return getSupplyChain(topology, seedId, e -> true);
    }

    private Map<Integer, SupplyChainNode> getSupplyChain(
            @Nonnull TopologyGraph<TestGraphEntity> topology, Set<Long> seedIds) {
        return getSupplyChain(topology, seedIds, e -> true);
    }

    private Map<Integer, SupplyChainNode> getSupplyChain(
            @Nonnull TopologyGraph<TestGraphEntity> topology, long seedId,
            @Nonnull Predicate<TestGraphEntity> entityFilter) {
        return getSupplyChain(topology, Collections.singleton(seedId), entityFilter);
    }

    private Map<Integer, SupplyChainNode> getSupplyChain(
            @Nonnull TopologyGraph<TestGraphEntity> topology, Set<Long> seedIds,
            @Nonnull Predicate<TestGraphEntity> entityFilter) {
        return new SupplyChainCalculator()
                    .getSupplyChainNodes(topology, seedIds, entityFilter, new TraversalRulesLibrary<>());
    }
}
