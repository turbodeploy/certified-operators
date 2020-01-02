package com.vmturbo.topology.graph.supplychain;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;

import org.junit.Test;

import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.topology.graph.TestGraphEntity;
import com.vmturbo.topology.graph.TopologyGraph;

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
    private TopologyGraph<TestGraphEntity> cloudTopology;
    private SupplyChainNode region;
    private SupplyChainNode zone;
    private SupplyChainNode vm;
    private SupplyChainNode volume;
    private SupplyChainNode account;
    private SupplyChainNode app;

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
                TestGraphEntity.newBuilder(444L, UIEntityType.DISKARRAY),
                TestGraphEntity.newBuilder(22L, UIEntityType.STORAGE)
                    .addProviderId(444L),
                TestGraphEntity.newBuilder(33L, UIEntityType.STORAGE)
                    .addProviderId(444L),
                TestGraphEntity.newBuilder(1L, UIEntityType.VIRTUAL_MACHINE)
                    .addProviderId(22L)
                    .addProviderId(33L));

        final Map<Integer, SupplyChainNode> supplychain = getSupplyChain(graph, 1L);

        assertThat(supplychain.get(UIEntityType.VIRTUAL_MACHINE.typeNumber())
                              .getConnectedProviderTypesList(),
                   containsInAnyOrder(UIEntityType.STORAGE.apiStr()));
        assertThat(RepositoryDTOUtil.getMemberCount(
                        supplychain.get(UIEntityType.VIRTUAL_MACHINE.typeNumber())),
                   is(1));

        assertThat(supplychain.get(UIEntityType.STORAGE.typeNumber())
                              .getConnectedProviderTypesList(),
                   containsInAnyOrder(UIEntityType.DISKARRAY.apiStr()));
        assertThat(supplychain.get(UIEntityType.STORAGE.typeNumber())
                              .getConnectedConsumerTypesList(),
                   containsInAnyOrder(UIEntityType.VIRTUAL_MACHINE.apiStr()));
        assertThat(RepositoryDTOUtil.getMemberCount(
                        supplychain.get(UIEntityType.STORAGE.typeNumber())),
                   is(2));

        assertThat(supplychain.get(UIEntityType.DISKARRAY.typeNumber())
                              .getConnectedConsumerTypesList(),
                   containsInAnyOrder(UIEntityType.STORAGE.apiStr()));
        assertThat(RepositoryDTOUtil.getMemberCount(
                        supplychain.get(UIEntityType.DISKARRAY.typeNumber())),
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
                TestGraphEntity.newBuilder(444L, UIEntityType.DISKARRAY),
                TestGraphEntity.newBuilder(22L, UIEntityType.STORAGE)
                    .addProviderId(444L),
                TestGraphEntity.newBuilder(33L, UIEntityType.STORAGE),
                TestGraphEntity.newBuilder(1L, UIEntityType.VIRTUAL_MACHINE)
                    .addProviderId(22L));

        final Map<Integer, SupplyChainNode> supplychain = getSupplyChain(graph, 33L);

        assertThat(supplychain.size(), is(1));
        assertThat(RepositoryDTOUtil.getMemberCount(supplychain.get(UIEntityType.STORAGE.typeNumber())),
                   is(1));
        assertTrue(supplychain.get(UIEntityType.STORAGE.typeNumber())
                        .getConnectedProviderTypesList().isEmpty());
        assertTrue(supplychain.get(UIEntityType.STORAGE.typeNumber())
                        .getConnectedConsumerTypesList().isEmpty());
    }

    /*
     * TODO: Revisit this unit test once the behavior of VDCs in the supply chain
     * is agreed upon.  This is part of the task OM-51365
     *
     * @throws Exception If the test files can't be loaded.
     */
/*    @Test
    public void testVdcBuyingFromOtherVdc() {
        /*
         *   1 VM
         *  /
         * 22 VDC
         * |
         * 33 VDC
         * |
         * 44 PM
         */
  /*      final TopologyGraph<TestGraphEntity> graph = TestGraphEntity.newGraph(
            TestGraphEntity.newBuilder(44L, UIEntityType.PHYSICAL_MACHINE),
            TestGraphEntity.newBuilder(33L, UIEntityType.VIRTUAL_DATACENTER)
                .addProviderId(44L),
            TestGraphEntity.newBuilder(22L, UIEntityType.VIRTUAL_DATACENTER)
                .addProviderId(33L),
            TestGraphEntity.newBuilder(1L, UIEntityType.VIRTUAL_MACHINE)
                .addProviderId(22L)
        );

        final Map<UIEntityType, SupplyChainNode> supplychain =
            supplyChainResolver.getSupplyChainNodes(Collections.singleton(1L), graph, e -> true);
        assertThat(supplychain.keySet(), containsInAnyOrder(UIEntityType.VIRTUAL_MACHINE,
            UIEntityType.PHYSICAL_MACHINE, UIEntityType.VIRTUAL_DATACENTER));

        final SupplyChainNode vmNode = supplychain.get(UIEntityType.VIRTUAL_MACHINE);
        assertThat(RepositoryDTOUtil.getMemberCount(vmNode), is(1));
        assertThat(vmNode.getConnectedProviderTypesList(), containsInAnyOrder(UIEntityType.VIRTUAL_DATACENTER.apiStr()));

        final SupplyChainNode vdcNode = supplychain.get(UIEntityType.VIRTUAL_DATACENTER);
        assertThat(RepositoryDTOUtil.getMemberCount(vdcNode), is(2));
        assertThat(vdcNode.getConnectedConsumerTypesList(),
            containsInAnyOrder(UIEntityType.VIRTUAL_MACHINE.apiStr(), UIEntityType.VIRTUAL_DATACENTER.apiStr()));
        assertThat(vdcNode.getConnectedProviderTypesList(),
            containsInAnyOrder(UIEntityType.PHYSICAL_MACHINE.apiStr(), UIEntityType.VIRTUAL_DATACENTER.apiStr()));

        final SupplyChainNode pmNode = supplychain.get(UIEntityType.PHYSICAL_MACHINE);
        assertThat(RepositoryDTOUtil.getMemberCount(pmNode), is(1));
        assertThat(pmNode.getConnectedConsumerTypesList(), containsInAnyOrder(UIEntityType.VIRTUAL_DATACENTER.apiStr()));
    }*/

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
                TestGraphEntity.newBuilder(444L, UIEntityType.DISKARRAY),
                TestGraphEntity.newBuilder(33L, UIEntityType.STORAGE)
                        .addProviderId(444L),
                // Buying from storage.
                TestGraphEntity.newBuilder(2L, UIEntityType.VIRTUAL_MACHINE)
                        .addProviderId(33L),
                // Buying from the DA directly.
                TestGraphEntity.newBuilder(1L, UIEntityType.VIRTUAL_MACHINE)
                        .addProviderId(444L));

        final Map<Integer, SupplyChainNode> supplychain = getSupplyChain(graph, 444L);

        assertThat(supplychain.keySet(),
                   containsInAnyOrder(UIEntityType.VIRTUAL_MACHINE.typeNumber(),
                                      UIEntityType.STORAGE.typeNumber(),
                                      UIEntityType.DISKARRAY.typeNumber()));

        final SupplyChainNode vmNode = supplychain.get(UIEntityType.VIRTUAL_MACHINE.typeNumber());
        assertThat(RepositoryDTOUtil.getMemberCount(vmNode), is(2));
        assertThat(vmNode.getConnectedProviderTypesList(),
                   containsInAnyOrder(UIEntityType.DISKARRAY.apiStr(), UIEntityType.STORAGE.apiStr()));

        final SupplyChainNode stNode = supplychain.get(UIEntityType.STORAGE.typeNumber());
        assertThat(RepositoryDTOUtil.getMemberCount(stNode), is(1));
        // No VM provider, because the VM that's buying from the storage directly is not in
        // the DA's supply chain.
        assertThat(stNode.getConnectedProviderTypesList(),
                   containsInAnyOrder(UIEntityType.DISKARRAY.apiStr()));

        final SupplyChainNode daNode = supplychain.get(UIEntityType.DISKARRAY.typeNumber());
        assertThat(RepositoryDTOUtil.getMemberCount(daNode), is(1));
        assertThat(daNode.getConnectedConsumerTypesList(),
                   containsInAnyOrder(UIEntityType.VIRTUAL_MACHINE.apiStr(),
                                      UIEntityType.STORAGE.apiStr()));
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
               TestGraphEntity.newBuilder(vm1Id, UIEntityType.VIRTUAL_MACHINE)
                   .addProviderId(pmId)
                   .addProviderId(st1Id),
               TestGraphEntity.newBuilder(vm2Id, UIEntityType.VIRTUAL_MACHINE)
                   .addProviderId(pmId)
                   .addProviderId(st2Id),
               TestGraphEntity.newBuilder(pmId, UIEntityType.PHYSICAL_MACHINE)
                   .addProviderId(st1Id)
                   .addProviderId(st2Id),
               TestGraphEntity.newBuilder(st1Id, UIEntityType.STORAGE),
               TestGraphEntity.newBuilder(st2Id, UIEntityType.STORAGE));

       final Map<Integer, SupplyChainNode> supplychainFromVM1 = getSupplyChain(graph, vm1Id);
       final Map<Integer, SupplyChainNode> supplychainFromPM = getSupplyChain(graph, pmId);

       commonForSpecialPMStorageRule(supplychainFromVM1);
       commonForSpecialPMStorageRule(supplychainFromPM);

       final SupplyChainNode vmNodeFromChainScopedOnVM1 =
               supplychainFromVM1.get(UIEntityType.VIRTUAL_MACHINE.typeNumber());
       final SupplyChainNode pmNodeFromChainScopedOnVM1 =
               supplychainFromVM1.get(UIEntityType.PHYSICAL_MACHINE.typeNumber());
       final SupplyChainNode stNodeFromChainScopedOnVM1 =
               supplychainFromVM1.get(UIEntityType.STORAGE.typeNumber());
       assertThat(getAllNodeIds(vmNodeFromChainScopedOnVM1),
                  containsInAnyOrder(vm1Id));
       assertThat(getAllNodeIds(pmNodeFromChainScopedOnVM1),
                  containsInAnyOrder(pmId));
       assertThat(getAllNodeIds(stNodeFromChainScopedOnVM1),
                  containsInAnyOrder(st1Id));

       final SupplyChainNode vmNodeFromChainScopedOnPM =
               supplychainFromPM.get(UIEntityType.VIRTUAL_MACHINE.typeNumber());
       final SupplyChainNode pmNodeFromChainScopedOnPM =
               supplychainFromPM.get(UIEntityType.PHYSICAL_MACHINE.typeNumber());
       final SupplyChainNode stNodeFromChainScopedOnPM =
               supplychainFromPM.get(UIEntityType.STORAGE.typeNumber());
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
                  containsInAnyOrder(UIEntityType.VIRTUAL_MACHINE.typeNumber(),
                                     UIEntityType.STORAGE.typeNumber(),
                                     UIEntityType.PHYSICAL_MACHINE.typeNumber()));

       assertThat(supplychain.get(UIEntityType.VIRTUAL_MACHINE.typeNumber()).getConnectedProviderTypesList(),
                  containsInAnyOrder(UIEntityType.STORAGE.apiStr(),
                                     UIEntityType.PHYSICAL_MACHINE.apiStr()));
       assertThat(supplychain.get(UIEntityType.VIRTUAL_MACHINE.typeNumber()).getConnectedConsumerTypesList(),
                  containsInAnyOrder());

       assertThat(supplychain.get(UIEntityType.PHYSICAL_MACHINE.typeNumber())
                       .getConnectedProviderTypesList(),
                  containsInAnyOrder(UIEntityType.STORAGE.apiStr()));
       assertThat(supplychain.get(UIEntityType.PHYSICAL_MACHINE.typeNumber())
                       .getConnectedConsumerTypesList(),
                  containsInAnyOrder(UIEntityType.VIRTUAL_MACHINE.apiStr()));

       assertThat(supplychain.get(UIEntityType.STORAGE.typeNumber()).getConnectedConsumerTypesList(),
                  containsInAnyOrder(UIEntityType.VIRTUAL_MACHINE.apiStr(),
                                     UIEntityType.PHYSICAL_MACHINE.apiStr()));
       assertThat(supplychain.get(UIEntityType.STORAGE.typeNumber()).getConnectedProviderTypesList(),
                  containsInAnyOrder());
   }

    /**
     * Tests that scoping on storage includes that related DC in the result
     * and the reverse: scoping on DC includes related storage in the result.
     */
   @Test
   public void testDCAndStorageRelationship() {
       /*
        * Topology:
        *     PM
        *    /  \
        *   ST  DC
        */
        final TopologyGraph<TestGraphEntity> graph =
            TestGraphEntity.newGraph(
                TestGraphEntity.newBuilder(PM_ID, UIEntityType.PHYSICAL_MACHINE)
                    .addProviderId(ST_ID)
                    .addProviderId(DC_ID),
               TestGraphEntity.newBuilder(ST_ID, UIEntityType.STORAGE),
               TestGraphEntity.newBuilder(DC_ID, UIEntityType.DATACENTER));

       final Map<Integer, SupplyChainNode> supplychainFromST = getSupplyChain(graph, ST_ID);
       final Map<Integer, SupplyChainNode> supplychainFromDC = getSupplyChain(graph, DC_ID);

       commonForDCAndStorageRelationship(supplychainFromDC);
       commonForDCAndStorageRelationship(supplychainFromST);
   }

   private void commonForDCAndStorageRelationship(@Nonnull Map<Integer, SupplyChainNode> supplychain) {
       assertThat(supplychain.keySet(),
                  containsInAnyOrder(UIEntityType.DATACENTER.typeNumber(),
                                     UIEntityType.STORAGE.typeNumber(),
                                     UIEntityType.PHYSICAL_MACHINE.typeNumber()));

       final SupplyChainNode pmNode = supplychain.get(UIEntityType.PHYSICAL_MACHINE.typeNumber());
       final SupplyChainNode stNode = supplychain.get(UIEntityType.STORAGE.typeNumber());
       final SupplyChainNode dcNode = supplychain.get(UIEntityType.DATACENTER.typeNumber());

       assertThat(pmNode.getConnectedProviderTypesList(),
                  containsInAnyOrder(UIEntityType.DATACENTER.apiStr(),
                                     UIEntityType.STORAGE.apiStr()));
       assertThat(pmNode.getConnectedConsumerTypesList(), containsInAnyOrder());

       assertThat(dcNode.getConnectedConsumerTypesList(),
                  containsInAnyOrder(UIEntityType.PHYSICAL_MACHINE.apiStr()));
       assertThat(dcNode.getConnectedProviderTypesList(), containsInAnyOrder());

       assertThat(stNode.getConnectedConsumerTypesList(),
                  containsInAnyOrder(UIEntityType.PHYSICAL_MACHINE.apiStr()));
       assertThat(stNode.getConnectedProviderTypesList(), containsInAnyOrder());

       assertThat(getAllNodeIds(pmNode), containsInAnyOrder(PM_ID));
       assertThat(getAllNodeIds(stNode), containsInAnyOrder(ST_ID));
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
       assertFullCloudSupplyChain(supplyChain, false);
   }

    /**
     * Test the cloud topology, scoping on zone.
     */
    @Test
    public void testZoneScope() {
        createCloudTopology();
        final Map<Integer, SupplyChainNode> supplyChain = getSupplyChain(cloudTopology, ZONE_ID);
        populateCloudEntityFields(supplyChain);
        assertFullCloudSupplyChain(supplyChain, false);
    }

    /**
     * Test the cloud topology, scoping on VM1.
     */
    @Test
    public void testVM1Scope() {
        createCloudTopology();
        final Map<Integer, SupplyChainNode> supplyChain = getSupplyChain(cloudTopology, VM_1_ID);
        populateCloudEntityFields(supplyChain);
        assertFullCloudSupplyChain(supplyChain, true);
    }

    /**
     * Test the cloud topology, scoping on account.
     */
    @Test
    public void testAccountScope() {
        createCloudTopology();
        final Map<Integer, SupplyChainNode> supplyChain = getSupplyChain(cloudTopology, ACCOUNT_ID);
        populateCloudEntityFields(supplyChain);
        assertFullCloudSupplyChain(supplyChain, true);
    }

    /**
     * Test the cloud topology, scoping on volume.
     */
    @Test
    public void testVolumeScope() {
        createCloudTopology();
        final Map<Integer, SupplyChainNode> supplyChain = getSupplyChain(cloudTopology, VOLUME_ID);
        populateCloudEntityFields(supplyChain);
        assertFullCloudSupplyChain(supplyChain, true);
    }

    /**
     * Test the cloud topology, scoping on the application.
     */
    @Test
    public void testAppScope() {
        createCloudTopology();
        final Map<Integer, SupplyChainNode> supplyChain = getSupplyChain(cloudTopology, APP_ID);
        populateCloudEntityFields(supplyChain);
        assertFullCloudSupplyChain(supplyChain, true);
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
                   containsInAnyOrder(UIEntityType.REGION.typeNumber(),
                                      UIEntityType.AVAILABILITY_ZONE.typeNumber(),
                                      UIEntityType.VIRTUAL_MACHINE.typeNumber()));

        assertThat(getAllNodeIds(region), containsInAnyOrder(REGION_ID));
        assertThat(getAllNodeIds(zone), containsInAnyOrder(ZONE_ID));
        assertThat(getAllNodeIds(vm), containsInAnyOrder(VM_2_ID));

        assertThat(region.getConnectedConsumerTypesList(),
                   containsInAnyOrder(UIEntityType.AVAILABILITY_ZONE.apiStr()));
        assertThat(region.getConnectedProviderTypesList(), containsInAnyOrder());

        assertThat(zone.getConnectedConsumerTypesList(),
                   containsInAnyOrder(UIEntityType.VIRTUAL_MACHINE.apiStr()));
        assertThat(zone.getConnectedProviderTypesList(), containsInAnyOrder(UIEntityType.REGION.apiStr()));

        assertThat(vm.getConnectedConsumerTypesList(), containsInAnyOrder());
        assertThat(vm.getConnectedProviderTypesList(),
                   containsInAnyOrder(UIEntityType.AVAILABILITY_ZONE.apiStr()));
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
               TestGraphEntity.newBuilder(VOLUME_ID, UIEntityType.VIRTUAL_VOLUME);
       final TestGraphEntity.Builder vm1Builder =
               TestGraphEntity.newBuilder(VM_1_ID, UIEntityType.VIRTUAL_MACHINE);
       final TestGraphEntity.Builder vm2Builder =
               TestGraphEntity.newBuilder(VM_2_ID, UIEntityType.VIRTUAL_MACHINE);
       final TestGraphEntity.Builder appBuilder =
               TestGraphEntity.newBuilder(APP_ID, UIEntityType.APPLICATION);
       final TestGraphEntity.Builder zoneBuilder =
               TestGraphEntity.newBuilder(ZONE_ID, UIEntityType.AVAILABILITY_ZONE);
       final TestGraphEntity.Builder regionBuilder =
               TestGraphEntity.newBuilder(REGION_ID, UIEntityType.REGION);
       final TestGraphEntity.Builder accountBuilder =
               TestGraphEntity.newBuilder(ACCOUNT_ID, UIEntityType.BUSINESS_ACCOUNT);

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

   private void assertFullCloudSupplyChain(
           @Nonnull Map<Integer, SupplyChainNode> supplyChain, boolean excludeVM2) {
       assertThat(supplyChain.keySet(),
                  containsInAnyOrder(UIEntityType.REGION.typeNumber(),
                                     UIEntityType.AVAILABILITY_ZONE.typeNumber(),
                                     UIEntityType.VIRTUAL_MACHINE.typeNumber(),
                                     UIEntityType.VIRTUAL_VOLUME.typeNumber(),
                                     UIEntityType.BUSINESS_ACCOUNT.typeNumber(),
                                     UIEntityType.APPLICATION.typeNumber()));

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
                  containsInAnyOrder(UIEntityType.AVAILABILITY_ZONE.apiStr()));
       assertThat(region.getConnectedProviderTypesList(), containsInAnyOrder());

       assertThat(zone.getConnectedConsumerTypesList(),
                  containsInAnyOrder(UIEntityType.VIRTUAL_MACHINE.apiStr(),
                                     UIEntityType.VIRTUAL_VOLUME.apiStr()));
       assertThat(zone.getConnectedProviderTypesList(),
                  containsInAnyOrder(UIEntityType.REGION.apiStr()));

       assertThat(vm.getConnectedConsumerTypesList(),
                  containsInAnyOrder(UIEntityType.APPLICATION.apiStr()));
       assertThat(vm.getConnectedProviderTypesList(),
                  containsInAnyOrder(UIEntityType.VIRTUAL_VOLUME.apiStr(),
                                     UIEntityType.AVAILABILITY_ZONE.apiStr(),
                                     UIEntityType.BUSINESS_ACCOUNT.apiStr()));

       assertThat(volume.getConnectedConsumerTypesList(),
                  containsInAnyOrder(UIEntityType.VIRTUAL_MACHINE.apiStr()));
       assertThat(volume.getConnectedProviderTypesList(),
                  containsInAnyOrder(UIEntityType.AVAILABILITY_ZONE.apiStr(),
                                     UIEntityType.BUSINESS_ACCOUNT.apiStr()));

       assertThat(account.getConnectedProviderTypesList(), containsInAnyOrder());
       assertThat(account.getConnectedConsumerTypesList(),
                  containsInAnyOrder(UIEntityType.VIRTUAL_MACHINE.apiStr(),
                                     UIEntityType.VIRTUAL_VOLUME.apiStr(),
                                     UIEntityType.APPLICATION.apiStr()));

       assertThat(app.getConnectedConsumerTypesList(), containsInAnyOrder());
       assertThat(app.getConnectedProviderTypesList(),
                  containsInAnyOrder(UIEntityType.VIRTUAL_MACHINE.apiStr(),
                                     UIEntityType.BUSINESS_ACCOUNT.apiStr()));
   }

   private void populateCloudEntityFields(@Nonnull Map<Integer, SupplyChainNode> graph) {
       region = graph.get(UIEntityType.REGION.typeNumber());
       zone = graph.get(UIEntityType.AVAILABILITY_ZONE.typeNumber());
       vm = graph.get(UIEntityType.VIRTUAL_MACHINE.typeNumber());
       volume = graph.get(UIEntityType.VIRTUAL_VOLUME.typeNumber());
       account = graph.get(UIEntityType.BUSINESS_ACCOUNT.typeNumber());
       app = graph.get(UIEntityType.APPLICATION.typeNumber());
   }

   private Collection<Long> getAllNodeIds(@Nonnull SupplyChainNode supplyChainNode) {
       return supplyChainNode.getMembersByStateMap().values().stream()
                    .flatMap(m -> m.getMemberOidsList().stream())
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
                TestGraphEntity.newBuilder(444L, UIEntityType.PHYSICAL_MACHINE),
                TestGraphEntity.newBuilder(33L, UIEntityType.VIRTUAL_MACHINE) // <-- starting point
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
                TestGraphEntity.newBuilder(444L, UIEntityType.PHYSICAL_MACHINE),
                TestGraphEntity.newBuilder(33L, UIEntityType.VIRTUAL_MACHINE)
                    .addProviderId(444L),
                TestGraphEntity.newBuilder(1L, UIEntityType.APPLICATION) // <-- starting point
                    .addProviderId(33L));

        final Map<Integer, SupplyChainNode> supplychain = getSupplyChain(graph, 1L,
                                                                         e -> e.getOid() != 33L);

        // Only the VM is in the supply chain. The PM didn't match the predicate, and the ST is
        // only accessible from the PM.
        assertThat(supplychain.keySet(), containsInAnyOrder(UIEntityType.APPLICATION.typeNumber()));
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
                TestGraphEntity.newBuilder(33L, UIEntityType.PHYSICAL_MACHINE),
                TestGraphEntity.newBuilder(22L, UIEntityType.PHYSICAL_MACHINE),
                TestGraphEntity.newBuilder(2L, UIEntityType.VIRTUAL_MACHINE) // <-- starting point
                    .addProviderId(33L),
                TestGraphEntity.newBuilder(1L, UIEntityType.VIRTUAL_MACHINE) // <-- starting point
                    .addProviderId(22L));

        final Map<Integer, SupplyChainNode> supplychain = getSupplyChain(graph, ImmutableSet.of(1L, 2L));

        assertThat(supplychain.keySet(),
                   containsInAnyOrder(UIEntityType.VIRTUAL_MACHINE.typeNumber(),
                                      UIEntityType.PHYSICAL_MACHINE.typeNumber()));
        assertThat(RepositoryDTOUtil.getMemberCount(supplychain.get(UIEntityType.VIRTUAL_MACHINE.typeNumber())),
                                                    is(2));
        assertThat(RepositoryDTOUtil.getMemberCount(supplychain.get(UIEntityType.PHYSICAL_MACHINE.typeNumber())),
                                                    is(2));

        assertThat(supplychain.get(UIEntityType.PHYSICAL_MACHINE.typeNumber()).getConnectedConsumerTypesList(),
                                   containsInAnyOrder(UIEntityType.VIRTUAL_MACHINE.apiStr()));
        assertThat(supplychain.get(UIEntityType.VIRTUAL_MACHINE.typeNumber()).getConnectedProviderTypesList(),
                                   containsInAnyOrder(UIEntityType.PHYSICAL_MACHINE.apiStr()));
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
                TestGraphEntity.newBuilder(33L, UIEntityType.PHYSICAL_MACHINE),
                TestGraphEntity.newBuilder(22L, UIEntityType.PHYSICAL_MACHINE), // <-- starting point
                TestGraphEntity.newBuilder(2L, UIEntityType.VIRTUAL_MACHINE) // <-- starting point
                        .addProviderId(33L),
                TestGraphEntity.newBuilder(1L, UIEntityType.VIRTUAL_MACHINE)
                        .addProviderId(22L));

        final Map<Integer, SupplyChainNode> supplychain = getSupplyChain(graph, ImmutableSet.of(1L, 33L));

        assertThat(supplychain.keySet(),
                   containsInAnyOrder(UIEntityType.VIRTUAL_MACHINE.typeNumber(),
                                      UIEntityType.PHYSICAL_MACHINE.typeNumber()));
        assertThat(RepositoryDTOUtil.getMemberCount(supplychain.get(UIEntityType.VIRTUAL_MACHINE.typeNumber())),
                   is(2));
        assertThat(RepositoryDTOUtil.getMemberCount(supplychain.get(UIEntityType.PHYSICAL_MACHINE.typeNumber())),
                   is(2));

        assertThat(supplychain.get(UIEntityType.PHYSICAL_MACHINE.typeNumber()).getConnectedConsumerTypesList(),
                   containsInAnyOrder(UIEntityType.VIRTUAL_MACHINE.apiStr()));
        assertThat(supplychain.get(UIEntityType.VIRTUAL_MACHINE.typeNumber()).getConnectedProviderTypesList(),
                   containsInAnyOrder(UIEntityType.PHYSICAL_MACHINE.apiStr()));
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
            TestGraphEntity.newGraph(TestGraphEntity.newBuilder(daId, UIEntityType.DISKARRAY),
                                     TestGraphEntity.newBuilder(stId, UIEntityType.STORAGE)
                                        .addProviderId(daId),
                                     TestGraphEntity.newBuilder(volId, UIEntityType.VIRTUAL_VOLUME)
                                        .addConnectedEntity(stId, ConnectionType.NORMAL_CONNECTION));

        final Map<Integer, SupplyChainNode> supplychain = getSupplyChain(graph, volId);

        assertThat(supplychain.get(UIEntityType.VIRTUAL_VOLUME.typeNumber())
                        .getConnectedProviderTypesList(),
                   containsInAnyOrder(UIEntityType.STORAGE.apiStr()));
        assertThat(RepositoryDTOUtil.getMemberCount(
                        supplychain.get(UIEntityType.VIRTUAL_VOLUME.typeNumber())),
                   is(1));
        assertThat(supplychain.get(UIEntityType.STORAGE.typeNumber())
                        .getConnectedProviderTypesList(),
                   containsInAnyOrder(UIEntityType.DISKARRAY.apiStr()));
        assertThat(supplychain.get(UIEntityType.STORAGE.typeNumber())
                        .getConnectedConsumerTypesList(),
                   containsInAnyOrder(UIEntityType.VIRTUAL_VOLUME.apiStr()));
        assertThat(RepositoryDTOUtil.getMemberCount(
                        supplychain.get(UIEntityType.STORAGE.typeNumber())),
                   is(1));
        assertThat(supplychain.get(UIEntityType.DISKARRAY.typeNumber())
                        .getConnectedConsumerTypesList(),
                   containsInAnyOrder(UIEntityType.STORAGE.apiStr()));
        assertThat(RepositoryDTOUtil.getMemberCount(
                        supplychain.get(UIEntityType.DISKARRAY.typeNumber())),
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
                TestGraphEntity.newBuilder(acc1Id, UIEntityType.BUSINESS_ACCOUNT)
                    .addConnectedEntity(acc2Id, ConnectionType.OWNS_CONNECTION)
                    .addConnectedEntity(vm1Id, ConnectionType.OWNS_CONNECTION),
                TestGraphEntity.newBuilder(acc2Id, UIEntityType.BUSINESS_ACCOUNT)
                    .addConnectedEntity(vm2Id, ConnectionType.OWNS_CONNECTION),
                TestGraphEntity.newBuilder(vm1Id, UIEntityType.VIRTUAL_MACHINE)
                    .addConnectedEntity(regId, ConnectionType.AGGREGATED_BY_CONNECTION),
                TestGraphEntity.newBuilder(vm2Id, UIEntityType.VIRTUAL_MACHINE)
                    .addConnectedEntity(regId, ConnectionType.AGGREGATED_BY_CONNECTION),
                TestGraphEntity.newBuilder(regId, UIEntityType.REGION));

        final Map<Integer, SupplyChainNode> supplychain = getSupplyChain(graph, acc1Id);
        assertThat(RepositoryDTOUtil.getMemberCount(
                        supplychain.get(UIEntityType.BUSINESS_ACCOUNT.typeNumber())),
                   is(1));
        assertEquals(acc1Id, supplychain.get(UIEntityType.BUSINESS_ACCOUNT.typeNumber())
                                    .getMembersByStateMap().values().iterator().next()
                                    .getMemberOids(0));
        assertThat(RepositoryDTOUtil.getMemberCount(
                        supplychain.get(UIEntityType.VIRTUAL_MACHINE.typeNumber())),
                   is(1));
        assertEquals(vm1Id, supplychain.get(UIEntityType.VIRTUAL_MACHINE.typeNumber())
                                    .getMembersByStateMap().values().iterator().next()
                                    .getMemberOids(0));
        assertThat(RepositoryDTOUtil.getMemberCount(
                        supplychain.get(UIEntityType.REGION.typeNumber())),
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
                TestGraphEntity.newBuilder(REG_ID, UIEntityType.REGION)
                        .addConnectedEntity(AZ_ID, ConnectionType.OWNS_CONNECTION),
                TestGraphEntity.newBuilder(AZ_ID, UIEntityType.AVAILABILITY_ZONE),
                TestGraphEntity.newBuilder(VOL_ID, UIEntityType.VIRTUAL_VOLUME)
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
                TestGraphEntity.newBuilder(REG_ID, UIEntityType.REGION),
                TestGraphEntity.newBuilder(VOL_ID, UIEntityType.VIRTUAL_VOLUME)
                        .addConnectedEntity(REG_ID, ConnectionType.AGGREGATED_BY_CONNECTION));
    }

    private void checkAWSTopologyWithOrphanVolume(Map<Integer, SupplyChainNode> supplychain) {
        assertThat(supplychain.get(UIEntityType.VIRTUAL_VOLUME.typeNumber())
                        .getConnectedProviderTypesList(),
                containsInAnyOrder(UIEntityType.AVAILABILITY_ZONE.apiStr()));
        assertThat(RepositoryDTOUtil.getMemberCount(
                supplychain.get(UIEntityType.VIRTUAL_VOLUME.typeNumber())),
                is(1));
        assertThat(supplychain.get(UIEntityType.AVAILABILITY_ZONE.typeNumber())
                        .getConnectedConsumerTypesList(),
                containsInAnyOrder(UIEntityType.VIRTUAL_VOLUME.apiStr()));
        assertThat(supplychain.get(UIEntityType.AVAILABILITY_ZONE.typeNumber())
                        .getConnectedProviderTypesList(),
                containsInAnyOrder(UIEntityType.REGION.apiStr()));
        assertThat(RepositoryDTOUtil.getMemberCount(
                supplychain.get(UIEntityType.AVAILABILITY_ZONE.typeNumber())),
                is(1));
        assertThat(supplychain.get(UIEntityType.REGION.typeNumber())
                        .getConnectedConsumerTypesList(),
                containsInAnyOrder(UIEntityType.AVAILABILITY_ZONE.apiStr()));
        assertThat(RepositoryDTOUtil.getMemberCount(
                supplychain.get(UIEntityType.REGION.typeNumber())),
                is(1));
    }

    private void checkAzureTopologyWithOrphanVolume(Map<Integer, SupplyChainNode> supplychain) {
        assertThat(supplychain.get(UIEntityType.VIRTUAL_VOLUME.typeNumber())
                        .getConnectedProviderTypesList(),
                containsInAnyOrder(UIEntityType.REGION.apiStr()));
        assertThat(RepositoryDTOUtil.getMemberCount(
                supplychain.get(UIEntityType.VIRTUAL_VOLUME.typeNumber())),
                is(1));
        assertThat(supplychain.get(UIEntityType.REGION.typeNumber())
                        .getConnectedConsumerTypesList(),
                containsInAnyOrder(UIEntityType.VIRTUAL_VOLUME.apiStr()));
        assertThat(RepositoryDTOUtil.getMemberCount(
                supplychain.get(UIEntityType.REGION.typeNumber())),
                is(1));
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
