package com.vmturbo.topology.graph.supplychain;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.Map;

import org.junit.Test;

import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode.MemberList;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.UIEntityState;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.topology.graph.TestGraphEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.graph.supplychain.SupplyChainResolver.SupplyChainNodeBuilder;

public class SupplyChainResolverTest {

    private final SupplyChainResolver<TestGraphEntity> supplyChainResolver =
        new SupplyChainResolver<>();

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
        final TopologyGraph<TestGraphEntity> graph = TestGraphEntity.newGraph(
            TestGraphEntity.newBuilder(444L, UIEntityType.DISKARRAY),
            TestGraphEntity.newBuilder(22L, UIEntityType.STORAGE)
                .addProviderId(444L),
            TestGraphEntity.newBuilder(33L, UIEntityType.STORAGE)
                .addProviderId(444L),
            TestGraphEntity.newBuilder(1L, UIEntityType.VIRTUAL_MACHINE)
                .addProviderId(22L)
                .addProviderId(33L)
        );

        final Map<UIEntityType, SupplyChainNode> supplychain =
            supplyChainResolver.getSupplyChainNodes(Collections.singleton(1L), graph, e -> true);
        assertThat(supplychain.get(UIEntityType.VIRTUAL_MACHINE).getConnectedProviderTypesList(),
            containsInAnyOrder(UIEntityType.STORAGE.apiStr()));
        assertThat(RepositoryDTOUtil.getMemberCount(supplychain.get(UIEntityType.VIRTUAL_MACHINE)), is(1));

        assertThat(supplychain.get(UIEntityType.STORAGE).getConnectedProviderTypesList(),
            containsInAnyOrder(UIEntityType.DISKARRAY.apiStr()));
        assertThat(supplychain.get(UIEntityType.STORAGE).getConnectedConsumerTypesList(),
            containsInAnyOrder(UIEntityType.VIRTUAL_MACHINE.apiStr()));
        assertThat(RepositoryDTOUtil.getMemberCount(supplychain.get(UIEntityType.STORAGE)), is(2));

        assertThat(supplychain.get(UIEntityType.DISKARRAY).getConnectedConsumerTypesList(),
            containsInAnyOrder(UIEntityType.STORAGE.apiStr()));
        assertThat(RepositoryDTOUtil.getMemberCount(supplychain.get(UIEntityType.DISKARRAY)), is(1));
    }

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
        final TopologyGraph<TestGraphEntity> graph = TestGraphEntity.newGraph(
            TestGraphEntity.newBuilder(444L, UIEntityType.DISKARRAY),
            TestGraphEntity.newBuilder(22L, UIEntityType.STORAGE)
                .addProviderId(444L),
            TestGraphEntity.newBuilder(33L, UIEntityType.STORAGE),
            TestGraphEntity.newBuilder(1L, UIEntityType.VIRTUAL_MACHINE)
                .addProviderId(22L)
        );

        final Map<UIEntityType, SupplyChainNode> supplychain =
            supplyChainResolver.getSupplyChainNodes(Collections.singleton(33L), graph, e -> true);
        assertThat(supplychain.size(), is(1));
        assertThat(RepositoryDTOUtil.getMemberCount(supplychain.get(UIEntityType.STORAGE)), is(1));
        assertTrue(supplychain.get(UIEntityType.STORAGE).getConnectedProviderTypesList().isEmpty());
        assertTrue(supplychain.get(UIEntityType.STORAGE).getConnectedConsumerTypesList().isEmpty());
    }

    /**
     * Ensure self-loops in the supply chain introduced by VDC's that buy from other VDC's can be correctly
     * handled. If not properly handled, the supply chain does not contain any links below VDC (ie host,
     * datacenter, etc.)
     *
     * @throws Exception If the test files can't be loaded.
     */
    @Test
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
        final TopologyGraph<TestGraphEntity> graph = TestGraphEntity.newGraph(
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
    }

    /**
     * This tests the situation, in which an entity type can be found in two (or more) different depths.
     * Let A, B, and C represent entity types.  Let A1, A2 be entities of type A, B1 entity of type B,
     * and C1 entity of type C.  Let A1 consume from B1 and A2 from C2.  Consider the supply chain validation
     * graph generated with C1 as a starting point.
     *
     * Since C1 has a direct consumer A2, the indirect consumer A1 must be ignored.
     * In this test, we should see in the final graph all 3 entity types each having exactly one entity.
     */
    @Test
    public void testOneEntityTypeInMultipleDepths() {
        /*                 A (2 entities; one consumes from C and one from B)
         *               / |
         *   (1 entity) B  |
         *               \ |
         *                 C (1 entity) <-- starting point
         */
        final TopologyGraph<TestGraphEntity> graph = TestGraphEntity.newGraph(
            TestGraphEntity.newBuilder(444L, UIEntityType.DISKARRAY), // <-- starting point
            TestGraphEntity.newBuilder(33L, UIEntityType.STORAGE)
                .addProviderId(444L),
            // Buying from storage.
            TestGraphEntity.newBuilder(2L, UIEntityType.VIRTUAL_MACHINE)
                .addProviderId(33L),
            // Buying from the DA directly.
            TestGraphEntity.newBuilder(1L, UIEntityType.VIRTUAL_MACHINE)
                .addProviderId(444L)
        );

        final Map<UIEntityType, SupplyChainNode> supplychain =
            supplyChainResolver.getSupplyChainNodes(Collections.singleton(444L), graph, e -> true);
        assertThat(supplychain.keySet(), containsInAnyOrder(UIEntityType.VIRTUAL_MACHINE,
            UIEntityType.STORAGE, UIEntityType.DISKARRAY));

        final SupplyChainNode vmNode = supplychain.get(UIEntityType.VIRTUAL_MACHINE);
        assertThat(RepositoryDTOUtil.getMemberCount(vmNode), is(1));
        assertThat(vmNode.getConnectedProviderTypesList(), containsInAnyOrder(UIEntityType.DISKARRAY.apiStr()));

        final SupplyChainNode stNode = supplychain.get(UIEntityType.STORAGE);
        assertThat(RepositoryDTOUtil.getMemberCount(stNode), is(1));
        // No VM provider, because the VM that's buying from the storage directly is not in
        // the DA's supply chain.
        assertThat(stNode.getConnectedProviderTypesList(), containsInAnyOrder(UIEntityType.DISKARRAY.apiStr()));

        final SupplyChainNode daNode = supplychain.get(UIEntityType.DISKARRAY);
        assertThat(RepositoryDTOUtil.getMemberCount(daNode), is(1));
        assertThat(daNode.getConnectedConsumerTypesList(), containsInAnyOrder(UIEntityType.VIRTUAL_MACHINE.apiStr(), UIEntityType.STORAGE.apiStr()));

    }

    /**
     * This test is similar to {@link #testOneEntityTypeInMultipleDepths()}, but with a twist.  The
     * entities are now as follows: C=DiskArray, B=LogicalPool, A=Storage.  Since edges between
     * LogicalPool and DiskArray are mandatory, neither the direct consumer A2 nor the indirect consumer
     * A1 must be ignored.  The graph must contain 1 disk array, 1 logical pool, and 2 storage.
     */
    @Test
    public void testMandatoryEdges() {
        /*                 Storage (2 entities; one consumes from LogicalPool and one from DiskArray)
         *               /           |
         *(1 entity) LogicalPool     |
         *               \           |
         *              DiskArray (1 entity)
         */
        final TopologyGraph<TestGraphEntity> graph = TestGraphEntity.newGraph(
            TestGraphEntity.newBuilder(444L, UIEntityType.DISKARRAY), // <-- starting point
            TestGraphEntity.newBuilder(33L, UIEntityType.LOGICALPOOL)
                .addProviderId(444L),
            // Buying from logical pool.
            TestGraphEntity.newBuilder(2L, UIEntityType.STORAGE)
                .addProviderId(33L),
            // Buying from the DA directly.
            TestGraphEntity.newBuilder(1L, UIEntityType.STORAGE)
                .addProviderId(444L)
        );

        final Map<UIEntityType, SupplyChainNode> supplychain =
            supplyChainResolver.getSupplyChainNodes(Collections.singleton(444L), graph, e -> true);
        assertThat(supplychain.keySet(), containsInAnyOrder(UIEntityType.DISKARRAY,
            UIEntityType.STORAGE, UIEntityType.LOGICALPOOL));

        final SupplyChainNode stNode = supplychain.get(UIEntityType.STORAGE);
        // Both entities!
        assertThat(RepositoryDTOUtil.getMemberCount(stNode), is(2));
        assertThat(stNode.getConnectedProviderTypesList(),
            containsInAnyOrder(UIEntityType.DISKARRAY.apiStr(), UIEntityType.LOGICALPOOL.apiStr()));

        final SupplyChainNode lpNode = supplychain.get(UIEntityType.LOGICALPOOL);
        assertThat(RepositoryDTOUtil.getMemberCount(lpNode), is(1));
        assertThat(lpNode.getConnectedProviderTypesList(), containsInAnyOrder(UIEntityType.DISKARRAY.apiStr()));
        assertThat(lpNode.getConnectedConsumerTypesList(), containsInAnyOrder(UIEntityType.STORAGE.apiStr()));

        final SupplyChainNode daNode = supplychain.get(UIEntityType.DISKARRAY);
        assertThat(RepositoryDTOUtil.getMemberCount(daNode), is(1));
        assertThat(daNode.getConnectedConsumerTypesList(), containsInAnyOrder(UIEntityType.LOGICALPOOL.apiStr(), UIEntityType.STORAGE.apiStr()));
    }

    @Test
    public void testStartingPointNoMatchPredicate() {
        final TopologyGraph<TestGraphEntity> graph = TestGraphEntity.newGraph(
            TestGraphEntity.newBuilder(444L, UIEntityType.PHYSICAL_MACHINE),
            TestGraphEntity.newBuilder(33L, UIEntityType.VIRTUAL_MACHINE) // <-- starting point
                .addProviderId(444L)
        );

        final Map<UIEntityType, SupplyChainNode> supplychain =
            supplyChainResolver.getSupplyChainNodes(Collections.singleton(444L), graph, e -> e.getOid() != 444L);
        // The starting entity didn't match the predicate
        assertTrue(supplychain.isEmpty());
    }

    @Test
    public void testIgnoreNoMatchPredicateSubtree() {
        /*
         * VM <-- starting point
         * |
         * PM <-- doesn't match predicate
         * |
         * ST
         */
        final TopologyGraph<TestGraphEntity> graph = TestGraphEntity.newGraph(
            TestGraphEntity.newBuilder(444L, UIEntityType.STORAGE),
            TestGraphEntity.newBuilder(33L, UIEntityType.PHYSICAL_MACHINE)
                .addProviderId(444L),
            TestGraphEntity.newBuilder(1L, UIEntityType.VIRTUAL_MACHINE) // <-- starting point
                .addProviderId(33L)
        );

        final Map<UIEntityType, SupplyChainNode> supplychain =
            supplyChainResolver.getSupplyChainNodes(Collections.singleton(1L), graph, e -> e.getOid() != 33L);

        // Only the VM is in the supply chain. The PM didn't match the predicate, and the ST is
        // only accessible from the PM.
        assertThat(supplychain.keySet(), containsInAnyOrder(UIEntityType.VIRTUAL_MACHINE));
    }

    @Test
    public void testMultiStartingPoints() {
        /*
         * VM1  VM2 <-- starting points
         *  |    |
         * PM1  PM2
         *       |
         *      ST1
         *
         * The supply chain should contain both.
         */
        final TopologyGraph<TestGraphEntity> graph = TestGraphEntity.newGraph(
            TestGraphEntity.newBuilder(444L, UIEntityType.STORAGE),
            TestGraphEntity.newBuilder(33L, UIEntityType.PHYSICAL_MACHINE)
                .addProviderId(444L),
            TestGraphEntity.newBuilder(22L, UIEntityType.PHYSICAL_MACHINE),
            TestGraphEntity.newBuilder(2L, UIEntityType.VIRTUAL_MACHINE) // <-- starting point
                .addProviderId(33L),
            TestGraphEntity.newBuilder(1L, UIEntityType.VIRTUAL_MACHINE)
                .addProviderId(22L)
        );

        final Map<UIEntityType, SupplyChainNode> supplychain =
            supplyChainResolver.getSupplyChainNodes(Sets.newHashSet(1L, 2L), graph, e -> true);

        assertThat(supplychain.keySet(), containsInAnyOrder(UIEntityType.VIRTUAL_MACHINE, UIEntityType.PHYSICAL_MACHINE, UIEntityType.STORAGE));
        assertThat(RepositoryDTOUtil.getMemberCount(supplychain.get(UIEntityType.STORAGE)), is(1));
        assertThat(RepositoryDTOUtil.getMemberCount(supplychain.get(UIEntityType.VIRTUAL_MACHINE)), is(2));
        assertThat(RepositoryDTOUtil.getMemberCount(supplychain.get(UIEntityType.PHYSICAL_MACHINE)), is(2));

        assertThat(supplychain.get(UIEntityType.PHYSICAL_MACHINE).getConnectedConsumerTypesList(), containsInAnyOrder(UIEntityType.VIRTUAL_MACHINE.apiStr()));
        assertThat(supplychain.get(UIEntityType.PHYSICAL_MACHINE).getConnectedProviderTypesList(), containsInAnyOrder(UIEntityType.STORAGE.apiStr()));

        assertThat(supplychain.get(UIEntityType.VIRTUAL_MACHINE).getConnectedProviderTypesList(), containsInAnyOrder(UIEntityType.PHYSICAL_MACHINE.apiStr()));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMultiStartingPointsDiffTypes() {
        final TopologyGraph<TestGraphEntity> graph = TestGraphEntity.newGraph(
            TestGraphEntity.newBuilder(22L, UIEntityType.PHYSICAL_MACHINE),
            TestGraphEntity.newBuilder(1L, UIEntityType.VIRTUAL_MACHINE)
                .addProviderId(22L)
        );

        supplyChainResolver.getSupplyChainNodes(Sets.newHashSet(1L, 22L), graph, e -> true);
    }

    @Test
    public void testNodeBuilder() {
        final int activeStateInt = UIEntityState.ACTIVE.toEntityState().getNumber();
        final int idleStateInt = UIEntityState.IDLE.toEntityState().getNumber();


        final TestGraphEntity.Builder poweredOnHost = TestGraphEntity.newBuilder(3, UIEntityType.PHYSICAL_MACHINE)
            .setState(EntityState.POWERED_ON);

        final TestGraphEntity.Builder poweredOnVm = TestGraphEntity.newBuilder(1, UIEntityType.VIRTUAL_MACHINE)
            .setState(EntityState.POWERED_ON)
            .addProviderId(poweredOnHost.getOid());
        final TestGraphEntity.Builder poweredOffVm = TestGraphEntity.newBuilder(2, UIEntityType.VIRTUAL_MACHINE)
            .setState(EntityState.POWERED_OFF)
            .addProviderId(poweredOnHost.getOid());

        final TestGraphEntity.Builder poweredOnApp = TestGraphEntity.newBuilder(4, UIEntityType.APPLICATION)
            .setState(EntityState.POWERED_ON)
            .addProviderId(poweredOnVm.getOid());

        final TopologyGraph<TestGraphEntity> graph =
            TestGraphEntity.newGraph(poweredOnVm, poweredOffVm, poweredOnHost, poweredOnApp);

        // The VMs are members of this node.
        final SupplyChainNodeBuilder<TestGraphEntity> nodeBuilder =
            new SupplyChainNodeBuilder<>(7, UIEntityType.VIRTUAL_MACHINE.typeNumber());
        graph.entitiesOfType(UIEntityType.VIRTUAL_MACHINE.typeNumber()).forEach(nodeBuilder::addMember);

        SupplyChainNode node = nodeBuilder.buildNode(graph);
        assertThat(node.getEntityType(), is(UIEntityType.VIRTUAL_MACHINE.apiStr()));
        assertThat(node.getSupplyChainDepth(), is(7));
        assertThat(RepositoryDTOUtil.getAllMemberOids(node), containsInAnyOrder(1L, 2L));
        assertThat(node.getConnectedConsumerTypesList(), containsInAnyOrder(UIEntityType.APPLICATION.apiStr()));
        assertThat(node.getConnectedProviderTypesList(), containsInAnyOrder(UIEntityType.PHYSICAL_MACHINE.apiStr()));

        final Map<Integer, MemberList> membersByStateMap = node.getMembersByStateMap();
        assertThat(membersByStateMap.keySet(), containsInAnyOrder(activeStateInt, idleStateInt));
        assertThat(membersByStateMap.get(activeStateInt).getMemberOidsList(),
            containsInAnyOrder(1L));
        assertThat(membersByStateMap.get(idleStateInt).getMemberOidsList(),
            containsInAnyOrder(2L));
    }
}
