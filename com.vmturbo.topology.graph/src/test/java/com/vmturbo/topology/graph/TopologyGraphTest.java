package com.vmturbo.topology.graph;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.graph.TopologyGraph.TopsortEdgeSupplier;

public class TopologyGraphTest {

    /**
     * Graph constructed below.
     *
     * <pre>
     * 4   5     VIRTUAL_MACHINE
     *  \ /
     *   2   3   PHYSICAL_MACHINE
     *    \ /
     *     1     DATACENTER
     * </pre>
     */
    private final TestGraphEntity.Builder onPremDC = TestGraphEntity.newBuilder(1L, ApiEntityType.DATACENTER);
    private final TestGraphEntity.Builder onPremPM1 =
            TestGraphEntity.newBuilder(2L, ApiEntityType.PHYSICAL_MACHINE)
                    .addProviderId(1L);
    private final TestGraphEntity.Builder onPremPM2 =
            TestGraphEntity.newBuilder(3L, ApiEntityType.PHYSICAL_MACHINE)
                    .addProviderId(1L);
    private final TestGraphEntity.Builder onPremVM1 =
            TestGraphEntity.newBuilder(4L, ApiEntityType.VIRTUAL_MACHINE)
                .addProviderId(2L);
    private final TestGraphEntity.Builder onPremVM2 =
            TestGraphEntity.newBuilder(5L, ApiEntityType.VIRTUAL_MACHINE)
                .addProviderId(2L);
    private final Map<Long, TestGraphEntity.Builder> onPremTopologyMap = ImmutableMap.of(
        1L, onPremDC,
        2L, onPremPM1,
        3L, onPremPM2,
        4L, onPremVM1,
        5L, onPremVM2);


    private static final ImmutableList<TopsortEdgeSupplier<TestGraphEntity>> BY_PROVIDERS_EDGE_SUPPLIERS
            = ImmutableList.of(TopologyGraph::getProviders);
    private static final ImmutableList<TopsortEdgeSupplier<TestGraphEntity>> BY_CONSUMERS_EDGE_SUPPLIERS
            = ImmutableList.of((g, e) -> g.getConsumers(e));

    /**
     * These edge suppliers don't correspond to actual edges in this graph, but they test the use of
     * multiple edge suppliers with contradictory ordering.
     *
     * <pre>
     *   2 -> 3  |     3    5
     *   ^   ^   |     |    ^
     *    \ /    |     v    |
     *     1     |  1  2    4
     * </pre>
     *
     * <p>The 2 -> 3 edge should prevail over the 3 -> 2 edge, and the 4 -> 5 edge should show up
     * as well.</p>
     */
    private static final ImmutableList<TopsortEdgeSupplier<TestGraphEntity>> MULTI_COMPETING_EDGE_SUPPLIERS
            = ImmutableList.<TopsortEdgeSupplier<TestGraphEntity>>builder()
            .add((g, e) -> e.getOid() == 1L
                    ? Stream.of(g.getEntity(2L).get(), g.getEntity(3L).get())
                    : e.getOid() == 2L
                    ? Stream.of(g.getEntity(3L).get())
                    : e.getOid() == 3L
                    ? Stream.empty()
                    : Stream.empty())
            .add((g, e) -> e.getOid() == 3L
                    ? Stream.of(g.getEntity(2L).get())
                    : e.getOid() == 4L
                    ? Stream.of(g.getEntity(5L).get())
                    : e.getOid() == 5L
                    ? Stream.empty()
                    : Stream.empty())

            .build();
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testBuildConstructionEmptyMap() {
        final TopologyGraph<TestGraphEntity> graph = TestGraphEntity.newGraph(Collections.emptyMap());
        assertEquals(0, graph.size());
        assertEquals(0, producerCount(graph));
        assertEquals(0, consumerCount(graph));
    }

    @Test
    public void testConstruction() {
        final TopologyGraph<TestGraphEntity> graph = TestGraphEntity.newGraph(onPremTopologyMap);
        assertEquals(5, graph.size());
        assertEquals(4, producerCount(graph));
        assertEquals(4, consumerCount(graph));
    }

    @Test
    public void testEntityAppearsMoreThanOnce() {
        final TestGraphEntity.Builder entity4Duplicate = TestGraphEntity.newBuilder(4L, ApiEntityType.VIRTUAL_MACHINE)
            .addProviderId(2L);

        final Map<Long, TestGraphEntity.Builder> topologyMap = ImmutableMap.of(
            1L, onPremDC,
            2L, onPremPM1,
            3L, onPremPM2,
            4L, onPremVM1,
            5L, entity4Duplicate
        );

        expectedException.expect(IllegalArgumentException.class);
        TestGraphEntity.newGraph(topologyMap);
    }

    @Test
    public void testEntitiesInReverseOrder() {
        final Map<Long, TestGraphEntity.Builder> topologyMap = ImmutableMap.of(
            5L, onPremVM2,
            4L, onPremVM1,
            3L, onPremPM2,
            2L, onPremPM1,
            1L, onPremDC
        );

        // Verify that scanning an entity that links to an entity not yet seen
        // does not result in an error.
        TestGraphEntity.newGraph(topologyMap);
    }

    @Test
    public void testGetEmptyProducers() throws Exception {
        final TopologyGraph<TestGraphEntity> graph = TestGraphEntity.newGraph(onPremTopologyMap);

        assertThat(graph.getProviders(1L).collect(Collectors.toList()), is(empty()));
    }

    @Test
    public void testGetNonEmptyProducers() throws Exception {
        final TopologyGraph<TestGraphEntity> graph = TestGraphEntity.newGraph(onPremTopologyMap);

        assertThat(
            graph.getProviders(2L)
                .map(TestGraphEntity::getOid)
                .collect(Collectors.toList()),
            contains(onPremDC.getOid())
        );
    }

    @Test
    public void testMultipleProducers() throws Exception {
        /**
         *    3
         *   / \
         *  1   2
         */

        final TopologyGraph<TestGraphEntity> graph = TestGraphEntity.newGraph(
            TestGraphEntity.newBuilder(1L, ApiEntityType.PHYSICAL_MACHINE),
            TestGraphEntity.newBuilder(2L, ApiEntityType.STORAGE),
            TestGraphEntity.newBuilder(3L, ApiEntityType.VIRTUAL_MACHINE)
                .addProviderId(1L)
                .addProviderId(2L));

        assertThat(
            graph.getProviders(3L)
                .map(TestGraphEntity::getOid)
                .collect(Collectors.toList()),
            containsInAnyOrder(onPremDC.getOid(), onPremPM1.getOid())
        );
    }

    @Test
    public void testGetEmptyConsumers() throws Exception {
        final TopologyGraph<TestGraphEntity> graph = TestGraphEntity.newGraph(onPremTopologyMap);

        assertThat(graph.getConsumers(5L).collect(Collectors.toList()), is(empty()));
    }

    @Test
    public void testMultipleConsumers() throws Exception {
        final TopologyGraph<TestGraphEntity> graph = TestGraphEntity.newGraph(onPremTopologyMap);

        assertThat(
            graph.getConsumers(1L)
                .map(TestGraphEntity::getOid)
                .collect(Collectors.toList()),
            containsInAnyOrder(onPremPM1.getOid(), onPremPM2.getOid())
        );
        assertThat(
            graph.getConsumers(2L)
                .map(TestGraphEntity::getOid)
                .collect(Collectors.toList()),
            containsInAnyOrder(onPremVM1.getOid(), onPremVM2.getOid())
        );
    }

    @Test
    public void testGetConsumersNotInGraph() {
        final TopologyGraph<TestGraphEntity> graph = TestGraphEntity.newGraph(Collections.emptyMap());

        assertThat(
            graph.getConsumers(1L).collect(Collectors.toList()),
            is(empty())
        );
    }

    @Test
    public void testGetProducersNotInGraph() {
        final TopologyGraph<TestGraphEntity> graph = TestGraphEntity.newGraph(Collections.emptyMap());

        assertThat(
            graph.getProviders(1L).collect(Collectors.toList()),
            is(empty())
        );
    }

    /**
     *
     * <p>Build graph like below to test getting all consumers recursively.</p>
     *
     * <p>Graph:
     *        App1 (6)
     *        |
     * VM2(5) VM1(4)
     *   \    |  \
     *    \   |   \
     *     St1(2)  St2(3)
     *     |       \
     * Storage Controller (1)
     * </p>
     */
    @Test
    public void testGetAllConsumersRecursively() {
        final TestGraphEntity.Builder onPremSC = TestGraphEntity.newBuilder(1L, ApiEntityType.STORAGECONTROLLER);
        final TestGraphEntity.Builder onPremST1 =
            TestGraphEntity.newBuilder(2L, ApiEntityType.STORAGE)
                .addProviderId(1L);
        final TestGraphEntity.Builder onPremST2 =
            TestGraphEntity.newBuilder(3L, ApiEntityType.STORAGE)
                .addProviderId(1L);
        final TestGraphEntity.Builder onPremVM1 =
            TestGraphEntity.newBuilder(4L, ApiEntityType.VIRTUAL_MACHINE)
                .addProviderId(2L).addProviderId(3L);
        final TestGraphEntity.Builder onPremVM2 =
            TestGraphEntity.newBuilder(5L, ApiEntityType.VIRTUAL_MACHINE)
                .addProviderId(2L);
        final TestGraphEntity.Builder onPremApp1 =
            TestGraphEntity.newBuilder(6L, ApiEntityType.APPLICATION)
                .addProviderId(4L);
        final Map<Long, TestGraphEntity.Builder> onPremTopologyMap = Maps.newHashMap();
        onPremTopologyMap.put(1L, onPremSC);
        onPremTopologyMap.put(2L, onPremST1);
        onPremTopologyMap.put(3L, onPremST2);
        onPremTopologyMap.put(4L, onPremVM1);
        onPremTopologyMap.put(5L, onPremVM2);
        onPremTopologyMap.put(6L, onPremApp1);

        final TopologyGraph<TestGraphEntity> graph = TestGraphEntity.newGraph(onPremTopologyMap);

        Set<TestGraphEntity> expectedConsumers = Sets.newHashSet(
            onPremST1.build(), onPremST2.build(), onPremVM1.build(), onPremVM2.build(), onPremApp1.build());
        assertEquals(expectedConsumers, graph.getAllConsumersRecursively(
            onPremSC.getOid()).collect(Collectors.toSet()));
    }

    @Test
    public void testGetEntity() {
        final TopologyGraph<TestGraphEntity> graph = TestGraphEntity.newGraph(onPremTopologyMap);

        assertEquals(onPremDC.getOid(), graph.getEntity(1L).get().getOid());
    }

    @Test
    public void testGetEntityNotInGraph() {
        final TopologyGraph<TestGraphEntity> graph = TestGraphEntity.newGraph(Collections.emptyMap());

        assertFalse(graph.getEntity(1L).isPresent());
    }

    @Test
    public void testProducersForEntityWithMultipleConsumers() throws Exception {
        final TopologyGraph<TestGraphEntity> graph = TestGraphEntity.newGraph(onPremTopologyMap);

        assertThat(
            graph.getProviders(2L)
                .map(TestGraphEntity::getOid)
                .collect(Collectors.toList()),
            containsInAnyOrder(onPremDC.getOid())
        );
        assertThat(
            graph.getProviders(3L)
                .map(TestGraphEntity::getOid)
                .collect(Collectors.toList()),
            containsInAnyOrder(onPremDC.getOid())
        );
    }

    @Test
    public void testConsumersForEntityWithMultipleProducers() throws Exception {
        /**
         *    3
         *   / \
         *  1   2
         */
        final TestGraphEntity.Builder entity1 = TestGraphEntity.newBuilder(1L, ApiEntityType.PHYSICAL_MACHINE);
        final TestGraphEntity.Builder entity2 = TestGraphEntity.newBuilder(2L, ApiEntityType.STORAGE);
        final TestGraphEntity.Builder entity3 = TestGraphEntity.newBuilder(3L, ApiEntityType.VIRTUAL_MACHINE)
            .addProviderId(1L)
            .addProviderId(2L);

        final Map<Long, TestGraphEntity.Builder> topologyMap = ImmutableMap.of(
            1L, entity1,
            2L, entity2,
            3L, entity3);

        final TopologyGraph<TestGraphEntity> graph = TestGraphEntity.newGraph(topologyMap);

        assertThat(
            graph.getConsumers(1L)
                .map(TestGraphEntity::getOid)
                .collect(Collectors.toList()),
            contains(entity3.getOid())
        );
        assertThat(
            graph.getConsumers(2L)
                .map(TestGraphEntity::getOid)
                .collect(Collectors.toList()),
            contains(entity3.getOid())
        );
    }

    @Test
    public void testEntitiesOfType() {
        final TopologyGraph<TestGraphEntity> graph = TestGraphEntity.newGraph(onPremTopologyMap);

        assertThat(
            graph.entitiesOfType(EntityType.VIRTUAL_MACHINE)
                .map(TestGraphEntity::getOid)
                .collect(Collectors.toList()),
            containsInAnyOrder(onPremVM1.getOid(), onPremVM2.getOid()));
        assertThat(
            graph.entitiesOfType(EntityType.PHYSICAL_MACHINE)
                .map(TestGraphEntity::getOid)
                .collect(Collectors.toList()),
            containsInAnyOrder(onPremPM1.getOid(), onPremPM2.getOid()));
        assertThat(
            graph.entitiesOfType(EntityType.DATACENTER)
                .map(TestGraphEntity::getOid)
                .collect(Collectors.toList()),
            contains(onPremDC.getOid()));
        assertThat(
            graph.entitiesOfType(EntityType.STORAGE)
                .map(TestGraphEntity::getOid)
                .collect(Collectors.toList()),
            is(empty()));
    }

    @Test
    public void testEntitiesOfTypeByNumber() {
        final TopologyGraph<TestGraphEntity> graph = TestGraphEntity.newGraph(onPremTopologyMap);

        assertThat(
            graph.entitiesOfType(EntityType.VIRTUAL_MACHINE.getNumber())
                .map(TestGraphEntity::getOid)
                .collect(Collectors.toList()),
            containsInAnyOrder(onPremVM1.getOid(), onPremVM2.getOid()));
        assertThat(
            graph.entitiesOfType(EntityType.PHYSICAL_MACHINE.getNumber())
                .map(TestGraphEntity::getOid)
                .collect(Collectors.toList()),
            containsInAnyOrder(onPremPM1.getOid(), onPremPM2.getOid()));
        assertThat(
            graph.entitiesOfType(EntityType.DATACENTER.getNumber())
                .map(TestGraphEntity::getOid)
                .collect(Collectors.toList()),
            contains(onPremDC.getOid()));
        assertThat(
            graph.entitiesOfType(EntityType.STORAGE.getNumber())
                .map(TestGraphEntity::getOid)
                .collect(Collectors.toList()),
            is(empty()));
    }

    @Test
    public void testBuildTwiceFromSameMap() {
        TestGraphEntity.newGraph(onPremTopologyMap);
        final TopologyGraph<TestGraphEntity> graph2 = TestGraphEntity.newGraph(onPremTopologyMap);

        // Building a graph twice should not result in consumers and providers being added
        // multiple times to the same entity.
        graph2.entities().forEach(entity -> {
            assertEquals(entity.getConsumers().stream().distinct().count(), entity.getConsumers().size());
            assertEquals(entity.getProviders().stream().distinct().count(), entity.getProviders().size());
        });
    }

    /**
     * Check that connection getters return the correct results in the cloud topology.
     */
    @Test
    public void testConnections() {
        final TopologyGraph<TestGraphEntity> graph = SimpleCloudTopologyUtil.constructTopology();
        final TestGraphEntity region = graph.getEntity(SimpleCloudTopologyUtil.RG_ID).get();
        final TestGraphEntity zone = graph.getEntity(SimpleCloudTopologyUtil.AZ_ID).get();
        final TestGraphEntity vm = graph.getEntity(SimpleCloudTopologyUtil.VM_ID).get();
        final TestGraphEntity db = graph.getEntity(SimpleCloudTopologyUtil.DB_ID).get();
        final TestGraphEntity computeTier = graph.getEntity(SimpleCloudTopologyUtil.CT_ID).get();
        final TestGraphEntity storageTier = graph.getEntity(SimpleCloudTopologyUtil.ST_ID).get();

        // check owned and aggregated entities
        assertEquals(
                ImmutableSet.of(zone, computeTier, storageTier),
                graph.getOwnedOrAggregatedEntities(region).collect(Collectors.toSet()));
        assertEquals(
                ImmutableSet.of(vm, db),
                graph.getOwnedOrAggregatedEntities(zone).collect(Collectors.toSet()));
        assertEquals(Collections.emptySet(), graph.getOwnedOrAggregatedEntities(vm).collect(Collectors.toSet()));
        assertEquals(
                Collections.emptySet(),
                graph.getOwnedOrAggregatedEntities(computeTier).collect(Collectors.toSet()));

        // check owner and aggregators
        assertEquals(
                Collections.emptySet(),
                graph.getOwnersOrAggregators(region).collect(Collectors.toSet()));
        assertEquals(
                Collections.singleton(zone),
                graph.getOwnersOrAggregators(vm).collect(Collectors.toSet()));
        assertEquals(
                ImmutableSet.of(region),
                graph.getOwnersOrAggregators(storageTier).collect(Collectors.toSet()));

        // check aggregated entities only
        assertEquals(
                ImmutableSet.of(computeTier, storageTier),
                graph.getAggregatedEntities(region).collect(Collectors.toSet()));
        assertEquals(
                ImmutableSet.of(vm, db),
                graph.getAggregatedEntities(zone).collect(Collectors.toSet()));
        assertEquals(Collections.emptySet(), graph.getAggregatedEntities(vm).collect(Collectors.toSet()));
        assertEquals(
                Collections.emptySet(),
                graph.getAggregatedEntities(computeTier).collect(Collectors.toSet()));

        // check aggregators only
        assertEquals(Collections.emptySet(), graph.getAggregators(region).collect(Collectors.toSet()));
        assertEquals(Collections.singleton(zone), graph.getAggregators(vm).collect(Collectors.toSet()));
        assertEquals(ImmutableSet.of(region), graph.getAggregators(storageTier).collect(Collectors.toSet()));

        // check owned entities only
        assertEquals(
                Collections.singleton(zone),
                graph.getOwnedEntities(region).collect(Collectors.toSet()));
        assertEquals(Collections.emptySet(), graph.getOwnedEntities(zone).collect(Collectors.toSet()));
        assertEquals(Collections.emptySet(), graph.getOwnedEntities(vm).collect(Collectors.toSet()));
        assertEquals(Collections.emptySet(), graph.getOwnedEntities(computeTier).collect(Collectors.toSet()));

        // check owners only
        assertEquals(Collections.emptySet(), graph.getOwner(region).collect(Collectors.toSet()));
        assertEquals(Collections.singleton(region), graph.getOwner(zone).collect(Collectors.toSet()));
        assertEquals(Collections.emptySet(), graph.getOwner(vm).collect(Collectors.toSet()));
        assertEquals(Collections.emptySet(), graph.getOwner(storageTier).collect(Collectors.toSet()));
    }

    private int producerCount(@Nonnull final TopologyGraph<TestGraphEntity> graph) {
        return graph.entities().mapToInt(
                entity -> graph.getProviders(entity).mapToInt(v -> 1).sum()).sum();
    }

    private int consumerCount(@Nonnull final TopologyGraph<TestGraphEntity> graph) {
        return graph.entities().mapToInt(
                entity -> graph.getConsumers(entity).mapToInt(v -> 1).sum()).sum();
    }

    /**
     * Test a simple graph sort so providers precede consumers.
     */
    @Test
    public void testTopologicalSortByProviders() {
        final TopologyGraph<TestGraphEntity> graph = TestGraphEntity.newGraph(onPremTopologyMap);
        final Stream<TestGraphEntity> sortedEntities = graph.topSort(BY_PROVIDERS_EDGE_SUPPLIERS);
        final Map<Long, Integer> oidIndexes = oidToIndex(graph, sortedEntities);
        assertThat(oidIndexes.get(2L), greaterThan(oidIndexes.get(1L)));
        assertThat(oidIndexes.get(3L), greaterThan(oidIndexes.get(1L)));
        assertThat(oidIndexes.get(4L), greaterThan(oidIndexes.get(1L)));
        assertThat(oidIndexes.get(5L), greaterThan(oidIndexes.get(1L)));
        assertThat(oidIndexes.get(4L), greaterThan(oidIndexes.get(2L)));
        assertThat(oidIndexes.get(5L), greaterThan(oidIndexes.get(2L)));
    }

    /**
     * Test a simple graph with so that consumers precede providers (same result as "reverse" tests
     * by using an inverse relation).
     */
    @Test
    public void testTopologicalSortByConsumers() {
        final TopologyGraph<TestGraphEntity> graph = TestGraphEntity.newGraph(onPremTopologyMap);
        final Stream<TestGraphEntity> sortedEntities = graph.topSort(BY_PROVIDERS_EDGE_SUPPLIERS);
        final Map<Long, Integer> oidIndexes = oidToIndex(graph, sortedEntities);
        assertThat(oidIndexes.get(2L), greaterThan(oidIndexes.get(1L)));
        assertThat(oidIndexes.get(3L), greaterThan(oidIndexes.get(1L)));
        assertThat(oidIndexes.get(4L), greaterThan(oidIndexes.get(1L)));
        assertThat(oidIndexes.get(5L), greaterThan(oidIndexes.get(1L)));
        assertThat(oidIndexes.get(4L), greaterThan(oidIndexes.get(2L)));
        assertThat(oidIndexes.get(5L), greaterThan(oidIndexes.get(2L)));
    }

    /**
     * Test that muliple edge suppliers that don't aggree are properly prioritized.
     */
    @Test
    public void testContradicingEdgeSuppliers() {
        final TopologyGraph<TestGraphEntity> graph = TestGraphEntity.newGraph(onPremTopologyMap);
        final Stream<TestGraphEntity> sortedEntities = graph.topSort(MULTI_COMPETING_EDGE_SUPPLIERS);
        final Map<Long, Integer> oidIndexes = oidToIndex(graph, sortedEntities);
        assertThat(oidIndexes.get(1L), greaterThan(oidIndexes.get(2L)));
        assertThat(oidIndexes.get(1L), greaterThan(oidIndexes.get(3L)));
        assertThat(oidIndexes.get(4L), greaterThan(oidIndexes.get(5L)));
    }

    private static Map<Long, Integer> oidToIndex(final TopologyGraph<TestGraphEntity> graph,
            final Stream<TestGraphEntity> sortedEntities) {
        final List<Long> sortedOids = sortedEntities
                .map(TestGraphEntity::getOid)
                .collect(Collectors.toList());
        return IntStream.range(0, graph.size())
                .boxed()
                .collect(Collectors.toMap(i -> sortedOids.get(i), Functions.identity()));
    }

}
