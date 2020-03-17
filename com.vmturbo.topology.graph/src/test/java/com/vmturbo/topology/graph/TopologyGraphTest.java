package com.vmturbo.topology.graph;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class TopologyGraphTest {

    /**
     * 4   5     VIRTUAL_MACHINE
     *  \ /
     *   2   3   PHYSICAL_MACHINE
     *    \ /
     *     1     DATACENTER
     */
    private final TestGraphEntity.Builder onPremDC = TestGraphEntity.newBuilder(1L, UIEntityType.DATACENTER);
    private final TestGraphEntity.Builder onPremPM1 =
            TestGraphEntity.newBuilder(2L, UIEntityType.PHYSICAL_MACHINE)
                .addProviderId(1L);
    private final TestGraphEntity.Builder onPremPM2 =
            TestGraphEntity.newBuilder(3L, UIEntityType.PHYSICAL_MACHINE)
                .addProviderId(1L);
    private final TestGraphEntity.Builder onPremVM1 =
            TestGraphEntity.newBuilder(4L, UIEntityType.VIRTUAL_MACHINE)
                .addProviderId(2L);
    private final TestGraphEntity.Builder onPremVM2 =
            TestGraphEntity.newBuilder(5L, UIEntityType.VIRTUAL_MACHINE)
                .addProviderId(2L);
    private final Map<Long, TestGraphEntity.Builder> onPremTopologyMap = ImmutableMap.of(
        1L, onPremDC,
        2L, onPremPM1,
        3L, onPremPM2,
        4L, onPremVM1,
        5L, onPremVM2);

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
        final TestGraphEntity.Builder entity4Duplicate = TestGraphEntity.newBuilder(4L, UIEntityType.VIRTUAL_MACHINE)
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
            TestGraphEntity.newBuilder(1L, UIEntityType.PHYSICAL_MACHINE),
            TestGraphEntity.newBuilder(2L, UIEntityType.STORAGE),
            TestGraphEntity.newBuilder(3L, UIEntityType.VIRTUAL_MACHINE)
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
        final TestGraphEntity.Builder entity1 = TestGraphEntity.newBuilder(1L, UIEntityType.PHYSICAL_MACHINE);
        final TestGraphEntity.Builder entity2 = TestGraphEntity.newBuilder(2L, UIEntityType.STORAGE);
        final TestGraphEntity.Builder entity3 = TestGraphEntity.newBuilder(3L, UIEntityType.VIRTUAL_MACHINE)
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
}