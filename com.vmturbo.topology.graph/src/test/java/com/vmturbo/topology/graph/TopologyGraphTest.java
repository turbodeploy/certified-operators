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
    private final TestGraphEntity.Builder entity1 = TestGraphEntity.newBuilder(1L, UIEntityType.DATACENTER);
    private final TestGraphEntity.Builder entity2 = TestGraphEntity.newBuilder(2L, UIEntityType.PHYSICAL_MACHINE)
        .addProviderId(1L);
    private final TestGraphEntity.Builder entity3 = TestGraphEntity.newBuilder(3L, UIEntityType.PHYSICAL_MACHINE)
        .addProviderId(1L);
    private final TestGraphEntity.Builder entity4 = TestGraphEntity.newBuilder(4L, UIEntityType.VIRTUAL_MACHINE)
        .addProviderId(2L);
    private final TestGraphEntity.Builder entity5 = TestGraphEntity.newBuilder(5L, UIEntityType.VIRTUAL_MACHINE)
        .addProviderId(2L);

    private final Map<Long, TestGraphEntity.Builder> topologyMap = ImmutableMap.of(
        1L, entity1,
        2L, entity2,
        3L, entity3,
        4L, entity4,
        5L, entity5
    );

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
        final TopologyGraph<TestGraphEntity> graph = TestGraphEntity.newGraph(topologyMap);
        assertEquals(5, graph.size());
        assertEquals(4, producerCount(graph));
        assertEquals(4, consumerCount(graph));
    }

    @Test
    public void testEntityAppearsMoreThanOnce() {
        final TestGraphEntity.Builder entity4Duplicate = TestGraphEntity.newBuilder(4L, UIEntityType.VIRTUAL_MACHINE)
            .addProviderId(2L);

        final Map<Long, TestGraphEntity.Builder> topologyMap = ImmutableMap.of(
            1L, entity1,
            2L, entity2,
            3L, entity3,
            4L, entity4,
            5L, entity4Duplicate
        );

        expectedException.expect(IllegalArgumentException.class);
        TestGraphEntity.newGraph(topologyMap);
    }

    @Test
    public void testEntitiesInReverseOrder() {
        final Map<Long, TestGraphEntity.Builder> topologyMap = ImmutableMap.of(
            5L, entity5,
            4L, entity4,
            3L, entity3,
            2L, entity2,
            1L, entity1
        );

        // Verify that scanning an entity that links to an entity not yet seen
        // does not result in an error.
        TestGraphEntity.newGraph(topologyMap);
    }

    @Test
    public void testGetEmptyProducers() throws Exception {
        final TopologyGraph<TestGraphEntity> graph = TestGraphEntity.newGraph(topologyMap);

        assertThat(graph.getProviders(1L).collect(Collectors.toList()), is(empty()));
    }

    @Test
    public void testGetNonEmptyProducers() throws Exception {
        final TopologyGraph<TestGraphEntity> graph = TestGraphEntity.newGraph(topologyMap);

        assertThat(
            graph.getProviders(2L)
                .map(TestGraphEntity::getOid)
                .collect(Collectors.toList()),
            contains(entity1.getOid())
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
            containsInAnyOrder(entity1.getOid(), entity2.getOid())
        );
    }

    @Test
    public void testGetEmptyConsumers() throws Exception {
        final TopologyGraph<TestGraphEntity> graph = TestGraphEntity.newGraph(topologyMap);

        assertThat(graph.getConsumers(5L).collect(Collectors.toList()), is(empty()));
    }

    @Test
    public void testMultipleConsumers() throws Exception {
        final TopologyGraph<TestGraphEntity> graph = TestGraphEntity.newGraph(topologyMap);

        assertThat(
            graph.getConsumers(1L)
                .map(TestGraphEntity::getOid)
                .collect(Collectors.toList()),
            containsInAnyOrder(entity2.getOid(), entity3.getOid())
        );
        assertThat(
            graph.getConsumers(2L)
                .map(TestGraphEntity::getOid)
                .collect(Collectors.toList()),
            containsInAnyOrder(entity4.getOid(), entity5.getOid())
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
        final TopologyGraph<TestGraphEntity> graph = TestGraphEntity.newGraph(topologyMap);

        assertEquals(entity1.getOid(), graph.getEntity(1L).get().getOid());
    }

    @Test
    public void testGetEntityNotInGraph() {
        final TopologyGraph<TestGraphEntity> graph = TestGraphEntity.newGraph(Collections.emptyMap());

        assertFalse(graph.getEntity(1L).isPresent());
    }

    @Test
    public void testProducersForEntityWithMultipleConsumers() throws Exception {
        final TopologyGraph<TestGraphEntity> graph = TestGraphEntity.newGraph(topologyMap);

        assertThat(
            graph.getProviders(2L)
                .map(TestGraphEntity::getOid)
                .collect(Collectors.toList()),
            containsInAnyOrder(entity1.getOid())
        );
        assertThat(
            graph.getProviders(3L)
                .map(TestGraphEntity::getOid)
                .collect(Collectors.toList()),
            containsInAnyOrder(entity1.getOid())
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
        final TopologyGraph<TestGraphEntity> graph = TestGraphEntity.newGraph(topologyMap);

        assertThat(
            graph.entitiesOfType(EntityType.VIRTUAL_MACHINE)
                .map(TestGraphEntity::getOid)
                .collect(Collectors.toList()),
            containsInAnyOrder(entity4.getOid(), entity5.getOid()));
        assertThat(
            graph.entitiesOfType(EntityType.PHYSICAL_MACHINE)
                .map(TestGraphEntity::getOid)
                .collect(Collectors.toList()),
            containsInAnyOrder(entity2.getOid(), entity3.getOid()));
        assertThat(
            graph.entitiesOfType(EntityType.DATACENTER)
                .map(TestGraphEntity::getOid)
                .collect(Collectors.toList()),
            contains(entity1.getOid()));
        assertThat(
            graph.entitiesOfType(EntityType.STORAGE)
                .map(TestGraphEntity::getOid)
                .collect(Collectors.toList()),
            is(empty()));
    }

    @Test
    public void testEntitiesOfTypeByNumber() {
        final TopologyGraph<TestGraphEntity> graph = TestGraphEntity.newGraph(topologyMap);

        assertThat(
            graph.entitiesOfType(EntityType.VIRTUAL_MACHINE.getNumber())
                .map(TestGraphEntity::getOid)
                .collect(Collectors.toList()),
            containsInAnyOrder(entity4.getOid(), entity5.getOid()));
        assertThat(
            graph.entitiesOfType(EntityType.PHYSICAL_MACHINE.getNumber())
                .map(TestGraphEntity::getOid)
                .collect(Collectors.toList()),
            containsInAnyOrder(entity2.getOid(), entity3.getOid()));
        assertThat(
            graph.entitiesOfType(EntityType.DATACENTER.getNumber())
                .map(TestGraphEntity::getOid)
                .collect(Collectors.toList()),
            contains(entity1.getOid()));
        assertThat(
            graph.entitiesOfType(EntityType.STORAGE.getNumber())
                .map(TestGraphEntity::getOid)
                .collect(Collectors.toList()),
            is(empty()));
    }

    @Test
    public void testBuildTwiceFromSameMap() {
        TestGraphEntity.newGraph(topologyMap);
        final TopologyGraph<TestGraphEntity> graph2 = TestGraphEntity.newGraph(topologyMap);

        // Building a graph twice should not result in consumers and providers being added
        // multiple times to the same entity.
        graph2.entities().forEach(entity -> {
            assertEquals(entity.getConsumers().stream().distinct().count(), entity.getConsumers().size());
            assertEquals(entity.getProviders().stream().distinct().count(), entity.getProviders().size());
        });
    }

    private int producerCount(@Nonnull final TopologyGraph<TestGraphEntity> graph) {
        return graph.entities().mapToInt(
            entity -> graph.getProviders(entity)
                .collect(Collectors.summingInt(v -> 1))
        ).sum();
    }

    private int consumerCount(@Nonnull final TopologyGraph<TestGraphEntity> graph) {
        return graph.entities().mapToInt(
            entity -> graph.getConsumers(entity)
                .collect(Collectors.summingInt(v -> 1))
        ).sum();
    }
}