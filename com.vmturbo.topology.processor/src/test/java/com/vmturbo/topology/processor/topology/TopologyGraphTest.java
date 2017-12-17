package com.vmturbo.topology.processor.topology;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class TopologyGraphTest {

    /**
     * 4   5     VIRTUAL_MACHINE
     *  \ /
     *   2   3   PHYSICAL_MACHINE
     *    \ /
     *     1     DATACENTER
     */
    private final TopologyEntityDTO.Builder entity1 = TopologyEntityDTO.newBuilder()
        .setOid(1L)
        .setEntityType(EntityType.DATACENTER.getNumber());
    private final TopologyEntityDTO.Builder entity2 = TopologyEntityDTO.newBuilder()
        .setOid(2L)
        .setEntityType(EntityType.PHYSICAL_MACHINE.getNumber())
        .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
            .setProviderId(1L)
            .build());
    private final TopologyEntityDTO.Builder entity3 = TopologyEntityDTO.newBuilder()
        .setOid(3L)
        .setEntityType(EntityType.PHYSICAL_MACHINE.getNumber())
        .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
            .setProviderId(1L)
            .build());
    private final TopologyEntityDTO.Builder entity4 = TopologyEntityDTO.newBuilder()
        .setOid(4L)
        .setEntityType(EntityType.VIRTUAL_MACHINE.getNumber())
        .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
            .setProviderId(2L)
            .build());
    private final TopologyEntityDTO.Builder entity5 = TopologyEntityDTO.newBuilder()
        .setOid(5L)
        .setEntityType(EntityType.VIRTUAL_MACHINE.getNumber())
        .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
            .setProviderId(2L)
            .build());

    private final Map<Long, TopologyEntity.Builder> topologyMap = ImmutableMap.of(
        1L, TopologyEntityUtils.topologyEntityBuilder(entity1),
        2L, TopologyEntityUtils.topologyEntityBuilder(entity2),
        3L, TopologyEntityUtils.topologyEntityBuilder(entity3),
        4L, TopologyEntityUtils.topologyEntityBuilder(entity4),
        5L, TopologyEntityUtils.topologyEntityBuilder(entity5)
    );

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testBuildConstructionEmptyMap() {
        final TopologyGraph graph = TopologyGraph.newGraph(Collections.emptyMap());
        assertEquals(0, graph.size());
        assertEquals(0, producerCount(graph));
        assertEquals(0, consumerCount(graph));
    }

    @Test
    public void testConstruction() {
        final TopologyGraph graph = TopologyGraph.newGraph(topologyMap);
        assertEquals(5, graph.size());
        assertEquals(4, producerCount(graph));
        assertEquals(4, consumerCount(graph));
    }

    @Test
    public void testEntityAppearsMoreThanOnce() {
        final TopologyEntityDTO.Builder entity4Duplicate = TopologyEntityDTO.newBuilder()
            .setOid(4L)
            .setEntityType(4)
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .setProviderId(2L)
                .build());

        final Map<Long, TopologyEntity.Builder> topologyMap = ImmutableMap.of(
            1L, TopologyEntityUtils.topologyEntityBuilder(entity1),
            2L, TopologyEntityUtils.topologyEntityBuilder(entity2),
            3L, TopologyEntityUtils.topologyEntityBuilder(entity3),
            4L, TopologyEntityUtils.topologyEntityBuilder(entity4),
            5L, TopologyEntityUtils.topologyEntityBuilder(entity4Duplicate)
        );

        expectedException.expect(IllegalArgumentException.class);
        TopologyGraph.newGraph(topologyMap);
    }

    @Test
    public void testEntitiesInReverseOrder() {
        final Map<Long, TopologyEntity.Builder> topologyMap = ImmutableMap.of(
            5L, TopologyEntityUtils.topologyEntityBuilder(entity5),
            4L, TopologyEntityUtils.topologyEntityBuilder(entity4),
            3L, TopologyEntityUtils.topologyEntityBuilder(entity3),
            2L, TopologyEntityUtils.topologyEntityBuilder(entity2),
            1L, TopologyEntityUtils.topologyEntityBuilder(entity1)
        );

        // Verify that scanning an entity that links to an entity not yet seen
        // does not result in an error.
        TopologyGraph.newGraph(topologyMap);
    }

    @Test
    public void testGetEmptyProducers() throws Exception {
        final TopologyGraph graph = TopologyGraph.newGraph(topologyMap);

        assertThat(graph.getProviders(1L).collect(Collectors.toList()), is(empty()));
    }

    @Test
    public void testGetNonEmptyProducers() throws Exception {
        final TopologyGraph graph = TopologyGraph.newGraph(topologyMap);

        assertThat(
            graph.getProviders(2L)
                .map(TopologyEntity::getTopologyEntityDtoBuilder)
                .collect(Collectors.toList()),
            contains(entity1)
        );
    }

    @Test
    public void testMultipleProducers() throws Exception {
        /**
         *    3
         *   / \
         *  1   2
         */
        final TopologyEntityDTO.Builder entity1 = TopologyEntityDTO.newBuilder()
            .setOid(1L)
            .setEntityType(1);
        final TopologyEntityDTO.Builder entity2 = TopologyEntityDTO.newBuilder()
            .setOid(2L)
            .setEntityType(2);
        final TopologyEntityDTO.Builder entity3 = TopologyEntityDTO.newBuilder()
            .setOid(3L)
            .setEntityType(3)
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .setProviderId(1L))
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
            .setProviderId(2L));

        final Map<Long, TopologyEntity.Builder> topologyMap = ImmutableMap.of(
            1L, TopologyEntityUtils.topologyEntityBuilder(entity1),
            2L, TopologyEntityUtils.topologyEntityBuilder(entity2),
            3L, TopologyEntityUtils.topologyEntityBuilder(entity3)
        );

        final TopologyGraph graph = TopologyGraph.newGraph(topologyMap);

        assertThat(
            graph.getProviders(3L)
                .map(TopologyEntity::getTopologyEntityDtoBuilder)
                .collect(Collectors.toList()),
            containsInAnyOrder(entity1, entity2)
        );
    }

    @Test
    public void testGetEmptyConsumers() throws Exception {
        final TopologyGraph graph = TopologyGraph.newGraph(topologyMap);

        assertThat(graph.getConsumers(5L).collect(Collectors.toList()), is(empty()));
    }

    @Test
    public void testMultipleConsumers() throws Exception {
        final TopologyGraph graph = TopologyGraph.newGraph(topologyMap);

        assertThat(
            graph.getConsumers(1L)
                .map(TopologyEntity::getTopologyEntityDtoBuilder)
                .collect(Collectors.toList()),
            containsInAnyOrder(entity2, entity3)
        );
        assertThat(
            graph.getConsumers(2L)
                .map(TopologyEntity::getTopologyEntityDtoBuilder)
                .collect(Collectors.toList()),
            containsInAnyOrder(entity4, entity5)
        );
    }

    @Test
    public void testGetConsumersNotInGraph() {
        final TopologyGraph graph = TopologyGraph.newGraph(Collections.emptyMap());

        assertThat(
            graph.getConsumers(1L).collect(Collectors.toList()),
            is(empty())
        );
    }

    @Test
    public void testGetProducersNotInGraph() {
        final TopologyGraph graph = TopologyGraph.newGraph(Collections.emptyMap());

        assertThat(
            graph.getProviders(1L).collect(Collectors.toList()),
            is(empty())
        );
    }

    @Test
    public void testGetEntity() {
        final TopologyGraph graph = TopologyGraph.newGraph(topologyMap);

        assertEquals(entity1, graph.getEntity(1L).get().getTopologyEntityDtoBuilder());
    }

    @Test
    public void testGetEntityNotInGraph() {
        final TopologyGraph graph = TopologyGraph.newGraph(Collections.emptyMap());

        assertFalse(graph.getEntity(1L).isPresent());
    }

    @Test
    public void testProducersForEntityWithMultipleConsumers() throws Exception {
        final TopologyGraph graph = TopologyGraph.newGraph(topologyMap);

        assertThat(
            graph.getProviders(2L)
                .map(TopologyEntity::getTopologyEntityDtoBuilder)
                .collect(Collectors.toList()),
            containsInAnyOrder(entity1)
        );
        assertThat(
            graph.getProviders(3L)
                .map(TopologyEntity::getTopologyEntityDtoBuilder)
                .collect(Collectors.toList()),
            containsInAnyOrder(entity1)
        );
    }

    @Test
    public void testConsumersForEntityWithMultipleProducers() throws Exception {
        /**
         *    3
         *   / \
         *  1   2
         */
        final TopologyEntityDTO.Builder entity1 = TopologyEntityDTO.newBuilder()
            .setOid(1L)
            .setEntityType(1);
        final TopologyEntityDTO.Builder entity2 = TopologyEntityDTO.newBuilder()
            .setOid(2L)
            .setEntityType(2);
        final TopologyEntityDTO.Builder entity3 = TopologyEntityDTO.newBuilder()
            .setOid(3L)
            .setEntityType(3)
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .setProviderId(1L))
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .setProviderId(2L));

        final Map<Long, TopologyEntity.Builder> topologyMap = ImmutableMap.of(
            1L, TopologyEntityUtils.topologyEntityBuilder(entity1),
            2L, TopologyEntityUtils.topologyEntityBuilder(entity2),
            3L, TopologyEntityUtils.topologyEntityBuilder(entity3)
        );

        final TopologyGraph graph = TopologyGraph.newGraph(topologyMap);

        assertThat(
            graph.getConsumers(1L)
                .map(TopologyEntity::getTopologyEntityDtoBuilder)
                .collect(Collectors.toList()),
            contains(entity3)
        );
        assertThat(
            graph.getConsumers(2L)
                .map(TopologyEntity::getTopologyEntityDtoBuilder)
                .collect(Collectors.toList()),
            contains(entity3)
        );
    }

    @Test
    public void testEntitiesOfType() {
        final TopologyGraph graph = TopologyGraph.newGraph(topologyMap);

        assertThat(
            graph.entitiesOfType(EntityType.VIRTUAL_MACHINE)
                .map(TopologyEntity::getTopologyEntityDtoBuilder)
                .collect(Collectors.toList()),
            containsInAnyOrder(entity4, entity5));
        assertThat(
            graph.entitiesOfType(EntityType.PHYSICAL_MACHINE)
                .map(TopologyEntity::getTopologyEntityDtoBuilder)
                .collect(Collectors.toList()),
            containsInAnyOrder(entity2, entity3));
        assertThat(
            graph.entitiesOfType(EntityType.DATACENTER)
                .map(TopologyEntity::getTopologyEntityDtoBuilder)
                .collect(Collectors.toList()),
            contains(entity1));
        assertThat(
            graph.entitiesOfType(EntityType.STORAGE)
                .map(TopologyEntity::getTopologyEntityDtoBuilder)
                .collect(Collectors.toList()),
            is(empty()));
    }

    @Test
    public void testEntitiesOfTypeByNumber() {
        final TopologyGraph graph = TopologyGraph.newGraph(topologyMap);

        assertThat(
            graph.entitiesOfType(EntityType.VIRTUAL_MACHINE.getNumber())
                .map(TopologyEntity::getTopologyEntityDtoBuilder)
                .collect(Collectors.toList()),
            containsInAnyOrder(entity4, entity5));
        assertThat(
            graph.entitiesOfType(EntityType.PHYSICAL_MACHINE.getNumber())
                .map(TopologyEntity::getTopologyEntityDtoBuilder)
                .collect(Collectors.toList()),
            containsInAnyOrder(entity2, entity3));
        assertThat(
            graph.entitiesOfType(EntityType.DATACENTER.getNumber())
                .map(TopologyEntity::getTopologyEntityDtoBuilder)
                .collect(Collectors.toList()),
            contains(entity1));
        assertThat(
            graph.entitiesOfType(EntityType.STORAGE.getNumber())
                .map(TopologyEntity::getTopologyEntityDtoBuilder)
                .collect(Collectors.toList()),
            is(empty()));
    }

    private int producerCount(@Nonnull final TopologyGraph graph) {
        return graph.entities().mapToInt(
            entity -> graph.getProviders(entity)
                .collect(Collectors.summingInt(v -> 1))
        ).sum();
    }

    private int consumerCount(@Nonnull final TopologyGraph graph) {
        return graph.entities().mapToInt(
            entity -> graph.getConsumers(entity)
                .collect(Collectors.summingInt(v -> 1))
        ).sum();
    }
}