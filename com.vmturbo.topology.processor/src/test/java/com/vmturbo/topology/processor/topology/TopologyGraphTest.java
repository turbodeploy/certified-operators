package com.vmturbo.topology.processor.topology;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommodityBoughtList;
import com.vmturbo.topology.processor.topology.TopologyGraph.Vertex;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class TopologyGraphTest {

    /**
     * 4   5
     *  \ /
     *   2   3
     *    \ /
     *     1
     */
    private final TopologyEntityDTO.Builder entity1 = TopologyEntityDTO.newBuilder()
        .setOid(1L)
        .setEntityType(1);
    private final TopologyEntityDTO.Builder entity2 = TopologyEntityDTO.newBuilder()
        .setOid(2L)
        .setEntityType(2)
        .putCommodityBoughtMap(1L, CommodityBoughtList.getDefaultInstance());
    private final TopologyEntityDTO.Builder entity3 = TopologyEntityDTO.newBuilder()
        .setOid(3L)
        .setEntityType(3)
        .putCommodityBoughtMap(1L, CommodityBoughtList.getDefaultInstance());
    private final TopologyEntityDTO.Builder entity4 = TopologyEntityDTO.newBuilder()
        .setOid(4L)
        .setEntityType(4)
        .putCommodityBoughtMap(2L, CommodityBoughtList.getDefaultInstance());
    private final TopologyEntityDTO.Builder entity5 = TopologyEntityDTO.newBuilder()
        .setOid(5L)
        .setEntityType(5)
        .putCommodityBoughtMap(2L, CommodityBoughtList.getDefaultInstance());

    private final Map<Long, TopologyEntityDTO.Builder> topologyMap = ImmutableMap.of(
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
        final TopologyGraph graph = new TopologyGraph(Collections.emptyMap());
        assertEquals(0, graph.vertexCount());
        assertEquals(0, producerCount(graph));
        assertEquals(0, consumerCount(graph));
    }

    @Test
    public void testConstruction() {
        final TopologyGraph graph = new TopologyGraph(topologyMap);
        assertEquals(5, graph.vertexCount());
        assertEquals(4, producerCount(graph));
        assertEquals(4, consumerCount(graph));
    }

    @Test
    public void testEntityAppearsMoreThanOnce() {
        final TopologyEntityDTO.Builder entity4Duplicate = TopologyEntityDTO.newBuilder()
            .setOid(4L)
            .setEntityType(4)
            .putCommodityBoughtMap(2L, CommodityBoughtList.getDefaultInstance());

        final Map<Long, TopologyEntityDTO.Builder> topologyMap = ImmutableMap.of(
            1L, entity1,
            2L, entity2,
            3L, entity3,
            4L, entity4,
            5L, entity4Duplicate
        );

        expectedException.expect(IllegalArgumentException.class);
        new TopologyGraph(topologyMap);
    }

    @Test
    public void testEntitiesInReverseOrder() {
        final Map<Long, TopologyEntityDTO.Builder> topologyMap = ImmutableMap.of(
            5L, entity5,
            4L, entity4,
            3L, entity3,
            2L, entity2,
            1L, entity1
        );

        // Verify that scanning an entity that links to an entity not yet seen
        // does not result in an error.
        new TopologyGraph(topologyMap);
    }

    @Test
    public void testGetEmptyProducers() throws Exception {
        final TopologyGraph graph = new TopologyGraph(topologyMap);

        assertThat(graph.getProducers(1L).collect(Collectors.toList()), is(empty()));
    }

    @Test
    public void testGetNonEmptyProducers() throws Exception {
        final TopologyGraph graph = new TopologyGraph(topologyMap);

        assertThat(
            graph.getProducers(2L)
                .map(Vertex::getTopologyEntityDtoBuilder)
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
            .putCommodityBoughtMap(1L, CommodityBoughtList.getDefaultInstance())
            .putCommodityBoughtMap(2L, CommodityBoughtList.getDefaultInstance());

        final Map<Long, TopologyEntityDTO.Builder> topologyMap = ImmutableMap.of(
            1L, entity1,
            2L, entity2,
            3L, entity3
        );

        final TopologyGraph graph = new TopologyGraph(topologyMap);

        assertThat(
            graph.getProducers(3L)
                .map(Vertex::getTopologyEntityDtoBuilder)
                .collect(Collectors.toList()),
            containsInAnyOrder(entity1, entity2)
        );
    }

    @Test
    public void testGetEmptyConsumers() throws Exception {
        final TopologyGraph graph = new TopologyGraph(topologyMap);

        assertThat(graph.getConsumers(5L).collect(Collectors.toList()), is(empty()));
    }

    @Test
    public void testMultipleConsumers() throws Exception {
        final TopologyGraph graph = new TopologyGraph(topologyMap);

        assertThat(
            graph.getConsumers(1L)
                .map(Vertex::getTopologyEntityDtoBuilder)
                .collect(Collectors.toList()),
            containsInAnyOrder(entity2, entity3)
        );
        assertThat(
            graph.getConsumers(2L)
                .map(Vertex::getTopologyEntityDtoBuilder)
                .collect(Collectors.toList()),
            containsInAnyOrder(entity4, entity5)
        );
    }

    @Test
    public void testGetConsumersNotInGraph() {
        final TopologyGraph graph = new TopologyGraph(Collections.emptyMap());

        assertThat(
            graph.getConsumers(1L).collect(Collectors.toList()),
            is(empty())
        );
    }

    @Test
    public void testGetProducersNotInGraph() {
        final TopologyGraph graph = new TopologyGraph(Collections.emptyMap());

        assertThat(
            graph.getProducers(1L).collect(Collectors.toList()),
            is(empty())
        );
    }

    @Test
    public void testGetVertex() {
        final TopologyGraph graph = new TopologyGraph(topologyMap);

        assertEquals(entity1, graph.getVertex(1L).get().getTopologyEntityDtoBuilder());
    }

    @Test
    public void testGetVertexNotInGraph() {
        final TopologyGraph graph = new TopologyGraph(Collections.emptyMap());

        assertFalse(graph.getVertex(1L).isPresent());
    }

    @Test
    public void testProducersForVertexWithMultipleConsumers() throws Exception {
        final TopologyGraph graph = new TopologyGraph(topologyMap);

        assertThat(
            graph.getProducers(2L)
                .map(Vertex::getTopologyEntityDtoBuilder)
                .collect(Collectors.toList()),
            containsInAnyOrder(entity1)
        );
        assertThat(
            graph.getProducers(3L)
                .map(Vertex::getTopologyEntityDtoBuilder)
                .collect(Collectors.toList()),
            containsInAnyOrder(entity1)
        );
    }

    @Test
    public void testConsumersForVertexWithMultipleProducers() throws Exception {
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
            .putCommodityBoughtMap(1L, CommodityBoughtList.getDefaultInstance())
            .putCommodityBoughtMap(2L, CommodityBoughtList.getDefaultInstance());

        final Map<Long, TopologyEntityDTO.Builder> topologyMap = ImmutableMap.of(
            1L, entity1,
            2L, entity2,
            3L, entity3
        );

        final TopologyGraph graph = new TopologyGraph(topologyMap);

        assertThat(
            graph.getConsumers(1L)
                .map(Vertex::getTopologyEntityDtoBuilder)
                .collect(Collectors.toList()),
            contains(entity3)
        );
        assertThat(
            graph.getConsumers(2L)
                .map(Vertex::getTopologyEntityDtoBuilder)
                .collect(Collectors.toList()),
            contains(entity3)
        );
    }

    private int producerCount(@Nonnull final TopologyGraph graph) {
        return graph.vertices().mapToInt(
            vertex -> graph.getProducers(vertex)
                .collect(Collectors.summingInt(v -> 1))
        ).sum();
    }

    private int consumerCount(@Nonnull final TopologyGraph graph) {
        return graph.vertices().mapToInt(
            vertex -> graph.getConsumers(vertex)
                .collect(Collectors.summingInt(v -> 1))
        ).sum();
    }
}