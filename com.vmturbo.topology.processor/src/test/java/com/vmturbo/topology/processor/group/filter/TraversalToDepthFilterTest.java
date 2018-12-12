package com.vmturbo.topology.processor.group.filter;

import static com.vmturbo.topology.processor.group.filter.FilterUtils.filterOids;
import static com.vmturbo.topology.processor.group.filter.FilterUtils.topologyEntity;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.vmturbo.common.protobuf.search.Search.ComparisonOperator;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.NumericFilter;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.StoppingCondition.VerticesCondition;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.TraversalDirection;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.group.filter.TraversalFilter.TraversalToDepthFilter;
import com.vmturbo.topology.processor.topology.TopologyGraph;

/**
 * Test traversal to a fixed depth.
 *
 * The tests use the following topology (no links are provided below 1, 2, or 3):
 *
 *   7
 *   |
 *   4   5  6
 *    \ /   |
 *     1    2   3
 */
public class TraversalToDepthFilterTest {
    private TopologyGraph topologyGraph;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setup() {
        final Map<Long, TopologyEntity.Builder> topologyMap = new HashMap<>();
        topologyMap.put(1L, topologyEntity(1L, EntityType.PHYSICAL_MACHINE));
        topologyMap.put(2L, topologyEntity(2L, EntityType.PHYSICAL_MACHINE));
        topologyMap.put(3L, topologyEntity(3L, EntityType.PHYSICAL_MACHINE));
        topologyMap.put(4L, topologyEntity(4L, EntityType.VIRTUAL_MACHINE, 1));
        topologyMap.put(5L, topologyEntity(5L, EntityType.VIRTUAL_MACHINE, 1));
        topologyMap.put(6L, topologyEntity(6L, EntityType.VIRTUAL_MACHINE, 2));
        topologyMap.put(7L, topologyEntity(7L, EntityType.APPLICATION, 4));

        topologyGraph = TopologyGraph.newGraph(topologyMap);
    }

    @Test
    public void testNegativeDepth() {
        expectedException.expect(IllegalArgumentException.class);

        new TraversalToDepthFilter(TraversalDirection.PRODUCES, -1, null);
    }

    @Test
    public void testZeroDepthProduces() {
        final TraversalToDepthFilter filter =
            new TraversalToDepthFilter(TraversalDirection.PRODUCES, 0, null);

        assertThat(filterOids(filter, topologyGraph, 1L), contains(1L));
    }

    @Test
    public void testZeroDepthProducesNotInGraph() {
        final TraversalToDepthFilter filter =
            new TraversalToDepthFilter(TraversalDirection.PRODUCES, 0, null);

        assertThat(filterOids(filter, topologyGraph, 9999L), is(empty()));
    }

    @Test
    public void testOneDepthProduces() {
        final TraversalToDepthFilter filter =
            new TraversalToDepthFilter(TraversalDirection.PRODUCES, 1, null);

        assertThat(filterOids(filter, topologyGraph, 1L), contains(4L, 5L));
    }

    @Test
    public void testOneDepthProducesNotInGraph() {
        final TraversalToDepthFilter filter =
            new TraversalToDepthFilter(TraversalDirection.PRODUCES, 1, null);

        assertThat(filterOids(filter, topologyGraph, 999L), is(empty()));
    }

    @Test
    public void testMultistartProduces() {
        final TraversalToDepthFilter filter =
            new TraversalToDepthFilter(TraversalDirection.PRODUCES, 1, null);

        assertThat(filterOids(filter, topologyGraph, 1L, 2L, 3L), contains(4L, 5L, 6L));
    }

    @Test
    public void testMultistartProducesNoRepeats() {
        final TraversalToDepthFilter filter =
            new TraversalToDepthFilter(TraversalDirection.PRODUCES, 1, null);

        assertThat(filterOids(filter, topologyGraph, 2L, 2L), contains(6L));
    }

    @Test
    public void testMultiLevelProduces() {
        final TraversalToDepthFilter filter =
            new TraversalToDepthFilter(TraversalDirection.PRODUCES, 2, null);

        assertThat(filterOids(filter, topologyGraph, 1L), contains(7L));
    }

    @Test
    public void testMultistartMultiLevelProduces() {
        final TraversalToDepthFilter filter =
            new TraversalToDepthFilter(TraversalDirection.PRODUCES, 2, null);

        assertThat(filterOids(filter, topologyGraph, 1L, 2L, 3L), contains(7L));
    }

    @Test
    public void testDeeperThanGraphProduces() {
        final TraversalToDepthFilter filter =
            new TraversalToDepthFilter(TraversalDirection.PRODUCES, 3, null);

        assertThat(filterOids(filter, topologyGraph, 1L), is(empty()));
    }

    @Test
    public void testZeroDepthConsumes() {
        final TraversalToDepthFilter filter =
            new TraversalToDepthFilter(TraversalDirection.CONSUMES, 0, null);

        assertThat(filterOids(filter, topologyGraph, 1L), contains(1L));
    }

    @Test
    public void testZeroDepthConsumesNotInGraph() {
        final TraversalToDepthFilter filter =
            new TraversalToDepthFilter(TraversalDirection.CONSUMES, 0, null);

        assertThat(filterOids(filter, topologyGraph, 9999L), is(empty()));
    }

    @Test
    public void testOneDepthConsumes() {
        final TraversalToDepthFilter filter =
            new TraversalToDepthFilter(TraversalDirection.CONSUMES, 1, null);

        assertThat(filterOids(filter, topologyGraph, 4L), contains(1L));
    }

    @Test
    public void testMultistartPrduces() {
        final TraversalToDepthFilter filter =
            new TraversalToDepthFilter(TraversalDirection.CONSUMES, 1, null);

        assertThat(filterOids(filter, topologyGraph, 4L, 6L), contains(1L, 2L));
    }

    @Test
    public void testMultistartConsumesNoRepeats() {
        final TraversalToDepthFilter filter =
            new TraversalToDepthFilter(TraversalDirection.CONSUMES, 1, null);

        assertThat(filterOids(filter, topologyGraph, 4L, 5L), contains(1L));
    }

    @Test
    public void testMultiLevelConsumes() {
        final TraversalToDepthFilter filter =
            new TraversalToDepthFilter(TraversalDirection.CONSUMES, 2, null);

        assertThat(filterOids(filter, topologyGraph, 7L), contains(1L));
    }

    @Test
    public void testMultistartMultiLevelConsumes() {
        final TraversalToDepthFilter filter =
            new TraversalToDepthFilter(TraversalDirection.CONSUMES, 2, null);

        assertThat(filterOids(filter, topologyGraph, 7L, 6L, 5L), contains(1L));
    }

    @Test
    public void testDeeperThanGraphConsumes() {
        final TraversalToDepthFilter filter =
            new TraversalToDepthFilter(TraversalDirection.CONSUMES, 3, null);

        assertThat(filterOids(filter, topologyGraph, 7L), is(empty()));
    }

    @Test
    public void testOneDepthProducesAndFilterByTwoConnectedVertices() {
        final TraversalToDepthFilter filter =
                new TraversalToDepthFilter(TraversalDirection.PRODUCES, 1,
                        VerticesCondition.newBuilder()
                                .setNumConnectedVertices(NumericFilter.newBuilder()
                                        .setValue(2)
                                        .setComparisonOperator(ComparisonOperator.EQ)
                                        .build())
                                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                                .build());

        assertThat(filterOids(filter, topologyGraph, 1L), contains(1L));
    }

    @Test
    public void testOneDepthProducesAndFilterByMoreThanTwoConnectedVertices() {
        final TraversalToDepthFilter filter =
                new TraversalToDepthFilter(TraversalDirection.PRODUCES, 1,
                        VerticesCondition.newBuilder()
                                .setNumConnectedVertices(NumericFilter.newBuilder()
                                        .setValue(2)
                                        .setComparisonOperator(ComparisonOperator.GT)
                                        .build())
                                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                                .build());

        assertThat(filterOids(filter, topologyGraph, 1L), empty());
    }

    @Test
    public void testTwoDepthProducesAndFilterByTwoConnectedVertices() {
        final TraversalToDepthFilter filter =
                new TraversalToDepthFilter(TraversalDirection.PRODUCES, 2,
                        VerticesCondition.newBuilder()
                                .setNumConnectedVertices(NumericFilter.newBuilder()
                                        .setValue(1)
                                        .setComparisonOperator(ComparisonOperator.EQ)
                                        .build())
                                .setEntityType(EntityType.APPLICATION_VALUE)
                                .build());

        assertThat(filterOids(filter, topologyGraph, 1L), contains(1L));
    }

    @Test
    public void testMultiStartOneDepthProducesAndFilterByMoreThanZeroConnectedVertices() {
        final TraversalToDepthFilter filter =
                new TraversalToDepthFilter(TraversalDirection.PRODUCES, 1,
                        VerticesCondition.newBuilder()
                                .setNumConnectedVertices(NumericFilter.newBuilder()
                                        .setValue(0)
                                        .setComparisonOperator(ComparisonOperator.GT)
                                        .build())
                                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                                .build());

        assertThat(filterOids(filter, topologyGraph, 1L, 2L, 3L), contains(1L, 2L));
    }
}