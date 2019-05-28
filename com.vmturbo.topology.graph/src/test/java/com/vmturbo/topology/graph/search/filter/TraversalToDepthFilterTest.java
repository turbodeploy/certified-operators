package com.vmturbo.topology.graph.search.filter;

import static com.vmturbo.topology.graph.search.filter.FilterUtils.filterOids;
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
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.graph.TestGraphEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.graph.search.filter.TraversalFilter.TraversalToDepthFilter;

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
    private TopologyGraph<TestGraphEntity> topologyGraph;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setup() {
        final Map<Long, TestGraphEntity.Builder> topologyMap = new HashMap<>();

        topologyGraph = TestGraphEntity.newGraph(
            TestGraphEntity.newBuilder(1L, UIEntityType.PHYSICAL_MACHINE),
            TestGraphEntity.newBuilder(2L, UIEntityType.PHYSICAL_MACHINE),
            TestGraphEntity.newBuilder(3L, UIEntityType.PHYSICAL_MACHINE),
            TestGraphEntity.newBuilder(4L, UIEntityType.VIRTUAL_MACHINE)
                .addProviderId(1),
            TestGraphEntity.newBuilder(5L, UIEntityType.VIRTUAL_MACHINE)
                .addProviderId(1),
            TestGraphEntity.newBuilder(6L, UIEntityType.VIRTUAL_MACHINE)
                .addProviderId(2),
            TestGraphEntity.newBuilder(7L, UIEntityType.APPLICATION)
                .addProviderId(4)
        );
    }

    @Test
    public void testNegativeDepth() {
        expectedException.expect(IllegalArgumentException.class);

        new TraversalToDepthFilter<TestGraphEntity>(TraversalDirection.PRODUCES, -1, null);
    }

    @Test
    public void testZeroDepthProduces() {
        final TraversalToDepthFilter<TestGraphEntity> filter =
            new TraversalToDepthFilter<TestGraphEntity>(TraversalDirection.PRODUCES, 0, null);

        assertThat(filterOids(filter, topologyGraph, 1L), contains(1L));
    }

    @Test
    public void testZeroDepthProducesNotInGraph() {
        final TraversalToDepthFilter<TestGraphEntity> filter =
            new TraversalToDepthFilter<TestGraphEntity>(TraversalDirection.PRODUCES, 0, null);

        assertThat(filterOids(filter, topologyGraph, 9999L), is(empty()));
    }

    @Test
    public void testOneDepthProduces() {
        final TraversalToDepthFilter<TestGraphEntity> filter =
            new TraversalToDepthFilter<TestGraphEntity>(TraversalDirection.PRODUCES, 1, null);

        assertThat(filterOids(filter, topologyGraph, 1L), contains(4L, 5L));
    }

    @Test
    public void testOneDepthProducesNotInGraph() {
        final TraversalToDepthFilter<TestGraphEntity> filter =
            new TraversalToDepthFilter<TestGraphEntity>(TraversalDirection.PRODUCES, 1, null);

        assertThat(filterOids(filter, topologyGraph, 999L), is(empty()));
    }

    @Test
    public void testMultistartProduces() {
        final TraversalToDepthFilter<TestGraphEntity> filter =
            new TraversalToDepthFilter<TestGraphEntity>(TraversalDirection.PRODUCES, 1, null);

        assertThat(filterOids(filter, topologyGraph, 1L, 2L, 3L), contains(4L, 5L, 6L));
    }

    @Test
    public void testMultistartProducesNoRepeats() {
        final TraversalToDepthFilter<TestGraphEntity> filter =
            new TraversalToDepthFilter<TestGraphEntity>(TraversalDirection.PRODUCES, 1, null);

        assertThat(filterOids(filter, topologyGraph, 2L, 2L), contains(6L));
    }

    @Test
    public void testMultiLevelProduces() {
        final TraversalToDepthFilter<TestGraphEntity> filter =
            new TraversalToDepthFilter<TestGraphEntity>(TraversalDirection.PRODUCES, 2, null);

        assertThat(filterOids(filter, topologyGraph, 1L), contains(7L));
    }

    @Test
    public void testMultistartMultiLevelProduces() {
        final TraversalToDepthFilter<TestGraphEntity> filter =
            new TraversalToDepthFilter<TestGraphEntity>(TraversalDirection.PRODUCES, 2, null);

        assertThat(filterOids(filter, topologyGraph, 1L, 2L, 3L), contains(7L));
    }

    @Test
    public void testDeeperThanGraphProduces() {
        final TraversalToDepthFilter<TestGraphEntity> filter =
            new TraversalToDepthFilter<TestGraphEntity>(TraversalDirection.PRODUCES, 3, null);

        assertThat(filterOids(filter, topologyGraph, 1L), is(empty()));
    }

    @Test
    public void testZeroDepthConsumes() {
        final TraversalToDepthFilter<TestGraphEntity> filter =
            new TraversalToDepthFilter<TestGraphEntity>(TraversalDirection.CONSUMES, 0, null);

        assertThat(filterOids(filter, topologyGraph, 1L), contains(1L));
    }

    @Test
    public void testZeroDepthConsumesNotInGraph() {
        final TraversalToDepthFilter<TestGraphEntity> filter =
            new TraversalToDepthFilter<TestGraphEntity>(TraversalDirection.CONSUMES, 0, null);

        assertThat(filterOids(filter, topologyGraph, 9999L), is(empty()));
    }

    @Test
    public void testOneDepthConsumes() {
        final TraversalToDepthFilter<TestGraphEntity> filter =
            new TraversalToDepthFilter<TestGraphEntity>(TraversalDirection.CONSUMES, 1, null);

        assertThat(filterOids(filter, topologyGraph, 4L), contains(1L));
    }

    @Test
    public void testMultistartPrduces() {
        final TraversalToDepthFilter<TestGraphEntity> filter =
            new TraversalToDepthFilter<TestGraphEntity>(TraversalDirection.CONSUMES, 1, null);

        assertThat(filterOids(filter, topologyGraph, 4L, 6L), contains(1L, 2L));
    }

    @Test
    public void testMultistartConsumesNoRepeats() {
        final TraversalToDepthFilter<TestGraphEntity> filter =
            new TraversalToDepthFilter<TestGraphEntity>(TraversalDirection.CONSUMES, 1, null);

        assertThat(filterOids(filter, topologyGraph, 4L, 5L), contains(1L));
    }

    @Test
    public void testMultiLevelConsumes() {
        final TraversalToDepthFilter<TestGraphEntity> filter =
            new TraversalToDepthFilter<TestGraphEntity>(TraversalDirection.CONSUMES, 2, null);

        assertThat(filterOids(filter, topologyGraph, 7L), contains(1L));
    }

    @Test
    public void testMultistartMultiLevelConsumes() {
        final TraversalToDepthFilter<TestGraphEntity> filter =
            new TraversalToDepthFilter<TestGraphEntity>(TraversalDirection.CONSUMES, 2, null);

        assertThat(filterOids(filter, topologyGraph, 7L, 6L, 5L), contains(1L));
    }

    @Test
    public void testDeeperThanGraphConsumes() {
        final TraversalToDepthFilter<TestGraphEntity> filter =
            new TraversalToDepthFilter<TestGraphEntity>(TraversalDirection.CONSUMES, 3, null);

        assertThat(filterOids(filter, topologyGraph, 7L), is(empty()));
    }

    @Test
    public void testOneDepthProducesAndFilterByTwoConnectedVertices() {
        final TraversalToDepthFilter<TestGraphEntity> filter =
                new TraversalToDepthFilter<TestGraphEntity>(TraversalDirection.PRODUCES, 1,
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
        final TraversalToDepthFilter<TestGraphEntity> filter =
                new TraversalToDepthFilter<TestGraphEntity>(TraversalDirection.PRODUCES, 1,
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
        final TraversalToDepthFilter<TestGraphEntity> filter =
                new TraversalToDepthFilter<TestGraphEntity>(TraversalDirection.PRODUCES, 2,
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
        final TraversalToDepthFilter<TestGraphEntity> filter =
                new TraversalToDepthFilter<TestGraphEntity>(TraversalDirection.PRODUCES, 1,
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