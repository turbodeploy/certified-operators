package com.vmturbo.topology.graph.search.filter;

import static com.vmturbo.topology.graph.search.filter.FilterUtils.filterOids;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;

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
import com.vmturbo.topology.graph.SimpleCloudTopologyUtil;
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

    /**
     * Set up the topology.
     */
    @Before
    public void setup() {
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

    /**
     * Test the behavior of all connection filters that correspond to {@link TraversalDirection}.
     */
    @Test
    public void testConnectionFilters() {
        final TopologyGraph<TestGraphEntity> graph = SimpleCloudTopologyUtil.constructTopology();

        // check outgoing connection filter: this includes connections to owned and aggregated objects
        final TraversalToDepthFilter<TestGraphEntity> outboundConnectionFilter =
                new TraversalToDepthFilter<>(TraversalDirection.CONNECTED_TO, 1, null);
        assertThat(filterOids(outboundConnectionFilter, graph, SimpleCloudTopologyUtil.RG_ID),
                   containsInAnyOrder(SimpleCloudTopologyUtil.AZ_ID, SimpleCloudTopologyUtil.CT_ID,
                                      SimpleCloudTopologyUtil.ST_ID));
        assertThat(filterOids(outboundConnectionFilter, graph, SimpleCloudTopologyUtil.VM_ID), empty());
        assertThat(filterOids(outboundConnectionFilter, graph, SimpleCloudTopologyUtil.CT_ID),
                   contains(SimpleCloudTopologyUtil.ST_ID));

        // check ingoing connection filter: this includes connections to owner and aggregators
        final TraversalToDepthFilter<TestGraphEntity> inboundConnectionFilter =
                new TraversalToDepthFilter<>(TraversalDirection.CONNECTED_FROM, 1, null);
        assertThat(filterOids(inboundConnectionFilter, graph, SimpleCloudTopologyUtil.RG_ID), empty());
        assertThat(filterOids(inboundConnectionFilter, graph, SimpleCloudTopologyUtil.VM_ID),
                   contains(SimpleCloudTopologyUtil.AZ_ID));
        assertThat(filterOids(inboundConnectionFilter, graph, SimpleCloudTopologyUtil.ST_ID),
                   containsInAnyOrder(SimpleCloudTopologyUtil.RG_ID, SimpleCloudTopologyUtil.CT_ID));

        // check owned and aggregated entities filter
        final TraversalToDepthFilter<TestGraphEntity> ownedOrAggregatedFilter =
                new TraversalToDepthFilter<>(TraversalDirection.AGGREGATES, 1, null);
        assertThat(filterOids(ownedOrAggregatedFilter, graph, SimpleCloudTopologyUtil.RG_ID),
                   containsInAnyOrder(SimpleCloudTopologyUtil.AZ_ID, SimpleCloudTopologyUtil.CT_ID,
                                      SimpleCloudTopologyUtil.ST_ID));
        assertThat(filterOids(ownedOrAggregatedFilter, graph, SimpleCloudTopologyUtil.AZ_ID),
                   containsInAnyOrder(SimpleCloudTopologyUtil.VM_ID, SimpleCloudTopologyUtil.DB_ID));
        assertThat(filterOids(ownedOrAggregatedFilter, graph, SimpleCloudTopologyUtil.VM_ID), empty());
        assertThat(filterOids(ownedOrAggregatedFilter, graph, SimpleCloudTopologyUtil.CT_ID), empty());

        // check owner and aggregators filter
        final TraversalToDepthFilter<TestGraphEntity> ownsOrAggregatesFilter =
                new TraversalToDepthFilter<>(TraversalDirection.AGGREGATED_BY, 1, null);
        assertThat(filterOids(ownsOrAggregatesFilter, graph, SimpleCloudTopologyUtil.RG_ID), empty());
        assertThat(filterOids(ownsOrAggregatesFilter, graph, SimpleCloudTopologyUtil.VM_ID),
                   contains(SimpleCloudTopologyUtil.AZ_ID));
        assertThat(filterOids(ownsOrAggregatesFilter, graph, SimpleCloudTopologyUtil.ST_ID),
                   contains(SimpleCloudTopologyUtil.RG_ID));
        // check owned entities only filter
        final TraversalToDepthFilter<TestGraphEntity> ownsFilter =
                new TraversalToDepthFilter<>(TraversalDirection.OWNS, 1, null);
        assertThat(filterOids(ownsFilter, graph, SimpleCloudTopologyUtil.RG_ID),
                   contains(SimpleCloudTopologyUtil.AZ_ID));
        assertThat(filterOids(ownsFilter, graph, SimpleCloudTopologyUtil.AZ_ID), empty());
        assertThat(filterOids(ownsFilter, graph, SimpleCloudTopologyUtil.VM_ID), empty());
        assertThat(filterOids(ownsFilter, graph, SimpleCloudTopologyUtil.CT_ID), empty());

        // check owners only
        final TraversalToDepthFilter<TestGraphEntity> ownerFilter =
                new TraversalToDepthFilter<>(TraversalDirection.OWNED_BY, 1, null);
        assertThat(filterOids(ownerFilter, graph, SimpleCloudTopologyUtil.RG_ID), empty());
        assertThat(filterOids(ownerFilter, graph, SimpleCloudTopologyUtil.AZ_ID),
                   contains(SimpleCloudTopologyUtil.RG_ID));
        assertThat(filterOids(ownerFilter, graph, SimpleCloudTopologyUtil.VM_ID), empty());
        assertThat(filterOids(ownerFilter, graph, SimpleCloudTopologyUtil.ST_ID), empty());
    }
}