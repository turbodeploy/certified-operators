package com.vmturbo.topology.graph.search.filter;

import static com.vmturbo.topology.graph.search.filter.FilterUtils.filterOids;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.LongStream;

import javax.annotation.Nonnull;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.search.Search.TraversalFilter.TraversalDirection;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.graph.TestGraphEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.graph.search.filter.TraversalFilter.TraversalToPropertyFilter;

/**
 * Test traversal to a specific property. The tests here choose to traverse to a particular entity type
 * but any property may be chosen.
 *
 * The tests use the following topology (no links are provided below 1, 2, or 3):
 *
 * Consider the following topology (links below E,F,G are not shown):
 *
 * VM-10
 *    |
 * VDC-9
 *    | \
 * VDC-5 VDC-6  VDC-7     VM-8
 *     \ /       |        /  \
 *    PM-1      PM-2   PM-3  PM-4
 */
public class TraversalToPropertyFilterTest {
    private TopologyGraph<TestGraphEntity> topologyGraph;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setup() {
        topologyGraph = TestGraphEntity.newGraph(
            TestGraphEntity.newBuilder(1L, UIEntityType.PHYSICAL_MACHINE),
            TestGraphEntity.newBuilder(2L, UIEntityType.PHYSICAL_MACHINE),
            TestGraphEntity.newBuilder(3L, UIEntityType.PHYSICAL_MACHINE),
            TestGraphEntity.newBuilder(4L, UIEntityType.PHYSICAL_MACHINE),
            TestGraphEntity.newBuilder(5L, UIEntityType.VIRTUAL_DATACENTER)
                .addProviderId(1L),
            TestGraphEntity.newBuilder(6L, UIEntityType.VIRTUAL_DATACENTER)
                .addProviderId(1L),
            TestGraphEntity.newBuilder(7L, UIEntityType.VIRTUAL_DATACENTER)
                .addProviderId(2L),
            TestGraphEntity.newBuilder(8L, UIEntityType.VIRTUAL_MACHINE)
                .addProviderId(3L)
                .addProviderId(4L),
            TestGraphEntity.newBuilder(9L, UIEntityType.VIRTUAL_DATACENTER)
                .addProviderId(5L)
                .addProviderId(6L),
            TestGraphEntity.newBuilder(10L, UIEntityType.VIRTUAL_MACHINE)
                .addProviderId(9L)
        );
    }

    @Test
    public void testTraverseProducesToEntityTypeAtStart() {
        final TraversalToPropertyFilter<TestGraphEntity> filter =
            new TraversalToPropertyFilter<>(TraversalDirection.PRODUCES,
                entityTypeFilter(EntityType.PHYSICAL_MACHINE));

        assertThat(filterOids(filter, topologyGraph, 1L), contains(1L));
    }

    @Test
    public void testZeroDepthProducesNotInGraph() {
        final TraversalToPropertyFilter<TestGraphEntity> filter =
            new TraversalToPropertyFilter<>(TraversalDirection.PRODUCES,
                entityTypeFilter(EntityType.PHYSICAL_MACHINE));

        assertThat(filterOids(filter, topologyGraph, 9999L), is(empty()));
    }

    @Test
    public void testTraverseProducesDirectNeighbor() {
        final TraversalToPropertyFilter<TestGraphEntity> filter =
            new TraversalToPropertyFilter<>(TraversalDirection.PRODUCES,
                entityTypeFilter(EntityType.VIRTUAL_DATACENTER));

        assertThat(filterOids(filter, topologyGraph, 2L), contains(7L));
    }

    @Test
    public void testTraverseProducesMultiLevel() {
        final TraversalToPropertyFilter<TestGraphEntity> filter =
            new TraversalToPropertyFilter<>(TraversalDirection.PRODUCES,
                entityTypeFilter(EntityType.VIRTUAL_MACHINE));

        assertThat(filterOids(filter, topologyGraph, 1L), contains(10L));
    }

    @Test
    public void testTraverseProducesStopsAtFirstMatch() {
        final TraversalToPropertyFilter<TestGraphEntity> filter =
            new TraversalToPropertyFilter<>(TraversalDirection.PRODUCES,
                entityTypeFilter(EntityType.VIRTUAL_DATACENTER));

        assertThat(filterOids(filter, topologyGraph, 5L, 6L), containsInAnyOrder(5L, 6L));
    }

    @Test
    public void testTraverseProducesToTypeNotInGraph() {
        final TraversalToPropertyFilter<TestGraphEntity> filter =
            new TraversalToPropertyFilter<>(TraversalDirection.PRODUCES,
                entityTypeFilter(EntityType.CONTAINER));

        assertThat(filterOids(filter, topologyGraph, 1L), is(empty()));
    }

    @Test
    public void testTraverseConsumesToEntityTypeAtStart() {
        final TraversalToPropertyFilter<TestGraphEntity> filter =
            new TraversalToPropertyFilter<>(TraversalDirection.CONSUMES,
                entityTypeFilter(EntityType.VIRTUAL_DATACENTER));

        assertThat(filterOids(filter, topologyGraph, 9L), contains(9L));
    }

    @Test
    public void testZeroDepthConsumesNotInGraph() {
        final TraversalToPropertyFilter<TestGraphEntity> filter =
            new TraversalToPropertyFilter<>(TraversalDirection.CONSUMES,
                entityTypeFilter(EntityType.PHYSICAL_MACHINE));

        assertThat(filterOids(filter, topologyGraph, 9999L), is(empty()));
    }

    @Test
    public void testTraverseConsumesDirectNeighbor() {
        final TraversalToPropertyFilter<TestGraphEntity> filter =
            new TraversalToPropertyFilter<>(TraversalDirection.CONSUMES,
                entityTypeFilter(EntityType.PHYSICAL_MACHINE));

        assertThat(filterOids(filter, topologyGraph, 7L), contains(2L));
    }

    @Test
    public void testTraverseConsumesMultiLevel() {
        final TraversalToPropertyFilter<TestGraphEntity> filter =
            new TraversalToPropertyFilter<>(TraversalDirection.CONSUMES,
                entityTypeFilter(EntityType.PHYSICAL_MACHINE));

        assertThat(filterOids(filter, topologyGraph, 10L), contains(1L));
    }

    @Test
    public void testTraverseConsumesMultipleMatches() {
        final TraversalToPropertyFilter<TestGraphEntity> filter =
            new TraversalToPropertyFilter<>(TraversalDirection.CONSUMES,
                entityTypeFilter(EntityType.PHYSICAL_MACHINE));

        assertThat(filterOids(filter, topologyGraph, 8L), contains(3L, 4L));
    }

    @Test
    public void testTraverseConsumesStopsAtFirstMatch() {
        final TraversalToPropertyFilter<TestGraphEntity> filter =
            new TraversalToPropertyFilter<>(TraversalDirection.CONSUMES,
                entityTypeFilter(EntityType.VIRTUAL_DATACENTER));

        assertThat(filterOids(filter, topologyGraph, 10L), containsInAnyOrder(9L));
    }

    @Test
    public void testTraverseConsumesToTypeNotInGraph() {
        final TraversalToPropertyFilter<TestGraphEntity> filter =
            new TraversalToPropertyFilter<>(TraversalDirection.CONSUMES,
                entityTypeFilter(EntityType.CONTAINER));

        assertThat(filterOids(filter, topologyGraph, 9L), is(empty()));
    }

    @Test
    public void testSupplyChainDeeperThanMaxRecursionDepth() {
        final Map<Long, TestGraphEntity.Builder> topologyMap = new HashMap<>();
        // Builds a graph that is a long chain
        // 0 <-- 1 <-- 2 <-- ... <-- (MAX_RECURSION_DEPTH + 1)
        topologyMap.put(0L, TestGraphEntity.newBuilder(0L, UIEntityType.VIRTUAL_DATACENTER));

        LongStream.range(1, TraversalToPropertyFilter.MAX_RECURSION_DEPTH + 1).forEach(i -> {
            topologyMap.put(i, TestGraphEntity.newBuilder(i, UIEntityType.VIRTUAL_DATACENTER)
                .addProviderId(i - 1));
        });
        long lastOid = TraversalToPropertyFilter.MAX_RECURSION_DEPTH + 1;
        topologyMap.put(lastOid, TestGraphEntity.newBuilder(lastOid, UIEntityType.VIRTUAL_MACHINE)
            .addProviderId( - 1));

        // This will cause us to exceed the max traversal depth, which should cause an early exit
        // that returns empty
        final TraversalToPropertyFilter<TestGraphEntity> filter = new TraversalToPropertyFilter<>(TraversalDirection.PRODUCES,
                entityTypeFilter(EntityType.VIRTUAL_MACHINE));

        assertThat(filterOids(filter, TestGraphEntity.newGraph(topologyMap), 0L), is(empty()));
    }

    /**
     * A simple property filter that returns true when a vertex's entityType matches the entityTYpe
     *
     * @param entityType The entityType to filterFor.
     * @return A {@link PropertyFilter} that returns true when a vertex's entityType matches the entityTYpe
     */
    private static PropertyFilter entityTypeFilter(@Nonnull final EntityType entityType) {
        PropertyFilter<TestGraphEntity> entityTypeFilter = Mockito.mock(PropertyFilter.class);
        doAnswer(invocation -> ((TestGraphEntity)invocation.getArguments()[0]).getEntityType() == entityType.getNumber())
            .when(entityTypeFilter).test(any(TestGraphEntity.class));

        return entityTypeFilter;
    }
}