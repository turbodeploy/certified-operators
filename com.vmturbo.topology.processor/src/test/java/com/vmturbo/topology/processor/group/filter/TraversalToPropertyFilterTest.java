package com.vmturbo.topology.processor.group.filter;

import static com.vmturbo.topology.processor.group.filter.FilterUtils.filterOids;
import static com.vmturbo.topology.processor.group.filter.FilterUtils.topologyEntity;
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

import com.vmturbo.common.protobuf.search.Search.SearchFilter.TraversalFilter.TraversalDirection;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.processor.group.filter.TraversalFilter.TraversalToPropertyFilter;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.topology.TopologyGraph;

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
    private TopologyGraph topologyGraph;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setup() {
        final Map<Long, TopologyEntity.Builder> topologyMap = new HashMap<>();
        topologyMap.put(1L, topologyEntity(1L, EntityType.PHYSICAL_MACHINE));
        topologyMap.put(2L, topologyEntity(2L, EntityType.PHYSICAL_MACHINE));
        topologyMap.put(3L, topologyEntity(3L, EntityType.PHYSICAL_MACHINE));
        topologyMap.put(4L, topologyEntity(4L, EntityType.PHYSICAL_MACHINE));
        topologyMap.put(5L, topologyEntity(5L, EntityType.VIRTUAL_DATACENTER, 1));
        topologyMap.put(6L, topologyEntity(6L, EntityType.VIRTUAL_DATACENTER, 1));
        topologyMap.put(7L, topologyEntity(7L, EntityType.VIRTUAL_DATACENTER, 2));
        topologyMap.put(8L, topologyEntity(8L, EntityType.VIRTUAL_MACHINE, 3, 4));
        topologyMap.put(9L, topologyEntity(9L, EntityType.VIRTUAL_DATACENTER, 5, 6));
        topologyMap.put(10L, topologyEntity(10L, EntityType.VIRTUAL_MACHINE, 9));

        topologyGraph = TopologyGraph.newGraph(topologyMap);
    }

    @Test
    public void testTraverseProducesToEntityTypeAtStart() {
        final TraversalToPropertyFilter filter =
            new TraversalToPropertyFilter(TraversalDirection.PRODUCES,
                entityTypeFilter(EntityType.PHYSICAL_MACHINE));

        assertThat(filterOids(filter, topologyGraph, 1L), contains(1L));
    }

    @Test
    public void testZeroDepthProducesNotInGraph() {
        final TraversalToPropertyFilter filter =
            new TraversalToPropertyFilter(TraversalDirection.PRODUCES,
                entityTypeFilter(EntityType.PHYSICAL_MACHINE));

        assertThat(filterOids(filter, topologyGraph, 9999L), is(empty()));
    }

    @Test
    public void testTraverseProducesDirectNeighbor() {
        final TraversalToPropertyFilter filter =
            new TraversalToPropertyFilter(TraversalDirection.PRODUCES,
                entityTypeFilter(EntityType.VIRTUAL_DATACENTER));

        assertThat(filterOids(filter, topologyGraph, 2L), contains(7L));
    }

    @Test
    public void testTraverseProducesMultiLevel() {
        final TraversalToPropertyFilter filter =
            new TraversalToPropertyFilter(TraversalDirection.PRODUCES,
                entityTypeFilter(EntityType.VIRTUAL_MACHINE));

        assertThat(filterOids(filter, topologyGraph, 1L), contains(10L));
    }

    @Test
    public void testTraverseProducesStopsAtFirstMatch() {
        final TraversalToPropertyFilter filter =
            new TraversalToPropertyFilter(TraversalDirection.PRODUCES,
                entityTypeFilter(EntityType.VIRTUAL_DATACENTER));

        assertThat(filterOids(filter, topologyGraph, 5L, 6L), containsInAnyOrder(5L, 6L));
    }

    @Test
    public void testTraverseProducesToTypeNotInGraph() {
        final TraversalToPropertyFilter filter =
            new TraversalToPropertyFilter(TraversalDirection.PRODUCES,
                entityTypeFilter(EntityType.CONTAINER));

        assertThat(filterOids(filter, topologyGraph, 1L), is(empty()));
    }

    @Test
    public void testTraverseConsumesToEntityTypeAtStart() {
        final TraversalToPropertyFilter filter =
            new TraversalToPropertyFilter(TraversalDirection.CONSUMES,
                entityTypeFilter(EntityType.VIRTUAL_DATACENTER));

        assertThat(filterOids(filter, topologyGraph, 9L), contains(9L));
    }

    @Test
    public void testZeroDepthConsumesNotInGraph() {
        final TraversalToPropertyFilter filter =
            new TraversalToPropertyFilter(TraversalDirection.CONSUMES,
                entityTypeFilter(EntityType.PHYSICAL_MACHINE));

        assertThat(filterOids(filter, topologyGraph, 9999L), is(empty()));
    }

    @Test
    public void testTraverseConsumesDirectNeighbor() {
        final TraversalToPropertyFilter filter =
            new TraversalToPropertyFilter(TraversalDirection.CONSUMES,
                entityTypeFilter(EntityType.PHYSICAL_MACHINE));

        assertThat(filterOids(filter, topologyGraph, 7L), contains(2L));
    }

    @Test
    public void testTraverseConsumesMultiLevel() {
        final TraversalToPropertyFilter filter =
            new TraversalToPropertyFilter(TraversalDirection.CONSUMES,
                entityTypeFilter(EntityType.PHYSICAL_MACHINE));

        assertThat(filterOids(filter, topologyGraph, 10L), contains(1L));
    }

    @Test
    public void testTraverseConsumesMultipleMatches() {
        final TraversalToPropertyFilter filter =
            new TraversalToPropertyFilter(TraversalDirection.CONSUMES,
                entityTypeFilter(EntityType.PHYSICAL_MACHINE));

        assertThat(filterOids(filter, topologyGraph, 8L), contains(3L, 4L));
    }

    @Test
    public void testTraverseConsumesStopsAtFirstMatch() {
        final TraversalToPropertyFilter filter =
            new TraversalToPropertyFilter(TraversalDirection.CONSUMES,
                entityTypeFilter(EntityType.VIRTUAL_DATACENTER));

        assertThat(filterOids(filter, topologyGraph, 10L), containsInAnyOrder(9L));
    }

    @Test
    public void testTraverseConsumesToTypeNotInGraph() {
        final TraversalToPropertyFilter filter =
            new TraversalToPropertyFilter(TraversalDirection.CONSUMES,
                entityTypeFilter(EntityType.CONTAINER));

        assertThat(filterOids(filter, topologyGraph, 9L), is(empty()));
    }

    @Test
    public void testSupplyChainDeeperThanMaxRecursionDepth() {
        final Map<Long, TopologyEntity.Builder> topologyMap = new HashMap<>();
        // Builds a graph that is a long chain
        // 0 <-- 1 <-- 2 <-- ... <-- (MAX_RECURSION_DEPTH + 1)
        topologyMap.put(0L, topologyEntity(0L, EntityType.VIRTUAL_DATACENTER));

        LongStream.range(1, TraversalToPropertyFilter.MAX_RECURSION_DEPTH + 1).forEach(i -> {
            topologyMap.put(i, topologyEntity(i, EntityType.VIRTUAL_DATACENTER, i - 1));
        });
        long lastOid = TraversalToPropertyFilter.MAX_RECURSION_DEPTH + 1;
        topologyMap.put(lastOid, topologyEntity(lastOid, EntityType.VIRTUAL_MACHINE, lastOid - 1));

        // This will cause us to exceed the max traversal depth, which should cause an early exit
        // that returns empty
        final TraversalToPropertyFilter filter = new TraversalToPropertyFilter(TraversalDirection.PRODUCES,
                entityTypeFilter(EntityType.VIRTUAL_MACHINE));

        assertThat(filterOids(filter, TopologyGraph.newGraph(topologyMap), 0L), is(empty()));
    }

    /**
     * A simple property filter that returns true when a vertex's entityType matches the entityTYpe
     *
     * @param entityType The entityType to filterFor.
     * @return A {@link PropertyFilter} that returns true when a vertex's entityType matches the entityTYpe
     */
    private static PropertyFilter entityTypeFilter(@Nonnull final EntityType entityType) {
        PropertyFilter entityTypeFilter = Mockito.mock(PropertyFilter.class);
        doAnswer(invocation -> ((TopologyEntity)invocation.getArguments()[0]).getEntityType() == entityType.getNumber())
            .when(entityTypeFilter).test(any(TopologyEntity.class));

        return entityTypeFilter;
    }
}