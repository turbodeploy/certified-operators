package com.vmturbo.topology.graph;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * Unit tests for {@link OwnershipGraph}.
 */
public class OwnershipGraphTest {

    /**
     * Test adding edges to the ownership graph.
     */
    @Test
    public void testOwnershipGraph() {
        final OwnershipGraph.Builder<Long> graphBldr = OwnershipGraph.newBuilder(id -> id);
        assertTrue(graphBldr.addOwner(1L, 2L));
        assertTrue(graphBldr.addOwner(1L, 4L));
        assertTrue(graphBldr.addOwner(2L, 3L));
        assertTrue(graphBldr.addOwner(3L, 5L));

        final OwnershipGraph<Long> graph = graphBldr.build();
        assertThat(graph.getOwners(1L), empty());
        assertThat(graph.getOwners(2L), contains(1L));
        assertThat(graph.getOwners(4L), contains(1L));
        assertThat(graph.getOwners(3L), contains(2L, 1L));
        assertThat(graph.getOwners(5L), contains(3L, 2L, 1L));

        assertThat(graph.getOwned(1L, false), containsInAnyOrder(2L, 4L));
        assertThat(graph.getOwned(1L, true), containsInAnyOrder(2L, 3L, 4L, 5L));
        assertThat(graph.getOwned(2L, false), containsInAnyOrder(3L));
        assertThat(graph.getOwned(2L, true), containsInAnyOrder(3L, 5L));
        assertThat(graph.getOwned(4L, false), empty());
        assertThat(graph.getOwned(4L, true), empty());
        assertThat(graph.getOwned(3L, false), containsInAnyOrder(5L));
        assertThat(graph.getOwned(3L, true), containsInAnyOrder(5L));
        assertThat(graph.getOwned(5L, false), empty());
        assertThat(graph.getOwned(5L, true), empty());
    }

    /**
     * Test that the ownership graph disallows adding multiple owners to a single entity.
     */
    @Test
    public void testGraphMultipleOwners() {
        final OwnershipGraph.Builder<Long> graphBldr = OwnershipGraph.newBuilder(id -> id);
        assertTrue(graphBldr.addOwner(1L, 2L));
        assertTrue(graphBldr.addOwner(2L, 3L));
        assertTrue(graphBldr.addOwner(1L, 4L));

        // This would add multiple owners to 2.
        assertFalse(graphBldr.addOwner(3L, 2L));

        // Edge "across" children. This would add multiple owners to 3.
        assertFalse(graphBldr.addOwner(4L, 3L));

        final OwnershipGraph<Long> graph = graphBldr.build();
        assertThat(graph.getOwners(2L), contains(1L));
        assertThat(graph.getOwners(4L), contains(1L));
        assertThat(graph.getOwners(3L), contains(2L, 1L));

        assertThat(graph.getOwned(1L, true), containsInAnyOrder(2L, 3L, 4L));
        assertThat(graph.getOwned(2L, true), containsInAnyOrder(3L));
    }

    /**
     * Test that the ownership graph disallows ownership cycles.
     */
    @Test
    public void testGraphCycle() {
        final OwnershipGraph.Builder<Long> graphBldr = OwnershipGraph.newBuilder(id -> id);
        assertTrue(graphBldr.addOwner(1L, 2L));
        assertTrue(graphBldr.addOwner(1L, 4L));
        assertTrue(graphBldr.addOwner(2L, 3L));
        assertTrue(graphBldr.addOwner(4L, 5L));

        // Adding 2 -> 1 would be the inverse of an existing edge.
        assertFalse(graphBldr.addOwner(2L, 1L));
        // Adding 4 -> 1 would be the inverse of an existing edge.
        assertFalse(graphBldr.addOwner(4L, 1L));

        // Adding 3 -> 1 would introduce a cycle.
        assertFalse(graphBldr.addOwner(3L, 1L));
        // Adding 5 -> 1 would introduce a cycle.
        assertFalse(graphBldr.addOwner(5L, 1L));

        final OwnershipGraph<Long> graph = graphBldr.build();
        assertThat(graph.getOwners(1L), empty());
        assertThat(graph.getOwners(2L), contains(1L));
        assertThat(graph.getOwners(4L), contains(1L));
        assertThat(graph.getOwners(3L), contains(2L, 1L));
        assertThat(graph.getOwners(5L), contains(4L, 1L));

        assertThat(graph.getOwned(1L, true), containsInAnyOrder(2L, 3L, 4L, 5L));
        assertThat(graph.getOwned(2L, true), containsInAnyOrder(3L));
        assertThat(graph.getOwned(3L, true), empty());
        assertThat(graph.getOwned(4L, true), containsInAnyOrder(5L));
        assertThat(graph.getOwned(5L, true), empty());
    }

}