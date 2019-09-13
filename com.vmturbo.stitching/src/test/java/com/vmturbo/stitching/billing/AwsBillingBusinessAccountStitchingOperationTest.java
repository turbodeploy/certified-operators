package com.vmturbo.stitching.billing;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.vmturbo.stitching.billing.AwsBillingBusinessAccountStitchingOperation.OwnershipGraph;

/**
 * Unit tests for the stitching operation utilities.
 * The real test is in the topology-processor module, under the stitching integration tests.
 */
public class AwsBillingBusinessAccountStitchingOperationTest {

    /**
     * Test adding edges to the ownership graph.
     */
    @Test
    public void testOwnershipGraph() {
        final OwnershipGraph graph = new OwnershipGraph();
        assertTrue(graph.addOwner(1L, 2L));
        assertTrue(graph.addOwner(1L, 4L));
        assertTrue(graph.addOwner(2L, 3L));
    }

    /**
     * Test that the ownership graph disallows adding multiple owners to a single entity.
     */
    @Test
    public void testGraphMultipleOwners() {
        final OwnershipGraph graph = new OwnershipGraph();
        assertTrue(graph.addOwner(1L, 2L));

        // This would add multiple owners to 2.
        assertFalse(graph.addOwner(3L, 2L));
    }

    /**
     * Test that the ownership graph disallows ownership cycles.
     */
    @Test
    public void testGraphCycle() {
        final OwnershipGraph graph = new OwnershipGraph();
        assertTrue(graph.addOwner(1L, 2L));
        assertTrue(graph.addOwner(2L, 3L));

        // Adding 2 -> 1 would be the inverse of an existing edge.
        assertFalse(graph.addOwner(2L, 1L));

        // Adding 3 -> 1 would introduce a cycle.
        assertFalse(graph.addOwner(3L, 1L));
    }

}