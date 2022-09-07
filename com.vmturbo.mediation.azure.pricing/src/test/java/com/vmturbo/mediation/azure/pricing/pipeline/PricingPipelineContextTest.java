package com.vmturbo.mediation.azure.pricing.pipeline;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ImmutableList;

import org.junit.Test;

import com.vmturbo.mediation.util.target.status.ProbeStageTracker;

/**
 * Tests for PricingPipelineContext.
 */
public class PricingPipelineContextTest {
    /**
     * Verify that objects given to the context to close are closed in reverse order.
     *
     * @throws Exception should not happen and indicates a test failure.
     */
    @Test
    public void testAutoCloseSuccess() throws Exception {
        ProbeStageTracker<MockPricingProbeStage> tracker = new ProbeStageTracker<>(
                MockPricingProbeStage.DISCOVERY_STAGES);
        PricingPipelineContext<MockPricingProbeStage> context = new PricingPipelineContext<>(
                "text", tracker);

        List<Integer> closed = new ArrayList<>();

        // Add some closeables
        context.autoClose(new AutoCloseable() {
            @Override
            public void close() throws Exception {
                closed.add(1);
            }
        });

        context.autoClose(new AutoCloseable() {
            @Override
            public void close() throws Exception {
                closed.add(2);
            }
        });

        context.autoClose(new AutoCloseable() {
            @Override
            public void close() throws Exception {
                closed.add(3);
            }
        });

        context.close();

        // Verify they were closed in LIFO order.

        assertEquals(ImmutableList.of(3, 2, 1), closed);
    }

    /**
     * Verify handling of exceptions from objects given to the context to close.
     *
     * @throws Exception should not happen and indicates a test failure.
     */
    @Test
    public void testAutoCloseError() throws Exception {
        ProbeStageTracker<MockPricingProbeStage> tracker = new ProbeStageTracker<>(
                MockPricingProbeStage.DISCOVERY_STAGES);
        PricingPipelineContext<MockPricingProbeStage> context = new PricingPipelineContext<>(
                "text", tracker);

        List<Integer> closed = new ArrayList<>();

        // Add some closeables. The last two throw exceptions.

        context.autoClose(new AutoCloseable() {
            @Override
            public void close() throws Exception {
                closed.add(1);
            }
        });

        context.autoClose(new AutoCloseable() {
            @Override
            public void close() throws Exception {
                closed.add(2);
                throw new RuntimeException("A");
            }
        });

        context.autoClose(new AutoCloseable() {
            @Override
            public void close() throws Exception {
                closed.add(3);
                throw new RuntimeException("B");
            }
        });

        RuntimeException ex = null;
        try {
            context.close();
        } catch (RuntimeException e) {
            ex = e;
        }

        // Verify that the close call re-threw the first exception...
        assertNotNull(ex);
        assertEquals("B", ex.getMessage());

        // ...but that it didn't bail out, and carried on closing the others first
        assertEquals(ImmutableList.of(3, 2, 1), closed);
    }
}
