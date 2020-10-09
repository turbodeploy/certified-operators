package com.vmturbo.components.common.tracing;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.Tracer.SpanBuilder;

import org.junit.Test;

import com.vmturbo.commons.ITracer.ITracerScope;

/**
 * Tests for {@link ClassicTracer}.
 */
public class ClassicTracerTest {

    /**
     * Test that the ClassicTracer interacts with the Tracing implementation to create
     * and close spans as expected.
     */
    @Test
    public void testTracingInteraction() {
        final Tracer tracer = mock(Tracer.class);
        final SpanBuilder spanBuilder = mock(SpanBuilder.class);
        when(spanBuilder.start()).thenReturn(mock(Span.class));
        when(tracer.buildSpan(eq("foo"))).thenReturn(spanBuilder);

        try (ITracerScope unused = new ClassicTracer(tracer).trace("foo")) {
            verify(tracer).buildSpan("foo");
        }
    }
}