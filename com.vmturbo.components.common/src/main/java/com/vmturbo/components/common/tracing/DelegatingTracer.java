package com.vmturbo.components.common.tracing;

import javax.annotation.Nonnull;

import io.opentracing.Scope;
import io.opentracing.ScopeManager;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.noop.NoopTracerFactory;
import io.opentracing.propagation.Format;

/**
 * A {@link Tracer} with a dynamic underlying {@link Tracer} implementation.
 * <p>
 * We use this because we want to allow the {@link TracingManager} to change the tracing
 * configuration at runtime. Unfortunately, Jaeger's tracing implementation doesn't have
 * a good way of doing that. To get around it, we use the {@link DelegatingTracer} and re-create
 * the underlying tracer with a new configuration whenever it's necessary.
 */
public class DelegatingTracer implements Tracer {

    private volatile Tracer delegate = NoopTracerFactory.create();

    /**
     * Package-level visibility because it should only be used by {@link TracingManager}.
     */
    synchronized void updateDelegate(@Nonnull final Tracer tracer) {
        final Tracer oldDelegate = delegate;
        delegate = tracer;

        oldDelegate.close();
    }

    @Override
    public ScopeManager scopeManager() {
        return delegate.scopeManager();
    }

    @Override
    public Span activeSpan() {
        return delegate.activeSpan();
    }

    @Override
    public Scope activateSpan(final Span span) {
        return delegate.activateSpan(span);
    }

    @Override
    public SpanBuilder buildSpan(final String operationName) {
        return delegate.buildSpan(operationName);
    }

    @Override
    public <C> void inject(final SpanContext spanContext, final Format<C> format, final C carrier) {
        delegate.inject(spanContext, format, carrier);
    }

    @Override
    public <C> SpanContext extract(final Format<C> format, final C carrier) {
        return delegate.extract(format, carrier);
    }

    @Override
    public void close() {
        delegate.close();
    }
}
