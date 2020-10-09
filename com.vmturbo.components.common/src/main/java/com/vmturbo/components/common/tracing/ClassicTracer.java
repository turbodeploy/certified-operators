package com.vmturbo.components.common.tracing;

import java.util.Objects;
import java.util.function.Supplier;

import javax.annotation.Nonnull;

import io.opentracing.Tracer;

import com.vmturbo.commons.ITracer;
import com.vmturbo.components.api.tracing.Tracing;
import com.vmturbo.components.api.tracing.Tracing.TracingScope;

/**
 * Provides basic interoperability for jaeger tracing to libraries in the Classic codebase.
 */
public class ClassicTracer implements ITracer {

    private final Tracer tracer;

    /**
     * Create a new ClassicTracer using the default tracer.
     */
    public ClassicTracer() {
        this(Tracing.tracer());
    }

    /**
     * Create a new ClassicTracer using a specific tracer.
     *
     * @param tracer The tracer to use.
     */
    public ClassicTracer(@Nonnull final Tracer tracer) {
        this.tracer = Objects.requireNonNull(tracer);
    }

    @Override
    public ITracerScope trace(@Nonnull final String name) {
        return new ClassicTracerScope(Tracing.trace(name, tracer));
    }

    /**
     * Implementation of {@link com.vmturbo.commons.ITracer.ITracerScope} for ClassicTracer.
     */
    public static class ClassicTracerScope implements ITracerScope {

        private final TracingScope scope;

        private ClassicTracerScope(@Nonnull final TracingScope scope) {
            this.scope = Objects.requireNonNull(scope);
        }

        @Override
        public void close() {
            scope.close();
        }

        @Override
        public void log(@Nonnull final Supplier<String> supplier) {
            Tracing.log(supplier::get);
        }
    }
}
