package com.vmturbo.components.api.tracing;

import java.util.Optional;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnull;

import com.google.protobuf.InvalidProtocolBufferException;

import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.Tracer.SpanBuilder;
import io.opentracing.contrib.concurrent.TracedExecutorService;
import io.opentracing.contrib.grpc.OpenTracingContextKey;
import io.opentracing.util.GlobalTracer;

/**
 * Contains shared utilities for interacting with the tracing framework.
 */
public class Tracing {

    private Tracing() {}

    /**
     * Get the {@link Tracer} to use for all tracing operations.
     */
    @Nonnull
    public static synchronized Tracer tracer() {
        return GlobalTracer.get();
    }

    /**
     * A utility class to allow an "optional" {@link Scope} to be used in a try-with-resources
     * statement.
     */
    public static class OptScope implements AutoCloseable {
        private final Optional<Span> span;
        private final Optional<Scope> scope;
        private boolean closed = false;

        public OptScope(@Nonnull final Optional<Span> span) {
            this.span = span;
            this.scope = span.map(s -> Tracing.tracer().scopeManager().activate(s));
        }

        @Override
        public void close() {
            if (!closed) {
                scope.ifPresent(Scope::close);
                span.ifPresent(Span::finish);
                closed = true;
            }
        }
    }

    /**
     * Add an sub-operation to the current operation, if the current operation is being traced.
     * No effect if there is no active trace.
     *
     * In opentracing terms - if there is an active span, create a new child of the span and
     * start it. If not, this is a noop.
     *
     * @param name The name of the operation.
     * @return An {@link OptScope}, which must be closed when the sub-operation completes.
     */
    @Nonnull
    public static OptScope addOpToTrace(@Nonnull final String name) {
        final Optional<Span> childSpan = activeSpan().map(span -> tracer().buildSpan(name)
            .asChildOf(span)
            .start());

        return new OptScope(childSpan);
    }

    /**
     * Create a new trace. If there is an ongoing trace, the new trace will be a child of the
     * active trace. If not, the new trace will be a "root" trace.
     *
     * In opentracing terms - create and activate a new span.
     *
     * @param name The name of the operation.
     * @return A {@link OptScope}, which must be closed when the span completes.
     */
    @Nonnull
    public static OptScope newTrace(@Nonnull final String name) {
        final SpanBuilder spanBuilder = tracer().buildSpan(name);
        activeSpan().ifPresent(spanBuilder::asChildOf);
        // Because we're building a new span just for this scope, we want to finish
        // the span when the scope is closed.
        final Span span = spanBuilder.start();
        return new OptScope(Optional.of(span));
    }

    /**
     * Log a message to the currently ongoing trace. Noop if there is no ongoing trace.
     *
     * @param message The meesage.
     */
    public static void log(@Nonnull final String message) {
        activeSpan().ifPresent(span -> span.log(message));
    }

    /**
     * Log a message to the currently ongoing trace. Noop if there is no ongoing trace.
     * This is a lambda-version of {@link Tracing#log(String)}. Use it if the message requires
     * some computation.
     *
     * @param logSupplier The {@link TraceLogSupplier}.
     */
    public static void log(@Nonnull final TraceLogSupplier logSupplier) {
        activeSpan().ifPresent(span -> {
            try {
                span.log(logSupplier.getLog());
            } catch (InvalidProtocolBufferException e) {
                span.log("Log failed due to invalid protobuf: " + e.getMessage());
            }
        });
    }

    /**
     * A supplier for a log message, passed to {@link Tracing#log(TraceLogSupplier)} .
     */
    @FunctionalInterface
    public interface TraceLogSupplier {

        /**
         * @return The log message.
         * @throws InvalidProtocolBufferException If there is a problem formatting a protobuf
         *  for the log message. This is to prevent users of {@link Tracing#log(TraceLogSupplier)}
         *  from having to always catch error when the log includes a formatted protobuf.
         */
        @Nonnull
        String getLog() throws InvalidProtocolBufferException;

    }

    /**
     * Make an executor trace-aware. Use this on any {@link ExecutorService}s that need to
     * propagate active trace information to child threads.
     *
     * @param executor The {@link ExecutorService} to trace-ify.
     * @return The new {@link ExecutorService} to use.
     */
    @Nonnull
    public static ExecutorService traceAwareExecutor(@Nonnull final ExecutorService executor) {
        return new TracedExecutorService(executor, tracer());
    }

    /**
     * Get the currently active span (i.e. the active trace), if any.
     */
    @Nonnull
    public static Optional<Span> activeSpan() {
        Span activeSpan = tracer().activeSpan();
        if (activeSpan == null) {
            return Optional.ofNullable(OpenTracingContextKey.activeSpan());
        } else {
            return Optional.of(activeSpan);
        }
    }
}
