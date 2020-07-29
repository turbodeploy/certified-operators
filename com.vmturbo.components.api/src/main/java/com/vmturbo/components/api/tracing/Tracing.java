package com.vmturbo.components.api.tracing;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.protobuf.InvalidProtocolBufferException;

import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.SpanContext;
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
     * A utility class to allow a {@link Scope} to be used in a try-with-resources statement.
     * Bundles an OpenTracing Span with its associated scope.
     */
    public static class TracingScope implements AutoCloseable {
        private final Span span;
        private final Scope scope;
        private boolean closed = false;

        /**
         * Create a new {@link TracingScope} for the givenv span.
         *
         * @param span the span in the scope.
         */
        public TracingScope(@Nonnull final Span span) {
            this.span = span;
            this.scope = Tracing.tracer().scopeManager().activate(span);
        }

        @Override
        public void close() {
            if (!closed) {
                // We explicitly close the scope otherwise you wind up with a huge ladder of scopes
                // in your trace where follows-from spans are incorrectly continued as children of
                // earlier spans because the earlier spans scopes are still open.
                scope.close();
                span.finish();
                closed = true;
            }
        }

        /**
         * Tag the trace  in the scope with a key-value pair.
         * Noop if there is no trace in the scope or if the {@link TracingScope} has been closed.
         *
         * @param key The tag key.
         * @param value The tag value.
         * @return {@link this} for method chaining.
         */
        public TracingScope tag(@Nonnull final String key, @Nonnull final String value) {
            if (!closed) {
                span.setTag(key, value);
            }

            return this;
        }

        /**
         * Tag the trace  in the scope with a key-value pair.
         * Noop if the {@link TracingScope} has been closed.
         *
         * @param key The tag key.
         * @param value The tag value.
         * @return {@link this} for method chaining.
         */
        public TracingScope tag(@Nonnull final String key, @Nullable final Number value) {
            return tag(key, value == null ? "" : value.toString());
        }

        /**
         * Get the {@link SpanContext} associated with the contained span.
         *
         * @return the {@link SpanContext} associated with the contained span.
         */
        public SpanContext spanContext() {
            return span.context();
        }
    }

    /**
     * Small helper class that allows an {@code Optional<TracingScope>} to be used in
     * a try-with-resources statement.
     */
    public static class OptScope implements AutoCloseable {
        private final Optional<TracingScope> scope;

        /**
         * Create a new {@link OptScope} wrapping the Optional TracingScope.
         *
         * @param scope The Optional TracingScope to wrap.
         */
        public OptScope(@Nonnull final Optional<TracingScope> scope) {
            this.scope = Objects.requireNonNull(scope);
        }

        @Override
        public void close() {
            scope.ifPresent(TracingScope::close);
        }

        /**
         * Get the Optional TracingScope wrapped by the {@link OptScope}.
         *
         * @return the Optional TracingScope wrapped by the {@link OptScope}.
         */
        public Optional<TracingScope> getScope() {
            return scope;
        }

        /**
         * Tag the trace  in the scope with a key-value pair.
         * Noop if there is no trace in the scope or if the {@link TracingScope} has been closed.
         *
         * @param key The tag key.
         * @param value The tag value.
         * @return {@link this} for method chaining.
         */
        public OptScope tag(@Nonnull final String key, @Nonnull final String value) {
            scope.ifPresent(s -> s.tag(key, value));

            return this;
        }

        /**
         * Tag the trace  in the scope with a key-value pair.
         * Noop if there is no trace in the scope or if the {@link TracingScope} has been closed.
         *
         * @param key The tag key.
         * @param value The tag value.
         * @return {@link this} for method chaining.
         */
        public OptScope tag(@Nonnull final String key, @Nullable final Number value) {
            return tag(key, value == null ? "" : value.toString());
        }
    }

    /**
     * Add a child sub-operation to the current operation, if the current operation is being traced.
     * No effect if there is no active trace.
     * <p/>
     * In opentracing terms - if there is an active span, create a new child of the span and
     * start it. If not, this is a noop.
     *
     * @param name The name of the operation.
     * @return An {@link TracingScope}, which must be closed when the sub-operation completes.
     */
    @Nonnull
    public static OptScope childOfActiveSpan(@Nonnull final String name) {
        return new OptScope(activeSpan().map(span -> new TracingScope(
            tracer().buildSpan(name)
                .asChildOf(span)
                .start())));
    }

    /**
     * Create a new trace. If there is an ongoing trace, the new trace will be a child of the
     * active trace. If not, the new trace will be a "root" trace.
     *
     * In opentracing terms - create and activate a new span.
     *
     * @param name The name of the operation.
     * @return A {@link TracingScope}, which must be closed when the span completes.
     */
    @Nonnull
    public static TracingScope trace(@Nonnull final String name) {
        final SpanBuilder spanBuilder = tracer().buildSpan(name);
        activeSpan().ifPresent(spanBuilder::asChildOf);
        // Because we're building a new span just for this scope, we want to finish
        // the span when the scope is closed.
        final Span span = spanBuilder.start();
        return new TracingScope(span);
    }

    /**
     * Create a new trace for a given {@link SpanContext}. If there is an ongoing trace,
     * the new trace will become the active trace which results in the currently active trace
     * being paused.
     * <p/>
     * In opentracing terms - create and activate a span for the given span context.
     *
     * @param name The name of the operation.
     * @param tracingContext The OpenTracing {@link SpanContext} for the trace.
     * @return A {@link TracingScope}, which must be closed when the span completes.
     */
    @Nonnull
    public static TracingScope trace(@Nonnull final String name,
                                     @Nonnull final SpanContext tracingContext) {
        final SpanBuilder spanBuilder = tracer()
            .buildSpan(name)
            .asChildOf(tracingContext);
        // Because we're building a new span just for this scope, we want to finish
        // the span when the scope is closed.
        final Span span = spanBuilder.start();
        return new TracingScope(span);
    }

    /**
     * Create a new trace using the given tracer's sampling rules, but activated in the scope
     * of the global tracer. If there is an ongoing trace for the GLOBAL tracer,
     * the new trace will be a child of the active trace. If not, the new trace will be a "root" trace.
     * <p/>
     * In opentracing terms - create a span using the sampling rules of the sampling tracer,
     * but activate it for the global tracer.
     * <p/>
     * This can be used to, for example, force the creation of a span even when the global tracer has not
     * been configured to allow it if the samplingTracer has been configured to always sample.
     *
     * @param name The name of the operation.
     * @param samplingTracer The tracer whose sampling rules should be used to decide whether to actually
     *                       create or drop the new trace span.
     * @return A {@link TracingScope}, which must be closed when the span completes.
     */
    @Nonnull
    public static TracingScope trace(@Nonnull final String name,
                                     @Nonnull final Tracer samplingTracer) {
        final SpanBuilder spanBuilder = samplingTracer.buildSpan(name);
        activeSpan().ifPresent(spanBuilder::asChildOf);
        // Because we're building a new span just for this scope, we want to finish
        // the span when the scope is closed.
        final Span span = spanBuilder.start();
        return new TracingScope(span);
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
     * Tag the ongoing trace with a key-value pair. Noop if there is no ongoing trace.
     *
     * @param key The tag key.
     * @param value The tag value.
     */
    public static void tag(@Nonnull final String key, @Nonnull final String value) {
        activeSpan().ifPresent(span -> span.setTag(key, value));
    }

    /**
     * Tag the currently ongoing trace with a key-value pair. Noop if there is no ongoing trace.
     * This is a lambda-version of {@link Tracing#tag(String, String)}. Use it if the tag requires
     * some computation.
     *
     * @param tagSupplier The {@link TraceTagSupplier}.
     */
    public static void tag(@Nonnull final TraceTagSupplier tagSupplier) {
        activeSpan().ifPresent(span -> {
            final TraceTag tag = tagSupplier.getTag();
            span.setTag(tag.tagKey, tag.tagValue);
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
     * A supplier for a log message, passed to {@link Tracing#log(TraceLogSupplier)} .
     */
    public interface TraceTagSupplier {
        /**
         * Get the tag.
         *
         * @return The tag key-value pair.
         */
        @Nonnull
        TraceTag getTag();

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

    /**
     * Simple key-value pair for attaching tags to a trace.
     */
    public static class TraceTag {
        /**
         * Key for the tag.
         */
        public final String tagKey;
        /**
         * Value for the tag.
         */
        public final String tagValue;

        /**
         * Create a new {@link TraceTag}.
         *
         * @param key The tag key.
         * @param value The tag value.
         */
        public TraceTag(@Nonnull final String key, @Nonnull final String value) {
            this.tagKey = Objects.requireNonNull(key);
            this.tagValue = Objects.requireNonNull(value);
        }
    }
}
