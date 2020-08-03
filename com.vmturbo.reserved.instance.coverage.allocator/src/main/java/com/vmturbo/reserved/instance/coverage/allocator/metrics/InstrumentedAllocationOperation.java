package com.vmturbo.reserved.instance.coverage.allocator.metrics;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.proactivesupport.DataMetricTimer;

/**
 * Provides a wrapper around an operation (e.g. running the coverage analysis), in which operation
 * runtime will be monitored and collected (if a metric is configured).
 */
public class InstrumentedAllocationOperation implements RICoverageAllocationOperation {

    private final DataMetricTimerProvider timerProvider;
    private final Optional<Runnable> onCompletionListener;

    /**
     * Creates an instrumented allocation operation, with a configured callback (if provided)
     *
     * @param timerProvider The {@link DataMetricTimerProvider}, used to collect runtime duration
     *                      (if a timer is provided)
     * @param onCompletionListener An optional callback once the instrumented operation completes
     */
    public InstrumentedAllocationOperation(@Nonnull DataMetricTimerProvider timerProvider,
                                           @Nullable Runnable onCompletionListener) {
        this.timerProvider = Objects.requireNonNull(timerProvider);
        this.onCompletionListener = Optional.ofNullable(onCompletionListener);
    }

    /**
     * Creates an instrumented allocation operation
     *
     * @param timerProvider The {@link DataMetricTimerProvider}, used to collect runtime duration
     *                      (if a timer is provided)
     */
    public InstrumentedAllocationOperation(@Nonnull DataMetricTimerProvider timerProvider) {
        this(timerProvider, null);
    }

    /**
     * Measures the runtime duration of the provided {@code operation}
     * @param operation The operation to measure
     * @param <T> The return type of the operation
     * @return The return value of {@code operation}
     */
    @Override
    @Nullable
    public <T> T observe(@Nonnull Supplier<T> operation) {

        try (DataMetricTimer timer = timerProvider.startTimer()) {
            return operation.get();
        } finally {
            onCompletionListener.ifPresent(Runnable::run);
        }
    }

    /**
     * Measures the runtime duration of the provided {@code operation}
     * @param operation The operation to measure
     */
    @Override
    public void observe(@Nonnull Runnable operation) {
        try (DataMetricTimer timer = timerProvider.startTimer()) {
            operation.run();
        } finally {
            onCompletionListener.ifPresent(Runnable::run);
        }
    }
}
