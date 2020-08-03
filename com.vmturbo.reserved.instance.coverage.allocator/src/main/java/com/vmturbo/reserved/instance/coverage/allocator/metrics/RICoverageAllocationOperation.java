package com.vmturbo.reserved.instance.coverage.allocator.metrics;

import java.util.function.Supplier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Provides a wrapping interface around discrete steps within the RI coverage allocation analysis. This allows
 * for collecting runtime metrics of different steps throughout the analysis lifecycle.
 */
public interface RICoverageAllocationOperation {

    /**
     * Observe an operation defined as a {@link Supplier}
     * @param operation The operation to observe
     * @param <T> The return type of {@code operation}
     * @return The value returned by {@code operation}
     */
    @Nullable
    <T> T observe(@Nonnull Supplier<T> operation);

    /**
     * Observe an operation defined as a {@link Runnable}
     * @param operation The operation to observe
     */
    void observe(@Nonnull Runnable operation);

    /**
     * A pass through wrapper, which simply invokes the operation supplied, without collecting any
     * metrics based on the operation.
     */
    RICoverageAllocationOperation PASS_THROUGH_OPERATION = new RICoverageAllocationOperation() {
        @Nullable
        @Override
        public <T> T observe(@Nonnull final Supplier<T> operation) {
            return operation.get();
        }

        @Override
        public void observe(@Nonnull final Runnable operation) {
            operation.run();
        }
    };
}
