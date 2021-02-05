package com.vmturbo.cloud.common.data;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nonnull;

/**
 * A mutable implementation mirroring {@link Duration}. This is really just a wrapper around a {@link Duration}
 * value, providing utility methods to perform operations on the wrapped {@link Duration} and persist
 * the output in a single operation.
 */
public class MutableDuration {

    private AtomicReference<Duration> duration = new AtomicReference<>(Duration.ZERO);

    private MutableDuration() {}

    /**
     * Adds {@code operand} to this mutable duration value.
     * @param operand The {@link Duration} to add.
     * @return The updated {@link Duration} value wrapped by this {@link MutableDuration} instance.
     */
    @Nonnull
    public Duration add(Duration operand) {
        return duration.updateAndGet((d) -> d.plus(operand));
    }

    /**
     * Subtracts {@code operand} to this mutable duration value.
     * @param operand The {@link Duration} to subtract.
     * @return The updated {@link Duration} value wrapped by this {@link MutableDuration} instance.
     */
    @Nonnull
    public Duration subtract(Duration operand) {
        return duration.updateAndGet((d) -> d.minus(operand));
    }

    /**
     * Gets the wrapped {@link Duration} value.
     * @return The wrapped {@link Duration} value.
     */
    @Nonnull
    public Duration toDuration() {
        return duration.get();
    }

    /**
     * Converts the current duration to milliseconds.
     * @return The current duration of this instance, in milliseconds.
     */
    public long toMillis() {
        return duration.get().toMillis();
    }

    /**
     * Creates and returns a new {@link MutableDuration}, initialized to {@link Duration#ZERO}.
     * @return A new {@link MutableDuration}, initialized to {@link Duration#ZERO}.
     */
    @Nonnull
    public static MutableDuration zero() {
        return new MutableDuration();
    }

    /**
     * Creates and returns a new {@link MutableDuration}, initialized to {@code initialDuration}.
     * @param initialDuration The initial duration value of the newly created instance.
     * @return A new {@link MutableDuration}, initialized to {@code initialDuration}.
     */
    @Nonnull
    public static MutableDuration fromDuration(@Nonnull Duration initialDuration) {

        final MutableDuration mutableDuration = MutableDuration.zero();
        mutableDuration.add(initialDuration);

        return mutableDuration;
    }
}
