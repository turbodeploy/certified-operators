package com.vmturbo.components.api;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.collect.ImmutableMap;

/**
 * A utility class with runtime "final" semantics.
 *
 * @param <CONTENT> The type of value to set.
 */
@ThreadSafe
public class SetOnce<CONTENT> {

    @GuardedBy("lock")
    private CONTENT value = null;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * Get the set value, if any.
     *
     * @return An {@link Optional} containing the value, or an empty optional if no value has
     *         been set.
     */
    @Nonnull
    public Optional<CONTENT> getValue() {
        lock.readLock().lock();
        try {
            return Optional.ofNullable(value);
        } finally {
            lock.readLock().unlock();
        }
    }

    public boolean trySetValue(@Nonnull final Supplier<CONTENT> valueSupplier) {
        lock.writeLock().lock();
        try {
            if (value == null) {
                final CONTENT newValue = valueSupplier.get();
                Objects.requireNonNull(newValue);
                value = newValue;
                return true;
            } else {
                return false;
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Try to set the value. If the value is already set, this has no effect.
     *
     * @param newValue The value to set.
     * @return True if the value was set successfully. False otherwise. Note - if the existing value
     *         is equal to the value to set to, this will still return false.
     */
    public boolean trySetValue(@Nonnull final CONTENT newValue) {
        return trySetValue(() -> newValue);
    }
}
