package com.vmturbo.components.api;

import java.util.Optional;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

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

    /**
     * Ensure that the {@link SetOnce} is set to a value, using the provided supplier if necessary.
     *
     * @param valueSupplier Supplier for the value if it's not currently set.
     * @return The value the {@link SetOnce} is set to. This will only return null if:
     *           1) The value is not currently set AND
     *           2) The provided supplier returns null.
     */
    @Nullable
    public CONTENT ensureSet(@Nonnull Supplier<CONTENT> valueSupplier) {
        CONTENT val;
        // The fast path, to avoid a write lock if the value is already set (which should be
        // the case most of the time).
        lock.readLock().lock();
        try {
            val = value;
        } finally {
            lock.readLock().unlock();
        }

        if (val == null) {
            trySetValue(valueSupplier);
        }
        // This shouldn't be null, because trySetValue should have initialized the value.
        return value;
    }

    public boolean trySetValue(@Nonnull final Supplier<CONTENT> valueSupplier) {
        // Careful with the locks! OM-50075
        // Functions passed here might be using other locks, such as the concurrent hashmap
        // targetsById. We need to make sure that from the moment we take the write lock to the
        // moment we release it we don't need other locks such as those from a concurrent hashmap,
        // since this can cause a deadlock.
        final CONTENT newValue = valueSupplier.get();

        lock.writeLock().lock();
        try {
            if (value == null) {
                if (newValue != null) {
                    value = newValue;
                }
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
