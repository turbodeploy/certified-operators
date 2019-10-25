package com.vmturbo.matrix.component;

import javax.annotation.Nonnull;

/**
 * The volatile long.
 */
public class VolatileLong implements Comparable<VolatileLong> {
    /**
     * The underlying value.
     * We use that directly in some cases for performance reasons.
     */
    long value;

    /**
     * The hash code.
     */
    private transient int hash;

    /**
     * The private constructor.
     * Used for creation of copies.
     *
     * @param v The value.
     * @param h The hash.
     */
    private VolatileLong(final long v, final int h) {
        value = v;
        hash = h;
    }

    /**
     * Constructs the volatile long.
     *
     * @param v The value.
     */
    VolatileLong(final long v) {
        setValue(v);
    }

    /**
     * Sets the value and pre-calculates hash code..
     *
     * @param v The new value.
     * @return {@code this}.
     */
    VolatileLong setValue(final long v) {
        final boolean changed = v != value;
        value = v;
        // Maybe not saving a lot, but every bit counts.
        if (changed) {
            hash = Long.hashCode(value);
        }
        return this;
    }

    /**
     * Returns the value.
     *
     * @return The value.
     */
    long getValue() {
        return value;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return hash;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object obj) {
        return (obj instanceof VolatileLong) && (value == ((VolatileLong)obj).value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int compareTo(@Nonnull VolatileLong o) {
        if (this == o || value == o.value) {
            return 0;
        } else if (value < o.value) {
            return -1;
        } else {
            return 1;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return String.valueOf(value);
    }

    /**
     * Creates a copy of this object.
     *
     * @return The new instance with this value.
     */
    public VolatileLong copyOf() {
        return new VolatileLong(value, hash);
    }
}
