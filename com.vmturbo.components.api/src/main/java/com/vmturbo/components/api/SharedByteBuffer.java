package com.vmturbo.components.api;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A utility to allow easy reuse (+ resizing, if necessary) of a byte buffer between multiple
 * sequential operations.
 *
 * This is NOT thread-safe - using the same SharedByteBuffer in multiple threads will not go well.
 */
@NotThreadSafe
public class SharedByteBuffer {
    private byte[] buffer;

    public SharedByteBuffer(final int initialSize) {
        buffer = new byte[initialSize];
    }

    public SharedByteBuffer() {
        this(0);
    }

    /**
     * Get the underlying shared buffer. This will always return a buffer of the desired length, or
     * larger.
     *
     * @param length The desired length (in bytes).
     * @return A byte array of at least the desired length. The contents of the byte array are
     *         undefined.
     */
    public byte[] getBuffer(final int length) {
        if (buffer.length < length) {
            buffer = new byte[length];
        }
        return buffer;
    }

    public int getSize() {
        return buffer.length;
    }
}
