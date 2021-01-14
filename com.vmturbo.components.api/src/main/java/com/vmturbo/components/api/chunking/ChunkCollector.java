package com.vmturbo.components.api.chunking;

import java.util.ArrayList;
import java.util.Collection;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Utility to collect messages into chunks, respecting max size (in bytes) and desired size (in
 * bytes) limits for the chunk.
 * In practice, we send these chunks to Kafka.
 * In the future we may also send these chunks to other brokers.
 *
 * @param <T> The object in the chunk.
 */
public abstract class ChunkCollector<T> {

    /**
     * The max chunk size in bytes.
     */
    private final int maxSizeBytes;

    /**
     * The desired chunk size in bytes.
     */
    private final int desiredSizeBytes;

    private final Collection<T> elements;

    /**
     * Track the total serialized size of the elements. Required to enforce size limits.
     */
    private int totalSerializedSizeBytes = 0;

    /**
     * Create a new instance of the chunk collector.
     *
     * @param desiredSizeBytes The desired size of a chunk. The collector will try to size each
     *                         chunk to this amount without splitting up individual elements.
     *                         e.g. if desiredSize is 100 bytes and we get three elements of 30,
     *                         80, and 60 bytes, we will put the first two into one chunk, and the
     *                         next into another chunk.
     * @param maxSizeBytes The maximum size of a chunk in bytes. The collector will ensure size of
     *                     each chunk doesn't exceed this limit. e.g. if maxSizeBytes is 200, and
     *                     current chunk size is 50, an element of size 150 is fine to be added to
     *                     current chunk, but an element of size 151 will be added to next chunk
     */
    public ChunkCollector(final int desiredSizeBytes, final int maxSizeBytes) {
        this.desiredSizeBytes = desiredSizeBytes;
        this.maxSizeBytes = maxSizeBytes;
        this.elements = new ArrayList<>();
    }

    /**
     * Get the size of the object in bytes after serialization.
     *
     * @param element the object to get size for
     * @return size of the object in bytes after serialization
     */
    public abstract int getSerializedSize(T element);

    /**
     * Attempt to add an element to the current chunk.
     *
     * @param element The element to add.
     * @return If the element doesn't fit in the current chunk, exceed the memory size limit for
     *         the chunk, returns the current chunk, starts constructing a new chunk, and adds the
     *         element to the new chunk. If the element fits in the current chunk, returns null.
     * @throws OversizedElementException If the chunk is empty, and the element is still too big for
     * the chunk. This means that the element is too big to be sent over the wire!
     * @throws GetSerializedSizeException If the serialized size of the element can not be determined
     */
    @Nullable
    public Collection<T> addToCurrentChunk(@Nonnull final T element)
            throws OversizedElementException, GetSerializedSizeException {
        final int elementSize;
        try {
            elementSize = getSerializedSize(element);
        } catch (Exception e) {
            throw new GetSerializedSizeException(element.toString());
        }
        if (elementSize > maxSizeBytes) {
            // The message won't fit, even if the chunk is empty.
            // This will never get sent anyway, so we raise the error early.
            throw new OversizedElementException(elementSize, maxSizeBytes);
        }

        Collection<T> ret = null;
        if ((totalSerializedSizeBytes + elementSize) > maxSizeBytes) {
            // This element takes the current chunk over the memory limit.
            // This element will go in the next chunk, so we take the current chunk to return it.
            // This also resets the state.
            ret = takeCurrentChunk();
        }

        totalSerializedSizeBytes += elementSize;
        elements.add(element);

        if (totalSerializedSizeBytes >= desiredSizeBytes) {
            // Adding the entity took the current chunk over the desired memory limit.
            // Return the current chunk.
            // This also resets the state.
            ret = takeCurrentChunk();
        }
        return ret;
    }

    /**
     * Clear the chunk, removing all elements.
     */
    private void clear() {
        elements.clear();
        totalSerializedSizeBytes = 0;
    }

    /**
     * Get the number of elements in the chunk.
     *
     * @return The number of elements in the chunk.
     */
    public int count() {
        return elements.size();
    }

    /**
     * Return the currently in-progress chunk, and reset
     * the {@link ChunkCollector}. All subsequent additions
     * via {@link ChunkCollector#addToCurrentChunk(T)}
     * will go into the next chunk.
     *
     * <p>THIS MODIFIES THE STATE!
     *
     * @return The {@link Collection} of elements added
     * to this chunk.
     */
    @Nonnull
    public Collection<T> takeCurrentChunk() {
        Collection<T> ret = new ArrayList<>(elements);
        clear();
        return ret;
    }
}
