package com.vmturbo.components.api.chunking;

import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Consumer;

import javax.annotation.Nonnull;

import com.google.protobuf.AbstractMessage;

/**
 * Iterator over a collection that returns chunks. Not implementing the {@link Iterator}
 * interface because we want {@link ProtobufChunkIterator#next()} to throw checked exceptions.
 *
 * @param <T> Type of elements in the source and result collections.
 */
public class ProtobufChunkIterator<T extends AbstractMessage> {
    private final Iterator<T> iterator;

    private final ProtobufChunkCollector<T> inProgressChunk;

    private ProtobufChunkIterator(@Nonnull final Iterator<T> iterator,
                                 final int targetSizeBytes,
                                 final int maxSizeBytes) {
        this.iterator = iterator;
        this.inProgressChunk = new ProtobufChunkCollector<>(targetSizeBytes, maxSizeBytes);
    }

    /**
     * Partition an iterator into appropriately-sized chunks.
     *
     * @param iterator The iterator to partition.
     * @param targetSizeBytes The desired size of each chunk, in bytes. Note - this will be the
     *                        serialized size of the messages, not the size of the POJOs.
     * @param maxSizeBytes The maximum size of each chunk, in bytes.
     * @param <T> Type of protobuf message.
     * @return A {@link ProtobufChunkIterator} that can be used to iterate over the chunks in a
     * streaming fashion.
     */
    public static <T extends AbstractMessage> ProtobufChunkIterator<T> partition(
            @Nonnull final Iterator<T> iterator,
            final int targetSizeBytes,
            final int maxSizeBytes) {
        return new ProtobufChunkIterator<>(iterator, targetSizeBytes, maxSizeBytes);
    }

    /**
     * Partition an iterable into appropriately-sized chunks.
     *
     * @param iterable The iterable to partition.
     * @param targetSizeBytes The desired size of each chunk, in bytes. Note - this will be the
     *                        serialized size of the messages, not the size of the POJOs.
     * @param maxSizeBytes The maximum size of each chunk, in bytes.
     * @param <T> Type of protobuf message.
     * @return A {@link ProtobufChunkIterator} that can be used to iterate over the chunks in a
     * streaming fashion.
     */
    public static <T extends AbstractMessage> ProtobufChunkIterator<T> partition(
            @Nonnull final Iterable<T> iterable,
            final int targetSizeBytes,
            final int maxSizeBytes) {
        return partition(iterable.iterator(), targetSizeBytes, maxSizeBytes);
    }

    /**
     * See {@link Iterator#next()}.
     *
     * @return The next collection of elements.
     * @throws NoSuchElementException If there are no more elements.
     * @throws OversizedElementException If one of the elements exceeds size limits.
     */
    @Nonnull
    public Collection<T> next() throws NoSuchElementException, OversizedElementException {
        Collection<T> result = null;
        // If the iterator is not yet drained, continue adding to the current in-progress
        // chunk. The current in-progress chunk may have an element left over from the
        // last call to next().
        while (iterator.hasNext()) {
            final T nextElement = iterator.next();
            // We don't know if we can actually include the next element in the returned chunk
            // until we take the next element from the iterator. Since we can't put it back,
            // we will save it for the NEXT returned chunk.
            final Collection<T> finalizedChunk = inProgressChunk.addToCurrentChunk(nextElement);
            if (finalizedChunk != null) {
                result = finalizedChunk;
                break;
            }
        }

        if (result == null) {
            // If the iterator is already drained, we may still have an element left over from
            // the last call to next(). Put it into the results.
            result = inProgressChunk.takeCurrentChunk();
        }

        // The contract of the iterator
        if (result.isEmpty()) {
            throw new NoSuchElementException();
        }

        return result;
    }

    /**
     * See {@link Iterator#hasNext()}.
     *
     * @return True if there are more chunks in the iterator.
     */
    public boolean hasNext() {
        return iterator.hasNext() || inProgressChunk.count() > 0;
    }

    /**
     * See {@link Iterator#forEachRemaining(Consumer)} .
     *
     * @param action The action to apply to each chunk.
     * @throws OversizedElementException See {@link ProtobufChunkIterator#next()}.
     */
    public void forEachRemaining(@Nonnull final Consumer<Collection<T>> action)
            throws OversizedElementException {
        while (hasNext()) {
            action.accept(next());
        }
    }
}
