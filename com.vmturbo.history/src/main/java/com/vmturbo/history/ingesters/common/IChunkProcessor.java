package com.vmturbo.history.ingesters.common;

import java.util.Collection;

import javax.annotation.Nonnull;

/**
 * Interface for a class that performs processing for a chunked broadcast of a given type of
 * object.
 *
 * <p>The processing is performed on a per-chunk basis, by an instance of the processor class
 * that is used for all the chunks in a given broadcast.</p>
 *
 * @param <T>  the underlying object type of the broadcast (and hence for each chunk)
 */
public interface IChunkProcessor<T> {

    /**
     * Process the next collection of objects from the broadcast.
     *
     * <p>This processor will have already processed all prior chunks when this method is invoked.
     * </p>
     *
     * <p>If this method throws any exception, that will be logged by the controlling broadcast
     * processor and otherwise handled as if the {@link #getDispositionOnException()} disposition
     * value had been returned.</p>
     *
     * @param chunk the chunk to be processed
     * @param infoSummary summary of broadcast info, for logging
     * @return indication of whether processing succeeded and if not, what to do with remaining
     * broadcast chunks
     * @throws InterruptedException if interrupted
     */
    ChunkDisposition processChunk(@Nonnull Collection<T> chunk, @Nonnull String infoSummary)
            throws InterruptedException;

    /**
     * Get a time limit for processing a single chunk.
     *
     * @return time limit in milliseconds, or null to use the default
     */
    default Long getChunkTimeLimitMsec() {
        return null;
    }

    /**
     * Return values for chunk processing.
     */
    enum ChunkDisposition {
        /**
         * Returned when a chunk is successfully processed.
         */
        SUCCESS,
        /**
         * Returned when a chunk fails processing, but additional chunks should nevertheless be
         * delivered to this processor for processing.
         */
        CONTINUE,
        /**
         * Returned when a chunk fails processing, and no further chunks should be delivered to
         * this processor.
         */
        DISCONTINUE,
        /**
         * Returned when a chunk fails processing, and remaining chunks in the broadcast should
         * be discarded. This will cause the controlling chunk processor to terminate processing
         * of the overall broadcast once processing for this chunk has completed.
         */
        TERMINATE
    }

    /**
     * Perform any final processing required for this chunk processor.
     *
     * @param objectCount number of objects processed
     * @param expedite    true if this thread has already been interrupted,
     *                    and noncritical processing should be avoided
     * @param infoSummary summary of broadcast info, for logging
     * @throws InterruptedException if interrupted
     */
    default void finish(int objectCount, boolean expedite, String infoSummary)
            throws InterruptedException {
        // default to no-op for processors that do all their work in processChunk
    }

    /**
     * Define a default disposition to be used by this chunk processor, in cases where using the
     * processor results in an exception (other than timeout) caught by the broadcast processor.
     *
     * <p>The exception could be thrown by the {@link #processChunk(Collection, String)} method, or
     * it could occur while attempting to schedule a chunk processing task for execution. Both cases
     * will be handled in the same way by the broadcast processor.</p>
     *
     * @return chunk disposition when exceptions occur with this chunk processor
     */
    default ChunkDisposition getDispositionOnException() {
        return ChunkDisposition.DISCONTINUE;
    }

    /**
     * Define a default disposition to be used by this chunk processor, in cases where the
     * processor takes too long to process a given chunk.
     *
     * @return chunk disposition when a processor times out processing a chunk
     */
    default ChunkDisposition getDispositionOnTimeout() {
        return ChunkDisposition.CONTINUE;
    }

    /**
     * Return a label for this processor, primarily for use in logging.
     *
     * <p>The default implementation just uses the class name of the processor.</p>
     *
     * @return processor label
     */
    default String getLabel() {
        return this.getClass().getSimpleName();
    }
}
