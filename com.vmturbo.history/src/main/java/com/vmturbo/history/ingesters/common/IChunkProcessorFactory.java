package com.vmturbo.history.ingesters.common;

import java.util.Optional;

/**
 * Interface for a factory to create {@link IChunkProcessor} instances.
 *
 * @param <T> type of objects in broadcasts to be processed
 * @param <Info> type of broadcast info objects
 * @param <SS> type of shared state objects
 */
public interface IChunkProcessorFactory<T, Info, SS> {

    /**
     * Create a new chunk processor.
     *
     * @param info  broadcast info
     * @param state shared state
     * @return the new instance
     */
    Optional<IChunkProcessor<T>> getChunkProcessor(Info info, SS state);

    /**
     * Indicate whether this chunk processor's chunk processing cost is sufficient to warrant a
     * dedicated thread in the thread pool.
     *
     * <p>In reality, "dedicated" threads are not really dedicated to this processor. It just means
     * that when configuring the pool, its capacity will reflect a thread for this processor.
     * There may also be a thread allocated for processors that don't ask for their own, depending
     * on how many of those there are. Also, threads that declare this will have their tasks
     * scheduled first, so they'll start executing immediately. Tasks for other processors will
     * execute as threads become available from the pool.
     *
     * @return true if this processor wants its own thread
     */
    default boolean needsOwnThread() {
        return false;
    }
}
