package com.vmturbo.history.ingesters.common;

import java.util.concurrent.ExecutorService;

import org.immutables.value.Value;

/**
 * Configuration for an {@link AbstractChunkedBroadcastProcessor}.
 */
@Value.Immutable
public interface ChunkedBroadcastProcessorConfig {
    /**
     * Thread pool for executing chunk processors.
     *
     * @return thread pool
     */
    ExecutorService threadPool();

    /**
     * Get the default time limit for processing a single chunk by a single chunk processor.
     *
     * @return default time limit in milliseconds
     */
    long defaultChunkTimeLimitMsec();
}
