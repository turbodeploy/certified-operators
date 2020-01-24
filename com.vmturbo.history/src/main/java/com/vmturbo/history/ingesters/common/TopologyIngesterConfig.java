package com.vmturbo.history.ingesters.common;

import org.immutables.value.Value;

/**
 * Structure containing configuration values for topology ingesters.
 */
@Value.Immutable
public interface TopologyIngesterConfig extends ChunkedBroadcastProcessorConfig {
    /**
     * Whether to flush (and therefore commit) all bulk writers after each chunk has been
     * processed.
     *
     * @return true if this ingester will flush bulk writers after every chunk
     */
    boolean perChunkCommit();
}
