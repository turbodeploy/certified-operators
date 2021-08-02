package com.vmturbo.topology.processor.history;

import java.time.Clock;

import javax.annotation.Nonnull;

import com.vmturbo.topology.processor.KVConfig;

/**
 * Configuration parameters for blobs-based commodity editor.
 */
public abstract class AbstractBlobsHistoricalEditorConfig extends CachingHistoricalEditorConfig {

    private final int grpcStreamTimeoutSec;
    private final int blobReadWriteChunkSizeKb;

    /**
     * Construct the configuration for   history editors.
     *
     * @param loadingChunkSize          how to partition commodities for loading from db
     * @param calculationChunkSize      how to partition commodities for aggregating
     *                                  values
     * @param realtimeTopologyContextId identifier of the realtime topology.
     * @param clock                     provides information about current time
     * @param kvConfig                  the config to access the topology processor key value store.
     * @param grpcStreamTimeoutSec      the timeout for history access streaming operations
     * @param blobReadWriteChunkSizeKb  the size of chunks for reading and writing from persistent store
     */
    public AbstractBlobsHistoricalEditorConfig(
        int loadingChunkSize, int calculationChunkSize, long realtimeTopologyContextId,
        @Nonnull Clock clock, @Nonnull KVConfig kvConfig, int grpcStreamTimeoutSec,
        int blobReadWriteChunkSizeKb) {
        super(loadingChunkSize, calculationChunkSize, realtimeTopologyContextId, clock, kvConfig);
        this.grpcStreamTimeoutSec = grpcStreamTimeoutSec;
        this.blobReadWriteChunkSizeKb = blobReadWriteChunkSizeKb;
    }

    /**
     * Get the stream read/write operations timeout.
     *
     * @return timeout in seconds
     */
    public int getGrpcStreamTimeoutSec() {
        return grpcStreamTimeoutSec;
    }

    /**
     * Get the size of chunks for reading and writing from persistent store.
     *
     * @return chunk size
     */
    public int getBlobReadWriteChunkSizeKb() {
        return blobReadWriteChunkSizeKb;
    }
}
