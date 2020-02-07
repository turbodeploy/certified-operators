package com.vmturbo.topology.processor.history;

import java.time.Clock;

import javax.annotation.Nonnull;

/**
 * Base class for history editor configurations.
 */
public class CachingHistoricalEditorConfig extends HistoricalEditorConfig {
    private final int loadingChunkSize;
    private final int calculationChunkSize;
    private final Clock clock;

    /**
     * Construct the configuration for history editors.
     *
     * @param loadingChunkSize how to partition commodities for loading from db
     * @param calculationChunkSize how to partition commodities for aggregating values
     * @param clock provides information about current time
     */
    public CachingHistoricalEditorConfig(int loadingChunkSize, int calculationChunkSize, @Nonnull Clock clock) {
        this.loadingChunkSize = loadingChunkSize;
        this.calculationChunkSize = calculationChunkSize;
        this.clock = clock;
    }

    /**
     * Get the chunk size for loading from db (how many commodities to pull in per remote request).
     *
     * @return chunk size
     */
    public int getLoadingChunkSize() {
        return loadingChunkSize;
    }

    /**
     * Get the chunk size for aggregating values (how many commodity fields to aggregate in a single task).
     *
     * @return chunk size
     */
    public int getCalculationChunkSize() {
        return calculationChunkSize;
    }

    /**
     * Returns {@link Clock} instance used across system to get current time.
     *
     * @return {@link Clock} instance used across system to get current time.
     */
    @Nonnull
    public Clock getClock() {
        return clock;
    }

}
