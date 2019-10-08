package com.vmturbo.topology.processor.history;

/**
 * Base class for history editor configurations.
 */
public class CachingHistoricalEditorConfig extends HistoricalEditorConfig {
    private final int loadingChunkSize;
    private final int calculationChunkSize;

    /**
     * Construct the configuration for history editors.
     *
     * @param loadingChunkSize how to partition commodities for loading from db
     * @param calculationChunkSize how to partition commodities for aggregating values
     */
    public CachingHistoricalEditorConfig(int loadingChunkSize, int calculationChunkSize) {
        this.loadingChunkSize = loadingChunkSize;
        this.calculationChunkSize = calculationChunkSize;
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

}
