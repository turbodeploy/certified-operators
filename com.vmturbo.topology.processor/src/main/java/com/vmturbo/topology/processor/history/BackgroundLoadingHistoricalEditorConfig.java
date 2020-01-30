package com.vmturbo.topology.processor.history;

/**
 * Configuration settings for historical editor with background loading.
 */
public class BackgroundLoadingHistoricalEditorConfig extends CachingHistoricalEditorConfig {
    private final int backgroundLoadThreshold;
    private final int backgroundLoadRetries;
    private final int backgroundLoadTimeoutMin;

    /**
     * Construct the configuration settings.
     *
     * @param loadingChunkSize chunk size for loading from the persistence store
     * @param calculationChunkSize chunk size for calculating values
     * @param backgroundLoadThreshold if that many commodities are not initialized, run loading in the background
     * @param backgroundLoadRetries how many load failures to tolerate before giving up
     * @param backgroundLoadTimeoutMin how much time to give to a single loading attempt
     */
    public BackgroundLoadingHistoricalEditorConfig(int loadingChunkSize, int calculationChunkSize,
                                                   int backgroundLoadThreshold, int backgroundLoadRetries,
                                                   int backgroundLoadTimeoutMin) {
        super(loadingChunkSize, calculationChunkSize);
        this.backgroundLoadThreshold = backgroundLoadThreshold;
        this.backgroundLoadRetries = backgroundLoadRetries;
        this.backgroundLoadTimeoutMin = backgroundLoadTimeoutMin;
    }

    public int getBackgroundLoadThreshold() {
        return backgroundLoadThreshold;
    }

    public int getBackgroundLoadRetries() {
        return backgroundLoadRetries;
    }

    public int getBackgroundLoadTimeoutMin() {
        return backgroundLoadTimeoutMin;
    }
}
