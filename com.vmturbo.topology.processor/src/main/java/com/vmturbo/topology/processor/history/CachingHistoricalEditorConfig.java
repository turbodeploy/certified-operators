package com.vmturbo.topology.processor.history;

import java.time.Clock;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.topology.processor.KVConfig;

/**
 * Base class for history editor configurations.
 */
public class CachingHistoricalEditorConfig extends HistoricalEditorConfig {
    /**
     * The property name in the topology processor key value store whose value
     * represents the OID of the entity for which percentile counts are logged.
     */
    private static final String OID_FOR_PERCENTILE_COUNTS_LOG_PROPERTY = "oidForPercentileCountsLog";

    private final int loadingChunkSize;
    private final int calculationChunkSize;
    private final Clock clock;
    private final KVConfig kvConfig;

    /**
     * Construct the configuration for history editors.
     *
     * @param loadingChunkSize how to partition commodities for loading from db
     * @param calculationChunkSize how to partition commodities for aggregating
     *                 values
     * @param clock provides information about current time
     * @param kvConfig the config to access the topology processor key value store.
     */
    public CachingHistoricalEditorConfig(int loadingChunkSize, int calculationChunkSize,
                    @Nonnull Clock clock, KVConfig kvConfig) {
        this.loadingChunkSize = loadingChunkSize;
        this.calculationChunkSize = calculationChunkSize;
        this.clock = clock;
        this.kvConfig = kvConfig;
    }

    /**
     * The configured OID whose percentile counts needs to be logged when debug enabled.
     *
     * @return the configured OID.
     */
    public Optional<String> getOidToBeTracedInLog() {
        return Optional.ofNullable(kvConfig)
                        .map(KVConfig::keyValueStore)
                        .map(store -> store.get(OID_FOR_PERCENTILE_COUNTS_LOG_PROPERTY))
                        .orElse(Optional.empty());
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
