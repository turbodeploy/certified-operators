package com.vmturbo.topology.processor.history;

import java.time.Clock;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.topology.processor.KVConfig;

/**
 * Base class for history editor configurations.
 */
public abstract class CachingHistoricalEditorConfig extends HistoricalEditorConfig {
    /**
     * The property name in the topology processor key value store whose value
     * represents the OID of the entity for which percentile counts are logged.
     */
    private static final String OID_FOR_PERCENTILE_COUNTS_LOG_PROPERTY = "oidForPercentileCountsLog";

    private final int loadingChunkSize;
    private final int calculationChunkSize;
    private final long realtimeTopologyContextId;
    private final Clock clock;
    private final KVConfig kvConfig;

    /**
     * Construct the configuration for history editors.
     *
     * @param loadingChunkSize how to partition commodities for loading from db
     * @param calculationChunkSize how to partition commodities for aggregating
     *                 values
     * @param realtimeTopologyContextId identifier of the realtime topology.
     * @param clock provides information about current time
     * @param kvConfig the config to access the topology processor key value store.
     */
    protected CachingHistoricalEditorConfig(int loadingChunkSize, int calculationChunkSize,
                    long realtimeTopologyContextId, @Nonnull Clock clock, KVConfig kvConfig) {
        this.loadingChunkSize = loadingChunkSize;
        this.calculationChunkSize = calculationChunkSize;
        this.realtimeTopologyContextId = realtimeTopologyContextId;
        this.clock = clock;
        this.kvConfig = kvConfig;
    }

    /**
     * Getting the name of the property responsible for saving the cache to diagnostics.
     *
     * @return the name of the property.
     */
    protected abstract String getDiagnosticsEnabledPropertyName();

    /**
     * The configured OID whose percentile counts needs to be logged when debug enabled.
     *
     * @return the configured OID.
     */
    public Optional<String> getOidToBeTracedInLog() {
        return getConsulValue(OID_FOR_PERCENTILE_COUNTS_LOG_PROPERTY);
    }

    /**
     * Returns identifier of the realtime topology.
     *
     * @return identifier of the realtime topology.
     */
    public long getRealtimeTopologyContextId() {
        return realtimeTopologyContextId;
    }

    /**
     * Returns {@code true} in case cache needs to be stored in diagnostic file, otherwise returns
     * {@code false}.
     *
     * @return {@code true} in case cache needs to be stored in diagnostic file, otherwise
     *         returns {@code false}.
     */
    public boolean isCacheExportingToDiagnostics() {
        // TODO Alexander Vasin this property needs to be moved to yaml file, when properties reload
        //  implementation will be completed, i.e. when
        //  https://vmturbo.atlassian.net/browse/PLAT-218 will be closed
        return getConsulValue(getDiagnosticsEnabledPropertyName()).map(
                Boolean.TRUE.toString()::equals).orElse(false);
    }

    @Nonnull
    private Optional<String> getConsulValue(@Nonnull String propertyName) {
        return Optional.ofNullable(kvConfig).map(KVConfig::keyValueStore)
                        .flatMap(store -> store.get(propertyName));
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
