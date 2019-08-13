package com.vmturbo.topology.processor.history;

import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;

/**
 * Base class for history-related commodity value editors.
 *
 * @param <Config> configuration values
 */
public abstract class AbstractHistoricalEditor<Config> implements IHistoricalEditor<Config> {
    private final StatsHistoryServiceBlockingStub statsHistoryClient;
    private final Config config;

    /**
     * Construct the instance of a historical editor.
     *
     * @param config configuration values
     * @param statsHistoryClient history db client
     */
    public AbstractHistoricalEditor(Config config, StatsHistoryServiceBlockingStub statsHistoryClient) {
        this.config = config;
        this.statsHistoryClient = statsHistoryClient;
    }

    protected Config getConfig() {
        return config;
    }

    protected StatsHistoryServiceBlockingStub getStatsHistoryClient() {
        return statsHistoryClient;
    }

}
