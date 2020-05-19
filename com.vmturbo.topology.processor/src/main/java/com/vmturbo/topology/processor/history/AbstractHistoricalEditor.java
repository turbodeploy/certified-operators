package com.vmturbo.topology.processor.history;

import java.util.List;

import javax.annotation.Nonnull;

import com.vmturbo.stitching.EntityCommodityReference;

/**
 * Base class for history-related commodity value editors.
 *
 * @param <Config> configuration values
 * @param <Stub> type of history component stub
 */
public abstract class AbstractHistoricalEditor<Config extends HistoricalEditorConfig,
                    Stub extends io.grpc.stub.AbstractStub<Stub>>
                implements IHistoricalEditor<Config> {
    private final Stub statsHistoryClient;
    private final Config config;

    /**
     * Construct the instance of a historical editor.
     *
     * @param config configuration values
     * @param statsHistoryClient history db client
     */
    public AbstractHistoricalEditor(Config config, Stub statsHistoryClient) {
        this.config = config;
        this.statsHistoryClient = statsHistoryClient;
    }

    protected Config getConfig() {
        return config;
    }

    protected Stub getStatsHistoryClient() {
        return statsHistoryClient;
    }

    @Override
    public void initContext(@Nonnull HistoryAggregationContext context,
                            @Nonnull List<EntityCommodityReference> eligibleComms)
                    throws HistoryCalculationException, InterruptedException {}

    @Override
    public void completeBroadcast(@Nonnull HistoryAggregationContext context)
                    throws HistoryCalculationException, InterruptedException {}

    @Override
    public void cleanupCache(@Nonnull final List<EntityCommodityReference> commodities) {}
}
