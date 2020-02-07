package com.vmturbo.topology.processor.history;

import java.util.List;

import javax.annotation.Nonnull;

import com.vmturbo.stitching.EntityCommodityReference;
import com.vmturbo.topology.processor.group.settings.GraphWithSettings;

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
    private ICommodityFieldAccessor commodityFieldAccessor;

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

    protected ICommodityFieldAccessor getCommodityFieldAccessor() {
        return commodityFieldAccessor;
    }

    @Override
    public void initContext(@Nonnull GraphWithSettings graph,
                            @Nonnull ICommodityFieldAccessor accessor,
                            @Nonnull List<EntityCommodityReference> eligibleComms,
                            boolean isPlan)
                    throws HistoryCalculationException, InterruptedException {
        config.initSettings(graph, isPlan);
        this.commodityFieldAccessor = accessor;
    }

    @Override
    public void completeBroadcast() throws HistoryCalculationException, InterruptedException {
        config.deinit();
    }
}
