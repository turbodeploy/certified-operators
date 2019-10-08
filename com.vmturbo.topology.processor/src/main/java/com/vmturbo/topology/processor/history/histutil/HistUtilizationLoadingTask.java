package com.vmturbo.topology.processor.history.histutil;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.stitching.EntityCommodityReference;
import com.vmturbo.topology.processor.history.CachingHistoricalEditorConfig;
import com.vmturbo.topology.processor.history.EntityCommodityFieldReference;
import com.vmturbo.topology.processor.history.HistoryCalculationException;
import com.vmturbo.topology.processor.history.IHistoryLoadingTask;

/**
 * Loader of hist utilization from historydb.
 */
public class HistUtilizationLoadingTask implements IHistoryLoadingTask<CachingHistoricalEditorConfig, Float> {
    private final StatsHistoryServiceBlockingStub statsHistoryClient;

    public HistUtilizationLoadingTask(StatsHistoryServiceBlockingStub statsHistoryClient) {
        this.statsHistoryClient = statsHistoryClient;
    }

    @Override
    public Map<EntityCommodityFieldReference, Float>
           load(Collection<EntityCommodityReference> commodities,
                CachingHistoricalEditorConfig config) throws HistoryCalculationException {
        // TODO dmitry make a request to historydb for given commodities
        return Collections.emptyMap();
    }

}
