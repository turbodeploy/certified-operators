package com.vmturbo.topology.processor.history.maxvalue;

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
 * Loader of max quantities from historydb.
 */
public class MaxValueLoadingTask implements IHistoryLoadingTask<CachingHistoricalEditorConfig, Float> {
    private final StatsHistoryServiceBlockingStub statsHistoryClient;

    public MaxValueLoadingTask(StatsHistoryServiceBlockingStub statsHistoryClient) {
        this.statsHistoryClient = statsHistoryClient;
    }

    @Override
    public Map<EntityCommodityFieldReference, Float>
           load(Collection<EntityCommodityReference> commodities, CachingHistoricalEditorConfig config)
                           throws HistoryCalculationException {
        // TODO dmitry make a request to historydb for given commodities
        return Collections.emptyMap();
    }

}
