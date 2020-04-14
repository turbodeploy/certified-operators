package com.vmturbo.topology.processor.history.histutil;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.platform.sdk.common.util.Pair;
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

    /**
     * Creates {@link HistUtilizationLoadingTask} instance.
     *
     * @param statsHistoryClient client which is doing requests of history data from
     *                 DB.
     * @param range range from start timestamp till end timestamp for which we want
     *                 to request data.
     */
    public HistUtilizationLoadingTask(@Nonnull StatsHistoryServiceBlockingStub statsHistoryClient,
                    @Nonnull Pair<Long, Long> range) {
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
