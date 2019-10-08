package com.vmturbo.topology.processor.history.percentile;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceStub;
import com.vmturbo.stitching.EntityCommodityReference;
import com.vmturbo.topology.processor.history.AbstractStatsLoadingTask;
import com.vmturbo.topology.processor.history.EntityCommodityFieldReference;
import com.vmturbo.topology.processor.history.HistoryCalculationException;
import com.vmturbo.topology.processor.history.percentile.PercentileDto.PercentileCounts;
import com.vmturbo.topology.processor.history.percentile.PercentileDto.PercentileCounts.PercentileRecord;

/**
 * Persist the percentile data to/from historydb.
 */
public class PercentilePersistenceTask extends AbstractStatsLoadingTask<PercentileHistoricalEditorConfig, PercentileRecord> {
    private static final Logger logger = LogManager.getLogger();
    private final StatsHistoryServiceStub statsHistoryClient;
    private final long startTimestamp;

    /**
     * Construct the task to load percentile data for the 'full window' from the persistent store.
     *
     * @param statsHistoryClient persistent store grpc interface
     */
    public PercentilePersistenceTask(StatsHistoryServiceStub statsHistoryClient) {
        this.statsHistoryClient = statsHistoryClient;
        this.startTimestamp = 0;
    }

    /**
     * Construct the task to load percentile data from the persistent store.
     *
     * @param statsHistoryClient persistent store grpc interface
     * @param startTimestamp starting timestamp for the page to load, 0 for 'full window'
     */
    public PercentilePersistenceTask(StatsHistoryServiceStub statsHistoryClient,
                                 long startTimestamp) {
        this.statsHistoryClient = statsHistoryClient;
        this.startTimestamp = startTimestamp;
    }

    @Override
    public Map<EntityCommodityFieldReference, PercentileRecord>
           load(@Nonnull Collection<EntityCommodityReference> commodities,
                @Nonnull PercentileHistoricalEditorConfig config)
                           throws HistoryCalculationException {
        // TODO dmitry load the blob from the history client and parse
        // statsHistoryClient.getPercentileCounts();
        Map<EntityCommodityFieldReference, PercentileRecord> result = new HashMap<>();
        logger.debug("Loaded {} percentile commodity entries for timestamp {}", result.size(), startTimestamp);
        return result;
    }

    /**
     * Store the percentile blob.
     * Percentile is the only editor as of now that will store data explicitly,
     * not using utilizations from the broadcast.
     *
     * @param counts payload
     * @throws HistoryCalculationException when failed to persist
     */
    public void save(@Nonnull PercentileCounts counts) throws HistoryCalculationException {
        // TODO dmitry persist the blob when the history part gets committed
        // getStatsHistoryClient().setPercentileCounts();
        logger.debug("Saved {} percentile commodity entries for timestamp {}",
                     counts.getPercentileRecordsCount(), startTimestamp);
    }
}
