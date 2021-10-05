package com.vmturbo.stitching.poststitching;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;

/**
 * Operation config hold specific operations data and the grpc history client for component communication.
 */
public class CommodityPostStitchingOperationConfig {

    private final StatsHistoryServiceBlockingStub statsHistoryClient;

    private final long maxValuesBackgroundLoadFrequencyMinutes;

    public CommodityPostStitchingOperationConfig(
            @Nonnull StatsHistoryServiceBlockingStub statsClient,
            long maxValuesBackgroundLoadFrequencyMinutes) {

        this.statsHistoryClient = statsClient;
        this.maxValuesBackgroundLoadFrequencyMinutes =
            maxValuesBackgroundLoadFrequencyMinutes;
    }

    public StatsHistoryServiceBlockingStub getStatsClient() {
        return statsHistoryClient;
    }

    public long getMaxValuesBackgroundLoadFrequencyMinutes() {
        return maxValuesBackgroundLoadFrequencyMinutes;
    }
}
