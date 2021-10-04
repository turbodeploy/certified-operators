package com.vmturbo.stitching.poststitching;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;

/**
 * Operation config hold specific operations data and the grpc history client for component communication.
 */
public class CommodityPostStitchingOperationConfig {

    private final StatsHistoryServiceBlockingStub statsHistoryClient;

    private final long maxValuesBackgroundLoadFrequencyMinutes;

    private final long maxValuesBackgroundLoadDelayOnInitFailureMinutes;

    private final boolean maxQueryOnTPStartup;


    public CommodityPostStitchingOperationConfig(
            @Nonnull StatsHistoryServiceBlockingStub statsClient,
            long maxValuesBackgroundLoadFrequencyMinutes,
            long maxValuesBackgroundLoadDelayOnInitFailureMinutes,
            boolean maxQueryOnTPStartup) {

        this.statsHistoryClient = statsClient;
        this.maxValuesBackgroundLoadFrequencyMinutes =
            maxValuesBackgroundLoadFrequencyMinutes;
        this.maxValuesBackgroundLoadDelayOnInitFailureMinutes =
            maxValuesBackgroundLoadDelayOnInitFailureMinutes;
        this.maxQueryOnTPStartup = maxQueryOnTPStartup;
    }

    public StatsHistoryServiceBlockingStub getStatsClient() {
        return statsHistoryClient;
    }

    public long getMaxValuesBackgroundLoadFrequencyMinutes() {
        return maxValuesBackgroundLoadFrequencyMinutes;
    }

    public long getMaxValuesBackgroundLoadDelayOnInitFailureMinutes() {
        return maxValuesBackgroundLoadDelayOnInitFailureMinutes;
    }

    public boolean getMaxQueryOnTPStartup() {
        return maxQueryOnTPStartup;
    }
}
