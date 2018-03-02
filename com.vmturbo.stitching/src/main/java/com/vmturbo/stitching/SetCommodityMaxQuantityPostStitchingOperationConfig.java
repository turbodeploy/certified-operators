package com.vmturbo.stitching.poststitching;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;

public class SetCommodityMaxQuantityPostStitchingOperationConfig {

    private final StatsHistoryServiceBlockingStub statsHistoryClient;

    private final long maxValuesBackgroundLoadFrequencyMinutes;

    private final long maxValuesBackgroundLoadDelayOnInitFailureMinutes;

    public SetCommodityMaxQuantityPostStitchingOperationConfig(
            @Nonnull StatsHistoryServiceBlockingStub statsClient,
            long maxValuesBackgroundLoadFrequencyMinutes,
            long maxValuesBackgroundLoadDelayOnInitFailureMinutes) {

        this.statsHistoryClient = statsClient;
        this.maxValuesBackgroundLoadFrequencyMinutes =
            maxValuesBackgroundLoadFrequencyMinutes;
        this.maxValuesBackgroundLoadDelayOnInitFailureMinutes =
            maxValuesBackgroundLoadDelayOnInitFailureMinutes;
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
}
