package com.vmturbo.cost.component.cloud.commitment;

import java.util.DoubleSummaryStatistics;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.CloudCommitmentStatRecord;

/**
 * A utility class for cloud commitment stats.
 */
public class CloudCommitmentStatsUtils {

    private CloudCommitmentStatsUtils() {}

    /**
     * Converts the double stats to a {@link CloudCommitmentStatRecord.StatValue}.
     * @param amountStats THe double statistics.
     * @return The stat value.
     */
    @Nonnull
    public static CloudCommitmentStatRecord.StatValue convertToCommitmentStat(
            @Nonnull DoubleSummaryStatistics amountStats) {

        return CloudCommitmentStatRecord.StatValue.newBuilder()
                .setMax(amountStats.getMax())
                .setMin(amountStats.getMin())
                .setAvg(amountStats.getAverage())
                .setTotal(amountStats.getSum())
                .build();
    }
}
