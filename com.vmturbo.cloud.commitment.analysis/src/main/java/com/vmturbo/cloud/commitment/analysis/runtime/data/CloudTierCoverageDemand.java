package com.vmturbo.cloud.commitment.analysis.runtime.data;

import javax.annotation.Nonnull;

import com.google.common.collect.Table;

import com.vmturbo.cloud.commitment.analysis.demand.TimeInterval;
import com.vmturbo.cloud.commitment.analysis.demand.TimeSeriesData;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.AggregateCloudTierDemand;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.AggregateDemandSegment;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentInventory.CloudCommitment;

/**
 * This will be the output of the covered demand calculation stage for a single time window.
 */
public interface CloudTierCoverageDemand extends TimeSeriesData {

    /**
     * The demand segment of all demand considered in this set. The demand is aggregated by scope.
     * @return The aggregate demand segment.
     */
    @Nonnull
    AggregateDemandSegment demandSegment();

    /**
     * A mapping of the aggregate cloud tier demand -> cloud commitment (e.g. RI) -> the number of
     * coupons of the aggregate demand covered by the cloud commitment.
     * @return The coverage table.
     */
    @Nonnull
    Table<AggregateCloudTierDemand, CloudCommitment, Double> coverageTable();

    /**
     * The time interval of the demand.
     * @return The time interval of the demand.
     */
    @Override
    default TimeInterval timeInterval() {
        return demandSegment().timeInterval();
    }
}
