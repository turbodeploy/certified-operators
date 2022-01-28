package com.vmturbo.cost.component.cloud.commitment;

import java.util.Collection;
import java.util.DoubleSummaryStatistics;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;

import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverageGroupBy;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverageTypeInfo;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverageVector;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentUtilizationGroupBy;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentUtilizationVector;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.CloudCommitmentStatRecord;
import com.vmturbo.cost.component.cloud.commitment.coverage.CommitmentCoverageAggregator;
import com.vmturbo.cost.component.cloud.commitment.coverage.CommitmentCoverageAggregator.CommitmentCoverageGrouping;
import com.vmturbo.cost.component.cloud.commitment.coverage.CommitmentCoverageAggregator.CoverageGroupKey;
import com.vmturbo.cost.component.cloud.commitment.coverage.CoverageInfo;
import com.vmturbo.cost.component.cloud.commitment.utilization.CommitmentUtilizationAggregator;
import com.vmturbo.cost.component.cloud.commitment.utilization.CommitmentUtilizationAggregator.CommitmentUtilizationGrouping;
import com.vmturbo.cost.component.cloud.commitment.utilization.CommitmentUtilizationAggregator.UtilizationGroupKey;
import com.vmturbo.cost.component.cloud.commitment.utilization.UtilizationInfo;

/**
 * A converter of cloud commitment related data (coverage, utilization, etc) to {@link CloudCommitmentStatRecord}
 * instances.
 */
public class CloudCommitmentStatsConverter {

    private final CommitmentCoverageAggregator coverageAggregator;

    private final CommitmentUtilizationAggregator utilizationAggregator;

    /**
     * Constructs a new {@link CloudCommitmentStatsConverter} instance.
     * @param coverageAggregator A commitment coverage aggregator.
     * @param utilizationAggregator A commitment utilization aggregator.
     */
    public CloudCommitmentStatsConverter(@Nonnull CommitmentCoverageAggregator coverageAggregator,
                                         @Nonnull CommitmentUtilizationAggregator utilizationAggregator) {

        this.coverageAggregator = Objects.requireNonNull(coverageAggregator);
        this.utilizationAggregator = Objects.requireNonNull(utilizationAggregator);
    }

    /**
     * Converts {@link CoverageInfo} to {@link CloudCommitmentStatRecord}, based on {@code groupByCollection}.
     * @param coverageInfo The coverage info.
     * @param groupByCollection The group by requirements for stat conversion.
     * @return The stat record list.
     */
    @Nonnull
    public List<CloudCommitmentStatRecord> convertToCoverageStats(@Nonnull CoverageInfo coverageInfo,
                                                                 @Nonnull Collection<CloudCommitmentCoverageGroupBy> groupByCollection) {

        final long topologyTime = coverageInfo.topologyInfo().getCreationTime();

        final List<CommitmentCoverageGrouping> coverageGroupings =
                coverageAggregator.groupCoverageCollection(coverageInfo.entityCoverageMap().values(), groupByCollection);
        return coverageGroupings.stream()
                .map(coverageGrouping -> convertCoverageGroupingToStat(coverageGrouping, topologyTime))
                .collect(ImmutableList.toImmutableList());
    }

    /**
     * Converts {@link UtilizationInfo} to {@link CloudCommitmentStatRecord}, based on {@code groupByCollection}.
     * @param utilizationInfo The utilization info.
     * @param groupByCollection The group by requirements for stat conversion.
     * @return The stat record list.
     */
    @Nonnull
    public List<CloudCommitmentStatRecord> convertToUtilizationStats(@Nonnull UtilizationInfo utilizationInfo,
                                                                    @Nonnull Collection<CloudCommitmentUtilizationGroupBy> groupByCollection) {

        final long topologyTime = utilizationInfo.topologyInfo().getCreationTime();

        final List<CommitmentUtilizationGrouping> utilizationGroupings =
                utilizationAggregator.groupUtilizationCollection(utilizationInfo.commitmentUtilizationMap().values(), groupByCollection);
        return utilizationGroupings.stream()
                .map(utilizationGrouping -> convertUtilizationGroupingToStat(utilizationGrouping, topologyTime))
                .collect(ImmutableList.toImmutableList());
    }

    private CloudCommitmentStatRecord convertCoverageGroupingToStat(@Nonnull CommitmentCoverageGrouping coverageGrouping,
                                                                    long snapshotTime) {

        final CoverageGroupKey groupKey = coverageGrouping.key();

        // each coverage grouping should have only one coverage type
        final CloudCommitmentCoverageTypeInfo coverageTypeInfo = groupKey.coverageTypeInfo();

        final DoubleSummaryStatistics usedStats = coverageGrouping.coverageList().stream()
                .collect(Collectors.summarizingDouble(CloudCommitmentCoverageVector::getUsed));

        final DoubleSummaryStatistics capacityStats = coverageGrouping.coverageList().stream()
                .collect(Collectors.summarizingDouble(CloudCommitmentCoverageVector::getCapacity));

        final CloudCommitmentStatRecord.Builder statRecord = CloudCommitmentStatRecord.newBuilder()
                // use capacity as the count. it should always be equal to or greater than the used count
                .setSampleCount(capacityStats.getCount())
                .setSnapshotDate(snapshotTime)
                .setCoverageTypeInfo(coverageTypeInfo)
                .setValues(CloudCommitmentStatsUtils.convertToCommitmentStat(usedStats))
                .setCapacity(CloudCommitmentStatsUtils.convertToCommitmentStat(capacityStats));

        if (groupKey.accountOid().isPresent()) {
            statRecord.setAccountId(groupKey.accountOid().getAsLong());
        }

        if (groupKey.regionOid().isPresent()) {
            statRecord.setRegionId(groupKey.regionOid().getAsLong());
        }

        if (groupKey.cloudServiceOid().isPresent()) {
            statRecord.setCloudServiceId(groupKey.cloudServiceOid().getAsLong());
        }

        if (groupKey.serviceProviderOid().isPresent()) {
            statRecord.setServiceProviderId(groupKey.serviceProviderOid().getAsLong());
        }

        return statRecord.build();
    }

    private CloudCommitmentStatRecord convertUtilizationGroupingToStat(@Nonnull CommitmentUtilizationGrouping utilizationGrouping,
                                                                       long snapshotTime) {

        final UtilizationGroupKey groupKey = utilizationGrouping.key();

        // each coverage grouping should have only one coverage type
        final CloudCommitmentCoverageTypeInfo coverageTypeInfo = groupKey.coverageTypeInfo();

        final DoubleSummaryStatistics usedStats = utilizationGrouping.utilizationList().stream()
                .collect(Collectors.summarizingDouble(CloudCommitmentUtilizationVector::getUsed));

        final DoubleSummaryStatistics capacityStats = utilizationGrouping.utilizationList().stream()
                .collect(Collectors.summarizingDouble(CloudCommitmentUtilizationVector::getCapacity));

        final CloudCommitmentStatRecord.Builder statRecord = CloudCommitmentStatRecord.newBuilder()
                // use capacity as the count. it should always be equal to or greater than the used count
                .setSampleCount(capacityStats.getCount())
                .setSnapshotDate(snapshotTime)
                .setCoverageTypeInfo(coverageTypeInfo)
                .setValues(CloudCommitmentStatsUtils.convertToCommitmentStat(usedStats))
                .setCapacity(CloudCommitmentStatsUtils.convertToCommitmentStat(capacityStats));

        if (groupKey.commitmentOid().isPresent()) {
            statRecord.setCommitmentId(groupKey.commitmentOid().getAsLong());
        }

        if (groupKey.accountOid().isPresent()) {
            statRecord.setAccountId(groupKey.accountOid().getAsLong());
        }

        if (groupKey.regionOid().isPresent()) {
            statRecord.setRegionId(groupKey.regionOid().getAsLong());
        }

        if (groupKey.serviceProviderOid().isPresent()) {
            statRecord.setServiceProviderId(groupKey.serviceProviderOid().getAsLong());
        }

        return statRecord.build();
    }

}
