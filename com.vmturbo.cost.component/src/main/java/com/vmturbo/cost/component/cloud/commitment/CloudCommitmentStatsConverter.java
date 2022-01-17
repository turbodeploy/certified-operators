package com.vmturbo.cost.component.cloud.commitment;

import java.util.List;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;

import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.CloudCommitmentStatRecord;
import com.vmturbo.cost.component.cloud.commitment.coverage.CoverageInfo;
import com.vmturbo.cost.component.cloud.commitment.utilization.UtilizationInfo;

/**
 * A converter of cloud commitment related data (coverage, utilization, etc) to {@link CloudCommitmentStatRecord}
 * instances.
 */
public class CloudCommitmentStatsConverter {

    /**
     * Converts {@link CoverageInfo} to {@link CloudCommitmentStatRecord}.
     * @param coverageInfo The coverage info.
     * @return The stat record list.
     */
    @Nonnull
    public List<CloudCommitmentStatRecord> convertToCoverageStats(@Nonnull CoverageInfo coverageInfo) {

        final long topologyTime = coverageInfo.topologyInfo().getCreationTime();

        return coverageInfo.entityCoverageMap().values()
                .stream()
                .flatMap(commitmentCoverage -> commitmentCoverage.getCoverageVectorList().stream()
                        .map(coverageVector -> CloudCommitmentStatRecord.newBuilder()
                                .setSnapshotDate(topologyTime)
                                .setEntityId(commitmentCoverage.getEntityOid())
                                .setCloudServiceId(commitmentCoverage.getCloudServiceOid())
                                .setRegionId(commitmentCoverage.getRegionOid())
                                .setAccountId(commitmentCoverage.getAccountOid())
                                .setServiceProviderId(commitmentCoverage.getServiceProviderOid())
                                .setCoverageTypeInfo(coverageVector.getVectorType())
                                .setSampleCount(1)
                                .setValues(CloudCommitmentStatRecord.StatValue.newBuilder()
                                        .setAvg(coverageVector.getUsed())
                                        .setTotal(coverageVector.getUsed())
                                        .setMax(coverageVector.getUsed())
                                        .setMin(coverageVector.getUsed()))
                                .setCapacity(CloudCommitmentStatRecord.StatValue.newBuilder()
                                        .setAvg(coverageVector.getCapacity())
                                        .setTotal(coverageVector.getCapacity())
                                        .setMax(coverageVector.getCapacity())
                                        .setMin(coverageVector.getCapacity()))
                                .build()))
                .collect(ImmutableList.toImmutableList());
    }

    /**
     * Converts {@link UtilizationInfo} to {@link CloudCommitmentStatRecord}.
     * @param utilizationInfo The utilization info.
     * @return The stat record list.
     */
    @Nonnull
    public List<CloudCommitmentStatRecord> convertToUtilizationStats(@Nonnull UtilizationInfo utilizationInfo) {

        final long topologyTime = utilizationInfo.topologyInfo().getCreationTime();

        return utilizationInfo.commitmentUtilizationMap().values()
                .stream()
                .flatMap(commitmentUtilization -> commitmentUtilization.getUtilizationVectorList().stream()
                        .map(utilizationVector -> CloudCommitmentStatRecord.newBuilder()
                                .setSnapshotDate(topologyTime)
                                .setCommitmentId(commitmentUtilization.getCloudCommitmentOid())
                                .setRegionId(commitmentUtilization.getRegionOid())
                                .setAccountId(commitmentUtilization.getAccountOid())
                                .setServiceProviderId(commitmentUtilization.getServiceProviderOid())
                                .setCoverageTypeInfo(utilizationVector.getVectorType())
                                .setSampleCount(1)
                                .setValues(CloudCommitmentStatRecord.StatValue.newBuilder()
                                        .setAvg(utilizationVector.getUsed())
                                        .setTotal(utilizationVector.getUsed())
                                        .setMax(utilizationVector.getUsed())
                                        .setMin(utilizationVector.getUsed()))
                                .setCapacity(CloudCommitmentStatRecord.StatValue.newBuilder()
                                        .setAvg(utilizationVector.getCapacity())
                                        .setTotal(utilizationVector.getCapacity())
                                        .setMax(utilizationVector.getCapacity())
                                        .setMin(utilizationVector.getCapacity()))
                                .build()))
                .collect(ImmutableList.toImmutableList());
    }
}
