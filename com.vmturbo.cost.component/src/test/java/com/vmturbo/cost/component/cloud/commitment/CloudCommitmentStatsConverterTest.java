package com.vmturbo.cost.component.cloud.commitment;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.cloud.common.commitment.CloudCommitmentUtils;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverageVector;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentUtilizationVector;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.ScopedCommitmentCoverage;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.ScopedCommitmentUtilization;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.CloudCommitmentStatRecord;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.CloudCommitmentStatRecord.StatValue;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.cost.component.cloud.commitment.coverage.CommitmentCoverageAggregator;
import com.vmturbo.cost.component.cloud.commitment.coverage.CommitmentCoverageAggregator.CommitmentCoverageGrouping;
import com.vmturbo.cost.component.cloud.commitment.coverage.CommitmentCoverageAggregator.CoverageGroupKey;
import com.vmturbo.cost.component.cloud.commitment.coverage.CoverageInfo;
import com.vmturbo.cost.component.cloud.commitment.utilization.CommitmentUtilizationAggregator;
import com.vmturbo.cost.component.cloud.commitment.utilization.CommitmentUtilizationAggregator.CommitmentUtilizationGrouping;
import com.vmturbo.cost.component.cloud.commitment.utilization.CommitmentUtilizationAggregator.UtilizationGroupKey;
import com.vmturbo.cost.component.cloud.commitment.utilization.UtilizationInfo;

public class CloudCommitmentStatsConverterTest {

    private CommitmentCoverageAggregator coverageAggregator;

    private CommitmentUtilizationAggregator utilizationAggregator;

    private CloudCommitmentStatsConverter statsConverter;

    @Before
    public void setup() {
        coverageAggregator = mock(CommitmentCoverageAggregator.class);
        utilizationAggregator = mock(CommitmentUtilizationAggregator.class);

        statsConverter = new CloudCommitmentStatsConverter(coverageAggregator, utilizationAggregator);
    }


    @Test
    public void testConvertToUtilizationStats() {

        final CloudCommitmentUtilizationVector utilizationVector = CloudCommitmentUtilizationVector.newBuilder()
                .setVectorType(CloudCommitmentUtils.COUPON_COVERAGE_TYPE_INFO)
                .setUsed(4.0)
                .setCapacity(8.0)
                .build();

        final ScopedCommitmentUtilization commitmentUtilization = ScopedCommitmentUtilization.newBuilder()
                .setCloudCommitmentOid(1)
                .setAccountOid(2)
                .setRegionOid(3)
                .setServiceProviderOid(4)
                .addUtilizationVector(utilizationVector)
                .build();
        final UtilizationInfo utilizationInfo = UtilizationInfo.builder()
                .topologyInfo(TopologyInfo.newBuilder()
                        .setCreationTime(123)
                        .build())
                .putCommitmentUtilizationMap(1, commitmentUtilization)
                .build();
        final CommitmentUtilizationGrouping utilizationGrouping = CommitmentUtilizationGrouping.of(
                UtilizationGroupKey.builder()
                        .coverageTypeInfo(CloudCommitmentUtils.COUPON_COVERAGE_TYPE_INFO)
                        .commitmentOid(commitmentUtilization.getCloudCommitmentOid())
                        .accountOid(commitmentUtilization.getAccountOid())
                        .regionOid(commitmentUtilization.getRegionOid())
                        .serviceProviderOid(commitmentUtilization.getServiceProviderOid())
                        .build(),
                ImmutableList.of(utilizationVector));

        // setup utilization aggregator mock
        when(utilizationAggregator.groupUtilizationCollection(any(), any())).thenReturn(ImmutableList.of(utilizationGrouping));

        // invoke converter
        final List<CloudCommitmentStatRecord> actualStats = statsConverter.convertToUtilizationStats(
                utilizationInfo,
                Collections.emptySet());

        // ASSERTIONS
        assertThat(actualStats, hasSize(1));

        final CloudCommitmentStatRecord expectedStatRecord = CloudCommitmentStatRecord.newBuilder()
                .setSnapshotDate(123)
                .setCommitmentId(commitmentUtilization.getCloudCommitmentOid())
                .setAccountId(commitmentUtilization.getAccountOid())
                .setRegionId(commitmentUtilization.getRegionOid())
                .setServiceProviderId(commitmentUtilization.getServiceProviderOid())
                .setCoverageTypeInfo(utilizationVector.getVectorType())
                .setSampleCount(1)
                .setValues(StatValue.newBuilder()
                        .setAvg(4.0)
                        .setMax(4.0)
                        .setMin(4.0)
                        .setTotal(4.0)
                        .build())
                .setCapacity(StatValue.newBuilder()
                        .setAvg(8.0)
                        .setMax(8.0)
                        .setMin(8.0)
                        .setTotal(8.0)
                        .build())
                .build();

        assertThat(actualStats.get(0), equalTo(expectedStatRecord));
    }

    @Test
    public void testConvertToCoverageStats() {

        final CloudCommitmentCoverageVector coverageVector = CloudCommitmentCoverageVector.newBuilder()
                .setVectorType(CloudCommitmentUtils.COUPON_COVERAGE_TYPE_INFO)
                .setUsed(1)
                .setCapacity(2)
                .build();
        final ScopedCommitmentCoverage entityCoverage = ScopedCommitmentCoverage.newBuilder()
                .setEntityOid(1)
                .setAccountOid(2)
                .setRegionOid(3)
                .setServiceProviderOid(4)
                .setCloudServiceOid(5)
                .addCoverageVector(coverageVector)
                .build();
        final CoverageInfo coverageInfo = CoverageInfo.builder()
                .topologyInfo(TopologyInfo.newBuilder()
                        .setCreationTime(123)
                        .build())
                .putEntityCoverageMap(entityCoverage.getEntityOid(), entityCoverage)
                .build();
        final CommitmentCoverageGrouping coverageGrouping = CommitmentCoverageGrouping.of(
                CoverageGroupKey.builder()
                        .coverageTypeInfo(coverageVector.getVectorType())
                        .accountOid(entityCoverage.getAccountOid())
                        .regionOid(entityCoverage.getRegionOid())
                        .cloudServiceOid(entityCoverage.getCloudServiceOid())
                        .serviceProviderOid(entityCoverage.getServiceProviderOid())
                        .build(),
                ImmutableList.of(coverageVector));

        // setup coverage aggregator mock
        when(coverageAggregator.groupCoverageCollection(any(), any())).thenReturn(ImmutableList.of(coverageGrouping));

        // invoke stats converter
        final List<CloudCommitmentStatRecord> actualRecords = statsConverter.convertToCoverageStats(coverageInfo, Collections.emptySet());

        // ASSERTIONS
        assertThat(actualRecords, hasSize(1));

        final CloudCommitmentStatRecord expectedStatRecord = CloudCommitmentStatRecord.newBuilder()
                .setSnapshotDate(123)
                .setAccountId(entityCoverage.getAccountOid())
                .setRegionId(entityCoverage.getRegionOid())
                .setServiceProviderId(entityCoverage.getServiceProviderOid())
                .setCloudServiceId(entityCoverage.getCloudServiceOid())
                .setCoverageTypeInfo(coverageVector.getVectorType())
                .setSampleCount(1)
                .setValues(StatValue.newBuilder()
                        .setAvg(1.0)
                        .setMax(1.0)
                        .setMin(1.0)
                        .setTotal(1.0)
                        .build())
                .setCapacity(StatValue.newBuilder()
                        .setAvg(2.0)
                        .setMax(2.0)
                        .setMin(2.0)
                        .setTotal(2.0)
                        .build())
                .build();

        assertThat(actualRecords.get(0), equalTo(expectedStatRecord));
    }
}
