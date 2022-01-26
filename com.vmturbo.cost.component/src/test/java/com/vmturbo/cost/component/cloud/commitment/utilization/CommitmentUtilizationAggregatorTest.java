package com.vmturbo.cost.component.cloud.commitment.utilization;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.apache.commons.collections4.ListUtils;
import org.junit.Test;

import com.vmturbo.cloud.common.commitment.CloudCommitmentUtils;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverageType;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverageTypeInfo;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentUtilizationGroupBy;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentUtilizationVector;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.ScopedCommitmentUtilization;
import com.vmturbo.cost.component.cloud.commitment.utilization.CommitmentUtilizationAggregator.CommitmentUtilizationGrouping;
import com.vmturbo.cost.component.cloud.commitment.utilization.CommitmentUtilizationAggregator.UtilizationGroupKey;

public class CommitmentUtilizationAggregatorTest {


    private static final ScopedCommitmentUtilization SCOPED_UTILIZATION_A = ScopedCommitmentUtilization.newBuilder()
            .setCloudCommitmentOid(1)
            .setAccountOid(2)
            .setRegionOid(3)
            .setServiceProviderOid(4)
            .addUtilizationVector(CloudCommitmentUtilizationVector.newBuilder()
                    .setVectorType(CloudCommitmentUtils.COUPON_COVERAGE_TYPE_INFO)
                    .setUsed(1)
                    .setCapacity(2)
                    .build())
            .build();

    private static final ScopedCommitmentUtilization SCOPED_UTILIZATION_B = ScopedCommitmentUtilization.newBuilder()
            .setCloudCommitmentOid(5)
            .setAccountOid(6)
            .setRegionOid(3)
            .setServiceProviderOid(4)
            .addUtilizationVector(CloudCommitmentUtilizationVector.newBuilder()
                    .setVectorType(CloudCommitmentUtils.COUPON_COVERAGE_TYPE_INFO)
                    .setUsed(2)
                    .setCapacity(4)
                    .build())
            .build();

    private static final ScopedCommitmentUtilization SCOPED_UTILIZATION_C = ScopedCommitmentUtilization.newBuilder()
            .setCloudCommitmentOid(7)
            .setAccountOid(6)
            .setRegionOid(8)
            .setServiceProviderOid(4)
            .addUtilizationVector(CloudCommitmentUtilizationVector.newBuilder()
                    .setVectorType(
                            CloudCommitmentCoverageTypeInfo.newBuilder()
                                    .setCoverageType(CloudCommitmentCoverageType.SPEND_COMMITMENT)
                                    .build())
                    .setUsed(4)
                    .setCapacity(8)
                    .build())
            .build();

    private static final CommitmentUtilizationAggregator AGGREGATOR = new CommitmentUtilizationAggregator();

    @Test
    public void testDefaultAggregation() {

        final List<CommitmentUtilizationGrouping> actualGroupings = AGGREGATOR.groupUtilizationCollection(
                ImmutableList.of(SCOPED_UTILIZATION_A, SCOPED_UTILIZATION_B, SCOPED_UTILIZATION_C), Collections.emptySet());


        final UtilizationGroupKey expectedCouponGroupKey = UtilizationGroupKey.builder()
                .coverageTypeInfo(CloudCommitmentUtils.COUPON_COVERAGE_TYPE_INFO)
                .build();

        // ASSERTIONS
        assertThat(actualGroupings, hasSize(2));

        final Optional<CommitmentUtilizationGrouping> actualCouponGrouping = actualGroupings.stream()
                .filter(grouping -> grouping.key().equals(expectedCouponGroupKey))
                .findFirst();
        assertTrue(actualCouponGrouping.isPresent());
        assertThat(actualCouponGrouping.get().utilizationList(), hasSize(2));

        final List<CloudCommitmentUtilizationVector> expectedUtilizationVectors =
                ListUtils.union(SCOPED_UTILIZATION_A.getUtilizationVectorList(),
                        SCOPED_UTILIZATION_B.getUtilizationVectorList());
        assertThat(actualCouponGrouping.get().utilizationList(), containsInAnyOrder(expectedUtilizationVectors.toArray()));

        final UtilizationGroupKey expectedSpendGroupKey = UtilizationGroupKey.builder()
                .coverageTypeInfo(CloudCommitmentCoverageTypeInfo.newBuilder()
                        .setCoverageType(CloudCommitmentCoverageType.SPEND_COMMITMENT)
                        .build())
                .build();

        final Optional<CommitmentUtilizationGrouping> actualSpendGrouping = actualGroupings.stream()
                .filter(grouping -> grouping.key().equals(expectedSpendGroupKey))
                .findFirst();
        assertTrue(actualSpendGrouping.isPresent());
        assertThat(actualSpendGrouping.get().utilizationList(), hasSize(1));
        assertThat(actualSpendGrouping.get().utilizationList(), containsInAnyOrder(SCOPED_UTILIZATION_C.getUtilizationVectorList().toArray()));
    }

    @Test
    public void testCommitmentAggregation() {

        final List<CommitmentUtilizationGrouping> actualGroupings = AGGREGATOR.groupUtilizationCollection(
                ImmutableList.of(SCOPED_UTILIZATION_A, SCOPED_UTILIZATION_B),
                ImmutableSet.of(CloudCommitmentUtilizationGroupBy.COMMITMENT_UTILIZATION_GROUP_BY_COMMITMENT));

        // ASSERTIONS
        assertThat(actualGroupings, hasSize(2));

        // GROUP A
        final UtilizationGroupKey expectedGroupAKey = UtilizationGroupKey.builder()
                .coverageTypeInfo(CloudCommitmentUtils.COUPON_COVERAGE_TYPE_INFO)
                .commitmentOid(SCOPED_UTILIZATION_A.getCloudCommitmentOid())
                .accountOid(SCOPED_UTILIZATION_A.getAccountOid())
                .regionOid(SCOPED_UTILIZATION_A.getRegionOid())
                .serviceProviderOid(SCOPED_UTILIZATION_A.getServiceProviderOid())
                .build();

        final Optional<CommitmentUtilizationGrouping> actualGroupingA = actualGroupings.stream()
                .filter(grouping -> grouping.key().equals(expectedGroupAKey))
                .findFirst();
        assertTrue(actualGroupingA.isPresent());
        assertThat(actualGroupingA.get().utilizationList(), hasSize(1));
        assertThat(actualGroupingA.get().utilizationList(), containsInAnyOrder(SCOPED_UTILIZATION_A.getUtilizationVectorList().toArray()));

        // GROUP B
        final UtilizationGroupKey expectedGroupBKey = UtilizationGroupKey.builder()
                .coverageTypeInfo(CloudCommitmentUtils.COUPON_COVERAGE_TYPE_INFO)
                .commitmentOid(SCOPED_UTILIZATION_B.getCloudCommitmentOid())
                .accountOid(SCOPED_UTILIZATION_B.getAccountOid())
                .regionOid(SCOPED_UTILIZATION_B.getRegionOid())
                .serviceProviderOid(SCOPED_UTILIZATION_B.getServiceProviderOid())
                .build();

        final Optional<CommitmentUtilizationGrouping> actualGroupingB = actualGroupings.stream()
                .filter(grouping -> grouping.key().equals(expectedGroupBKey))
                .findFirst();
        assertTrue(actualGroupingB.isPresent());
        assertThat(actualGroupingB.get().utilizationList(), hasSize(1));
        assertThat(actualGroupingB.get().utilizationList(), containsInAnyOrder(SCOPED_UTILIZATION_B.getUtilizationVectorList().toArray()));
    }

    @Test
    public void testRegionAggregation() {

        final List<CommitmentUtilizationGrouping> actualGroupings = AGGREGATOR.groupUtilizationCollection(
                ImmutableList.of(SCOPED_UTILIZATION_A, SCOPED_UTILIZATION_B, SCOPED_UTILIZATION_C),
                ImmutableSet.of(CloudCommitmentUtilizationGroupBy.COMMITMENT_UTILIZATION_GROUP_BY_REGION));

        // ASSERTIONS
        assertThat(actualGroupings, hasSize(2));

        // Region 3 coupons
        final UtilizationGroupKey expectedGroup3Key = UtilizationGroupKey.builder()
                .coverageTypeInfo(CloudCommitmentUtils.COUPON_COVERAGE_TYPE_INFO)
                .regionOid(SCOPED_UTILIZATION_A.getRegionOid())
                .serviceProviderOid(SCOPED_UTILIZATION_A.getServiceProviderOid())
                .build();

        final Optional<CommitmentUtilizationGrouping> actualGrouping3 = actualGroupings.stream()
                .filter(grouping -> grouping.key().equals(expectedGroup3Key))
                .findFirst();
        assertTrue(actualGrouping3.isPresent());
        assertThat(actualGrouping3.get().utilizationList(), hasSize(2));

        final List<CloudCommitmentUtilizationVector> expectedUtilizationVectors3 =
                ListUtils.union(SCOPED_UTILIZATION_A.getUtilizationVectorList(),
                        SCOPED_UTILIZATION_B.getUtilizationVectorList());
        assertThat(actualGrouping3.get().utilizationList(), containsInAnyOrder(expectedUtilizationVectors3.toArray()));

        // Region 8 spend
        final UtilizationGroupKey expectedGroup8Key = UtilizationGroupKey.builder()
                .coverageTypeInfo(CloudCommitmentCoverageTypeInfo.newBuilder()
                        .setCoverageType(CloudCommitmentCoverageType.SPEND_COMMITMENT)
                        .build())
                .regionOid(SCOPED_UTILIZATION_C.getRegionOid())
                .serviceProviderOid(SCOPED_UTILIZATION_C.getServiceProviderOid())
                .build();

        final Optional<CommitmentUtilizationGrouping> actualGrouping8 = actualGroupings.stream()
                .filter(grouping -> grouping.key().equals(expectedGroup8Key))
                .findFirst();
        assertTrue(actualGrouping8.isPresent());
        assertThat(actualGrouping8.get().utilizationList(), hasSize(1));
        assertThat(actualGrouping8.get().utilizationList(), containsInAnyOrder(SCOPED_UTILIZATION_C.getUtilizationVectorList().toArray()));
    }
}
