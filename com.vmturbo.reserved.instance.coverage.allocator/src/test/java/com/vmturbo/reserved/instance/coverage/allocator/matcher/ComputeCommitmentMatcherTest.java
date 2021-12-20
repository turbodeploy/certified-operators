package com.vmturbo.reserved.instance.coverage.allocator.matcher;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

import java.util.OptionalLong;
import java.util.Set;

import com.google.common.collect.Iterables;

import org.junit.Test;

import com.vmturbo.cloud.common.commitment.aggregator.ReservedInstanceAggregate;
import com.vmturbo.cloud.common.commitment.aggregator.ReservedInstanceAggregateInfo;
import com.vmturbo.cloud.common.commitment.aggregator.ReservedInstanceAggregateInfo.PlatformInfo;
import com.vmturbo.cloud.common.commitment.aggregator.ReservedInstanceAggregateInfo.TierInfo;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverageType;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentEntityScope;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentEntityScope.GroupScope;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CloudCommitmentData.CloudCommitmentScope;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.reserved.instance.coverage.allocator.matcher.ComputeCommitmentMatcher.ComputeCommitmentMatcherFactory;

public class ComputeCommitmentMatcherTest {

    private final ComputeCommitmentMatcherFactory matcherFactory =
            new ComputeCommitmentMatcherFactory();

    @Test
    public void testBaseReservedInstanceMatching() {

        // Setup the RI aggregate
        final ReservedInstanceAggregateInfo riAggregateInfo = ReservedInstanceAggregateInfo.builder()
                .coverageType(CloudCommitmentCoverageType.COUPONS)
                .serviceProviderOid(7L)
                .purchasingAccountOid(2L)
                .regionOid(3L)
                .tierInfo(TierInfo.builder()
                        .tierFamily("A")
                        .tierType(EntityType.COMPUTE_TIER)
                        .tierOid(4L)
                        .isSizeFlexible(true)
                        .build())
                .platformInfo(PlatformInfo.builder()
                        .isPlatformFlexible(true)
                        .build())
                .entityScope(CloudCommitmentEntityScope.newBuilder()
                        .setScopeType(CloudCommitmentScope.CLOUD_COMMITMENT_SCOPE_BILLING_FAMILY_GROUP)
                        .setGroupScope(GroupScope.newBuilder()
                                .addGroupId(1L)
                                .build())
                        .build())
                .build();
        final ReservedInstanceAggregate riAggregate = ReservedInstanceAggregate.builder()
                .aggregateId(5L)
                .aggregateInfo(riAggregateInfo)
                .build();

        // setup the matcher config
        final CommitmentMatcherConfig matcherConfig = CommitmentMatcherConfig.builder()
                .scope(CloudCommitmentScope.CLOUD_COMMITMENT_SCOPE_BILLING_FAMILY_GROUP)
                .build();

        // setup and invoke the matcher
        final ComputeCommitmentMatcher commitmentMatcher = matcherFactory.newMatcher(matcherConfig);

        final Set<CoverageKey> coverageKeys = commitmentMatcher.createKeysForCommitment(riAggregate);

        assertThat(coverageKeys, hasSize(1));
        final CoverageKey coverageKey = Iterables.getOnlyElement(coverageKeys);

        assertThat(coverageKey.billingFamilyId(), equalTo(OptionalLong.of(1L)));
        assertFalse(coverageKey.accountOid().isPresent());
        assertThat(coverageKey.regionOid(), equalTo(OptionalLong.of(riAggregateInfo.regionOid())));

        assertThat(coverageKey, instanceOf(ComputeCoverageKey.class));
        final ComputeCoverageKey computeKey = (ComputeCoverageKey)coverageKey;
        assertThat(computeKey.tierFamily(), equalTo(riAggregateInfo.tierInfo().tierFamily().get()));
        assertFalse(computeKey.tierOid().isPresent());
    }

    @Test
    public void testSizeInflexbileRI() {

        // Setup the RI aggregate
        final ReservedInstanceAggregateInfo riAggregateInfo = ReservedInstanceAggregateInfo.builder()
                .coverageType(CloudCommitmentCoverageType.COUPONS)
                .serviceProviderOid(7L)
                .purchasingAccountOid(2L)
                .regionOid(3L)
                .tierInfo(TierInfo.builder()
                        .tierFamily("A")
                        .tierType(EntityType.COMPUTE_TIER)
                        .tierOid(4L)
                        .isSizeFlexible(false)
                        .build())
                .platformInfo(PlatformInfo.builder()
                        .isPlatformFlexible(true)
                        .build())
                .entityScope(CloudCommitmentEntityScope.newBuilder()
                        .setScopeType(CloudCommitmentScope.CLOUD_COMMITMENT_SCOPE_BILLING_FAMILY_GROUP)
                        .setGroupScope(GroupScope.newBuilder()
                                .addGroupId(1L)
                                .build())
                        .build())
                .build();
        final ReservedInstanceAggregate riAggregate = ReservedInstanceAggregate.builder()
                .aggregateId(5L)
                .aggregateInfo(riAggregateInfo)
                .build();

        // setup the matcher config
        final CommitmentMatcherConfig matcherConfig = CommitmentMatcherConfig.builder()
                .scope(CloudCommitmentScope.CLOUD_COMMITMENT_SCOPE_BILLING_FAMILY_GROUP)
                .build();

        // setup and invoke the matcher
        final ComputeCommitmentMatcher commitmentMatcher = matcherFactory.newMatcher(matcherConfig);

        final Set<CoverageKey> coverageKeys = commitmentMatcher.createKeysForCommitment(riAggregate);

        // ASSERTIONS
        assertThat(coverageKeys, hasSize(1));
        final CoverageKey coverageKey = Iterables.getOnlyElement(coverageKeys);

        assertThat(coverageKey, instanceOf(ComputeCoverageKey.class));
        final ComputeCoverageKey computeKey = (ComputeCoverageKey)coverageKey;
        assertThat(computeKey.tierFamily(), equalTo(riAggregateInfo.tierInfo().tierFamily().get()));
        assertThat(computeKey.tierOid(), equalTo(OptionalLong.of(riAggregateInfo.tierInfo().tierOid())));
    }
}
