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

import com.vmturbo.cloud.common.commitment.CloudCommitmentResourceScope.ComputeTierResourceScope;
import com.vmturbo.cloud.common.commitment.CloudCommitmentResourceScope.ComputeTierResourceScope.PlatformInfo;
import com.vmturbo.cloud.common.commitment.aggregator.ReservedInstanceAggregate;
import com.vmturbo.cloud.common.commitment.aggregator.ReservedInstanceAggregationInfo;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverageType;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentEntityScope;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentEntityScope.GroupScope;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentLocation;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentLocationType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CloudCommitmentData.CloudCommitmentScope;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;
import com.vmturbo.reserved.instance.coverage.allocator.matcher.ComputeCommitmentMatcher.ComputeCommitmentMatcherFactory;

public class ComputeCommitmentMatcherTest {

    private final ComputeCommitmentMatcherFactory matcherFactory =
            new ComputeCommitmentMatcherFactory();

    @Test
    public void testBaseReservedInstanceMatching() {

        // Setup the RI aggregate
        final ReservedInstanceAggregationInfo riAggregateInfo = ReservedInstanceAggregationInfo.builder()
                .coverageType(CloudCommitmentCoverageType.COUPONS)
                .serviceProviderOid(7L)
                .purchasingAccountOid(2L)
                .location(CloudCommitmentLocation.newBuilder()
                        .setLocationType(CloudCommitmentLocationType.REGION)
                        .setLocationOid(3L)
                        .build())
                .resourceScope(ComputeTierResourceScope.builder()
                        .computeTierFamily("A")
                        .platformInfo(PlatformInfo.builder()
                                .isPlatformFlexible(true)
                                .build())
                        .addTenancies(Tenancy.DEFAULT)
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
                .aggregationInfo(riAggregateInfo)
                .build();

        // set up the matcher config
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
        assertThat(coverageKey.regionOid(), equalTo(OptionalLong.of(riAggregateInfo.location().getLocationOid())));

        assertThat(coverageKey, instanceOf(ComputeCoverageKey.class));
        final ComputeCoverageKey computeKey = (ComputeCoverageKey)coverageKey;
        assertThat(computeKey.tierFamily(), equalTo(riAggregateInfo.resourceScope().computeTierFamily()));
        assertFalse(computeKey.tierOid().isPresent());
    }

    @Test
    public void testSizeInflexbileRI() {

        // Setup the RI aggregate
        final ReservedInstanceAggregationInfo riAggregateInfo = ReservedInstanceAggregationInfo.builder()
                .coverageType(CloudCommitmentCoverageType.COUPONS)
                .serviceProviderOid(7L)
                .purchasingAccountOid(2L)
                .location(CloudCommitmentLocation.newBuilder()
                        .setLocationType(CloudCommitmentLocationType.REGION)
                        .setLocationOid(3L)
                        .build())
                .resourceScope(ComputeTierResourceScope.builder()
                        .computeTier(4L)
                        .platformInfo(PlatformInfo.builder()
                                .isPlatformFlexible(true)
                                .build())
                        .addTenancies(Tenancy.DEFAULT)
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
                .aggregationInfo(riAggregateInfo)
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
        assertThat(computeKey.tierFamily(), equalTo(null));
        assertThat(computeKey.tierOid(), equalTo(riAggregateInfo.resourceScope().computeTier()));
    }
}
