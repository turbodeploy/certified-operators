package com.vmturbo.reserved.instance.coverage.allocator.matcher.entity;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import com.google.common.collect.Iterables;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.cloud.CloudCommitment.CloudCommitmentLocation;
import com.vmturbo.common.protobuf.cloud.CloudCommitment.CloudCommitmentScope;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;
import com.vmturbo.reserved.instance.coverage.allocator.matcher.ComputeCoverageKey;
import com.vmturbo.reserved.instance.coverage.allocator.matcher.CoverageKey;
import com.vmturbo.reserved.instance.coverage.allocator.matcher.entity.DefaultCoverageEntityMatcher.DefaultCoverageEntityMatcherFactory;
import com.vmturbo.reserved.instance.coverage.allocator.matcher.entity.VirtualMachineMatcherConfig.TierMatcher;
import com.vmturbo.reserved.instance.coverage.allocator.topology.CloudAggregationInfo;
import com.vmturbo.reserved.instance.coverage.allocator.topology.ComputeTierInfo;
import com.vmturbo.reserved.instance.coverage.allocator.topology.CoverageTopology;
import com.vmturbo.reserved.instance.coverage.allocator.topology.VirtualMachineInfo;

public class DefaultCoverageEntityMatcherTest {

    private final DefaultCoverageEntityMatcherFactory entityMatcherFactory =
            new DefaultCoverageEntityMatcherFactory();

    private final CoverageTopology coverageTopology = mock(CoverageTopology.class);

    private final long entityOid = 1L;

    private final CloudAggregationInfo aggregateInfo = CloudAggregationInfo.builder()
            .accountOid(2L)
            .billingFamilyId(3L)
            .regionOid(4L)
            .zoneOid(5L)
            .build();

    private final VirtualMachineInfo vmInfo = VirtualMachineInfo.builder()
            .entityState(EntityState.POWERED_ON)
            .platform(OSType.WINDOWS)
            .tenancy(Tenancy.HOST)
            .build();

    private final ComputeTierInfo tierInfo = ComputeTierInfo.builder()
            .tierOid(6L)
            .family("tierFamily")
            .build();


    @Before
    public void setup() {
        when(coverageTopology.getAggregationInfo(eq(entityOid))).thenReturn(Optional.of(aggregateInfo));
        when(coverageTopology.getComputeTierInfoForEntity(eq(entityOid))).thenReturn(Optional.of(tierInfo));
        when(coverageTopology.getEntityInfo(eq(entityOid))).thenReturn(Optional.of(vmInfo));
    }

    @Test
    public void testBaseMatching() {

        // setup the EntityMatcherConfig
        final EntityMatcherConfig matcherConfig = VirtualMachineMatcherConfig.builder()
                .addScopes(CloudCommitmentScope.BILLING_FAMILY)
                .addLocations(CloudCommitmentLocation.REGION)
                .addTierMatchers(TierMatcher.FAMILY)
                .includePlatform(true)
                .includeTenancy(true)
                .build();

        // setup the coverage matcher
        final CoverageEntityMatcher entityMatcher = entityMatcherFactory.createEntityMatcher(
                coverageTopology, Collections.singleton(matcherConfig));

        final Set<CoverageKey> coverageKeys = entityMatcher.createCoverageKeys(entityOid);

        // ASSERTIONS
        assertThat(coverageKeys, hasSize(1));

        final CoverageKey coverageKey = Iterables.getOnlyElement(coverageKeys);
        assertThat(coverageKey.billingFamilyId(), equalTo(aggregateInfo.billingFamilyId()));
        assertFalse(coverageKey.accountOid().isPresent());
        assertThat(coverageKey.regionOid(), equalTo(OptionalLong.of(aggregateInfo.regionOid())));
        assertFalse(coverageKey.zoneOid().isPresent());

        assertThat(coverageKey, instanceOf(ComputeCoverageKey.class));
        final ComputeCoverageKey computeKey = (ComputeCoverageKey)coverageKey;
        assertThat(computeKey.tierFamily(), equalTo(tierInfo.family().get()));
        assertFalse(computeKey.tierOid().isPresent());
        assertThat(computeKey.platform(), equalTo(vmInfo.platform()));
        assertThat(computeKey.tenancy(), equalTo(vmInfo.tenancy()));
    }


    @Test
    public void testAccountMatching() {

        // setup the EntityMatcherConfig
        final EntityMatcherConfig matcherConfig = VirtualMachineMatcherConfig.builder()
                .addScopes(CloudCommitmentScope.ACCOUNT)
                .addLocations(CloudCommitmentLocation.REGION)
                .addTierMatchers(TierMatcher.FAMILY)
                .includePlatform(true)
                .includeTenancy(true)
                .build();

        // setup the coverage matcher
        final CoverageEntityMatcher entityMatcher = entityMatcherFactory.createEntityMatcher(
                coverageTopology, Collections.singleton(matcherConfig));

        final Set<CoverageKey> coverageKeys = entityMatcher.createCoverageKeys(entityOid);

        // ASSERTIONS
        assertThat(coverageKeys, hasSize(1));

        final CoverageKey coverageKey = Iterables.getOnlyElement(coverageKeys);
        assertThat(coverageKey.billingFamilyId(), equalTo(aggregateInfo.billingFamilyId()));
        assertThat(coverageKey.accountOid(), equalTo(OptionalLong.of(aggregateInfo.accountOid())));
    }

    @Test
    public void testTierMatching() {

        // setup the EntityMatcherConfig
        final EntityMatcherConfig matcherConfig = VirtualMachineMatcherConfig.builder()
                .addScopes(CloudCommitmentScope.BILLING_FAMILY)
                .addLocations(CloudCommitmentLocation.REGION)
                .addTierMatchers(TierMatcher.TIER)
                .includePlatform(true)
                .includeTenancy(true)
                .build();

        // setup the coverage matcher
        final CoverageEntityMatcher entityMatcher = entityMatcherFactory.createEntityMatcher(
                coverageTopology, Collections.singleton(matcherConfig));

        final Set<CoverageKey> coverageKeys = entityMatcher.createCoverageKeys(entityOid);

        // ASSERTIONS
        assertThat(coverageKeys, hasSize(1));

        final CoverageKey coverageKey = Iterables.getOnlyElement(coverageKeys);
        assertThat(coverageKey, instanceOf(ComputeCoverageKey.class));
        final ComputeCoverageKey computeKey = (ComputeCoverageKey)coverageKey;
        assertThat(computeKey.tierFamily(), equalTo(tierInfo.family().get()));
        assertThat(computeKey.tierOid(), equalTo(OptionalLong.of(tierInfo.tierOid())));
    }

    @Test
    public void testPlatformFlexible() {

        // setup the EntityMatcherConfig
        final EntityMatcherConfig matcherConfig = VirtualMachineMatcherConfig.builder()
                .addScopes(CloudCommitmentScope.BILLING_FAMILY)
                .addLocations(CloudCommitmentLocation.REGION)
                .addTierMatchers(TierMatcher.TIER)
                .includePlatform(false)
                .includeTenancy(true)
                .build();

        // setup the coverage matcher
        final CoverageEntityMatcher entityMatcher = entityMatcherFactory.createEntityMatcher(
                coverageTopology, Collections.singleton(matcherConfig));

        final Set<CoverageKey> coverageKeys = entityMatcher.createCoverageKeys(entityOid);

        // ASSERTIONS
        assertThat(coverageKeys, hasSize(1));

        final CoverageKey coverageKey = Iterables.getOnlyElement(coverageKeys);
        assertThat(coverageKey, instanceOf(ComputeCoverageKey.class));
        final ComputeCoverageKey computeKey = (ComputeCoverageKey)coverageKey;
        assertThat(computeKey.platform(), equalTo(null));
    }
}
