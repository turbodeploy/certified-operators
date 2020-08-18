package com.vmturbo.cloud.commitment.analysis.spec;

import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.Lists;

import org.junit.Test;

import com.vmturbo.cloud.commitment.analysis.topology.ComputeTierFamilyResolver.ComputeTierFamilyResolverFactory;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CommitmentPurchaseProfile;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CommitmentPurchaseProfile.ReservedInstancePurchaseProfile;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopologyFactory.DefaultTopologyEntityCloudTopologyFactory;
import com.vmturbo.group.api.GroupMemberRetriever;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType.OfferingClass;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType.PaymentOption;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

public class RISpecPurchaseFilterTest {

    private final CloudCommitmentSpecResolver<ReservedInstanceSpec> riSpecResolver =
            mock(CloudCommitmentSpecResolver.class);

    private final ComputeTierFamilyResolverFactory computeTierFamilyResolverFactory =
            new ComputeTierFamilyResolverFactory();

    private final RISpecPurchaseFilterFactory riSpecPurchaseFilterFactory =
            new RISpecPurchaseFilterFactory(riSpecResolver, computeTierFamilyResolverFactory);


    private final TopologyEntityDTO computeTierSmallFamilyA = TopologyEntityDTO.newBuilder()
            .setEnvironmentType(EnvironmentType.CLOUD)
            .setEntityType(EntityType.COMPUTE_TIER_VALUE)
            .setOid(1L)
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                    .setComputeTier(ComputeTierInfo.newBuilder()
                            .setNumCoupons(1)
                            .setFamily("a")))
            .build();

    private final TopologyEntityDTO computeTierLargeFamilyA = TopologyEntityDTO.newBuilder()
            .setEnvironmentType(EnvironmentType.CLOUD)
            .setEntityType(EntityType.COMPUTE_TIER_VALUE)
            .setOid(2L)
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                    .setComputeTier(ComputeTierInfo.newBuilder()
                            .setNumCoupons(10)
                            .setFamily("a")))
            .build();

    private final TopologyEntityDTO computeTierSmallFamilyB = TopologyEntityDTO.newBuilder()
            .setEnvironmentType(EnvironmentType.CLOUD)
            .setEntityType(EntityType.COMPUTE_TIER_VALUE)
            .setOid(3L)
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                    .setComputeTier(ComputeTierInfo.newBuilder()
                            .setNumCoupons(1)
                            .setFamily("b")))
            .build();

    private final CloudTopology<TopologyEntityDTO> cloudTopology =
            new DefaultTopologyEntityCloudTopologyFactory(mock(GroupMemberRetriever.class))
                    .newCloudTopology(Stream.of(
                            computeTierSmallFamilyA,
                            computeTierLargeFamilyA,
                            computeTierSmallFamilyB));

    private final long regionOid = 123L;
    private final ReservedInstanceType targetRIType = ReservedInstanceType.newBuilder()
            .setOfferingClass(OfferingClass.STANDARD)
            .setPaymentOption(PaymentOption.ALL_UPFRONT)
            .setTermYears(3)
            .build();
    private final CommitmentPurchaseProfile commitmentPurchaseProfile = CommitmentPurchaseProfile.newBuilder()
            .setRiPurchaseProfile(ReservedInstancePurchaseProfile.newBuilder()
                    .putRiTypeByRegionOid(regionOid, targetRIType)
                    .build())
            .build();

    private final VirtualMachineCoverageScope coverageScopeSmall = ImmutableVirtualMachineCoverageScope.builder()
            .cloudTierOid(computeTierSmallFamilyA.getOid())
            .regionOid(regionOid)
            .osType(OSType.RHEL)
            .tenancy(Tenancy.DEFAULT)
            .build();
    private final VirtualMachineCoverageScope coverageScopeLarge = ImmutableVirtualMachineCoverageScope.builder()
            .cloudTierOid(computeTierLargeFamilyA.getOid())
            .regionOid(regionOid)
            .osType(OSType.RHEL)
            .tenancy(Tenancy.DEFAULT)
            .build();

    /**
     * Tests that the purchase filter correctly filters out RI specs that do not match the
     * purchase profile
     */
    @Test
    public void testPurchaseFilter() {

        // setup the RI specs in that region
        final ReservedInstanceSpecInfo baseSpecInfo = ReservedInstanceSpecInfo.newBuilder()
                .setTenancy(Tenancy.DEFAULT)
                .setOs(OSType.RHEL)
                .setTierId(computeTierSmallFamilyA.getOid())
                .setRegionId(regionOid)
                .setPlatformFlexible(false)
                .setSizeFlexible(false)
                .build();

        final ReservedInstanceSpec targetRISpec = ReservedInstanceSpec.newBuilder()
                .setId(10L)
                .setReservedInstanceSpecInfo(baseSpecInfo.toBuilder()
                        .setType(targetRIType))
                .build();

        final ReservedInstanceSpec otherRISpecA = ReservedInstanceSpec.newBuilder()
                .setId(10L)
                .setReservedInstanceSpecInfo(baseSpecInfo.toBuilder()
                        .setType(targetRIType.toBuilder()
                                .setOfferingClass(OfferingClass.CONVERTIBLE)))
                .build();

        final ReservedInstanceSpec otherRISpecB = ReservedInstanceSpec.newBuilder()
                .setId(10L)
                .setReservedInstanceSpecInfo(baseSpecInfo.toBuilder()
                        .setType(targetRIType.toBuilder()
                                .setTermYears(1)))
                .build();

        // setup RI spec resolver mock
        when(riSpecResolver.getSpecsForRegion(eq(regionOid)))
                .thenReturn(Lists.newArrayList(targetRISpec, otherRISpecA, otherRISpecB));


        // Invoke the ri spec filter
        final RISpecPurchaseFilter riSpecPurchaseFilter = riSpecPurchaseFilterFactory.createFilter(
                cloudTopology, commitmentPurchaseProfile);

        final Map<VirtualMachineCoverageScope, ReservedInstanceSpecData> riSpecsByCoverageScope =
                riSpecPurchaseFilter.getSpecsByCoverageScope(regionOid);

        // expected result
        final ReservedInstanceSpecData expectedRISpecData = ImmutableReservedInstanceSpecData.builder()
                                .spec(targetRISpec)
                                .cloudTier(computeTierSmallFamilyA)
                                .build();

        // assertions
        assertThat(riSpecsByCoverageScope.entrySet(), hasSize(1));
        assertThat(riSpecsByCoverageScope, hasEntry(coverageScopeSmall, expectedRISpecData));

    }

    /**
     * Tests that two RI specs with size flexibility are correct mapped
     */
    @Test
    public void testSizeFlexibleSpecs() {

        // Setup RI specs
        final ReservedInstanceSpec smallSpec = ReservedInstanceSpec.newBuilder()
                .setId(1)
                .setReservedInstanceSpecInfo(ReservedInstanceSpecInfo.newBuilder()
                    .setTenancy(Tenancy.DEFAULT)
                    .setOs(OSType.RHEL)
                    .setTierId(computeTierSmallFamilyA.getOid())
                    .setRegionId(regionOid)
                    .setPlatformFlexible(false)
                    .setSizeFlexible(true)
                    .setType(targetRIType))
                .build();

        final ReservedInstanceSpec largeSpec = ReservedInstanceSpec.newBuilder()
                .setId(2)
                .setReservedInstanceSpecInfo(smallSpec.getReservedInstanceSpecInfo()
                        .toBuilder()
                        .setTierId(computeTierLargeFamilyA.getOid()))
                .build();

        when(riSpecResolver.getSpecsForRegion(eq(regionOid)))
                .thenReturn(Lists.newArrayList(smallSpec, largeSpec));

        // Invoke the ri spec filter
        final RISpecPurchaseFilter riSpecPurchaseFilter = riSpecPurchaseFilterFactory.createFilter(
                cloudTopology, commitmentPurchaseProfile);

        final Map<VirtualMachineCoverageScope, ReservedInstanceSpecData> riSpecsByCoverageScope =
                riSpecPurchaseFilter.getSpecsByCoverageScope(regionOid);

        // expected result
        final ReservedInstanceSpecData expectedRISpecData = ImmutableReservedInstanceSpecData.builder()
                .spec(smallSpec)
                .cloudTier(computeTierSmallFamilyA)
                .build();


        // Asserts
        assertThat(riSpecsByCoverageScope.entrySet(), hasSize(2));
        assertThat(riSpecsByCoverageScope, hasEntry(coverageScopeSmall, expectedRISpecData));
        assertThat(riSpecsByCoverageScope, hasEntry(coverageScopeLarge, expectedRISpecData));
    }

    @Test
    public void testPlatformFlexibility() {

        // Setup RI specs
        final ReservedInstanceSpec riSpec = ReservedInstanceSpec.newBuilder()
                .setId(1)
                .setReservedInstanceSpecInfo(ReservedInstanceSpecInfo.newBuilder()
                        .setTenancy(Tenancy.DEFAULT)
                        .setOs(OSType.UNKNOWN_OS)
                        .setTierId(computeTierSmallFamilyA.getOid())
                        .setRegionId(regionOid)
                        .setPlatformFlexible(true)
                        .setSizeFlexible(false)
                        .setType(targetRIType))
                .build();

        when(riSpecResolver.getSpecsForRegion(eq(regionOid)))
                .thenReturn(Lists.newArrayList(riSpec));

        // Invoke the ri spec filter
        final RISpecPurchaseFilter riSpecPurchaseFilter = riSpecPurchaseFilterFactory.createFilter(
                cloudTopology, commitmentPurchaseProfile);

        final Map<VirtualMachineCoverageScope, ReservedInstanceSpecData> riSpecsByCoverageScope =
                riSpecPurchaseFilter.getSpecsByCoverageScope(regionOid);

        // expected result
        final Set<VirtualMachineCoverageScope> expectedCoverageScopes = Arrays.asList(OSType.values())
                .stream()
                .map(osType -> ImmutableVirtualMachineCoverageScope.builder()
                    .cloudTierOid(riSpec.getReservedInstanceSpecInfo().getTierId())
                    .regionOid(regionOid)
                    .osType(osType)
                    .tenancy(Tenancy.DEFAULT)
                    .build())
                .collect(Collectors.toSet());
        final ReservedInstanceSpecData expectedRISpecData = ImmutableReservedInstanceSpecData.builder()
                .spec(riSpec)
                .cloudTier(computeTierSmallFamilyA)
                .build();


        // Asserts
        assertThat(riSpecsByCoverageScope.entrySet(), hasSize(expectedCoverageScopes.size()));
        expectedCoverageScopes.forEach(expectedCoverageScope ->
                assertThat(riSpecsByCoverageScope, hasEntry(expectedCoverageScope, expectedRISpecData)));
    }

    @Test
    public void testLargerScopeSelection() {
        final ReservedInstanceSpec sizeFlexibleSpec = ReservedInstanceSpec.newBuilder()
                .setId(1)
                .setReservedInstanceSpecInfo(ReservedInstanceSpecInfo.newBuilder()
                        .setTenancy(Tenancy.DEFAULT)
                        .setOs(OSType.RHEL)
                        .setTierId(computeTierSmallFamilyA.getOid())
                        .setRegionId(regionOid)
                        .setPlatformFlexible(false)
                        .setSizeFlexible(true)
                        .setType(targetRIType))
                .build();

        final ReservedInstanceSpec sizeInflexibleSpec = ReservedInstanceSpec.newBuilder()
                .setId(2)
                .setReservedInstanceSpecInfo(sizeFlexibleSpec.getReservedInstanceSpecInfo()
                        .toBuilder()
                        .setSizeFlexible(false))
                .build();

        when(riSpecResolver.getSpecsForRegion(eq(regionOid)))
                .thenReturn(Lists.newArrayList(sizeFlexibleSpec, sizeInflexibleSpec));

        // Invoke the ri spec filter
        final RISpecPurchaseFilter riSpecPurchaseFilter = riSpecPurchaseFilterFactory.createFilter(
                cloudTopology, commitmentPurchaseProfile);

        final Map<VirtualMachineCoverageScope, ReservedInstanceSpecData> riSpecsByCoverageScope =
                riSpecPurchaseFilter.getSpecsByCoverageScope(regionOid);

        // expected result
        final ReservedInstanceSpecData expectedRISpecData = ImmutableReservedInstanceSpecData.builder()
                .spec(sizeFlexibleSpec)
                .cloudTier(computeTierSmallFamilyA)
                .build();


        // Asserts
        assertThat(riSpecsByCoverageScope.entrySet(), hasSize(2));
        assertThat(riSpecsByCoverageScope, hasEntry(coverageScopeSmall, expectedRISpecData));
        assertThat(riSpecsByCoverageScope, hasEntry(coverageScopeLarge, expectedRISpecData));
    }

    @Test
    public void testScopeConflict() {
        final ReservedInstanceSpec sizeFlexibleSpec = ReservedInstanceSpec.newBuilder()
                .setId(1)
                .setReservedInstanceSpecInfo(ReservedInstanceSpecInfo.newBuilder()
                        .setTenancy(Tenancy.DEFAULT)
                        .setOs(OSType.RHEL)
                        .setTierId(computeTierSmallFamilyA.getOid())
                        .setRegionId(regionOid)
                        .setPlatformFlexible(false)
                        .setSizeFlexible(true)
                        .setType(targetRIType))
                .build();

        final ReservedInstanceSpec platformFlexibleSpec = ReservedInstanceSpec.newBuilder()
                .setId(2)
                .setReservedInstanceSpecInfo(sizeFlexibleSpec.getReservedInstanceSpecInfo()
                        .toBuilder()
                        .setSizeFlexible(false)
                        .setPlatformFlexible(true))
                .build();

        when(riSpecResolver.getSpecsForRegion(eq(regionOid)))
                .thenReturn(Lists.newArrayList(sizeFlexibleSpec, platformFlexibleSpec));

        // Invoke the ri spec filter
        final RISpecPurchaseFilter riSpecPurchaseFilter = riSpecPurchaseFilterFactory.createFilter(
                cloudTopology, commitmentPurchaseProfile);

        final Map<VirtualMachineCoverageScope, ReservedInstanceSpecData> riSpecsByCoverageScope =
                riSpecPurchaseFilter.getSpecsByCoverageScope(regionOid);

        // verify nothing is recommended for the overlapping coverage scope
        assertFalse(riSpecsByCoverageScope.containsKey(coverageScopeSmall));
    }

    @Test
    public void testPurchaseConstraintsNotFound() {

        final RISpecPurchaseFilter riSpecPurchaseFilter = riSpecPurchaseFilterFactory.createFilter(
                cloudTopology, commitmentPurchaseProfile);

        final Map<VirtualMachineCoverageScope, ReservedInstanceSpecData> riSpecsByCoverageScope =
                riSpecPurchaseFilter.getSpecsByCoverageScope(regionOid + 1);

        // Asserts
        assertTrue(riSpecsByCoverageScope.isEmpty());
    }
}
