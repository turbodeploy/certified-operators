package com.vmturbo.cloud.commitment.analysis.spec;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.cloud.common.topology.ComputeTierFamilyResolver;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CommitmentPurchaseProfile.ReservedInstancePurchaseProfile;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

/**
 * A class for filtering RI specs according to defined purchase constraints in the cca analysis protobuf.
 */
public class RISpecPurchaseFilter implements CommitmentSpecPurchaseFilter<VirtualMachineCoverageScope, ReservedInstanceSpec> {

    private static final Logger logger = LogManager.getLogger();

    private static final Set<OSType> ALL_PLATFORMS = ImmutableSet.copyOf(OSType.values());

    private static final Comparator<ReservedInstanceSpecData> RI_SPEC_PREFERENCE =
            Comparator.comparing(ReservedInstanceSpecData::couponsPerInstance)
                    .thenComparing(ReservedInstanceSpecData::specId);

    private final CloudTopology<TopologyEntityDTO> cloudTierTopology;

    private final ReservedInstancePurchaseProfile riPurchaseProfile;

    private final CloudCommitmentSpecResolver<ReservedInstanceSpec> reservedInstanceSpecResolver;

    private final ComputeTierFamilyResolver computeTierFamilyResolver;



    /**
     * Constructor for the cloud commitment spec filter.
     *
     * @param cloudTierTopology The topology of cloud tiers..
     * @param riSpecResolver The cloud commitment spec resolver for getting ri specs.
     * @param computeTierFamilyResolver A resolver of related compute tiers, based on the tiers' family.
     *                                  This is used in cases where an RI spec is size flexible, in order
     *                                  to determine all tiers covered by the spec.
     * @param riPurchaseProfile The purchase profile, used to filter the RI spec inventory down to
     *                          potential specs to recommend.
     */
    public RISpecPurchaseFilter(@Nonnull CloudTopology<TopologyEntityDTO> cloudTierTopology,
                                @Nonnull CloudCommitmentSpecResolver<ReservedInstanceSpec> riSpecResolver,
                                @Nonnull ComputeTierFamilyResolver computeTierFamilyResolver,
                                @Nonnull ReservedInstancePurchaseProfile riPurchaseProfile) {
        this.cloudTierTopology = Objects.requireNonNull(cloudTierTopology);
        this.computeTierFamilyResolver = Objects.requireNonNull(computeTierFamilyResolver);
        this.riPurchaseProfile = Objects.requireNonNull(riPurchaseProfile);
        this.reservedInstanceSpecResolver = Objects.requireNonNull(riSpecResolver);
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public Map<VirtualMachineCoverageScope, ReservedInstanceSpecData> getSpecsByCoverageScope(final long regionOid) {

        if (riPurchaseProfile.getRiTypeByRegionOidMap().containsKey(regionOid)) {
            final ReservedInstanceType riTypeToPurchase = riPurchaseProfile.getRiTypeByRegionOidMap().get(regionOid);
            final Map<VirtualMachineCoverageScope, Optional<ReservedInstanceSpecData>> riSpecsByCoverageScope =
                    reservedInstanceSpecResolver.getSpecsForRegion(regionOid)
                            .stream()
                            .filter(riSpec -> filterRISpecsByType(riSpec, riTypeToPurchase))
                            .map(this::createRISpecData)
                            .filter(Optional::isPresent)
                            .map(Optional::get)
                            .map(riSpecData ->
                                    getCoverageScopes(riSpecData).stream()
                                            .map(coverageScope -> Pair.of(riSpecData, coverageScope))
                                            .collect(Collectors.toList()))
                            .flatMap(List::stream)
                            .collect(Collectors.groupingBy(
                                    Pair::getValue,
                                    Collectors.mapping(
                                            Pair::getKey,
                                            Collectors.reducing(this::choosePurchasingSpec))));

            return riSpecsByCoverageScope.entrySet()
                    .stream()
                    .filter(e -> e.getValue().isPresent())
                    .collect(ImmutableMap.toImmutableMap(
                            Map.Entry::getKey,
                            e -> e.getValue().get()));
        } else {
            logger.error("Unable to find purchase constraints for region (Region OID={})", regionOid);
            return Collections.emptyMap();
        }
    }

    private Optional<ReservedInstanceSpecData> createRISpecData(@Nonnull ReservedInstanceSpec riSpec) {
        final long computeTierOid = riSpec.getReservedInstanceSpecInfo().getTierId();
        return cloudTierTopology.getEntity(computeTierOid)
                .map(computeTier ->
                        ImmutableReservedInstanceSpecData.builder()
                                .spec(riSpec)
                                .cloudTier(computeTier)
                                .build());
    }

    private Set<VirtualMachineCoverageScope> getCoverageScopes(@Nonnull ReservedInstanceSpecData riSpecData) {

        final ReservedInstanceSpecInfo riSpecInfo = riSpecData.spec().getReservedInstanceSpecInfo();
        final long regionOid = riSpecInfo.getRegionId();
        final Tenancy tenancy = riSpecInfo.getTenancy();

        final long computeTierOid = riSpecData.cloudTier().getOid();
        final Set<Long> coveredComputeTiers = riSpecInfo.getSizeFlexible()
                ? computeTierFamilyResolver.getSiblingsInFamily(computeTierOid)
                : Collections.singleton(computeTierOid);

        final Set<OSType> coveredPlatforms = riSpecInfo.getPlatformFlexible()
                ? ALL_PLATFORMS
                : Collections.singleton(riSpecInfo.getOs());

        return coveredComputeTiers.stream()
                .flatMap(tierOid ->
                        coveredPlatforms.stream()
                                .map(platform -> ImmutableVirtualMachineCoverageScope.builder()
                                        .regionOid(regionOid)
                                        .tenancy(tenancy)
                                        .cloudTierOid(tierOid)
                                        .osType(platform)
                                        .build()))
                .collect(ImmutableSet.toImmutableSet());
    }

    @Nullable
    private ReservedInstanceSpecData choosePurchasingSpec(@Nullable ReservedInstanceSpecData riSpecDataA,
                                                          @Nullable ReservedInstanceSpecData riSpecDataB) {

        if (riSpecDataA == null || riSpecDataB == null) {
            return null;
        }

        final ReservedInstanceSpecInfo riSpecInfoA = riSpecDataA.spec()
                .getReservedInstanceSpecInfo();
        final ReservedInstanceSpecInfo riSpecInfoB = riSpecDataB.spec()
                .getReservedInstanceSpecInfo();

        if (riSpecInfoA.getSizeFlexible() == riSpecInfoB.getSizeFlexible()
                && riSpecInfoA.getPlatformFlexible() == riSpecInfoB.getPlatformFlexible()) {

            return RI_SPEC_PREFERENCE.compare(riSpecDataA, riSpecDataB) <= 0
                    ? riSpecDataA
                    : riSpecDataB;
        } else if ((riSpecInfoA.getSizeFlexible() || !riSpecInfoB.getSizeFlexible())
                && (riSpecInfoA.getPlatformFlexible() || !riSpecInfoB.getPlatformFlexible())) {
            return riSpecDataA;
        } else if ((riSpecInfoB.getSizeFlexible() || !riSpecInfoA.getSizeFlexible())
                && (riSpecInfoB.getPlatformFlexible() || !riSpecInfoA.getPlatformFlexible())) {
            return riSpecDataB;
        } else {
            // This must be a mismatch. One RI spec is size flexible while the other is platform
            // flexible. In this case, there is no correct RI spec to choose

            logger.error("Scope collision between two RI Specs (Spec 1={}, Tier={}, Spec 2={}, Tier={})",
                    riSpecDataA.specId(), riSpecDataA.cloudTier().getDisplayName(),
                    riSpecDataB.specId(), riSpecDataB.cloudTier().getDisplayName());
            return null;
        }
    }

    private boolean filterRISpecsByType(@Nonnull ReservedInstanceSpec riSpec,
                                        @Nonnull ReservedInstanceType riType) {
        return riSpec.getReservedInstanceSpecInfo().getType().equals(riType);
    }
}
