package com.vmturbo.cloud.commitment.analysis.spec;

import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType;

/**
 * A class for filtering RI specs according to defined purchase constraints in the cca analysis protobuf.
 */
public class ReservedInstanceSpecFilter implements CloudCommitmentSpecFilter<ReservedInstanceSpec> {

    private static final Logger logger = LogManager.getLogger();

    private final CloudTopology<TopologyEntityDTO> cloudTierTopology;

    private final Map<Long, ReservedInstanceType> purchaseConstraintsByRegionOid;

    private final CloudCommitmentSpecResolver cloudCommitmentSpecResolver;

    /**
     * Constructor for the cloud commitment spec filter.
     *
     * @param cloudTierTopology The topology of cloud tiers..
     * @param purchaseConstraintsByRegionOid Map of region oid to RIType.
     * @param cloudCommitmentSpecResolver The cloud commitment spec resolver for getting ri specs.
     */
    public ReservedInstanceSpecFilter(@Nonnull CloudTopology<TopologyEntityDTO> cloudTierTopology,
                                     @Nonnull Map<Long, ReservedInstanceType> purchaseConstraintsByRegionOid,
                                     @Nonnull CloudCommitmentSpecResolver cloudCommitmentSpecResolver) {
        this.cloudTierTopology = Objects.requireNonNull(cloudTierTopology);
        this.purchaseConstraintsByRegionOid = Objects.requireNonNull(purchaseConstraintsByRegionOid);
        this.cloudCommitmentSpecResolver = Objects.requireNonNull(cloudCommitmentSpecResolver);
    }

    @Override
    public Map<ReservedInstanceSpecKey, ReservedInstanceSpecData> filterRISpecs(long regionOid) {

        final Collection<ReservedInstanceSpec> riSpecs = cloudCommitmentSpecResolver.getRISpecsForRegion(regionOid);
        final Map<ReservedInstanceSpecKey, Set<ReservedInstanceSpecData>> riSpecsByKey =
                riSpecs.stream()
                        ///First convert each RI spec to an RI spec data instance,
                        // containing the associated compute tier of the RI spec.
                        .map(riSpec -> convertRISpecToData(cloudTierTopology, riSpec))
                        .filter(Objects::nonNull)
                        // Group the RI specs by an RI spec key, indicating the scope the RI spec
                        // could cover
                        .collect(Collectors.groupingBy(
                                this::convertRISpecDataToKey,
                                Collectors.toSet()));

        // Collect the set of RI specs which fit within the purchase constraints.
        final Map<ReservedInstanceSpecKey, ReservedInstanceSpecData> riSpecsToPurchaseByKey = riSpecsByKey.entrySet()
                .stream()
                .map(riSpecKeyEntry -> ImmutablePair.of(
                        riSpecKeyEntry.getKey(),
                        resolvePurchasingSpec(riSpecKeyEntry.getValue())))
                // It's possible none of the RI specs within this key fit the purchase constraints.
                // Therefore, ignore this key if it doesn't have a viable RI spec to recommend.
                .filter(riSpeKeyData -> riSpeKeyData.getRight() != null)
                .collect(ImmutableMap.toImmutableMap(
                        ImmutablePair::getLeft,
                        ImmutablePair::getRight));

        // Returning a map for now but this would be passed in to the RISpecMatcher going to be created
        // as part of OM-57946
        return riSpecsToPurchaseByKey;
    }

    @Nullable
    private ReservedInstanceSpecData convertRISpecToData(
            @Nonnull CloudTopology<TopologyEntityDTO> cloudTierTopology,
            @Nonnull ReservedInstanceSpec riSpec) {

        final Optional<TopologyEntityDTO> computeTier =
                cloudTierTopology.getEntity(riSpec.getReservedInstanceSpecInfo().getTierId());
        final boolean isConnectedToComputeTier =
                computeTier.map(tier -> EntityType.COMPUTE_TIER_VALUE == tier.getEntityType()
                        && tier.getTypeSpecificInfo().hasComputeTier()).orElse(false);

        if (isConnectedToComputeTier) {
            return ImmutableReservedInstanceSpecData.builder()
                    .computeTier(computeTier.get())
                    .couponsPerInstance(computeTier.get()
                            .getTypeSpecificInfo()
                            .getComputeTier()
                            .getNumCoupons())
                    .reservedInstanceSpec(riSpec)
                    .build();
        } else {
            logger.warn("Unable to find compute tier for RI Spec {}", computeTier.map(TopologyEntityDTO::getDisplayName)
                    .orElse(Long.toString(riSpec.getReservedInstanceSpecInfo().getTierId())));
            return null;
        }
    }

    @NotNull
    private ReservedInstanceSpecKey convertRISpecDataToKey(@Nonnull ReservedInstanceSpecData riSpecData) {

        final ComputeTierInfo computeTierInfo = riSpecData.computeTier()
                .getTypeSpecificInfo()
                .getComputeTier();
        final ReservedInstanceSpecInfo riSpecInfo =
                riSpecData.reservedInstanceSpec().getReservedInstanceSpecInfo();

        final ImmutableReservedInstanceSpecKey.Builder riSpecKeyBuilder = ImmutableReservedInstanceSpecKey.builder()
                .family(computeTierInfo.getFamily())
                .tenancy(riSpecInfo.getTenancy())
                .regionOid(riSpecInfo.getRegionId());

        if (!riSpecInfo.getSizeFlexible()) {
            riSpecKeyBuilder.computerTierOid(riSpecData.computeTier().getOid());
        }

        if (!riSpecInfo.getPlatformFlexible()) {
            riSpecKeyBuilder.osType(riSpecInfo.getOs());
        }

        return riSpecKeyBuilder.build();
    }

    @Nullable
    private ReservedInstanceSpecData resolvePurchasingSpec(
            @Nonnull Collection<ReservedInstanceSpecData> riSpecDataGrouping) {

        return riSpecDataGrouping.stream()
                .filter(riSpecData -> riSpecData.couponsPerInstance() > 0)
                .filter(riSpecData -> filterbyPurchasingConstraints(riSpecData))
                // Sort the RI specs by smallest to largest and then by OID as a tie-breaker.
                // In selecting the first RI spec, we'll recommend the smallest instance type
                // within a family.
                .sorted(Comparator.comparing(ReservedInstanceSpecData::couponsPerInstance)
                        .thenComparing(ReservedInstanceSpecData::reservedInstanceSpecId))
                .findFirst()
                .orElse(null);
    }

    /**
     * Filter method used to short list RISpecs that match the corresponding purchasing constraints.
     *
     * @param riSpecData the RI Spec data for which we want to resolve the constraints.
     * @return true if no constraints found or if applicable to the RISpec data.
     */
    private boolean filterbyPurchasingConstraints(
            @Nonnull final ReservedInstanceSpecData riSpecData) {
        final long regionId = riSpecData.reservedInstanceSpec()
                .getReservedInstanceSpecInfo().getRegionId();
        final ReservedInstanceType constraints = purchaseConstraintsByRegionOid.get(regionId);
        boolean constraintApplicable =  constraints != null
                && isRISpecWithinPurchaseConstraints(constraints,
                        riSpecData.reservedInstanceSpec());
        return constraintApplicable;
    }

    private boolean isRISpecWithinPurchaseConstraints(
            @Nonnull ReservedInstanceType purchaseConstraints,
            @Nonnull ReservedInstanceSpec riSpec) {

        final ReservedInstanceType riSpecType = riSpec.getReservedInstanceSpecInfo().getType();

        return purchaseConstraints.getOfferingClass() == riSpecType.getOfferingClass()
                && purchaseConstraints.getPaymentOption() == riSpecType.getPaymentOption()
                && purchaseConstraints.getTermYears() == riSpecType.getTermYears();
    }
}
