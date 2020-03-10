package com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.inventory;

import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceSpecStore;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.ReservedInstancePurchaseConstraints;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.inventory.ReservedInstanceSpecMatcher.ReservedInstanceSpecData;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.inventory.ReservedInstanceSpecMatcher.ReservedInstanceSpecKey;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType;

/**
 * A factory class for creating {@link ReservedInstanceSpecMatcher} instances. This factory takes on
 * responsibility of indexing RI specs by {@link ReservedInstanceSpecKey}.
 */
public class ReservedInstanceSpecMatcherFactory {

    private static final Logger logger = LogManager.getLogger();

    private final ReservedInstanceSpecStore riSpecStore;

    /**
     * Create a new factory instance.
     *
     * @param riSpecStore The RI spec store, used to populate created {@link ReservedInstanceSpecMatcher}
     *                    instances.
     */
    public ReservedInstanceSpecMatcherFactory(@Nonnull ReservedInstanceSpecStore riSpecStore) {
        this.riSpecStore = Objects.requireNonNull(riSpecStore);
    }

    /**
     * Creates a {@link ReservedInstanceSpecMatcher}, scoped to the specified {@code regionOid}. The
     * returned matcher will only be able to match demand clusters to RI specs within the target region.
     * The benefit in limiting the scope of the matcher is that it limits the DB I/O and memory
     * impact in loading the requisite RI specs.
     *
     * @param cloudTopology The cloud topology, used by the compute tier for each RI spec.
     * @param purchaseConstraints The purchasing constraints of the analysis mapped to each
     *                            service provider display name, used to scope the
     *                            potential RI specs for purchase recommendations
     * @param regionOid The target region OID
     * @return A newly created {@link ReservedInstanceSpecMatcher}, scoped to the target region
     */
    public ReservedInstanceSpecMatcher createRegionalMatcher(
            @Nonnull CloudTopology<TopologyEntityDTO> cloudTopology,
            @Nonnull Map<String, ReservedInstancePurchaseConstraints> purchaseConstraints,
            long regionOid) {

        return  createMatcherForRISpecs(
                cloudTopology,
                purchaseConstraints,
                riSpecStore.getAllRISpecsForRegion(regionOid));

    }

    private ReservedInstanceSpecMatcher createMatcherForRISpecs(
            @Nonnull CloudTopology<TopologyEntityDTO> cloudTopology,
            @Nonnull Map<String, ReservedInstancePurchaseConstraints> purchaseConstraints,
            @Nonnull Collection<ReservedInstanceSpec> riSpecs) {

        // First convert each RI spec to an RI spec data instance (containing the associated compute
        // tier).
        final Map<ReservedInstanceSpecKey, Set<ReservedInstanceSpecData>> riSpecsByKey =
                riSpecs.stream()
                        ///First convert each RI spec to an RI spec data instance,
                        // containing the associated compute tier of the RI spec.
                        .map(riSpec -> convertRISpecToData(cloudTopology, riSpec)).filter(Objects::nonNull)
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
                        resolvePurchasingSpec(riSpecKeyEntry.getValue(), purchaseConstraints, cloudTopology)))
                // It's possible none of the RI specs within this key fit the purchase constraints.
                // Therefore, ignore this key if it doesn't have a viable RI spec to recommend.
                .filter(riSpeKeyData -> riSpeKeyData.getRight() != null)
                .collect(ImmutableMap.toImmutableMap(
                        ImmutablePair::getLeft,
                        ImmutablePair::getRight));

        // Normalize the full set of RI specs to only the RI spec ID, given this is all that is required
        // after the key has been created to match RI inventory to demand clusters.
        final Map<ReservedInstanceSpecKey, Set<Long>> riSpecIdsByKey = riSpecsByKey.entrySet()
                .stream()
                .collect(ImmutableMap.toImmutableMap(
                        Map.Entry::getKey,
                        e -> FluentIterable.from(e.getValue())
                                .transform(ReservedInstanceSpecData::reservedInstanceSpecId)
                                .toSet()));

        return new ReservedInstanceSpecMatcher(riSpecsToPurchaseByKey, riSpecIdsByKey);
    }

    private boolean isRISpecWithinPurchaseConstraints(
            @Nonnull ReservedInstancePurchaseConstraints purchaseConstraints,
            @Nonnull ReservedInstanceSpec riSpec) {

        final ReservedInstanceType riSpecType = riSpec.getReservedInstanceSpecInfo().getType();

        return purchaseConstraints.getOfferingClass() == riSpecType.getOfferingClass() &&
                purchaseConstraints.getPaymentOption() == riSpecType.getPaymentOption() &&
                purchaseConstraints.getTermInYears() == riSpecType.getTermYears();
    }

    @Nullable
    private ReservedInstanceSpecData convertRISpecToData(
            @Nonnull CloudTopology<TopologyEntityDTO> cloudTopology,
            @Nonnull ReservedInstanceSpec riSpec) {

        final Optional<TopologyEntityDTO> computeTier =
                cloudTopology.getEntity(riSpec.getReservedInstanceSpecInfo().getTierId());
        final boolean isConnectedToComputeTier =
                computeTier.map(tier -> tier.getEntityType() == EntityType.COMPUTE_TIER_VALUE &&
                        tier.getTypeSpecificInfo().hasComputeTier()).orElse(false);

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
            @Nonnull Collection<ReservedInstanceSpecData> riSpecDataGrouping,
            @Nonnull final Map<String, ReservedInstancePurchaseConstraints> purchaseConstraintMap,
            @Nonnull final CloudTopology<TopologyEntityDTO> cloudTopology) {

        return riSpecDataGrouping.stream()
                .filter(riSpecData -> riSpecData.couponsPerInstance() > 0)
                .filter(riSpecData -> filterbyPurchasingConstraints(riSpecData, purchaseConstraintMap, cloudTopology))
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
     * @param purchaseConstraintMap - Map of all constraints by each service provider.
     * @param cloudTopology - handle to the cloud topology.
     * @return true if no constraints found or if applicable to the RISpec data.
     */
    private boolean filterbyPurchasingConstraints(
            final ReservedInstanceSpecData riSpecData,
            final Map<String, ReservedInstancePurchaseConstraints> purchaseConstraintMap,
            final CloudTopology<TopologyEntityDTO> cloudTopology) {
        final long regionId = riSpecData.reservedInstanceSpec()
                                .getReservedInstanceSpecInfo().getRegionId();
        final Optional<TopologyEntityDTO> provider = cloudTopology.getServiceProvider(regionId);
        boolean constraintApplicable = false;
        if (provider.isPresent()) {
            String providerName = provider.get().getDisplayName();
            ReservedInstancePurchaseConstraints constraints =
                    purchaseConstraintMap.get(providerName.toUpperCase());
            constraintApplicable =  constraints != null &&
                    isRISpecWithinPurchaseConstraints(
                            constraints,
                            riSpecData.reservedInstanceSpec());
        } else {
            logger.warn("Service Provider not found for region {}", regionId);
        }
        return constraintApplicable;
    }
}
