package com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.inventory;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.commitment.analysis.spec.catalog.CloudCommitmentCatalog.SpecAccountGrouping;
import com.vmturbo.cloud.commitment.analysis.spec.catalog.ReservedInstanceCatalog;
import com.vmturbo.cloud.commitment.analysis.spec.catalog.ReservedInstanceCatalog.ReservedInstanceCatalogFactory;
import com.vmturbo.cloud.commitment.analysis.spec.catalog.SpecCatalogKey;
import com.vmturbo.cloud.commitment.analysis.spec.catalog.SpecCatalogKey.OrganizationType;
import com.vmturbo.cloud.common.immutable.HiddenImmutableTupleImplementation;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.ReservedInstancePurchaseConstraints;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.demand.RIBuyAnalysisContextProvider.ScopedDemandCluster;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.inventory.RISpecPurchaseFilter.ReservedInstanceSpecPurchaseFilterFactory;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.inventory.ReservedInstanceSpecMatcher.ReservedInstanceSpecData;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.inventory.ReservedInstanceSpecMatcher.ReservedInstanceSpecKey;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Responsible for mapping RI specs available through the {@link ReservedInstanceCatalog} to demand.
 */
public class ReservedInstanceCatalogMatcher {

    private final Logger logger = LogManager.getLogger();

    private final RISpecPurchaseFilter riSpecPurchaseFilter;

    private final ReservedInstanceCatalog reservedInstanceCatalog;

    private final CloudTopology<TopologyEntityDTO> cloudTopology;

    private final Map<RegionalSpecCatalogKey, Map<ReservedInstanceSpecKey, ReservedInstanceSpecData>> riSpecsByKeyAndCatalogMap =
            new HashMap<>();

    private ReservedInstanceCatalogMatcher(@Nonnull RISpecPurchaseFilter riSpecPurchaseFilter,
                                           @Nonnull ReservedInstanceCatalog reservedInstanceCatalog,
                                           @Nonnull CloudTopology<TopologyEntityDTO> cloudTopology) {

        this.riSpecPurchaseFilter = Objects.requireNonNull(riSpecPurchaseFilter);
        this.reservedInstanceCatalog = Objects.requireNonNull(reservedInstanceCatalog);
        this.cloudTopology = Objects.requireNonNull(cloudTopology);
    }

    /**
     * Attempts to match the {@code demandCluster} to the available RI specs from the RI spec catalog. The
     * RI spec catalog will first be filtered by {@link RISpecPurchaseFilter} to match demand to only those
     * specs that meet the purchase constraints.
     * @param demandCluster The target demand cluster.
     * @return The {@link ReservedInstanceSpecData}, if one is found.
     */
    @Nonnull
    public Optional<ReservedInstanceSpecData> matchToPurchasingRISpecData(@Nonnull ScopedDemandCluster demandCluster) {

        final OrganizationType organizationType = (demandCluster.billingFamily() != null)
                ? OrganizationType.BILLING_FAMILY
                : OrganizationType.STANDALONE_ACCOUNT;
        final SpecCatalogKey catalogKey = SpecCatalogKey.of(
                organizationType,
                (organizationType == OrganizationType.BILLING_FAMILY)
                        ? demandCluster.billingFamily().group().getId()
                        : demandCluster.account().getOid());
        final RegionalSpecCatalogKey regionalCatalogKey = RegionalSpecCatalogKey.of(catalogKey, demandCluster.region().getOid());

        // Check if we've already resolved the available specs for the catalog key and region
        final Map<ReservedInstanceSpecKey, ReservedInstanceSpecData> riSpecsByKey = riSpecsByKeyAndCatalogMap.computeIfAbsent(
                regionalCatalogKey, this::resolveSpecsForRegionalKey);

        return createSpecKeysFromMatchingCriteria(demandCluster)
                .stream()
                .filter(riSpecsByKey::containsKey)
                .map(riSpecsByKey::get)
                // The order of spec keys returned from createSpecKeysFromMatchingCriteria is
                // deliberate - iterating from the largest scoped RIs to smallest scope. In most
                // (if not all) cases, there will only be one spec key which will have a match
                .findFirst();
    }

    @Nonnull
    private Map<ReservedInstanceSpecKey, ReservedInstanceSpecData> resolveSpecsForRegionalKey(
            @Nonnull RegionalSpecCatalogKey regionalCatalogKey) {

        final List<ReservedInstanceSpec> riSpecsInCatalog =
                reservedInstanceCatalog.getRegionalSpecs(regionalCatalogKey.specCatalogKey(), regionalCatalogKey.regionOid())
                        .stream()
                        .map(SpecAccountGrouping::commitmentSpecs)
                        .flatMap(Set::stream)
                        .collect(ImmutableList.toImmutableList());

        // Group the RI specs by the coverage scope (ReservedInstanceSpecKey)
        final Map<ReservedInstanceSpecKey, Set<ReservedInstanceSpecData>> riSpecsByKey =
                riSpecsInCatalog.stream()
                        ///First convert each RI spec to an RI spec data instance,
                        // containing the associated compute tier of the RI spec.
                        .map(this::convertRISpecToData)
                        .filter(Objects::nonNull)
                        // Group the RI specs by an RI spec key, indicating the scope the RI spec
                        // could cover
                        .collect(Collectors.groupingBy(
                                this::convertRISpecDataToKey,
                                Collectors.toSet()));

        // Collect the set of RI specs which fit within the purchase constraints.
        return riSpecsByKey.entrySet()
                .stream()
                .map(riSpecKeyEntry -> ImmutablePair.of(
                        riSpecKeyEntry.getKey(),
                        riSpecPurchaseFilter.resolvePurchasingSpec(riSpecKeyEntry.getValue())))
                // It's possible none of the RI specs within this key fit the purchase constraints.
                // Therefore, ignore this key if it doesn't have a viable RI spec to recommend.
                .filter(riSpeKeyData -> riSpeKeyData.getRight() != null)
                .collect(ImmutableMap.toImmutableMap(
                        ImmutablePair::getLeft,
                        ImmutablePair::getRight));
    }

    @Nullable
    private ReservedInstanceSpecData convertRISpecToData(
            @Nonnull ReservedInstanceSpec riSpec) {

        final Optional<TopologyEntityDTO> computeTier =
                cloudTopology.getEntity(riSpec.getReservedInstanceSpecInfo().getTierId());
        final boolean isConnectedToComputeTier =
                computeTier.map(tier -> tier.getEntityType() == EntityType.COMPUTE_TIER_VALUE
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

    @Nonnull
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

        // The num coupons check on the compute tier is required if the probe for some reason does not
        // set the family on the tier in which case we want to treat it as instance size inflexible.
        if (!riSpecInfo.getSizeFlexible() || computeTierInfo.getNumCoupons() <= 0) {
            riSpecKeyBuilder.computerTierOid(riSpecData.computeTier().getOid());
        }

        if (!riSpecInfo.getPlatformFlexible()) {
            riSpecKeyBuilder.osType(riSpecInfo.getOs());
        }

        return riSpecKeyBuilder.build();
    }

    @Nonnull
    private List<ReservedInstanceSpecKey> createSpecKeysFromMatchingCriteria(
            @Nonnull ScopedDemandCluster demandCluster) {

        final String computeTierFamily = demandCluster.computeTier().getTypeSpecificInfo()
                .getComputeTier()
                .getFamily();

        return Lists.newArrayList(
                // First, check for platform-flexible and instance size flexible
                ImmutableReservedInstanceSpecKey.builder()
                        .family(computeTierFamily)
                        .regionOid(demandCluster.region().getOid())
                        .tenancy(demandCluster.tenancy())
                        .build(),
                // Second, check for platform-flexible and non-ISF. The order between
                // platform-flexible + non-ISF and ISF + platform-inflexible is not immaterial.
                // Azure RIs are always platform-flexible and AWS RIs are always platform-inflexible.
                ImmutableReservedInstanceSpecKey.builder()
                        .family(computeTierFamily)
                        .regionOid(demandCluster.region().getOid())
                        .tenancy(demandCluster.tenancy())
                        .computerTierOid(demandCluster.computeTier().getOid())
                        .build(),
                // Third, check for platform-inflexible and ISF
                ImmutableReservedInstanceSpecKey.builder()
                        .family(computeTierFamily)
                        .regionOid(demandCluster.region().getOid())
                        .tenancy(demandCluster.tenancy())
                        .osType(demandCluster.platform())
                        .build(),

                // Last, check for platform-inflexible and non-ISF.
                ImmutableReservedInstanceSpecKey.builder()
                        .family(computeTierFamily)
                        .regionOid(demandCluster.region().getOid())
                        .tenancy(demandCluster.tenancy())
                        .computerTierOid(demandCluster.computeTier().getOid())
                        .osType(demandCluster.platform())
                        .build());
    }

    /**
     * A factory class for constructing {@link ReservedInstanceCatalogMatcher} instances.
     */
    public static class ReservedInstanceCatalogMatcherFactory {

        private final ReservedInstanceSpecPurchaseFilterFactory riSpecPurchaseFilterFactory;

        private final ReservedInstanceCatalogFactory reservedInstanceCatalogFactory;

        /**
         * Constructs a new factory instance.
         * @param riSpecPurchaseFilterFactory The {@link ReservedInstanceSpecPurchaseFilterFactory}.
         * @param reservedInstanceCatalogFactory The {@link ReservedInstanceCatalogFactory}.
         */
        public ReservedInstanceCatalogMatcherFactory(@Nonnull ReservedInstanceSpecPurchaseFilterFactory riSpecPurchaseFilterFactory,
                                                     @Nonnull ReservedInstanceCatalogFactory reservedInstanceCatalogFactory) {
            this.riSpecPurchaseFilterFactory = Objects.requireNonNull(riSpecPurchaseFilterFactory);
            this.reservedInstanceCatalogFactory = Objects.requireNonNull(reservedInstanceCatalogFactory);
        }

        /**
         * Constructs a new {@link ReservedInstanceCatalogMatcher} instance.
         * @param cloudTopology The cloud topology, used to construct a new {@link RISpecPurchaseFilter}.
         * @param purchaseConstraintsByProvider The purchase constraints by provider name.
         * @param purchasingAccounts The set of purchasing accounts the analysis is limited to. If the analysis
         *                           is scoped to a set of accounts, recommendations will only be made based
         *                           on RI specs available in those accounts.
         * @return The newly constructed {@link ReservedInstanceCatalogMatcher}.
         */
        @Nonnull
        public ReservedInstanceCatalogMatcher newMatcher(@Nonnull CloudTopology<TopologyEntityDTO> cloudTopology,
                                                         @Nonnull Map<String, ReservedInstancePurchaseConstraints> purchaseConstraintsByProvider,
                                                         @Nonnull Collection<Long> purchasingAccounts) {

            final RISpecPurchaseFilter riSpecPurchaseFilter = riSpecPurchaseFilterFactory.newFilter(
                    cloudTopology, purchaseConstraintsByProvider);
            final ReservedInstanceCatalog reservedInstanceCatalog = reservedInstanceCatalogFactory.createAccountRestrictedCatalog(purchasingAccounts);

            return new ReservedInstanceCatalogMatcher(riSpecPurchaseFilter, reservedInstanceCatalog, cloudTopology);
        }

    }

    /**
     * Represents the intersection of a {@link SpecCatalogKey} and a region.
     */
    @HiddenImmutableTupleImplementation
    @Immutable(builder = false, lazyhash = true)
    interface RegionalSpecCatalogKey {

        SpecCatalogKey specCatalogKey();

        long regionOid();

        static RegionalSpecCatalogKey of(@Nonnull SpecCatalogKey specCatalogKey,
                                         long regionOid) {
            return RegionalSpecCatalogKeyTuple.of(specCatalogKey, regionOid);
        }
    }
}
