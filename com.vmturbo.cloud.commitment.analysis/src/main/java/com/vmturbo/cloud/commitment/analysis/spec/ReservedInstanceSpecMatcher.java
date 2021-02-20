package com.vmturbo.cloud.commitment.analysis.spec;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.commitment.analysis.demand.ComputeTierDemand;
import com.vmturbo.cloud.commitment.analysis.demand.ScopedCloudTierInfo;
import com.vmturbo.cloud.commitment.analysis.spec.catalog.SpecCatalogKey;
import com.vmturbo.cloud.commitment.analysis.spec.catalog.SpecCatalogKey.OrganizationType;
import com.vmturbo.cloud.common.immutable.HiddenImmutableTupleImplementation;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.HistoricalDemandSelection.CloudTierType;

/**
 * Matchers cloud tier demand to the {@link com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec}
 * to recommend.
 */
public class ReservedInstanceSpecMatcher implements CloudCommitmentSpecMatcher<ReservedInstanceSpecData> {

    private static final Logger logger = LogManager.getLogger();

    private final RISpecPurchaseFilter riSpecPurchaseFilter;

    private final Map<RegionalSpecCatalogKey, Map<VirtualMachineCoverageScope, ReservedInstanceSpecData>> riSpecsByRegionalCatalog =
            new ConcurrentHashMap<>();

    /**
     * Constructs a new reserved instance spec matcher.
     * @param riSpecPurchaseFilter The spec inventory filter, responsible for filtering the inventory
     *                             based on the requested purchase profile.
     */
    public ReservedInstanceSpecMatcher(@Nonnull RISpecPurchaseFilter riSpecPurchaseFilter) {

        this.riSpecPurchaseFilter = Objects.requireNonNull(riSpecPurchaseFilter);
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public Optional<ReservedInstanceSpecData> matchDemandToSpecs(
            @Nonnull ScopedCloudTierInfo scopedCloudTierDemand) {

        if (scopedCloudTierDemand.cloudTierType() == CloudTierType.COMPUTE_TIER) {

            final ComputeTierDemand computeTierDemand =
                    (ComputeTierDemand)scopedCloudTierDemand.cloudTierDemand();

            final SpecCatalogKey catalogKey = SpecCatalogKey.of(
                    scopedCloudTierDemand.billingFamilyId().isPresent()
                            ? OrganizationType.BILLING_FAMILY
                            : OrganizationType.STANDALONE_ACCOUNT,
                    scopedCloudTierDemand.billingFamilyId().orElseGet(scopedCloudTierDemand::accountOid));
            final long regionOid = scopedCloudTierDemand.regionOid();
            final RegionalSpecCatalogKey regionalCatalogKey = RegionalSpecCatalogKey.of(catalogKey, regionOid);

            final Map<VirtualMachineCoverageScope, ReservedInstanceSpecData> riSpecsByCoverageScope =
                    riSpecsByRegionalCatalog.computeIfAbsent(regionalCatalogKey,
                            (key) -> riSpecPurchaseFilter.getAvailableRegionalSpecs(catalogKey, regionOid));

            final VirtualMachineCoverageScope coverageScope = ImmutableVirtualMachineCoverageScope.builder()
                    .regionOid(scopedCloudTierDemand.regionOid())
                    .cloudTierOid(computeTierDemand.cloudTierOid())
                    .osType(computeTierDemand.osType())
                    .tenancy(computeTierDemand.tenancy())
                    .build();

            return Optional.ofNullable(riSpecsByCoverageScope.get(coverageScope));

        } else {
            return Optional.empty();
        }
    }

    /**
     * A factory class for creating {@link ReservedInstanceSpecMatcher} instances.
     */
    public static class ReservedInstanceSpecMatcherFactory {

        /**
         * Constructs a new {@link ReservedInstanceSpecMatcher} instance.
         * @param riSpecPurchaseFilter The {@link RISpecPurchaseFilter} to use in mapping demand to
         *                             the appropriate spec. The filter is responsible for filter the
         *                             RI spec inventory based on the purchase profile constraints.
         * @return The newly created {@link ReservedInstanceSpecMatcher} instance.
         */
        public ReservedInstanceSpecMatcher newMatcher(@Nonnull RISpecPurchaseFilter riSpecPurchaseFilter) {
            return new ReservedInstanceSpecMatcher(riSpecPurchaseFilter);
        }
    }

    /**
     * A key for mapping to a region x {@link SpecCatalogKey}.
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
