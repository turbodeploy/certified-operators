package com.vmturbo.cloud.commitment.analysis.spec;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.cloud.commitment.analysis.demand.ComputeTierDemand;
import com.vmturbo.cloud.commitment.analysis.demand.ScopedCloudTierInfo;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.HistoricalDemandSelection.CloudTierType;

/**
 * Matchers cloud tier demand to the {@link com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec}
 * to recommend.
 */
public class ReservedInstanceSpecMatcher implements CloudCommitmentSpecMatcher<ReservedInstanceSpecData> {

    private static final Logger logger = LogManager.getLogger();

    private final RISpecPurchaseFilter riSpecPurchaseFilter;

    private final ConcurrentMap<Long, Map<VirtualMachineCoverageScope, ReservedInstanceSpecData>> riSpecsByScopeByRegion =
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

            final long regionOid = scopedCloudTierDemand.regionOid();
            final Map<VirtualMachineCoverageScope, ReservedInstanceSpecData> riSpecsByCoverageScope =
                    riSpecsByScopeByRegion.computeIfAbsent(regionOid, riSpecPurchaseFilter::getSpecsByCoverageScope);

            final VirtualMachineCoverageScope coverageScope = ImmutableVirtualMachineCoverageScope.builder()
                    .regionOid(regionOid)
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
}
