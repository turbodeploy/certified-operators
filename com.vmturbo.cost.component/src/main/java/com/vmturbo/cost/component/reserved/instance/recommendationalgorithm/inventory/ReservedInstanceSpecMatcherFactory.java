package com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.inventory;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.cloud.common.topology.ComputeTierFamilyResolver;
import com.vmturbo.components.common.utils.FuzzyDouble;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceSpecStore;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.inventory.ReservedInstanceSpecMatcher.ReservedInstanceSpecKey;

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

    public ReservedInstanceSpecMatcher createRegionalMatcher(@Nonnull ComputeTierFamilyResolver computeTierFamilyResolver,
                                                             long regionOid) {

        final SetMultimap<ReservedInstanceSpecKey, Long> riSpecsByKey = riSpecStore.getAllRISpecsForRegion(regionOid)
                .stream()
                .collect(ImmutableSetMultimap.toImmutableSetMultimap(
                        riSpec -> convertRISpecDataToKey(computeTierFamilyResolver, riSpec),
                        ReservedInstanceSpec::getId));

        return new ReservedInstanceSpecMatcher(riSpecsByKey);
    }

    @Nonnull
    private ReservedInstanceSpecKey convertRISpecDataToKey(@Nonnull ComputeTierFamilyResolver computeTierFamilyResolver,
                                                           @Nonnull ReservedInstanceSpec reservedInstanceSpec) {

        final ReservedInstanceSpecInfo riSpecInfo =
                reservedInstanceSpec.getReservedInstanceSpecInfo();
        final String family = computeTierFamilyResolver.getCoverageFamily(riSpecInfo.getTierId())
                // If we can't resolve the string, just use the tier ID to isolate the spec
                .orElse(String.valueOf(riSpecInfo.getTierId()));
        final FuzzyDouble numCoupons = FuzzyDouble.newFuzzy(
                computeTierFamilyResolver.getNumCoupons(riSpecInfo.getTierId())
                        .orElse(0.0)) ;

        final ImmutableReservedInstanceSpecKey.Builder riSpecKeyBuilder = ImmutableReservedInstanceSpecKey.builder()
                .family(family)
                .tenancy(riSpecInfo.getTenancy())
                .regionOid(riSpecInfo.getRegionId());

        // The num coupons check on the compute tier is required if the probe for some reason does not
        // set the family on the tier in which case we want to treat it as instance size inflexible.
        if (!riSpecInfo.getSizeFlexible() || !numCoupons.isPositive()) {
            riSpecKeyBuilder.computerTierOid(riSpecInfo.getTierId());
        }

        if (!riSpecInfo.getPlatformFlexible()) {
            riSpecKeyBuilder.osType(riSpecInfo.getOs());
        }

        return riSpecKeyBuilder.build();
    }
}
