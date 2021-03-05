package com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.inventory;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;

import org.immutables.value.Value;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.demand.RIBuyDemandCluster;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.demand.RIBuyRegionalContext;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.demand.calculator.RICoverageRule;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

/**
 * Responsible for matching {@link RIBuyDemandCluster} instances to an RI spec to purchase (based on
 * purchasing constraints) and to the set of RI spec inventory, in order to determine potential matches
 * between the demand cluster and RI inventory.
 *
 * <p>Matches are performed by indexing RI specs by {@link ReservedInstanceSpecKey} in a map, converting
 * demand clusters to a set of potential spec keys and looking for potential matches in the RI spec
 * map.
 */
public class ReservedInstanceSpecMatcher {

    private final SetMultimap<ReservedInstanceSpecKey, Long> riSpecIdsByKey;

    /**
     * Creates a new instance of {@link ReservedInstanceSpecMatcher} with the provided set of
     * RI specs.
     *
     * @param riSpecIdsByKey The set of RI specs to match against in determining potential RI
     *                       inventory matches for a demand cluster.
     */
    public ReservedInstanceSpecMatcher(
            @Nonnull SetMultimap<ReservedInstanceSpecKey, Long> riSpecIdsByKey) {
        this.riSpecIdsByKey = ImmutableSetMultimap.copyOf(Objects.requireNonNull(riSpecIdsByKey));
    }

    public Set<Long> matchDemandContextToRISpecs(@Nonnull RICoverageRule matchingRule,
                                                 @Nonnull RIBuyRegionalContext regionalContext,
                                                 @Nonnull RIBuyDemandCluster demandCluster) {
        final String family = regionalContext.computeTier()
                .getTypeSpecificInfo()
                .getComputeTier()
                .getFamily();

        ImmutableReservedInstanceSpecKey.Builder specKeyBuilder =
                ImmutableReservedInstanceSpecKey.builder()
                        .family(family)
                        .regionOid(regionalContext.regionOid())
                        .tenancy(demandCluster.tenancy());

        if (!matchingRule.platformFlexible().orElse(false)) {
            specKeyBuilder.osType(demandCluster.platform());
        }

        final Set<ReservedInstanceSpecKey> riSpecKeys = new HashSet<>();
        if (matchingRule.sizeFlexible().orElse(true)) {
            riSpecKeys.add(specKeyBuilder.build());
        }
        if (!matchingRule.sizeFlexible().orElse(false)) {
            specKeyBuilder.computerTierOid(demandCluster.computeTierOid());
            riSpecKeys.add(specKeyBuilder.build());
        }
        return riSpecKeys.stream()
                .map(riSpecIdsByKey::get)
                .flatMap(Set::stream)
                .collect(ImmutableSet.toImmutableSet());
    }



    /**
     * A key used to identify the scope of a {@link ReservedInstanceSpec}. This key helps to identify
     * when an RI spec can cover a demand cluster (e.g. when the demand cluster's region OID matches
     * the region OID of an RI spec key for an RI spec).
     */
    @Value.Immutable
    public interface ReservedInstanceSpecKey {

        /**
         * The region OID of the key.
         *
         * @return The region OID of the key.
         */
        long regionOid();

        /**
         * The computer tier OID of the key. This value will be null when the associated set.
         *
         * @return The computer tier OID of the key. This value will be null when the associated set
         * of RI specs are instance size flexible.
         */
        @Nullable
        Long computerTierOid();

        /**
         * The family of the associated compute tier for this key.
         *
         * @return The family of the associated compute tier for this key.
         */
        String family();

        /**
         * The OS of the key. This value will be null when the target set of RI specs.
         *
         * @return The OS of the key. This value will be null when the target set of RI specs
         * are platform-flexible.
         */
        @Nullable
        OSType osType();

        /**
         * The tenancy of the key.
         *
         * @return The tenancy of the key.
         */
        Tenancy tenancy();

    }

    /**
     * An aggregate set of data about a {@link ReservedInstanceSpec} instance.
     */
    @Value.Immutable
    public interface ReservedInstanceSpecData {

        /**
         * The target RI spec. This value is not used in equality or hashing checks.
         *
         * @return The {@link ReservedInstanceSpec}
         */
        @Value.Auxiliary
        ReservedInstanceSpec reservedInstanceSpec();

        /**
         * The compute tier associated with {@link #reservedInstanceSpec()}. This value is not used
         * in equality or hashing checks.
         *
         * @return The compute tier associated with the {@link ReservedInstanceSpec}.
         */
        @Value.Auxiliary
        TopologyEntityDTO computeTier();

        /**
         * The coupons per instance of RI spec. This is derived from the compute tier of the RI spec.
         * This value is not used in equality or hashing checks.
         *
         * @return THe coupon capacity of the compute tier.
         */
        @Value.Auxiliary
        double couponsPerInstance();

        /**
         * The RI spec ID, which is the sole identifier used in equality and hashing checks.
         *
         * @return The RI spec ID.
         */
        @Value.Derived
        default long reservedInstanceSpecId() {
            return reservedInstanceSpec().getId();
        }
    }

}
