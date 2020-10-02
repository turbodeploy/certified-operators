package com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.inventory;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import org.immutables.value.Value;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.demand.RIBuyDemandCluster;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.demand.RIBuyRegionalContext;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;
import com.vmturbo.reserved.instance.coverage.allocator.rules.RICoverageRuleConfig;

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

    private final Map<ReservedInstanceSpecKey, ReservedInstanceSpecData> purchasingRISpecDataByKey;
    private final Map<ReservedInstanceSpecKey, Set<Long>> riSpecIdsByKey;


    /**
     * Creates a new instance of {@link ReservedInstanceSpecMatcher} with the provided set of
     * RI specs.
     *
     * @param purchasingRISpecDataByKey The set of RI specs to match against in making purchasing
     *                                  recommendations.
     * @param riSpecIdsByKey The set of RI specs to match against in determining potential RI
     *                       inventory matches for a demand cluster.
     */
    public ReservedInstanceSpecMatcher(
            @Nonnull Map<ReservedInstanceSpecKey, ReservedInstanceSpecData> purchasingRISpecDataByKey,
            @Nonnull Map<ReservedInstanceSpecKey, Set<Long>> riSpecIdsByKey) {

        this.purchasingRISpecDataByKey = Objects.requireNonNull(purchasingRISpecDataByKey);
        this.riSpecIdsByKey = Objects.requireNonNull(riSpecIdsByKey);
    }

    /**
     * Matches the provided set of criteria to a potential RI spec to recommend in making
     * purchasing decisions. The returned RI spec will be based on the set of purchasing constraints
     * for the RI buy analysis.
     *
     * <p>Given there may be multiple RI specs at different scopes that match the provided criteria, the
     * matching analysis will work from the largest scope (i.e. platform-flexible and instance size flexible
     * specs) to the smallest scope, returning the first match found.
     *
     * @param region The target region.
     * @param computeTier The target compute tier.
     * @param platform The target OS.
     * @param tenancy The target tennacy.
     * @return An {@link ReservedInstanceSpecData} instance, if a match is found. If no match is found
     * {@link Optional#empty()} will be returned.
     */
    @Nonnull
    public Optional<ReservedInstanceSpecData> matchToPurchasingRISpecData(
            @Nonnull TopologyEntityDTO region,
            @Nonnull TopologyEntityDTO computeTier,
            @Nonnull OSType platform,
            @Nonnull Tenancy tenancy) {

        return createSpecKeysFromMatchingCriteria(region, computeTier, platform, tenancy)
                .stream()
                .filter(purchasingRISpecDataByKey::containsKey)
                .map(purchasingRISpecDataByKey::get)
                // The order of spec keys returned from createSpecKeysFromMatchingCriteria is
                // deliberate - iterating from the largest scoped RIs to smallest scope. In most
                // (if not all) cases, there will only be one spec key which will have a match
                .findFirst();
    }


    public Set<Long> matchDemandContextToRISpecs(@Nonnull RICoverageRuleConfig matchingRule,
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

        if (!matchingRule.isPlatformFlexible().orElse(false)) {
            specKeyBuilder.osType(demandCluster.platform());
        }

        final Set<ReservedInstanceSpecKey> riSpecKeys = new HashSet<>();
        if (matchingRule.isSizeFlexible().orElse(true)) {
            riSpecKeys.add(specKeyBuilder.build());
        }
        if (!matchingRule.isSizeFlexible().orElse(false)) {
            specKeyBuilder.computerTierOid(demandCluster.computeTierOid());
            riSpecKeys.add(specKeyBuilder.build());
        }
        return riSpecKeys.stream()
                .map(key -> riSpecIdsByKey.getOrDefault(key, Collections.emptySet()))
                .flatMap(Set::stream)
                .collect(ImmutableSet.toImmutableSet());
    }

    @Nonnull
    private List<ReservedInstanceSpecKey> createSpecKeysFromMatchingCriteria(
            @Nonnull TopologyEntityDTO region,
            @Nonnull TopologyEntityDTO computeTier,
            @Nonnull OSType platform,
            @Nonnull Tenancy tenancy) {

        final String computeTierFamily = computeTier.getTypeSpecificInfo()
                .getComputeTier()
                .getFamily();

        return Lists.newArrayList(
                // First, check for platform-flexible and instance size flexible
                ImmutableReservedInstanceSpecKey.builder()
                        .family(computeTierFamily)
                        .regionOid(region.getOid())
                        .tenancy(tenancy)
                        .build(),
                // Second, check for platform-flexible and non-ISF. The order between
                // platform-flexible + non-ISF and ISF + platform-inflexible is not immaterial.
                // Azure RIs are always platform-flexible and AWS RIs are always platform-inflexible.
                ImmutableReservedInstanceSpecKey.builder()
                        .family(computeTierFamily)
                        .regionOid(region.getOid())
                        .tenancy(tenancy)
                        .computerTierOid(computeTier.getOid())
                        .build(),
                // Third, check for platform-inflexible and ISF
                ImmutableReservedInstanceSpecKey.builder()
                        .family(computeTierFamily)
                        .regionOid(region.getOid())
                        .tenancy(tenancy)
                        .osType(platform)
                        .build(),

                // Last, check for platform-inflexible and non-ISF.
                ImmutableReservedInstanceSpecKey.builder()
                        .family(computeTierFamily)
                        .regionOid(region.getOid())
                        .tenancy(tenancy)
                        .computerTierOid(computeTier.getOid())
                        .osType(platform)
                        .build());
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
        int couponsPerInstance();

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
