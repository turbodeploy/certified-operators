package com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.inventory;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.immutables.value.Value;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.demand.AccountGroupingIdentifier;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.demand.AccountGroupingIdentifier.AccountGroupingType;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.demand.RIBuyDemandCluster;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.demand.RIBuyRegionalContext;
import com.vmturbo.reserved.instance.coverage.allocator.rules.RICoverageRuleConfig;
import com.vmturbo.reserved.instance.coverage.allocator.utils.ReservedInstanceHelper;

/**
 * This class is responsible for matching the RI bought inventory to demand clusters during
 * RI buy analysis, in order to calculate the uncovered demand for a potential RI buy recommendation.
 * It generally performs matching operations through two steps. First, it matches part of the criteria
 * of the demand clusters to RI specs through {@link ReservedInstanceSpecMatcher}. Second, it matches
 * the remaining RI matching criteria of demand clusters (account and zone) to the RI inventory (which
 * is filtered based on matching the target RI specs) through a map of RI scope keys to the set of
 * RIs matching the scope.
 */
public class ReservedInstanceInventoryMatcher {

    private final ReservedInstanceSpecMatcher riSpecMatcher;

    private final Map<RIBoughtKey, Set<ReservedInstanceBought>> riBoughtInstancesByKey;

    /**
     * Create a {@link ReservedInstanceInventoryMatcher} with the specified set of
     * {@link ReservedInstanceBought} instances and {@link ReservedInstanceSpecMatcher}.
     *
     * @param riSpecMatcher The {@link ReservedInstanceSpecMatcher} to use to match demand clusters
     *                      to RI specs.
     * @param riBoughtInstances The RI inventory in scope of this matcher
     * @param riOidToAccountGroupingId The account grouping IDs, indexed by the RI ID.
     */
    public ReservedInstanceInventoryMatcher(@Nonnull ReservedInstanceSpecMatcher riSpecMatcher,
                                            @Nonnull Collection<ReservedInstanceBought> riBoughtInstances,
                                            @Nonnull Map<Long, AccountGroupingIdentifier> riOidToAccountGroupingId) {
        this.riSpecMatcher = Objects.requireNonNull(riSpecMatcher);
        this.riBoughtInstancesByKey = riBoughtInstances.stream()
                .collect(Collectors.groupingBy(
                        ri -> ReservedInstanceInventoryMatcher.createKeyFromRIBought(riOidToAccountGroupingId, ri),
                        ReservedInstanceHelper.toRIBoughtSet()));

    }

    /**
     * Matches the specified {@code demandCluster} to a set of {@link ReservedInstanceBought} from
     * inventory.
     *
     * @param matchingRule The matching rule config, which specifies which RIs are in scope for this
     *                     matching analysis. The config will indicate whether to match based on
     *                     zone, instance size flexibility, etc.
     * @param regionalContext The regional context of the demand cluster.
     * @param demandCluster The target demand cluster.
     * @return A set of RI instances which could cover the target demand cluster.
     */
    public Set<ReservedInstanceBought> matchToDemandContext(
            @Nonnull RICoverageRuleConfig matchingRule,
            @Nonnull RIBuyRegionalContext regionalContext,
            @Nonnull RIBuyDemandCluster demandCluster) {

        Set<Long> matchingRISpecs =
                riSpecMatcher.matchDemandContextToRISpecs(
                        matchingRule,
                        regionalContext,
                        demandCluster);

        Set<RIBoughtKey> riBoughtKeys = matchingRISpecs.stream()
                .map(riSpecId -> {
                    final ImmutableRIBoughtKey.Builder keyBuilder = ImmutableRIBoughtKey.builder()
                            .riSpecId(riSpecId);

                    final AccountGroupingType accountGroupingType =
                            regionalContext.accountGroupingId().groupingType();

                    // If the rule is looking for shared scope and this cluster is grouped by a
                    // billing family, look for RIs that match the billing family ID.
                    if (matchingRule.isSharedScope() &&
                            accountGroupingType == AccountGroupingType.BILLING_FAMILY) {
                        keyBuilder.billingFamilyId(regionalContext.accountGroupingId().id());
                    } else {
                        // If the matching rule is either account scoped (not shared) or the
                        // cluster is a standalone account, lookup RIs by the account OID.
                        keyBuilder.accountOid(demandCluster.accountOid());
                    }

                    // If this is a zone-scoped rule, the demand context is assumed to be
                    // within a zone (it would not make sense to have zone scoped rules for Azure)
                    if (matchingRule.isZoneScoped()) {
                        keyBuilder.zoneOid(demandCluster.regionOrZoneOid());
                    }

                    return keyBuilder.build();

                }).collect(Collectors.toSet());

        return riBoughtKeys.stream()
                .map(key -> riBoughtInstancesByKey.getOrDefault(key, Collections.emptySet()))
                .flatMap(Set::stream)
                .collect(ReservedInstanceHelper.toRIBoughtSet());
    }

    private static RIBoughtKey createKeyFromRIBought(
            @Nonnull Map<Long, AccountGroupingIdentifier> riOidToAccountGroupingId,
            @Nonnull ReservedInstanceBought riBought) {
        final ReservedInstanceBoughtInfo riBoughtInfo = riBought.getReservedInstanceBoughtInfo();


        final ImmutableRIBoughtKey.Builder keyBuilder = ImmutableRIBoughtKey.builder()
                .riSpecId(riBoughtInfo.getReservedInstanceSpec())
                .zoneOid(riBoughtInfo.getAvailabilityZoneId());

        final AccountGroupingIdentifier accountGroupingId =
                riOidToAccountGroupingId.get(riBought.getId());
        if (accountGroupingId != null) {
            if (accountGroupingId.groupingType() == AccountGroupingType.BILLING_FAMILY &&
                    riBoughtInfo.getReservedInstanceScopeInfo().getShared()) {
                keyBuilder.billingFamilyId(accountGroupingId.id());
            } else {
                keyBuilder.accountOid(accountGroupingId.id());
            }
        } else {
            throw new IllegalStateException();
        }

        return keyBuilder.build();
    }

    @Value.Immutable
    interface RIBoughtKey {

        long riSpecId();

        @Nullable
        Long accountOid();

        @Nullable
        Long billingFamilyId();

        @Nullable
        Long zoneOid();
    }
}
