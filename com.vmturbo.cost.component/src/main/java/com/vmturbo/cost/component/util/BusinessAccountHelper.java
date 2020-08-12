package com.vmturbo.cost.component.util;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.tuple.ImmutablePair;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.vmturbo.cost.component.reserved.instance.AccountRIMappingStore;
import com.vmturbo.cost.component.reserved.instance.AccountRIMappingStore.AccountRIMappingItem;

/**
 * A helper class for business account. It's currently only used
 * for storing business accountId -> targetId mapping.
 */
public class BusinessAccountHelper {
    // Internal concurrent hash map to store business account id to target id.
    // It will be populated when new topology are sent to Cost component.
    //BA OID --> List of TargetID
    private Map<ImmutablePair<Long, String>, Set<Long>> businessAccountToTargetIdMap =
            Collections.synchronizedMap(Maps.newHashMap());

    /**
     * Store business accountId -> targetId to Map.
     *
     * @param businessAccountId business account oid.
     * @param baDisplayName business account display name.
     * @param targetIds list of targets used in BA.
     */
    public void storeTargetMapping(final long businessAccountId, String baDisplayName,
            @Nonnull final Collection<Long> targetIds) {
        businessAccountToTargetIdMap.put(ImmutablePair.of(businessAccountId, baDisplayName),
                Sets.newHashSet(targetIds));
    }

    /**
     * Resolve targetId per associatedAccountId.
     *
     * @param associatedAccountId business Account id.
     * @return Set of targetID which uses the business Account.
     */
    @Nonnull
    public Set<Long> resolveTargetId(final long associatedAccountId) {
        Set<Long> targetIds = new HashSet<>();
        for (ImmutablePair<Long, String> assocBA : businessAccountToTargetIdMap.keySet()) {
            if (assocBA.left.longValue() == associatedAccountId) {
                targetIds = businessAccountToTargetIdMap.getOrDefault(assocBA, Collections.singleton(0L));
            }
        }

        return targetIds;
    }

    /**
     * List of BAs which does not have any other targets attached to them.
     *
     * @return list of Business Accounts which were removed from {@link #businessAccountToTargetIdMap}.
     */
    @Nonnull
    public Set<ImmutablePair<Long, String>> removeBusinessAccountWithNoTargets() {
        Set<ImmutablePair<Long, String>> baWithNoAttachedTargets = businessAccountToTargetIdMap.entrySet()
                .stream()
                .filter(baToTargets -> baToTargets.getValue().isEmpty())
                .map(Entry::getKey).collect(Collectors.toSet());
        businessAccountToTargetIdMap.keySet().removeAll(baWithNoAttachedTargets);
        return baWithNoAttachedTargets;
    }

    /**
     * Get only business account id iff they are used in only 1 target id.
     *
     * @param targetId target entity to be removed from map.
     */
    public void removeTargetForBusinessAccount(final long targetId) {
        businessAccountToTargetIdMap.forEach((key, value) -> value.remove(targetId));
    }

    @Nonnull
    public Set<ImmutablePair<Long, String>> getAllBusinessAccounts() {
        return businessAccountToTargetIdMap.keySet();
    }

    /**

     * Returns the sum of usage from all undiscovered accounts.
     *
     * @param baOids the list of undiscovered ba oids for which we want the usage.
     * @param accountRIMappingStore An instance of {@link AccountRIMappingStore}
     * @return Map of RI id and corresponding usage from undiscovered accounts.
     */
    public static Map<Long, Double> getUndiscoveredAccountUsageForRI(
            final List<Long> baOids, AccountRIMappingStore accountRIMappingStore) {
        final Map<Long, List<AccountRIMappingItem>> usedCouponInUndiscAccounts =
                accountRIMappingStore.getAccountRICoverageMappings(baOids);
        Map<Long, Double> undiscoveredAccountRIUsage =
                usedCouponInUndiscAccounts.values().stream()
                        .flatMap(List::stream)
                        .collect(Collectors.toMap(AccountRIMappingItem::getReservedInstanceId,
                                AccountRIMappingItem::getUsedCoupons,
                                (oldValue, newValue) -> oldValue + newValue));
        return undiscoveredAccountRIUsage;
    }
}
