package com.vmturbo.cost.component.util;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * A helper class for business account. It's currently only used
 * for storing business accountId -> targetId mapping.
 */
public class BusinessAccountHelper {
    // Internal concurrent hash map to store business account id to target id.
    // It will be populated when new topology are sent to Cost component.
    //BA OID --> List of TargetID
    private Map<Long, Set<Long>> businessAccountToTargetIdMap = Collections.synchronizedMap(Maps.newHashMap());

    /**
     * Store business accountId -> targetId to Map.
     *
     * @param businessAccountId business account oid.
     * @param targetIds list of targets used in BA.
     */
    public void storeTargetMapping(final long businessAccountId, @Nonnull final Collection<Long> targetIds) {
        businessAccountToTargetIdMap.put(businessAccountId, Sets.newHashSet(targetIds));
    }

    /**
     * Resolve targetId per associatedAccountId.
     *
     * @param associatedAccountId business Account id.
     * @return Set of targetID which uses the business Account.
     */
    @Nonnull
    public Set<Long> resolveTargetId(final long associatedAccountId) {
        return businessAccountToTargetIdMap.getOrDefault(associatedAccountId, Collections.singleton(0L));
    }

    /**
     * List of BAs which does not have any other targets attached to them.
     *
     * @return list of businessAccount which were removed from {@link #businessAccountToTargetIdMap}.
     */
    @Nonnull
    public Set<Long> removeBusinessAccountWithNoTargets() {
        Set<Long> baWithNoAttachedTargets = businessAccountToTargetIdMap.entrySet().stream().filter(baToTargets ->
                baToTargets.getValue().isEmpty()).map(Entry::getKey).collect(Collectors.toSet());
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
    public Set<Long> getAllBusinessAccounts() {
        return businessAccountToTargetIdMap.keySet();
    }
}
