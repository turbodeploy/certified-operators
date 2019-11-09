package com.vmturbo.cost.component.util;

import java.util.Collection;
import java.util.Collections;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
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
    private BiMap<Long, Set<Long>> businessAccountToTargetIdMap = Maps.synchronizedBiMap(HashBiMap.create());

    /**
     * Store business accountId -> targetId to Map.
     *
     * @param businessAccountId business account oid.
     * @param targetIds list of targets used in BA.
     */
    public void storeTargetMapping(final long businessAccountId, @Nonnull final Collection<Long> targetIds) {
        businessAccountToTargetIdMap.forcePut(businessAccountId, Sets.newHashSet(targetIds));
    }

    /**
     * Reset map before new we receive new topology data.
     */
    public void resetBusinessAccountToTargetIdMap() {
        businessAccountToTargetIdMap.clear();
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
     * Get only business account id iff they are used in only 1 target id.
     *
     * @param targetId target entity.
     * @return list of businessAccount which are exclusive to the given targetId provided.
     */
    @Nonnull
    public Set<Long> getBusinessAccountsExclusiveToTargetId(Long targetId) {
        return businessAccountToTargetIdMap.inverse().entrySet()
                .stream()
                .filter(setLongEntry -> setLongEntry.getKey().size() == 1 &&
                        setLongEntry.getKey().contains(targetId))
                .map(Entry::getValue)
                .collect(Collectors.toSet());
    }

    @Nonnull
    public Set<Long> getAllBusinessAccounts() {
        return businessAccountToTargetIdMap.keySet();
    }
}
