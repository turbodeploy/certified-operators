package com.vmturbo.cost.component.utils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A helper class for business account. It's currently only used
 * for storing business accountId -> targetId mapping.
 */
public class BusinessAccountHelper {
    // Internal concurrent hash map to store business account id to target id.
    // It will be populated when new topology are sent to Cost component.
    private Map<Long, Long> businessAccountToTargetMap = new ConcurrentHashMap<>();

    /**
     * Store business accountId -> targetId to Map
     */
    public void storeTargetMapping(final long businessAccountId, final long targetId) {
        businessAccountToTargetMap.putIfAbsent(businessAccountId, targetId);
    }

    /**
     * Resolve targetId per associatedAccountId
     * @param associatedAccountId
     */
    public Long resolveTargetId(final long associatedAccountId) {
        // if we don't have the targetId for this account id, return 0 to avoid null
        return businessAccountToTargetMap.getOrDefault(associatedAccountId, 0l);
    }
}
