package com.vmturbo.group.policy;

import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredPolicyInfo;
import com.vmturbo.group.common.TargetCollectionUpdate.TargetPolicyUpdate;
import com.vmturbo.group.service.StoreOperationException;

/**
 * Placement policy store view for executing within a transaction.
 */
public interface IPlacementPolicyStore {
    /**
     * Update the set of policies discovered by a particular target.
     * The new set of policies will completely replace the old, even if the new set is empty.
     *
     * <p>See {@link TargetPolicyUpdate} for details on the update behavior.
     *
     * @param targetId The ID of the target that discovered the policies.
     * @param policyInfos The new set of {@link DiscoveredPolicyInfo}s.
     * @param groupOids A mapping from group display names to group OIDs. We need this
     *         mapping
     *         because discovered policies reference groups by display name.
     * @throws StoreOperationException If there is an error interacting with the database.
     */
    void updateTargetPolicies(long targetId,
            @Nonnull List<DiscoveredPolicyInfo> policyInfos,
            @Nonnull Map<String, Long> groupOids) throws StoreOperationException;

    /**
     * This method will attempt to remove any user-created policies associated with a specified
     * group that is undergoing deletion.
     * It will also check for discovered policies related to the group. Because discovered
     * policies are "immutable" (they are only modified during the discovered group/policy/settings
     * upload process rather than by user activity), we cannot delete them here. Instead, if we find
     * a discovered policy related to the group in question, we will follow these rules:
     * <li>If the group is a user group, we'll throw an exception instead of deleting any policies.
     * This is because the user group is being used in a discovered policy, and the group can't be
     * deleted without creating an invalid policy.</li>
     * <li>If the group is a discovered group, we won't do anything. Discovered groups and policies
     * are both handled elsewhere (See {@link com.vmturbo.group.service.GroupRpcService#storeDiscoveredGroupsPoliciesSettings})
     * and can be ignored in this method.</li>
     *
     * @param groupId id of the group deleted.
     * @throws StoreOperationException If there is an error deleting the policies.
     */
    void deletePoliciesForGroupBeingRemoved(long groupId) throws StoreOperationException;
}
