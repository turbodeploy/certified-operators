package com.vmturbo.group.policy;

import java.util.Collection;
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
     * This method will attempt to remove any placement policies associated with a specified
     * group that is undergoing deletion.
     * This is required as placement policy for a removed group makes no sense and is invalid.
     *
     * @param groupIds ids of the group deleted.
     * @throws StoreOperationException If there is an error deleting the policies.
     */
    void deletePoliciesForGroupBeingRemoved(@Nonnull Collection<Long> groupIds)
            throws StoreOperationException;
}
