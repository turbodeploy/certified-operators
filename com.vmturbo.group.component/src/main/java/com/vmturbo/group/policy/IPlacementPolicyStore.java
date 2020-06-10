package com.vmturbo.group.policy;

import java.util.Collection;
import java.util.Map;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.group.PolicyDTO.Policy;
import com.vmturbo.group.service.StoreOperationException;

/**
 * Placement policy store view for executing within a transaction.
 */
public interface IPlacementPolicyStore {

    /**
     * Returns discovered policies stored in this policy store.
     *
     * @return map of policy name -> policy Id grouped by target id
     * @throws StoreOperationException if store failed to operate
     */
    @Nonnull
    Map<Long, Map<String, Long>> getDiscoveredPolicies() throws StoreOperationException;

    /**
     * Deletes the specified policies in the store.
     *
     * @param policiesToDelete policies OIDs to delete.
     * @return number of removed policies.
     */
    int deletePolicies(@Nonnull Collection<Long> policiesToDelete);

    /**
     * Creates new policies in DAO.
     *
     * @param policies policies to create
     * @return number of policies created
     */
    int createPolicies(@Nonnull Collection<Policy> policies);

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
