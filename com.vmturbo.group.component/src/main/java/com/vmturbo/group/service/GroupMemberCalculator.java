package com.vmturbo.group.service;

import java.util.Collection;
import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.group.group.IGroupStore;

/**
 * Responsible for calculating members of a group to serve the {@link GroupRpcService}.
 */
public interface GroupMemberCalculator {

    /**
     * Get the members that would be in a particular {@link GroupDefinition}.
     * Use this method when either:
     * 1) You already have the {@link GroupDefinition} in memory, and want to avoid looking it up
     *    in the database again.
     * 2) The group does not exist with an ID yet.
     *
     * @param groupStore The {@link IGroupStore}.
     * @param groupDefinition The {@link GroupDefinition}.
     * @param expandNestedGroups True if nested groups should be expanded.
     * @return The set of members.
     * @throws StoreOperationException If there is an error interacting with the {@link IGroupStore}.
     */
    @Nonnull
    Set<Long> getGroupMembers(@Nonnull IGroupStore groupStore,
                              @Nonnull GroupDefinition groupDefinition,
                              boolean expandNestedGroups)
        throws StoreOperationException;

    /**
     * Get the members in a set of groups.
     *
     * @param groupStore The {@link IGroupStore}.
     * @param groupIds A collection of group ids.
     * @param expandNestedGroups True if nested groups should be expanded.
     * @return The set of members - the union of members in all the input groups.
     * @throws StoreOperationException If there is an error interacting with the {@link IGroupStore}.
     */
    @Nonnull
    Set<Long> getGroupMembers(@Nonnull IGroupStore groupStore,
                              @Nonnull Collection<Long> groupIds,
                              boolean expandNestedGroups)
        throws StoreOperationException;
}
