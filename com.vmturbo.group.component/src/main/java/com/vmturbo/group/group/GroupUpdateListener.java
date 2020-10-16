package com.vmturbo.group.group;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;

/**
 * Interface to be implemented by classes within the group component that want to listen for group
 * store updates.
 */
public interface GroupUpdateListener {

    /**
     * Called after a user group is created.
     *
     * @param createdGroup The id of the created group.
     * @param groupDefinition The {@link GroupDefinition} for the created group.
     */
    void onUserGroupCreated(long createdGroup, @Nonnull GroupDefinition groupDefinition);

    /**
     * Called after a user group is deleted.
     *
     * @param groupId The id of the deleted group.
     */
    void onUserGroupDeleted(long groupId);

    /**
     * Called after a user group is updated.
     *
     * @param updatedGroup The OID of the updated group.
     * @param newDefinition New {@link GroupDefinition}.
     */
    void onUserGroupUpdated(long updatedGroup, @Nonnull GroupDefinition newDefinition);
}
