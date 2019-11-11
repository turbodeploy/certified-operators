package com.vmturbo.group.setting;

import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredSettingPolicyInfo;
import com.vmturbo.group.common.TargetCollectionUpdate.TargetPolicyUpdate;
import com.vmturbo.group.db.tables.pojos.SettingPolicy;
import com.vmturbo.group.service.StoreOperationException;

/**
 * Setting policy store view for executing within a transaction.
 */
public interface ISettingPolicyStore {
    /**
     * Update the set of {@link SettingPolicy}s discovered by a particular target.
     * The new set of setting policies will completely replace the old, even if the new set is
     * empty.
     *
     * <p>See {@link TargetPolicyUpdate} for details on the update behavior.
     *
     * @param targetId The ID of the target that discovered the setting policies.
     * @param settingPolicyInfos The new set of {@link DiscoveredSettingPolicyInfo}s.
     * @param groupOids a mapping of group name to group oid.
     * @throws StoreOperationException If there is an error interacting with the database.
     */
    void updateTargetSettingPolicies(long targetId,
            @Nonnull List<DiscoveredSettingPolicyInfo> settingPolicyInfos,
            @Nonnull Map<String, Long> groupOids) throws StoreOperationException;

    /**
     * Handle the event of a group being deleted. When a user-created group is removed, we'll remove
     * references to the group being removed from all user-created {@link SettingPolicy} instances.
     *
     * @param deletedGroupId the group that was removed.
     * @return the number of setting policies affected by the change.
     */
    int onGroupDeleted(long deletedGroupId);
}
