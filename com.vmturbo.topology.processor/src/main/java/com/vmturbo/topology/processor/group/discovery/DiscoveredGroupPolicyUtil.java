package com.vmturbo.topology.processor.group.discovery;

import javax.annotation.Nonnull;

import com.vmturbo.platform.common.dto.CommonDTO;

/**
 * A utility class for discovered group and policy.
 */
public class DiscoveredGroupPolicyUtil {
    /**
     * Give a {@link CommonDTO.GroupDTO}, and extract its name properly. This function will be used
     * by both discovered group and discovered policy. Because for discovered group and discovered
     * policy, they should have exactly same logic to create group name if they have related.
     * Otherwise, discovered policy will not been created.
     *
     * @param sdkDTO a {@link CommonDTO.GroupDTO}
     * @return a group name.
     */
    @Nonnull
    public static String extractGroupName(@Nonnull final CommonDTO.GroupDTO sdkDTO) {
        if (sdkDTO.hasDisplayName()) {
            return sdkDTO.getDisplayName();
        } else if (sdkDTO.hasGroupName()) {
            return sdkDTO.getGroupName();
        } else if (sdkDTO.hasConstraintInfo() && sdkDTO.getConstraintInfo().hasConstraintName()) {
            return sdkDTO.getConstraintInfo().getConstraintName();
        } else {
            throw new IllegalArgumentException(
                    "One of displayName, groupName or constraintName must be present in groupDTO");
        }
    }
}
