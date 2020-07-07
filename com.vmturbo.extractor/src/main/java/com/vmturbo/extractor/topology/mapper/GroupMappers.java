package com.vmturbo.extractor.topology.mapper;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;

/**
 * Mappers pertaining to group definitions.
 */
public class GroupMappers {
    private GroupMappers() {
    }

    /** mapped name for REGULAR group type. */
    public static final String REGULAR_GROUP_TYPE_NAME = "GROUP";
    /** mapped name for RESOURCE group type. */
    public static final String RESOURCE_GROUP_TYPE_NAME = "RESOURCE_GROUP";
    /** mapped name for COMPUTE_HOST_CLUSTER group type. */
    public static final String COMPUTE_HOST_CLUSTER_TYPE_NAME = "COMPUTE_CLUSTER";
    /** mapped name for COMPUTE_VIRTUAL_MACHINE_CLUSTER group type. */
    public static final String COMPUTE_VIRTUAL_MACHINE_CLUSTER_TYPE_NAME = "K8S_CLUSTER";

    private static final ImmutableMap<GroupType, String> GROUP_TYPE_TO_NAME = ImmutableMap.of(
            GroupType.REGULAR, REGULAR_GROUP_TYPE_NAME,
            GroupType.RESOURCE, RESOURCE_GROUP_TYPE_NAME,
            GroupType.COMPUTE_HOST_CLUSTER, COMPUTE_HOST_CLUSTER_TYPE_NAME,
            GroupType.COMPUTE_VIRTUAL_MACHINE_CLUSTER, COMPUTE_VIRTUAL_MACHINE_CLUSTER_TYPE_NAME
    );

    /**
     * Map a group type to its "type" name in the entity table.
     *
     * <p>We use the group type enum name if the given type does not appear in our mapping
     * table.</p>
     *
     * @param groupType group type
     * @return name to use in entity table
     */
    public static String mapGroupTypeToName(GroupType groupType) {
        return GROUP_TYPE_TO_NAME.getOrDefault(groupType, groupType.name());
    }
}
