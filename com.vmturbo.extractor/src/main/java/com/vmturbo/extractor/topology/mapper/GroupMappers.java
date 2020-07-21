package com.vmturbo.extractor.topology.mapper;

import com.google.common.collect.ImmutableBiMap;

import com.vmturbo.extractor.schema.enums.EntityType;
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

    private static final ImmutableBiMap<GroupType, EntityType> GROUP_TYPE_TO_NAME = ImmutableBiMap.<GroupType, EntityType>builder()
            .put(GroupType.REGULAR, EntityType.GROUP)
            .put(GroupType.BILLING_FAMILY, EntityType.BILLING_FAMILY)
            .put(GroupType.STORAGE_CLUSTER, EntityType.STORAGE_CLUSTER)
            .put(GroupType.RESOURCE, EntityType.RESOURCE_GROUP)
            .put(GroupType.COMPUTE_HOST_CLUSTER, EntityType.COMPUTE_CLUSTER)
            .put(GroupType.COMPUTE_VIRTUAL_MACHINE_CLUSTER, EntityType.K8S_CLUSTER)
            .build();

    /**
     * Map a group type to its "type" name in the entity table.
     *
     * <p>We use the group type enum name if the given type does not appear in our mapping
     * table.</p>
     *
     * @param groupType group type
     * @return name to use in entity table
     */
    public static EntityType mapGroupTypeToName(GroupType groupType) {
        return GROUP_TYPE_TO_NAME.getOrDefault(groupType, EntityType.GROUP);
    }

    /**
     * Map a group type name (as mapped by this mapper) to the corresponding protobuf {@link
     * GroupType} value.
     *
     * <p>Unmapped type names are resolved using protobuf classes.</p>
     *
     * @param name name of group as produced by this mapper
     * @return corresponding {@link GroupType} value
     */
    public static GroupType mapNameToGroupType(final EntityType name) {
        final GroupType groupType = GROUP_TYPE_TO_NAME.inverse().getOrDefault(name, null);
        return groupType != null ? groupType : GroupType.REGULAR;
    }
}
