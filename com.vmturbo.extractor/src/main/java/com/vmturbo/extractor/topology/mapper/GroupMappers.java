package com.vmturbo.extractor.topology.mapper;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;

import com.vmturbo.extractor.schema.enums.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;

/**
 * Mappers pertaining to group definitions.
 */
public class GroupMappers {
    private GroupMappers() {
    }

    private static final ImmutableBiMap<GroupType, EntityType> GROUP_TYPE_TO_ENTITY_TYPE = ImmutableBiMap.of(
            GroupType.REGULAR, EntityType.GROUP,
            GroupType.RESOURCE, EntityType.RESOURCE_GROUP,
            GroupType.COMPUTE_HOST_CLUSTER, EntityType.COMPUTE_CLUSTER,
            GroupType.COMPUTE_VIRTUAL_MACHINE_CLUSTER, EntityType.K8S_CLUSTER
    );

    /**
     * Mapping between {@link com.vmturbo.api.enums.GroupType} and {@link GroupType}.
     */
    public static final BiMap<com.vmturbo.api.enums.GroupType, GroupType> GROUP_TYPE_MAPPING =
            new ImmutableBiMap.Builder<com.vmturbo.api.enums.GroupType, GroupDTO.GroupType>()
                    .put(com.vmturbo.api.enums.GroupType.Group, GroupDTO.GroupType.REGULAR)
                    .put(com.vmturbo.api.enums.GroupType.Resource, GroupDTO.GroupType.RESOURCE)
                    .put(com.vmturbo.api.enums.GroupType.Cluster, GroupDTO.GroupType.COMPUTE_HOST_CLUSTER)
                    .put(com.vmturbo.api.enums.GroupType.StorageCluster, GroupDTO.GroupType.STORAGE_CLUSTER)
                    .put(com.vmturbo.api.enums.GroupType.VMCluster, GroupDTO.GroupType.COMPUTE_VIRTUAL_MACHINE_CLUSTER)
                    .put(com.vmturbo.api.enums.GroupType.BillingFamily, GroupDTO.GroupType.BILLING_FAMILY)
                    .build();

    /**
     * Map a Protobuf group type to the corresponding DB entity type.
     *
     * <p>We attempt to use match enum names if the given type does not appear in our mapping
     * table.</p>
     *
     * @param groupType Protobuf group type
     * @return DB entity type for this group type
     * @throws IllegalArgumentException if the mapping fails
     */
    public static EntityType mapGroupTypeToEntityType(GroupDTO.GroupType groupType)
            throws IllegalArgumentException {
        final EntityType result = GROUP_TYPE_TO_ENTITY_TYPE.get(groupType);
        return result != null ? result : EntityType.valueOf(groupType.name());
    }

    /**
     * Map a DB entity type that corresponds to a group type, to the corresponding Protobuf group
     * type.
     *
     * <p>Unmapped type names are resolved using protobuf classes.</p>
     *
     * @param entityType a DB entity type that represents a group type
     * @return corresponding Protobuf group type
     * @throws IllegalArgumentException if conversion fails
     */
    public static GroupDTO.GroupType mapEntityTypeToGroupType(final EntityType entityType)
            throws IllegalArgumentException {
        final GroupType groupType = GROUP_TYPE_TO_ENTITY_TYPE.inverse().getOrDefault(entityType, null);
        return groupType != null ? groupType : GroupType.valueOf(entityType.name());
    }
}
