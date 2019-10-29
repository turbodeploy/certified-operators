package com.vmturbo.topology.processor.util;

import java.util.List;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredGroupsPoliciesSettings.UploadedGroup;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers.StaticMembersByType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;

/**
 * Utility functions for testing conversion from sdk group to xl group.
 */
public final class GroupTestUtils {

    /**
     * private default constructor to prevent this util class from being instantiated or extended.
     */
    private GroupTestUtils() {
    }

    /**
     * Create a GroupDefinition containing static group members.
     *
     * @param groupDisplayName display name of the group
     * @param entityType entity type of the members
     * @param memberOids member oids
     * @return {@link GroupDefinition.Builder}
     */
    public static GroupDefinition.Builder createStaticGroupDef(@Nonnull String groupDisplayName,
            int entityType, @Nonnull List<Long> memberOids) {
        return GroupDefinition.newBuilder()
                .setType(GroupType.REGULAR)
                .setDisplayName(groupDisplayName)
                .setStaticGroupMembers(StaticMembers.newBuilder()
                        .addMembersByType(StaticMembersByType.newBuilder()
                                .setType(MemberType.newBuilder()
                                        .setEntity(entityType))
                                .addAllMembers(memberOids)));
    }

    /**
     * Create a GroupDefinition which represent a cluster.
     *
     * @param clusterDisplayName display name of the cluster
     * @param clusterType type of the cluster
     * @param memberOids member oids
     * @return {@link GroupDefinition.Builder}
     */
    public static GroupDefinition.Builder createClusterDef(@Nonnull String clusterDisplayName,
            @Nonnull GroupType clusterType, @Nonnull List<Long> memberOids) {
        final int entityType;
        switch (clusterType) {
            case COMPUTE_HOST_CLUSTER:
                entityType = EntityType.PHYSICAL_MACHINE_VALUE;
                break;
            case STORAGE_CLUSTER:
                entityType = EntityType.STORAGE_VALUE;
                break;
            case COMPUTE_VIRTUAL_MACHINE_CLUSTER:
                entityType = EntityType.VIRTUAL_MACHINE_VALUE;
                break;
            default:
                entityType = EntityType.PHYSICAL_MACHINE_VALUE;
                break;
        }
        return createStaticGroupDef(clusterDisplayName, entityType, memberOids)
                .setType(clusterType);
    }

    /**
     * Create an UploadedGroup containing static members.
     *
     * @param groupId source id of the group
     * @param entityType entity type of the members
     * @param memberOids member oids
     * @return {@link UploadedGroup.Builder}
     */
    public static UploadedGroup.Builder createUploadedStaticGroup(@Nonnull String groupId,
            int entityType, @Nonnull List<Long> memberOids) {
        return UploadedGroup.newBuilder()
                .setSourceIdentifier(groupId)
                .setDefinition(createStaticGroupDef(groupId, entityType, memberOids));
    }

    /**
     * Create an UploadedGroup which represent a cluster.
     *
     * @param clusterId source id of the cluster
     * @param clusterType type of the cluster
     * @param memberOids member oids
     * @return {@link UploadedGroup.Builder}
     */
    public static UploadedGroup.Builder createUploadedCluster(@Nonnull String clusterId,
            @Nonnull GroupType clusterType, @Nonnull List<Long> memberOids) {
        return createUploadedCluster(clusterId, clusterId, clusterType, memberOids);
    }

    /**
     * Create an UploadedGroup which represent a cluster.
     *
     * @param clusterId source id of the cluster
     * @param clusterDisplayName display name of the cluster
     * @param clusterType type of the cluster
     * @param memberOids member oids
     * @return {@link UploadedGroup.Builder}
     */
    public static UploadedGroup.Builder createUploadedCluster(@Nonnull String clusterId,
            @Nonnull String clusterDisplayName, @Nonnull GroupType clusterType,
            @Nonnull List<Long> memberOids) {
        return UploadedGroup.newBuilder()
                .setSourceIdentifier(clusterId)
                .setDefinition(createClusterDef(clusterDisplayName, clusterType, memberOids));
    }
}
