package com.vmturbo.common.protobuf;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;

import com.vmturbo.common.protobuf.group.GroupDTO.ClusterFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.Group.Type;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupOrBuilder;
import com.vmturbo.common.protobuf.group.GroupDTO.NameFilter;
import com.vmturbo.common.protobuf.group.PolicyDTO.Policy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Miscellaneous utilities for messages defined in group/GroupDTO.proto.
 */
public class GroupProtoUtil {

    /**
     * @param name The name to compare with the filter.
     * @param filter The name filter.
     * @return True if the name matches the filter.
     */
    public static boolean nameFilterMatches(@Nonnull final String name,
                                            @Nonnull final NameFilter filter) {
        final boolean patternMatches = Pattern.matches(filter.getNameRegex(), name);
        return patternMatches ^ filter.getNegateMatch();
    }

    /**
     * Check that the input {@link Group} has a valid entity type.
     *
     * @param group The {@link Group}.
     * @throws IllegalArgumentException If the {@link Group} does not have a valid entity type.
     */
    public static void checkEntityType(@Nonnull final GroupOrBuilder group) {
        Preconditions.checkArgument(group.getType().equals(Type.CLUSTER) ||
                group.getTempGroup().hasEntityType() ||
                group.getGroup().hasEntityType());
    }

    /**
     * Get the entity type of entities in a {@link Group}.
     *
     * @param group The {@link Group}.
     * @return An integer representing the entity type.
     * @throws IllegalArgumentException If the {@link Group} does not have a valid entity type.
     */
    public static int getEntityType(@Nonnull final GroupOrBuilder group) {
        checkEntityType(group);
        switch (group.getType()) {
            case GROUP:
                return group.getGroup().getEntityType();
            case CLUSTER:
                switch (group.getCluster().getClusterType()) {
                    case COMPUTE:
                        return EntityType.PHYSICAL_MACHINE_VALUE;
                    case STORAGE:
                        return EntityType.STORAGE_VALUE;
                    default:
                        throw new IllegalArgumentException("Unknown cluster type: " + group.getType());
                }
            case TEMP_GROUP:
                return group.getTempGroup().getEntityType();
            default:
                throw new IllegalArgumentException("Unknown group type: " + group.getType());
        }
    }

    /**
     * Get the display name of a {@link Group}.
     *
     * @param group The {@link Group}.
     * @return The name of the {@link Group}.
     * @throws IllegalArgumentException If the {@link Group} is not properly formatted and does not
     *                                  have a name.
     */
    @Nonnull
    public static String getGroupName(@Nonnull final Group group) {
        final String name;
        switch (group.getType()) {
            case GROUP:
                Preconditions.checkArgument(group.hasGroup() && group.getGroup().hasName());
                name = group.getGroup().getName();
                break;
            case CLUSTER:
                Preconditions.checkArgument(group.hasCluster() && group.getCluster().hasName());
                name = group.getCluster().getName();
                break;
            case TEMP_GROUP:
                Preconditions.checkArgument(group.hasTempGroup() && group.getTempGroup().hasName());
                name = group.getTempGroup().getName();
                break;
            default:
                throw new IllegalArgumentException("Unknown group type: " + group.getType());
        }
        return name;
    }

    /**
     * For groups, the identifiers used by the group component are built from the name and entity
     * type of groups. This is done to distinguish groups of the same name but different entity types
     * (ie we may discover two groups named "foo" one for storage, one for hosts), and they need
     * to be distinguished from each other.
     *
     * This method operates on the SDK groups (ie as discovered by probes)
     *
     * @param group The group whose id should be constructed from its name.
     * @return The id of the discovered group as used by the group component.
     */
    @Nonnull
    public static String discoveredIdFromName(@Nonnull final CommonDTO.GroupDTO group) {
        return composeId(group.getDisplayName(), group.getEntityType());
    }

    /**
     * For groups, the identifiers used by the group component are built from the name and entity
     * type of groups. This is done to distinguish groups of the same name but different entity types
     * (ie we may discover two groups named "foo" one for storage, one for hosts), and they need
     * to be distinguished from each other.
     *
     * This method operates on XL-internal groups.
     *
     * @param group The group whose id should be constructed from its name.
     * @return The id of the discovered group as used by the group component.
     */
    @Nonnull
    public static String discoveredIdFromName(@Nonnull final GroupInfo group) {
        return composeId(group.getName(), EntityType.forNumber(group.getEntityType()));
    }

    /**
     * For clusters, the identifiers used by the group component are built from the name and entity
     * type of groups. This is done to distinguish clusters of the same name but different entity types
     * (ie we may discover two clusters named "foo" one for storage, one for hosts), and they need
     * to be distinguished from each other.
     *
     * @param clusterInfo The cluster whose id should be constructed from its name.
     * @return The id of the discovered cluster as used by the group component.
     */
    @Nonnull
    public static String discoveredIdFromName(@Nonnull final ClusterInfo clusterInfo) {
        return composeId(clusterInfo.getName(),
            clusterInfo.getClusterType() == ClusterInfo.Type.COMPUTE ?
                EntityDTO.EntityType.PHYSICAL_MACHINE : EntityDTO.EntityType.STORAGE);
    }

    private static String composeId(@Nonnull final String originalName,
                                    @Nonnull final EntityType entityType) {
        return originalName + "-" + entityType;
    }

    /**
     * Check whether a {@link Group} matches a {@link ClusterFilter}.
     *
     * @param group The {@link Group}.
     * @param filter The {@link ClusterFilter} to use for
     * @return True if the filter matches. False otherwise. If the {@link Group} is not a cluster,
     *         the filter is not applicable, and this method will return true.
     */
    public static boolean clusterFilterMatcher(@Nonnull final Group group,
                                               @Nonnull final ClusterFilter filter) {
        if (!group.getType().equals(Type.CLUSTER)) {
            return true;
        }

        return !filter.hasTypeFilter() ||
            group.getCluster().getClusterType().equals(filter.getTypeFilter());
    }

    /**
     * Get the list of members in a group with type == CLUSTER.
     *
     * @param cluster A {@link Group} representing a cluster.
     * @return The set of IDS of members in the cluster.
     * @throws IllegalArgumentException If the {@link Group} is not a cluster.
     */
    @Nonnull
    public static Set<Long> getClusterMembers(@Nonnull final Group cluster) {
        Preconditions.checkArgument(cluster.getType().equals(Type.CLUSTER) &&
                cluster.hasCluster());
        return new HashSet<>(cluster.getCluster().getMembers().getStaticMemberOidsList());
    }

    /**
     * Get the IDs of groups specified in a {@link Policy}.
     *
     * @param policy The {@link Policy}.
     * @return A set containing the IDs of {@link Group}s the policy relates to.
     */
    @Nonnull
    public static Set<Long> getPolicyGroupIds(@Nonnull final Policy policy) {
        final Set<Long> result = new HashSet<>();
        final PolicyInfo policyInfo = policy.getPolicyInfo();
        switch (policyInfo.getPolicyDetailCase()) {
            case MERGE:
                result.addAll(policyInfo.getMerge().getMergeGroupIdsList());
                break;
            case AT_MOST_N:
                result.add(policyInfo.getAtMostN().getConsumerGroupId());
                result.add(policyInfo.getAtMostN().getProviderGroupId());
                break;
            case BIND_TO_GROUP:
                result.add(policyInfo.getBindToGroup().getConsumerGroupId());
                result.add(policyInfo.getBindToGroup().getProviderGroupId());
                break;
            case AT_MOST_NBOUND:
                result.add(policyInfo.getAtMostNbound().getConsumerGroupId());
                result.add(policyInfo.getAtMostNbound().getProviderGroupId());
                break;
            case BIND_TO_GROUP_AND_LICENSE:
                result.add(policyInfo.getBindToGroupAndLicense().getConsumerGroupId());
                result.add(policyInfo.getBindToGroupAndLicense().getProviderGroupId());
                break;
            case BIND_TO_GROUP_AND_GEO_REDUNDANCY:
                result.add(policyInfo.getBindToGroupAndGeoRedundancy().getConsumerGroupId());
                result.add(policyInfo.getBindToGroupAndGeoRedundancy().getProviderGroupId());
                break;
            case BIND_TO_COMPLEMENTARY_GROUP:
                result.add(policyInfo.getBindToComplementaryGroup().getConsumerGroupId());
                result.add(policyInfo.getBindToComplementaryGroup().getProviderGroupId());
                break;
            case MUST_RUN_TOGETHER:
                result.add(policyInfo.getMustRunTogether().getGroupId());
                break;
            case MUST_NOT_RUN_TOGETHER:
                result.add(policyInfo.getMustNotRunTogether().getGroupId());
                break;
        }
        return result;
    }
}
