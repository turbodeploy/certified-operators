package com.vmturbo.common.protobuf;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

import com.vmturbo.common.protobuf.group.GroupDTO.ClusterFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.Group.Type;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupOrBuilder;
import com.vmturbo.common.protobuf.group.GroupDTO.NameFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.NestedGroupInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.NestedGroupInfo.TypeCase;
import com.vmturbo.common.protobuf.group.PolicyDTO.Policy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.ConstraintInfo;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.ConstraintType;

/**
 * Miscellaneous utilities for messages defined in group/GroupDTO.proto.
 */
public class GroupProtoUtil {

    public final static String GROUP_KEY_SEP = "-";

    /**
     * Prefix for buyers group id of placement constraint
     */
    public static final String BUYERS_GROUP_ID_PREFIX =  "BG-";

    /**
     * Prefix for sellers group id of placement constraint
     */
    public static final String SELLERS_GROUP_ID_PREFIX = "SG-";

    /**
     * Set of placement related constraints types
     */
    private static final Set<ConstraintType> PLACEMENT_CONSTRAINT_TYPES = ImmutableSet.of(
        ConstraintType.BUYER_BUYER_AFFINITY,
        ConstraintType.BUYER_BUYER_ANTI_AFFINITY,
        ConstraintType.BUYER_SELLER_AFFINITY,
        ConstraintType.BUYER_SELLER_ANTI_AFFINITY,
        ConstraintType.CLUSTER
        );

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
        if (group.getType() == Type.NESTED_GROUP) {
            // Nested groups don't have an explicitly-specified type. We currently support
            // just one type of nested group - a group of clusters - in which case we can infer
            // the entity type from the type of clusters in the group.
            Preconditions.checkArgument(group.getNestedGroup().getTypeCase() ==
                TypeCase.CLUSTER);
        } else {
            Preconditions.checkArgument(group.getType().equals(Type.CLUSTER) ||
                group.getTempGroup().hasEntityType() ||
                group.getGroup().hasEntityType());
        }
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
                        throw new IllegalArgumentException("Unknown cluster type: " +
                            group.getCluster().getClusterType());
                }
            case TEMP_GROUP:
                return group.getTempGroup().getEntityType();
            case NESTED_GROUP:
                if (group.getNestedGroup().getTypeCase() == TypeCase.CLUSTER) {
                    switch (group.getNestedGroup().getCluster()) {
                        case COMPUTE:
                            return EntityType.PHYSICAL_MACHINE_VALUE;
                        case STORAGE:
                            return EntityType.STORAGE_VALUE;
                        default:
                            throw new IllegalArgumentException("Unknown nested cluster type: " +
                                group.getNestedGroup().getCluster());
                    }
                } else {
                    throw new IllegalArgumentException("Unknown nested group type: " +
                        group.getNestedGroup().getTypeCase());
                }
            default:
                throw new IllegalArgumentException("Unknown group type: " + group.getType());
        }
    }

    /**
     * Get the name of a {@link Group}.
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
            case NESTED_GROUP:
                Preconditions.checkArgument(group.hasNestedGroup() && group.getNestedGroup().hasName());
                name = group.getNestedGroup().getName();
                break;
            default:
                throw new IllegalArgumentException("Unknown group type: " + group.getType());
        }
        return name;
    }

    /**
     * Get the display name of a {@link Group}.
     *
     * @param group The {@link Group}.
     * @return The display name of the {@link Group}.
     * @throws IllegalArgumentException If the {@link Group} is not properly formatted.
     */
    @Nonnull
    public static String getGroupDisplayName(@Nonnull final Group group) {
        final String name;
        switch (group.getType()) {
            case GROUP:
                Preconditions.checkArgument(group.hasGroup());
                name = group.getGroup().hasDisplayName() ?
                    group.getGroup().getDisplayName() :
                    group.getGroup().getName();
                break;
            case CLUSTER:
                Preconditions.checkArgument(group.hasCluster());
                name = group.getCluster().hasDisplayName() ?
                    group.getCluster().getDisplayName() :
                    group.getCluster().getName();
                break;
            case TEMP_GROUP:
                Preconditions.checkArgument(group.hasTempGroup());
                name = group.getTempGroup().getName();
                break;
            case NESTED_GROUP:
                Preconditions.checkArgument(group.hasNestedGroup() && group.getNestedGroup().hasName());
                name = group.getNestedGroup().getName();
                break;
            default:
                throw new IllegalArgumentException("Unknown group type: " + group.getType());
        }
        return name;
    }

    /**
     * Given a {@link CommonDTO.GroupDTO}, extract its id properly. "group_name" is preferred if
     * if it is available. If not, then it falls back to "constraint_id". For some special
     * placement constraint types, there may be two groups (buyer group and seller group) with
     * same constraint_id. So a prefix is appended in this case.
     *
     * This function is used when creating discovered groups and populating group members. The id
     * included in the parent group's members list is the "constraint_id" (not constraint_name)
     * for cluster (host/storage) group, and "group_name" for other types of groups. It assumes that
     * placement constraint groups are not members of other groups, which is true as far as now. If
     * this assumption changes, the membership may not be populated correctly due to the prefix.
     *
     * @param sdkDTO a {@link CommonDTO.GroupDTO}
     * @return the group id used for membership.
     */
    @Nonnull
    public static String extractId(@Nonnull final CommonDTO.GroupDTO sdkDTO) {
        if (sdkDTO.hasGroupName()) {
            return sdkDTO.getGroupName();
        } else if (sdkDTO.hasConstraintInfo()) {
            final ConstraintInfo constraintInfo = sdkDTO.getConstraintInfo();
            if (constraintInfo.hasConstraintId()) {
                // add prefix for placement constraint groups, since buyer group and seller group
                // have same constraint id
                if (PLACEMENT_CONSTRAINT_TYPES.contains(constraintInfo.getConstraintType())) {
                    final String prefix = constraintInfo.getIsBuyer()
                        ? BUYERS_GROUP_ID_PREFIX : SELLERS_GROUP_ID_PREFIX;
                    return prefix + constraintInfo.getConstraintId();
                } else {
                    // use constraint id for other types, since it will be unique in the probe
                    return constraintInfo.getConstraintId();
                }
            }
        }

        throw new IllegalArgumentException("GroupName or ConstraintId must be present in groupDTO");
    }

    /**
     * Give a {@link CommonDTO.GroupDTO}, and extract its displayName properly. For cluster, the
     * displayName may not be set, we use constraint_name as displayName in this case. Otherwise,
     * use group_name.
     *
     * @param sdkDTO a {@link CommonDTO.GroupDTO}
     * @return group displayName.
     */
    @Nonnull
    public static String extractDisplayName(@Nonnull final CommonDTO.GroupDTO sdkDTO) {
        if (sdkDTO.hasDisplayName()) {
            return sdkDTO.getDisplayName();
        }

        if (sdkDTO.hasConstraintInfo()) {
            return sdkDTO.getConstraintInfo().getConstraintName();
        }

        // fall back to extractId if none above is available
        return extractId(sdkDTO);
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
    public static String discoveredIdFromName(@Nonnull final CommonDTO.GroupDTO group,
                                              @Nonnull final long targetId) {
        return createGroupCompoundKey(extractId(group), group.getEntityType(), targetId);
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
    public static String discoveredIdFromName(@Nonnull final GroupInfo group,
                                              @Nonnull final long targetId) {
        return createGroupCompoundKey(group.getName(), EntityType.forNumber(group.getEntityType()),
                targetId);
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
    public static String discoveredIdFromName(@Nonnull final ClusterInfo clusterInfo,
                                              @Nonnull final long targetId) {
        return createGroupCompoundKey(clusterInfo.getName(), getClusterEntityType(clusterInfo),
                targetId);
    }

    /**
     *  Clusters can either have members which are all of type Host entities or
     *  all of type Storage(In the future it may involve more entity types).
     *  This is a helper function to determine the entity
     *  type the cluster contains.
     *
     * @param clusterInfo The cluster whose entity type needs to be displayed.
     * @return The entity type of the cluster.
     */
    @Nonnull
    public static EntityType getClusterEntityType(@Nonnull final ClusterInfo clusterInfo) {
        return clusterInfo.getClusterType() == ClusterInfo.Type.COMPUTE ?
                EntityDTO.EntityType.PHYSICAL_MACHINE : EntityDTO.EntityType.STORAGE;
    }

    /**
     *  Create the composite key for the Group.
     *
     * @param groupName Discovered name of the group
     * @param entityType Type of the group
     * @param targetId Id of the target that discovered the group.
     * @return
     */
    public static String createGroupCompoundKey(@Nonnull final String groupName,
                                                 @Nonnull final EntityType entityType,
                                                 @Nonnull final long targetId) {
        return String.join(GROUP_KEY_SEP, groupName,
                String.valueOf(entityType), String.valueOf(targetId));
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
     * If the group is a static group, get the list of members. This method is useful when we have
     * a {@link Group} outside the group component and want its members, but want to avoid an extra
     * RPC call.
     *
     * @param group The {@link Group} object.
     * @return If the group is a static group (of any type), return an {@link Optional} containing
     *         the static members. If the group is a dynamic group, return an empty optional.
     */
    public static Optional<List<Long>> getStaticMembers(@Nonnull final Group group) {
        List<Long> retGroup = null;
        switch (group.getType()) {
            case GROUP:
                if (group.getGroup().getSelectionCriteriaCase() == GroupInfo.SelectionCriteriaCase.STATIC_GROUP_MEMBERS) {
                    retGroup = group.getGroup().getStaticGroupMembers().getStaticMemberOidsList();
                }
                break;
            case CLUSTER:
                retGroup = group.getCluster().getMembers().getStaticMemberOidsList();
                break;
            case TEMP_GROUP:
                retGroup = group.getTempGroup().getMembers().getStaticMemberOidsList();
                break;
            case NESTED_GROUP:
                if (group.getNestedGroup().getSelectionCriteriaCase() == NestedGroupInfo.SelectionCriteriaCase.STATIC_GROUP_MEMBERS) {
                    retGroup = group.getNestedGroup().getStaticGroupMembers().getStaticMemberOidsList();
                }
                break;
            default:
                throw new IllegalArgumentException("Unhandled group type: " + group.getType());
        }
        return Optional.ofNullable(retGroup);
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
