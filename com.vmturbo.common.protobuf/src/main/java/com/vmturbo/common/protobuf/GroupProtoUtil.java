package com.vmturbo.common.protobuf;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredGroupsPoliciesSettings.UploadedGroup;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.EntityFilters.EntityFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinitionOrBuilder;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupingOrBuilder;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.group.GroupDTO.Origin.CreationOriginCase;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers.StaticMembersByType;
import com.vmturbo.common.protobuf.group.PolicyDTO.Policy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.StringFilter;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.search.SearchableProperties;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.ConstraintInfo;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.ConstraintType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;

/**
 * Miscellaneous utilities for messages defined in group/GroupDTO.proto.
 */
public class GroupProtoUtil {

    private static final Logger logger = LogManager.getLogger();

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
     * The mapping from cluster entity type to new group type. For example: for host cluster, the
     * entity type is PhysicalMachine, but in new group proto we use the
     * {@link GroupType#COMPUTE_HOST_CLUSTER} to represent host cluster.
     * Todo: this should be removed once all probes are changed to set new GroupType
     */
    private static final Map<EntityType, GroupType> CLUSTER_ENTITY_TYPE_TO_GROUP_TYPE_MAPPING =
            ImmutableMap.of(
                    EntityType.PHYSICAL_MACHINE, GroupType.COMPUTE_HOST_CLUSTER,
                    EntityType.STORAGE, GroupType.STORAGE_CLUSTER,
                    EntityType.VIRTUAL_MACHINE, GroupType.COMPUTE_VIRTUAL_MACHINE_CLUSTER
            );


    /**
     * The types of group that are considered clusters.
     */
    public static final Set<GroupType> CLUSTER_GROUP_TYPES = ImmutableSet.of(
                    GroupType.COMPUTE_HOST_CLUSTER,
                    GroupType.STORAGE_CLUSTER,
                    GroupType.COMPUTE_VIRTUAL_MACHINE_CLUSTER);

    /**
     * The entity types that we consider WORKLOAD.
     */
    public static final Set<UIEntityType> WORKLOAD_ENTITY_TYPES = ImmutableSet.of(
        UIEntityType.VIRTUAL_MACHINE, UIEntityType.DATABASE, UIEntityType.DATABASE_SERVER);

    /**
     * The API String for entity types that we consider as workload.
     */
    public static final Set<String> WORKLOAD_ENTITY_TYPES_API_STR = WORKLOAD_ENTITY_TYPES
        .stream().map(UIEntityType::apiStr).collect(Collectors.toSet());

    /**
     * Match the name with filter. If the filter has case sensitive set to true,
     * matching is case sensitive; otherwise matching is case insensitive.
     * @param name The name to compare with the filter.
     * @param filter The name filter.
     * @return True if the name matches the filter.
     */
    public static boolean nameFilterMatches(@Nonnull final String name,
                                            @Nonnull final StringFilter filter) {
        if (filter.hasCaseSensitive() && filter.getCaseSensitive()) {
            return Pattern.matches(filter.getStringPropertyRegex(), name) == filter.getPositiveMatch();
        }
        final Pattern pattern = Pattern.compile(filter.getStringPropertyRegex(), Pattern.CASE_INSENSITIVE);
        return pattern.matcher(name).find() == filter.getPositiveMatch();
    }

    /**
     * Check that the input {@link Grouping} has a valid entity type for a policy. For now a valid
     * entity type for a policy is a group with a single type.
     *
     * @param group The {@link Grouping}.
     * @throws IllegalArgumentException If the {@link Grouping} does not have a valid entity type.
     */
    public static void checkEntityTypeForPolicy(@Nonnull final GroupingOrBuilder group) {
        final Set<UIEntityType> entityTypes = getEntityTypes(group);
        if (entityTypes.isEmpty()) {
            throw new IllegalArgumentException(String.format("Cannot define policy for group " +
                    "'%s' (ID: %s). Entity types are empty.",
                    group.getDefinition().getDisplayName(), group.getId()));
        } else if (entityTypes.size() > 1) {
            throw new IllegalArgumentException(String.format("Cannot define policy for group " +
                    "'%s' (ID: %s). Multiple entity types found: %s",
                    group.getDefinition().getDisplayName(), group.getId(), entityTypes));
        }
    }

    /**
     * Returns a set of entity types of expected members of the specified group.
     *
     * @param group group to analyze
     * @return set of entity types.
     */
    public static Set<UIEntityType> getEntityTypes(GroupingOrBuilder group) {
        return group.getExpectedTypesList()
                        .stream()
                        .filter(MemberType::hasEntity)
                        .map(MemberType::getEntity)
                        .map(UIEntityType::fromType)
                        .collect(Collectors.toSet());

    }

    /**
     * Get the source identifier of a {@link Grouping}.
     *
     * @param group The {@link Grouping}.
     * @return The source identifier of the {@link Grouping} if present.
     * @throws IllegalArgumentException If the {@link Grouping} is not properly formatted.
     */
    @Nonnull
    public static Optional<String> getGroupSourceIdentifier(@Nonnull final Grouping group) {
        if (group.getOrigin().getCreationOriginCase() == CreationOriginCase.DISCOVERED) {
            return Optional.ofNullable(group.getOrigin().getDiscovered().getSourceIdentifier());
        }
        return Optional.empty();
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
     *  Create the composite key for the Group.
     *
     * @param groupName Discovered name of the group
     * @param entityType Type of the group
     * @param targetId Id of the target that discovered the group.
     * @return
     */
    // todo: remove once TargetClusterUpdate is removed (OM-51757)
    public static String createGroupCompoundKey(@Nonnull final String groupName,
                                                 @Nonnull final EntityType entityType,
                                                 @Nonnull final long targetId) {
        return String.join(GROUP_KEY_SEP, groupName,
                String.valueOf(entityType), String.valueOf(targetId));
    }

    /**
     * Create the identifying key for the given group, which is used to uniquely identify a group.
     * Currently it's a combination of source identifier and group type. This is used to set the
     * reference group "id" on the policy, so in group component it can find the correct oid for
     * this unique string "id".
     *
     * @param sdkDTO the original sdk group dto coming from the probe
     * @return unique identifying key for the group
     */
    public static String createIdentifyingKey(@Nonnull final GroupDTO sdkDTO) {
        return createIdentifyingKey(getGroupType(sdkDTO), extractId(sdkDTO));
    }

    /**
     * Create the identifying key for the given group, which is used to uniquely identify a group.
     * Currently it's a combination of source identifier and group type. This is used to set the
     * reference group "id" on the policy, so in group component it can find the correct oid for
     * this unique string "id".
     *
     * @param uploadedGroup the group uploaded to group component
     * @return unique identifying key for the group
     */
    public static String createIdentifyingKey(@Nonnull final UploadedGroup uploadedGroup) {
        return createIdentifyingKey(uploadedGroup.getDefinition().getType(),
                uploadedGroup.getSourceIdentifier());
    }

    /**
     * Create the identifying key for the given group, which is used to uniquely identify a group.
     * Currently it's a combination of source identifier and group type.
     *
     * @param groupType type of the group
     * @param sourceId original identifier coming from the probe
     * @return unique identifying key for the group
     */
    public static String createIdentifyingKey(@Nonnull GroupType groupType,
                                              @Nonnull String sourceId) {
        return String.join(GROUP_KEY_SEP, String.valueOf(groupType.getNumber()), sourceId);
    }

    /**
     * Get the group type of the given sdk dto.
     * Todo: this should be removed once all probes are changed to set new GroupType
     *
     * @param sdkDTO the group dto coming from probe
     * @return {@link GroupType}
     */
    public static GroupType getGroupType(@Nonnull GroupDTO sdkDTO) {
        if (sdkDTO.hasConstraintInfo() &&
                sdkDTO.getConstraintInfo().getConstraintType().equals(ConstraintType.CLUSTER)) {
            if (CLUSTER_ENTITY_TYPE_TO_GROUP_TYPE_MAPPING.containsKey(sdkDTO.getEntityType())) {
                return CLUSTER_ENTITY_TYPE_TO_GROUP_TYPE_MAPPING.get(sdkDTO.getEntityType());
            } else {
                logger.warn("New cluster type found, no GroupType mapping type for group {}, " +
                        "using {}", sdkDTO, sdkDTO.getGroupType());
            }
        }
        return sdkDTO.getGroupType();
    }

    /**
     * If the group is a static group of entities, get the list of immediate members.
     *
     * @param group The {@link Grouping} object.
     * @return If the group is a static group (of any type), return an {@link Optional} containing
     *         the immediate static members. If the group is a dynamic group, return an empty optional.
     */
    public static List<Long> getStaticMembers(@Nonnull final Grouping group) {
        if (!group.getDefinition().hasStaticGroupMembers()) {
            return Collections.emptyList();
        }
        return group.getDefinition()
                        .getStaticGroupMembers()
                        .getMembersByTypeList()
                        .stream()
                        .map(StaticMembersByType::getMembersList)
                        .flatMap(List::stream)
                        .collect(Collectors.toList());
    }

    /**
     * Returns true if the input is a group of groups.
     * @param group input group.
     * @return true if it is nested group and false otherwise.
     */
    public static boolean isNestedGroup(@Nonnull final Grouping group) {
        return group.getDefinition().hasGroupFilters()
                || (group.getDefinition().hasStaticGroupMembers()
                        && group.getExpectedTypesList().stream().anyMatch(MemberType::hasGroup));
    }

    /**
     * Get the IDs of groups specified in a {@link Policy}.
     *
     * @param policy The {@link Policy}.
     * @return A set containing the IDs of {@link Grouping}s the policy relates to.
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

    /**
     * Get all the static member oids in the given group definition.
     *
     * @param groupDefinition the group to get static member oids for
     * @return set of member oids
     */
    public static Set<Long> getAllStaticMembers(@Nonnull GroupDefinitionOrBuilder groupDefinition) {
        return groupDefinition.getStaticGroupMembers().getMembersByTypeList().stream()
                .map(StaticMembersByType::getMembersList)
                .flatMap(List::stream)
                .collect(Collectors.toSet());
    }

    /**
     * Creates group filter to retrieve groups by the specified IDs.
     *
     * @param ids OIDs to query groups by
     * @return a valid {@link GroupFilter}
     */
    @Nonnull
    public static GroupFilter createGroupFilterByIds(@Nonnull Collection<Long> ids) {
        final Set<String> options = ids.stream().map(String::valueOf).collect(Collectors.toSet());
        return GroupFilter.newBuilder()
                .addPropertyFilters(
                        SearchProtoUtil.stringPropertyFilterExact(SearchableProperties.OID,
                                options))
                .build();
    }

    /**
     * Get the entity types from the entity filter.
     *
     * @param entityFilter the entity filter
     * @return all the entity types of the filter
     */
    public static Set<UIEntityType> getEntityTypesFromEntityFilter(EntityFilter entityFilter) {
        // first try to get the entity types from the search parameters (criteria)
        Set<UIEntityType> entityTypes = entityFilter
                .getSearchParametersCollection()
                .getSearchParametersList()
                .stream()
                .map(SearchProtoUtil::getEntityTypeFromSearchParameters)
                .filter(uiEntityType -> uiEntityType != UIEntityType.UNKNOWN)
                .collect(Collectors.toSet());

        // if the search parameters have no entity types, get the type from the entity filter
        if (entityTypes.isEmpty()) {
            entityTypes = Collections.singleton(UIEntityType.fromType(
                    entityFilter.getEntityType()));
        }

        return entityTypes;
    }
}
