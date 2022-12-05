package com.vmturbo.topology.processor.group.discovery;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.EntityFilters;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.EntityFilters.EntityFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.group.GroupDTO.SearchParametersCollection;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticGroupMembers;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers.StaticMembersByType;
import com.vmturbo.common.protobuf.search.Search.ComparisonOperator;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.NumericFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.StringFilter;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.ConstraintInfo;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.ConstraintType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.MembersCase;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.SelectionSpec;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.SelectionSpec.ExpressionType;
import com.vmturbo.proactivesupport.DataMetricCounter;
import com.vmturbo.topology.processor.conversions.SdkToTopologyEntityConverter;
import com.vmturbo.topology.processor.entity.Entity;
import com.vmturbo.topology.processor.entity.EntityStore;

/**
 * The {@link DiscoveredGroupInterpreter} is responsible for providing conversion functions
 * from {@link CommonDTO.GroupDTO} to all possible representations of that group in the
 * XL system - most notably groups, clusters, and policies.
 */
class DiscoveredGroupInterpreter {
    /**
     * This is the maximum depth of nested groups we allow.
     * We currently do nested group resolution recursively, and with a stack size of 512k
     * we can support a nesting depth of about ~350. Instead of reimplementing iteratively,
     * we limit the depth. 100 seems like more than enough (who will have 100 levels of
     * nesting?!).
     *
     * In the (near) future, we expect to have proper support for group-of-groups. This code
     * and this limitation will then go away, since we won't be doing group resolution here.
     */
    @VisibleForTesting
    static final int MAX_NESTING_DEPTH = 100;

    /**
     * Set of group types which are considered as clusters.
     */
    private static final Set<GroupType> CLUSTER_TYPES = ImmutableSet.of(
            GroupType.COMPUTE_HOST_CLUSTER,
            GroupType.COMPUTE_VIRTUAL_MACHINE_CLUSTER,
            GroupType.STORAGE_CLUSTER);

    private static final Logger logger = LogManager.getLogger();

    private final EntityStore entityStore;

    private final PropertyFilterConverter propertyFilterConverter;

    /**
     * Create a new group converter that will look up entity-related information in
     * the provided {@link EntityStore}.
     *
     * @param entityStore The {@link EntityStore} to look information up in.
     */
    DiscoveredGroupInterpreter(@Nonnull final EntityStore entityStore) {
        this(entityStore, new DefaultPropertyFilterConverter());
    }

    /**
     * This constructor should only explicitly be used from test code, when it's necessary to
     * inject a (mock) {@link PropertyFilterConverter}.
     *
     * @param entityStore The entity store to use.
     * @param converter A {@link PropertyFilterConverter}.
     */
    @VisibleForTesting
    DiscoveredGroupInterpreter(@Nonnull final EntityStore entityStore,
                               @Nonnull final PropertyFilterConverter converter) {
        this.entityStore = entityStore;
        this.propertyFilterConverter = converter;
    }

    /**
     * Attempt to interpret a list of {@link CommonDTO.GroupDTO}s. Returns one
     * {@link InterpretedGroup} for every input {@link CommonDTO.GroupDTO}.
     *
     * @param dtoList The input list of {@link CommonDTO.GroupDTO}s.
     * @param targetId The target that discovered the groups.
     * @return The list of {@link InterpretedGroup}s, one for every input dto.
     */
    @Nonnull
    List<InterpretedGroup> interpretSdkGroupList(@Nonnull final List<CommonDTO.GroupDTO> dtoList,
                                                 final long targetId) {
        final List<CommonDTO.GroupDTO> legalDtoList = dtoList.stream()
            .filter(this::validGroupId)
            .collect(Collectors.toList());
        final Map<String, Long> targetEntityIdMap = entityStore.getTargetEntityIdMap(targetId)
                .orElseGet(() -> {
                    logger.warn("Unable to resolve target entity ID map for {}", targetId);
                    return Collections.emptyMap();
                });
        final GroupInterpretationContext context =
            new GroupInterpretationContext(targetId, legalDtoList, targetEntityIdMap);
        legalDtoList.forEach(group -> {
            // We check to see if we need to interpret this particular group, because the
            // interpretation of a previous group that contains this one may have already triggered
            // the interpretation of this one. Re-interpreting it would be wasteful.
            context.computeInterpretedGroup(GroupProtoUtil.extractId(group),
                    () -> interpretGroup(group, context));
        });
        return new ArrayList<>(context.interpretedGroups());
    }

    /**
     * Check if the id of the given sdk group is valid or not.
     *
     * @param group the discovered group from probe
     * @return true of the group id is valid, otherwise false
     */
    private boolean validGroupId(@Nonnull final CommonDTO.GroupDTO group) {
        try {
            GroupProtoUtil.extractId(group);
            return true;
        } catch (IllegalArgumentException e) {
            Metrics.ERROR_COUNT.labels("invalid_group_dto").increment();
            logger.error("Invalid GroupDTO: {}", group, e);
            return false;
        }
    }

    @Nonnull
    public InterpretedGroup interpretGroup(@Nonnull final GroupDTO sdkDTO,
                                           @Nonnull final GroupInterpretationContext context) {
        Metrics.DISCOVERED_GROUP_COUNT.labels(sdkDTO.getGroupType().name()).increment();
        // convert sdk group to xl group and set source id and target id
        return new InterpretedGroup(context.targetId, sdkDTO,
                sdkToGroupDefinition(sdkDTO, context));
    }

    /**
     * Attempt to convert a {@link CommonDTO.GroupDTO} to a {@link GroupDefinition} describing a
     * group in the XL system.
     *
     * @param sdkDTO The {@link CommonDTO.GroupDTO} discovered by the target.
     * @param context the context object containing all needed info for interpretation
     * @return An optional containing the {@link GroupDefinition.Builder}, or an empty optional
     * if the conversion is not successful.
     */
    public Optional<GroupDefinition.Builder> sdkToGroupDefinition(
            @Nonnull final CommonDTO.GroupDTO sdkDTO,
            @Nonnull final GroupInterpretationContext context) {
        // validate group
        if (!validGroup(sdkDTO)) {
            return Optional.empty();
        }

        final GroupDefinition.Builder groupDefinition = GroupDefinition.newBuilder();
        // set members first, since it may return fast if there is any error during parsing
        switch (sdkDTO.getMembersCase()) {
            // dynamic members from probe, SelectionSpec is used to select group members, this
            // seems seldom used, since I don't see a dynamic group from probe as far as now
            case SELECTION_SPEC_LIST:
                Optional<EntityFilters.Builder> entityFilters = parseSelectionSpecList(sdkDTO);
                if (entityFilters.isPresent()) {
                    groupDefinition.setEntityFilters(entityFilters.get());
                } else {
                    logger.warn("Unable to parse selection spec list: {}", sdkDTO.getSelectionSpecList());
                    return Optional.empty();
                }
                break;
            // static members from probe, MembersList contains a list of member uuids
            case MEMBER_LIST:
                final Optional<StaticMembers> members = parseMemberList(sdkDTO, context);
                if (members.isPresent()) {
                    // We allow empty groups
                    groupDefinition.setStaticGroupMembers(members.get());
                } else {
                    logger.warn("Unable to parse group member list: {}", sdkDTO.getMemberList());
                    return Optional.empty();
                }
                break;
            default:
                logger.warn("Unhandled members type: {}", sdkDTO.getMembersCase());
                return Optional.empty();
        }

        // set other properties on the group
        groupDefinition.setType(GroupProtoUtil.getGroupType(sdkDTO));
        groupDefinition.setDisplayName(GroupProtoUtil.extractDisplayName(sdkDTO));

        // set tags for group
        SdkToTopologyEntityConverter.convertGroupTags(sdkDTO).ifPresent(groupDefinition::setTags);

        if (sdkDTO.hasOwner()) {
            final Optional<Long> ownerForResourceGroup =
                    getOwnerOIDForResourceGroup(sdkDTO, context);
            ownerForResourceGroup.ifPresent(groupDefinition::setOwner);
        }

        return Optional.of(groupDefinition);
    }

    private Optional<Long> getOwnerOIDForResourceGroup(final GroupDTO sdkDTO,
                                                       @Nonnull GroupInterpretationContext groupContext) {
        final String groupOwner = sdkDTO.getOwner();
        if (!StringUtils.isBlank(groupOwner)) {
            final Map<String, Long> targetEntityIdMap = groupContext.targetEntityidMap();
            if (!targetEntityIdMap.isEmpty()) {
                final Long ownerOID = targetEntityIdMap.get(groupOwner);
                if (ownerOID == null) {
                    logger.warn("OwnerID is null for '{}' group", sdkDTO.getGroupName());
                }
                return Optional.ofNullable(ownerOID);
            } else {
                logger.error("There are no entities for '{}' target ", groupContext.targetId());
            }
        } else {
            logger.error("Owner is not set for '{}' group", sdkDTO.getGroupName());
        }

        return Optional.empty();
    }

    /**
     * Check if the given sdk group is valid or not.
     *
     * @param sdkDTO the discovered group from probe
     * @return true of the group id is valid, otherwise false
     */
    private boolean validGroup(@Nonnull final CommonDTO.GroupDTO sdkDTO) {
        // it means probe has been changed to use new enum
        if (CLUSTER_TYPES.contains(sdkDTO.getGroupType())) {
            // all cluster should have constraint info
            if (!sdkDTO.hasConstraintInfo()) {
                logger.error("ConstraintInfo is not set for cluster : {}", sdkDTO);
                return false;
            }

            if (!MembersCase.MEMBER_LIST.equals(sdkDTO.getMembersCase())) {
                logger.error("Static members list is not configured for cluster: {}", sdkDTO);
                return false;
            }
        } else {
            // it means probe is still using old way to tell if it's cluster
            // todo: remove once probe is changed to use new group type
            if (sdkDTO.hasConstraintInfo()) {
                final ConstraintType constraintType = sdkDTO.getConstraintInfo().getConstraintType();
                if (constraintType == ConstraintType.CLUSTER &&
                        !MembersCase.MEMBER_LIST.equals(sdkDTO.getMembersCase())) {
                    logger.error("Static members list is not configured for cluster: {}", sdkDTO);
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Converts a SDK {@link CommonDTO.GroupDTO.SelectionSpecList} to the equivalent
     * {@link EntityFilters.Builder}, which can be used internally as the dynamic group filters.
     *
     * @param sdkDTO the sdk group dto coming from probe
     * @return optional {@link EntityFilters.Builder}
     */
    private Optional<EntityFilters.Builder> parseSelectionSpecList(@Nonnull GroupDTO sdkDTO) {
        final SearchParameters.Builder searchParametersBuilder = SearchParameters.newBuilder();
        final AtomicInteger successCount = new AtomicInteger(0);
        sdkDTO.getSelectionSpecList()
                .getSelectionSpecList()
                .stream()
                .map(propertyFilterConverter::selectionSpecToPropertyFilter)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .forEach(propFilter -> {
                    successCount.incrementAndGet();
                    if (searchParametersBuilder.hasStartingFilter()) {
                        searchParametersBuilder.addSearchFilter(
                                SearchFilter.newBuilder().setPropertyFilter(propFilter));
                    } else {
                        searchParametersBuilder.setStartingFilter(propFilter);
                    }
                });

        if (sdkDTO.getSelectionSpecList().getSelectionSpecList().size() != successCount.get()) {
            logger.warn("Failed to translate all selection specs {}", sdkDTO.getSelectionSpecList());
            return Optional.empty();
        }

        return Optional.of(EntityFilters.newBuilder().addEntityFilter(EntityFilter.newBuilder()
                .setEntityType(sdkDTO.getEntityType().getNumber())
                .setSearchParametersCollection(SearchParametersCollection.newBuilder()
                    .addSearchParameters(searchParametersBuilder))));
    }

    /**
     * Converts an SDK {@link CommonDTO.GroupDTO.MembersList} to the equivalent
     * {@link StaticGroupMembers}. The most important part of that is looking up entity
     * identifiers, since the input members list has UUID's as reported by the probe, and
     * the output members list has OIDs.
     *
     * @param groupDTO The {@link CommonDTO.GroupDTO}
     * @return An optional containing the {@link StaticGroupMembers} equivalent of the input
     *         members list. If the input is empty, the output should be an optional containing
     *         an empty {@link StaticGroupMembers}.
     *         <p>
     *         Returns an empty optional if there are any errors looking up entity ID information.
     */
    private Optional<StaticMembers> parseMemberList(
            @Nonnull final CommonDTO.GroupDTO groupDTO,
            @Nonnull final GroupInterpretationContext context) {
        final Map<String, Long> entityIdMap = context.targetEntityidMap();
        /*
         * If a group_name equals uuid of some entity of the same target, this means, that group
         * represents the entity. We do omit such groups, as this is a type of hacks in Legacy.
         * As we do not support group of folder, the group newly created will appear an empty
         * one. This is an example of group, reported for DC
         *
         * entity_type: PHYSICAL_MACHINE
         * display_name: "Datacenter-dc9"
         * group_name: "vsphere-dc9.eng.vmturbo.com-Datacenter-datacenter-21"
         * member_list {
         *     member: "vsphere-dc9.eng.vmturbo.com-Folder-group-n25"
         *     member: "vsphere-dc9.eng.vmturbo.com-Folder-group-s24"
         *     member: "vsphere-dc9.eng.vmturbo.com-Folder-group-v22"
         *     member: "vsphere-dc9.eng.vmturbo.com-Folder-group-h23"
         * }
         */
        final String groupId = GroupProtoUtil.extractId(groupDTO);
        if (entityIdMap.containsKey(groupId)) {
            logger.warn("Skipping group {} as it is represented by entity already", groupId);
            return Optional.empty();
        }

        List<String> memberList = groupDTO.getMemberList().getMemberList();

        final AtomicInteger missingMemberCount = new AtomicInteger(0);
        final Map<MemberType, StaticMemberIds> membersByType = new HashMap<>();
        memberList.forEach(uuid -> {
            final Long discoveredEntityId = entityIdMap.get(uuid);
            if (discoveredEntityId != null) {
                Optional<Entity> entity = entityStore.getEntity(discoveredEntityId);

                if (entity.isPresent()) {
                    // TODO (mahdi) OM-52028 The probe is sending VDC as a member of compute cluster
                    // this is unexpected as we only expect PM to be part of the cluster. And this
                    // will make the computer cluster heterogeneous group which UI does not expect.
                    // This condition is here temporarily here to prevent that from happening until
                    // we investigate why the probe is sending VDC as part of compute cluster.
                    if (GroupProtoUtil.getGroupType(groupDTO) == GroupType.COMPUTE_HOST_CLUSTER &&
                        entity.get().getEntityType() != groupDTO.getEntityType()) {
                        logger.debug("Ignoring entity `{}` since it does not match compute " +
                            "cluster type `{}`.", entity, groupDTO.getGroupType());
                    } else {
                        final MemberType memberType = MemberType.newBuilder()
                            .setEntity(entity.get().getEntityType().getNumber())
                            .build();
                        membersByType.computeIfAbsent(memberType, k -> new StaticMemberIds())
                            .addMemberOid(discoveredEntityId);
                    }
                } else {
                    missingMemberCount.incrementAndGet();
                    logger.warn("Member entity {} not found, thus not added to group {} (uuid: {})",
                            uuid, groupDTO.getDisplayName(), groupId);
                }
            } else {
                if (groupDTO.getGroupType() == GroupType.BUSINESS_ACCOUNT_FOLDER) {
                    // Folders (currently reported by GCP probe) support nesting. However, group
                    // OIDs are generated by Group component, so Topology Processor cannot populate
                    // OIDs for nested folders. Because of that Topology Processor sends unresolved
                    // vendor-specific IDs that are later translated to OIDs by Group component.
                    final MemberType memberType = MemberType.newBuilder()
                            .setGroup(GroupType.BUSINESS_ACCOUNT_FOLDER)
                            .build();
                    membersByType.computeIfAbsent(memberType, k -> new StaticMemberIds())
                            .addUnresolvedMemberId(uuid);
                } else {
                    // If the uuid is not an entity ID, it may refer to another group discovered
                    // by the target. In that case, we expand the group into the list of "leaf"
                    // entities, and add those entities to the group definition.
                    //
                    // Note (roman, March 25 2019): In the future we will want to preserve
                    // the group hierarchies.
                    Optional<Map<MemberType, List<Long>>> groupMembers =
                            tryExpandMemberGroup(uuid, groupDTO, context);
                    if (groupMembers.isPresent()) {
                        // The uuid referred to another one of the discovered groups.
                        groupMembers.get().forEach(((memberType, members) ->
                                membersByType.computeIfAbsent(memberType, k -> new StaticMemberIds())
                                        .addMemberOids(members)));
                    } else {
                        // This may happen if the probe sends members that aren't
                        // discovered (i.e. a bug in the probe) or if the Topology
                        // Processor doesn't record the entities before processing
                        // groups (i.e. a bug in the TP).
                        missingMemberCount.incrementAndGet();
                        logger.warn("Member group {} not found, thus not added to group {} (uuid: {})",
                                uuid, groupDTO.getDisplayName(), groupId);
                    }
                }
            }
        });

        if (missingMemberCount.get() > 0) {
            Metrics.MISSING_MEMBER_COUNT.increment((double)missingMemberCount.get());
            logger.warn("{} members could not be mapped to OIDs when processing group {} (uuid: {})",
                    missingMemberCount.get(), groupDTO.getDisplayName(), groupId);
        }

        // if no members provided (or the members can not be found in EntityStore) and
        // the group entity type is set, we should create an empty
        // StaticMembersByType with this entity type so user can define policy on it, it is
        // currently empty but may have entities of that type in future.
        // Note: now we introduced the heterogeneous group, the entityType field in sdk dto doesn't
        // actually make sense since actual types are based on members types. we may need to make
        // the field repeated so it supports empty group of multiple types
        if (membersByType.isEmpty() && groupDTO.hasEntityType()
                && groupDTO.getEntityType() != EntityType.UNKNOWN) {
            return Optional.of(StaticMembers.newBuilder()
                    .addMembersByType(StaticMembersByType.newBuilder()
                            .setType(MemberType.newBuilder()
                                    .setEntity(groupDTO.getEntityType().getNumber())))
                    .build());
        }

        // add all members for each type
        final StaticMembers.Builder staticMemberBldr = StaticMembers.newBuilder();
        membersByType.forEach(((memberType, members) -> staticMemberBldr.addMembersByType(
                StaticMembersByType.newBuilder()
                        .setType(memberType)
                        .addAllMembers(members.getMemberOids())
                        .addAllMemberLocalId(members.getUnresolvedMemberIds())
        )));
        return Optional.of(staticMemberBldr.build());
    }

    /**
     * If a {@link GroupDTO} contains a member that's not an entity, check if it's another one
     * of the discovered groups.
     *
     * If it is, expand the discovered group and return its members.
     *
     * Note - the process of expanding the group will also interpret it.
     *
     * @param memberId The ID of the member - potentially a group.
     * @param groupDto The parent {@link GroupDTO} - i.e. the group that contains the member group.
     * @param context The {@link GroupInterpretationContext}, containing information about other
     *                discovered groups and their interpretations.
     * @return If memberId is another discovered group, an optional containing the OIDs of it's
     *         entity members (expanding groups recursively).
     *         If memberId is not a discovered group, or if there is some kind of illegal
     *         condition, an empty optional.
     */
    @Nonnull
    private Optional<Map<MemberType, List<Long>>> tryExpandMemberGroup(
            @Nonnull final String memberId,
            @Nonnull final GroupDTO groupDto,
            @Nonnull final GroupInterpretationContext context) {
        final GroupDTO discoveredGroup;
        final GroupDTO noPrefixGroup = context.getGroupsByUuid().get(memberId);
        final String parsedMemberId;
        // We're trying to find if there's a reference to a group that has already been parsed.
        // Checking only for the id would not work for the groups that have been added a prefix.
        // In that case, we need to check if prexif+id exists.
        if (noPrefixGroup == null) {
            //Checking for group with seller prefix
            final GroupDTO sellerPrefixGroup =
                context.getGroupsByUuid().get(GroupProtoUtil.SELLERS_GROUP_ID_PREFIX + memberId);
            if (sellerPrefixGroup == null) {
                // References are valid only to group of sellers. Buyer groups are created at
                // the probe level and they do not represent "real" groups,
                // and thus can't be referenced.
                final GroupDTO buyerPrefixGroup =
                    context.getGroupsByUuid().get(GroupProtoUtil.BUYERS_GROUP_ID_PREFIX + memberId);
                if (buyerPrefixGroup != null) {
                    logger.error("Member (uuid: {}) references to buyer group {}",
                        memberId, groupDto.getDisplayName());
                }
                // No matching discovered group.
                return Optional.empty();
            } else {
                discoveredGroup = sellerPrefixGroup;
                parsedMemberId = GroupProtoUtil.SELLERS_GROUP_ID_PREFIX + memberId;
            }
        } else {
            discoveredGroup = noPrefixGroup;
            parsedMemberId = memberId;
        }

        // We push the "parent" group instead of the member group, because it makes the code
        // simpler. The only reason the pushing exists is for cycle detection, and if the member
        // group has a nested group we'll push the "member" group when we try to expand.
        final String groupId = GroupProtoUtil.extractId(groupDto);
        context.pushVisitedGroup(groupId);
        try {
            // If there is an error (cycle, or exceeding max depth), return empty without even
            // trying to resolve the member group.
            if (context.inCycle()) {
                return Optional.empty();
            } else if (context.maxDepthExceeded()) {
                // If we've exceeded the max depth, we stop expanding the group, and pretend
                // this "chain" of nested groups doesn't resolve to any members.
                Metrics.ERROR_COUNT.labels("max_depth").increment();
                logger.error("Max depth exceeded. Pretending this chain of " +
                    "groups has no members.");
                return Optional.empty();
            }

            final InterpretedGroup interpretedGroup =
                context.computeInterpretedGroup(parsedMemberId,
                    () -> interpretGroup(discoveredGroup, context));
            // We may have detected a cycle while interpreting the member group.
            // If so, we shouldn't include any of that member group's members in this group.
            if (context.inCycle()) {
                return Optional.empty();
            } else {
                final GroupDTO childGroup = interpretedGroup.getOriginalSdkGroup();
                if (childGroup.getMembersCase() != MembersCase.MEMBER_LIST) {
                    Metrics.ERROR_COUNT.labels("non_static_child").increment();
                    logger.warn("Group {} (uuid: {}) has non-static child group {} (uuid: {}). " +
                            "Not currently supported.",
                        groupDto.getDisplayName(), groupId,
                        childGroup.getDisplayName(), GroupProtoUtil.extractId(childGroup));
                }
                return Optional.of(interpretedGroup.getStaticMembersByType());
            }
        } finally {
            context.popVisitedGroup();
        }
    }

    /**
     * The {@link PropertyFilterConverter} is a utility interface to allow mocking of the
     * conversion of {@link SelectionSpec} to {@link PropertyFilter} when testing the
     * {@link DiscoveredGroupInterpreter}.
     */
    @FunctionalInterface
    @VisibleForTesting
    interface PropertyFilterConverter {
        @Nonnull
        Optional<PropertyFilter> selectionSpecToPropertyFilter(
                @Nonnull final SelectionSpec selectionSpec);
    }

    @VisibleForTesting
    static class DefaultPropertyFilterConverter implements PropertyFilterConverter {

        private final Logger logger = LogManager.getLogger();

        @Nonnull
        @Override
        public Optional<PropertyFilter> selectionSpecToPropertyFilter(@Nonnull final SelectionSpec selectionSpec) {
            final PropertyFilter.Builder builder = PropertyFilter.newBuilder();
            builder.setPropertyName(selectionSpec.getProperty());

            switch (selectionSpec.getPropertyValueCase()) {
                case PROPERTY_VALUE_DOUBLE:
                    // Cut off the fractional part.
                    // TODO (roman, Aug 2 2017): For the time being, it's not clear how important it
                    // is to represent the doubles, and adding support for double property
                    // values would require amending the property filter, so deferring that
                    // for now.
                    final long propAsLong = (long)selectionSpec.getPropertyValueDouble();
                    if (Double.compare(propAsLong, selectionSpec.getPropertyValueDouble()) != 0) {
                        logger.warn("Lost information truncating fractional part of the" +
                                "expected property value for a selection spec. Went from {} to {}",
                                selectionSpec.getPropertyValueDouble(), propAsLong);
                    }
                    final NumericFilter.Builder filterBldr = NumericFilter.newBuilder()
                            .setValue(propAsLong);
                    final Optional<ComparisonOperator> op =
                            expressionTypeToOperator(selectionSpec.getExpressionType());
                    if (!op.isPresent()) {
                        logger.warn("Unsupported expression type {} is invalid in the context " +
                                "of a double property value.", selectionSpec.getExpressionType());
                        return Optional.empty();
                    }
                    op.ifPresent(operator -> {
                        filterBldr.setComparisonOperator(operator);
                        builder.setNumericFilter(filterBldr);
                    });
                    break;
                case PROPERTY_VALUE_STRING:
                    switch (selectionSpec.getExpressionType()) {
                        case EQUAL_TO:
                            builder.setStringFilter(StringFilter.newBuilder()
                                    .setStringPropertyRegex(selectionSpec.getPropertyValueString()));
                            break;
                        case NOT_EQUAL_TO:
                            builder.setStringFilter(StringFilter.newBuilder()
                                    .setStringPropertyRegex(selectionSpec.getPropertyValueString())
                                    .setPositiveMatch(false));
                            break;
                        case CONTAINS:
                            builder.setStringFilter(StringFilter.newBuilder()
                                    .setStringPropertyRegex(".*" + selectionSpec.getPropertyValueString() + ".*"));
                            break;
                        case NOT_CONTAINS:
                            builder.setStringFilter(StringFilter.newBuilder()
                                    .setStringPropertyRegex(".*" + selectionSpec.getPropertyValueString() + ".*")
                                    .setPositiveMatch(false));
                            break;
                        case REGEX:
                            builder.setStringFilter(StringFilter.newBuilder()
                                    .setStringPropertyRegex(selectionSpec.getPropertyValueString()));
                            break;
                        default:
                            logger.warn("Unsupported expression type {} in the context of a " +
                                    "string property value.", selectionSpec.getExpressionType());
                            return Optional.empty();
                    }
                    break;
                default:
                    logger.warn("Unhandled property value: {}",
                            selectionSpec.getPropertyValueCase());
                    return Optional.empty();
            }

            return Optional.of(builder.build());
        }

        private Optional<ComparisonOperator> expressionTypeToOperator(ExpressionType expressionType) {
            switch (expressionType) {
                case EQUAL_TO:
                    return Optional.of(ComparisonOperator.EQ);
                case NOT_EQUAL_TO:
                    return Optional.of(ComparisonOperator.NE);
                case LARGER_THAN:
                    return Optional.of(ComparisonOperator.GT);
                case LARGER_THAN_OR_EQUAL_TO:
                    return Optional.of(ComparisonOperator.GTE);
                case SMALLER_THAN:
                    return Optional.of(ComparisonOperator.LT);
                case SMALLER_THAN_OR_EQUAL_TO:
                    return Optional.of(ComparisonOperator.LTE);
                default:
                    logger.warn("Failed to convert expression {} to a comparison operator",
                            expressionType);
                    return Optional.empty();
            }
        }
    }

    /**
     * A context object used when interpreting a list of {@link GroupDTO}s discovered from an
     * SDK target. All interpretations for a single target (i.e. in one call to
     * {@link DiscoveredGroupInterpreter#interpretSdkGroupList(List, long)}) should use a single
     * context.
     *
     * Used to allow group interpretations to access other group interpretations (for
     * group-of-group purposes). Also used to detect cycles in group-of-group interpretation.
     */
    @VisibleForTesting
    static class GroupInterpretationContext {

        private final long targetId;

        /**
         * The groups received from the target, arranged by id (group name).
         * This map is immutable.
         */
        private final Map<String, CommonDTO.GroupDTO> groupsByUuid;

        /**
         * The interpreted groups, arranged by id (group name).
         * This map gets populated during interpretation of the groups.
         */
        private final Map<String, InterpretedGroup> interpretedGroupByUuid;

        /**
         * When interpreting groups of groups, we use this stack to detect cycles when
         * interpreting a single group.
         */
        private final Deque<String> groupChain = new ArrayDeque<>();

        private final Map<String, Long> targetEntityIdMap;

        /**
         * When we detect a cycle, this variable is set to true until all the groups in the
         * cycle are "popped" off the chain. This is so every group in the cycle knows not to
         * expand a particular member.
         */
        private boolean cycleUnwindingInProgress = false;

        @VisibleForTesting
        GroupInterpretationContext(final long targetId,
                                   @Nonnull final List<GroupDTO> dtoList,
                                   @Nonnull Map<String, Long> targetEntityIdMap) {
            this.targetId = targetId;
            this.groupsByUuid = Collections.unmodifiableMap(dtoList.stream()
                .collect(Collectors.toMap(GroupProtoUtil::extractId, Function.identity(),
                    (g1, g2) -> {
                        if (!g1.equals(g2)) {
                            logger.error("Target {} discovered two groups with the same name - {} - " +
                                " and different definitions. Keeping the first.\n" +
                                "First definition: {}\n" +
                                "Second definition: {}\n",
                                targetId,
                                GroupProtoUtil.extractId(g1), printForLog(g1), printForLog(g2));
                        }
                        return g1;
                    })));
            this.interpretedGroupByUuid = new HashMap<>(this.groupsByUuid.size());
            this.targetEntityIdMap = ImmutableMap.copyOf(targetEntityIdMap);
        }

        long targetId() {
            return targetId;
        }

        private String printForLog(@Nonnull final GroupDTO group) {
            try {
                return JsonFormat.printer().print(group);
            } catch (InvalidProtocolBufferException e) {
                return "Cannot print due to error: " + e.getMessage();
            }
        }

        void pushVisitedGroup(@Nonnull final String groupName) {
            final boolean cycle = groupChain.contains(groupName);
            groupChain.push(groupName);
            if (cycle) {
                logger.error("Cycle detected: {}", String.join(" - ", groupChain));
                Metrics.ERROR_COUNT.labels("cycle").increment();
                cycleUnwindingInProgress = true;
            }
        }

        boolean inCycle() {
            return cycleUnwindingInProgress;
        }

        boolean maxDepthExceeded() {
            return groupChain.size() >= MAX_NESTING_DEPTH;
        }

        void popVisitedGroup() {
            groupChain.pop();
            if (groupChain.isEmpty() && cycleUnwindingInProgress) {
                cycleUnwindingInProgress = false;
            }
        }

        @Nonnull
        Map<String, CommonDTO.GroupDTO> getGroupsByUuid() {
            return Collections.unmodifiableMap(groupsByUuid);
        }

        @Nonnull
        Collection<InterpretedGroup> interpretedGroups() {
            return interpretedGroupByUuid.values();
        }

        @Nonnull
        Map<String, Long> targetEntityidMap() {
            return targetEntityIdMap;
        }

        /**
         * If a group with the ID has already been interpreted in this context, return the group.
         * Otherwise, use the provided supplier to interpret the group.
         * Similar to the {@link Map#computeIfAbsent(Object, Function)} method.
         *
         * @param groupId The ID of the group.
         * @param groupSupplier The supplier of {@link InterpretedGroup}s.
         * @return The {@link InterpretedGroup} in the context.
         */
        @Nonnull
        InterpretedGroup computeInterpretedGroup(
                @Nonnull final String groupId,
                @Nonnull Supplier<InterpretedGroup> groupSupplier) {
            // Note - we can't use "computeIfAbsent" here, because it may end up recursively
            // calling itself again, and nested "computeIfAbsent" calls either lead to really
            // weird bugs, or (after JDK 9) throw ConcurrentModification exceptions.
            // See: https://bugs.openjdk.java.net/browse/JDK-8172951
            final InterpretedGroup alreadyInterpretedGroup = interpretedGroupByUuid.get(groupId);
            if (alreadyInterpretedGroup == null) {
                final InterpretedGroup newInterpretedGroup = groupSupplier.get();
                interpretedGroupByUuid.putIfAbsent(groupId, newInterpretedGroup);
                return newInterpretedGroup;
            } else {
                return alreadyInterpretedGroup;
            }
        }
    }

    private static class Metrics {
         private static final DataMetricCounter DISCOVERED_GROUP_COUNT = DataMetricCounter.builder()
             .withName("tp_discovered_group_count")
             .withHelp("The number of groups discovered by the topology processor.")
             .withLabelNames("type")
             .build()
             .register();

        private static final DataMetricCounter MISSING_MEMBER_COUNT = DataMetricCounter.builder()
             .withName("tp_discovered_group_missing_member_count")
             .withHelp("The number of members of discovered groups that could not be resolved.")
             .build()
             .register();

        private static final DataMetricCounter ERROR_COUNT = DataMetricCounter.builder()
            .withName("tp_discovered_group_error_count")
            .withHelp("The number of errors encountered during discovered group processing.")
            .withLabelNames("type")
            .build()
            .register();
    }

    private static class StaticMemberIds {
        private final Set<Long> memberOids = new HashSet<>();
        private final Set<String> unresolvedMemberIds = new HashSet<>();

        void addMemberOid(final long oid) {
            memberOids.add(oid);
        }

        void addMemberOids(final Collection<Long> oids) {
            memberOids.addAll(oids);
        }

        void addUnresolvedMemberId(@Nonnull final String id) {
            unresolvedMemberIds.add(id);
        }

        Set<Long> getMemberOids() {
            return memberOids;
        }

        Set<String> getUnresolvedMemberIds() {
            return unresolvedMemberIds;
        }
    }
}
