package com.vmturbo.topology.processor.group.discovery;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;

import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo.Type;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.SearchParametersCollection;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticGroupMembers;
import com.vmturbo.common.protobuf.search.Search.ComparisonOperator;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.NumericFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.StringFilter;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.tag.Tag.Tags;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.ConstraintType;
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

    private boolean validGroup(@Nonnull final CommonDTO.GroupDTO group) {
        try {
            GroupProtoUtil.extractId(group);
            return true;
        } catch (IllegalArgumentException e) {
            Metrics.ERROR_COUNT.labels("invalid_group_dto").increment();
            logger.error("Invalid GroupDTO: {}", group, e);
            return false;
        }
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
            .filter(this::validGroup)
            .collect(Collectors.toList());
        final GroupInterpretationContext context =
            new GroupInterpretationContext(targetId, legalDtoList);
        legalDtoList.forEach(group -> {
            // We check to see if we need to interpret this particular group, because the
            // interpretation of a previous group that contains this one may have already triggered
            // the interpretation of this one. Re-interpreting it would be wasteful.
            context.computeInterpretedGroup(GroupProtoUtil.extractId(group), () -> interpretGroup(group, context));
        });
        return new ArrayList<>(context.interpretedGroups());
    }

    /**
     * Attempt to convert a {@link CommonDTO.GroupDTO} to a {@link ClusterInfo}, if it meets the
     * required criteria to be treated as a cluster in the XL system.
     *
     * @param sdkDTO The {@link CommonDTO.GroupDTO}.
     * @return An optional containing the {@link ClusterInfo}, or an empty optional if the
     *         {@link CommonDTO.GroupDTO} does not represent a cluster.
     */
    @VisibleForTesting
    @Nonnull
    Optional<ClusterInfo.Builder> sdkToCluster(@Nonnull final CommonDTO.GroupDTO sdkDTO,
                                               final GroupInterpretationContext context) {
        // Clusters must be statically-configured.
        if (!isCluster(sdkDTO) || !sdkDTO.getMembersCase().equals(MembersCase.MEMBER_LIST)) {
            return Optional.empty();
        }

        // We only support clusters of storages and physical machines.
        if (!(sdkDTO.getEntityType().equals(EntityType.PHYSICAL_MACHINE) ||
                sdkDTO.getEntityType().equals(EntityType.STORAGE))) {
            logger.warn("Unexpected cluster entity type: {}", sdkDTO.getEntityType());
            return Optional.empty();
        }

        final ClusterInfo.Builder builder = ClusterInfo.newBuilder();
        builder.setClusterType(sdkDTO.getEntityType().equals(EntityType.PHYSICAL_MACHINE)
                ? Type.COMPUTE : Type.STORAGE);
        builder.setName(GroupProtoUtil.extractId(sdkDTO));
        builder.setDisplayName(GroupProtoUtil.extractDisplayName(sdkDTO));

        final Optional<StaticGroupMembers> parsedMembersOpt =
                parseMemberList(sdkDTO, context);
        if (parsedMembersOpt.isPresent()) {
            // There may not be any members, but we allow empty clusters.
            builder.setMembers(parsedMembersOpt.get());
        } else {
            logger.warn("Unable to parse cluster member list: {}", sdkDTO.getMemberList());
            return Optional.empty();
        }

        final Tags.Builder tagsBuilder = Tags.newBuilder();
        SdkToTopologyEntityConverter.extractTags(sdkDTO.getEntityPropertiesList()).entrySet()
                .forEach(e -> tagsBuilder.putTags(e.getKey(), e.getValue().build()));
        builder.setTags(tagsBuilder.build());

        return Optional.of(builder);
    }

    /**
     * Attempt to convert a {@link CommonDTO.GroupDTO} to a {@link GroupInfo} describing a group
     * in the XL system. This should happen after an attempt for
     * {@link DiscoveredGroupInterpreter#sdkToCluster(GroupDTO, GroupInterpretationContext)}.
     *
     * @param sdkDTO The {@link CommonDTO.GroupDTO} discovered by the target.
     * @return An optional containing the {@link GroupInfo}, or an empty optional if the conversion
     *         is not successful.
     */
    @VisibleForTesting
    @Nonnull
    Optional<GroupInfo.Builder> sdkToGroup(@Nonnull final CommonDTO.GroupDTO sdkDTO,
                           @Nonnull final GroupInterpretationContext context) {
        final GroupInfo.Builder builder = GroupInfo.newBuilder();
        builder.setEntityType(sdkDTO.getEntityType().getNumber());
        builder.setName(GroupProtoUtil.extractId(sdkDTO));
        builder.setDisplayName(GroupProtoUtil.extractDisplayName(sdkDTO));

        switch (sdkDTO.getMembersCase()) {
            case SELECTION_SPEC_LIST:
                final SearchParameters.Builder searchParametersBuilder = SearchParameters.newBuilder();
                final AtomicInteger successCount = new AtomicInteger(0);
                sdkDTO.getSelectionSpecList().getSelectionSpecList().stream()
                        .map(propertyFilterConverter::selectionSpecToPropertyFilter)
                        .filter(Optional::isPresent)
                        .map(Optional::get)
                        .forEach(propFilter -> {
                            successCount.incrementAndGet();
                            if (searchParametersBuilder.hasStartingFilter()) {
                                searchParametersBuilder.addSearchFilter(SearchFilter.newBuilder()
                                        .setPropertyFilter(propFilter));
                            } else {
                                searchParametersBuilder.setStartingFilter(propFilter);
                            }
                        });

                if (sdkDTO.getSelectionSpecList().getSelectionSpecList().size() ==
                        successCount.get()) {
                    builder.setSearchParametersCollection(SearchParametersCollection.newBuilder()
                            .addSearchParameters(searchParametersBuilder));
                } else {
                    logger.warn("Failed to translate all selection specs.");
                    return Optional.empty();
                }
                break;
            case MEMBER_LIST:
                final Optional<StaticGroupMembers> members =
                        parseMemberList(sdkDTO, context);
                if (members.isPresent()) {
                    builder.setStaticGroupMembers(members.get());
                } else {
                    return Optional.empty();
                }
                break;
            default:
                logger.warn("Unhandled members type: {}", sdkDTO.getMembersCase());
                return Optional.empty();
        }

        final Tags.Builder tagsBuilder = Tags.newBuilder();
        SdkToTopologyEntityConverter.extractTags(sdkDTO.getEntityPropertiesList()).entrySet()
                .forEach(e -> tagsBuilder.putTags(e.getKey(), e.getValue().build()));
        builder.setTags(tagsBuilder.build());

        return Optional.of(builder);
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
    private Optional<StaticGroupMembers> parseMemberList(
            @Nonnull final CommonDTO.GroupDTO groupDTO,
            @Nonnull final GroupInterpretationContext context) {
        final Optional<Map<String, Long>> idMapOpt =
                entityStore.getTargetEntityIdMap(context.targetId);
        if (!idMapOpt.isPresent()) {
            logger.warn("No entity ID map available for target {}", context.targetId);
            return Optional.empty();
        } else {
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
            final Map<String, Long> idMap = idMapOpt.get();
            final String groupId = GroupProtoUtil.extractId(groupDTO);
            if (idMap.containsKey(groupId)) {
                logger.debug("Skipping group {} as it is represented by entity already", groupId);
                return Optional.empty();
            }
            final StaticGroupMembers.Builder staticMemberBldr =
                    StaticGroupMembers.newBuilder();
            final AtomicInteger missingMemberCount = new AtomicInteger(0);
            groupDTO.getMemberList().getMemberList()
                .forEach(uuid -> {
                    final Long discoveredEntityId = idMap.get(uuid);
                    if (discoveredEntityId != null) {
                        Optional<Entity> entity = entityStore.getEntity(discoveredEntityId);
                        if (entity.isPresent()) {
                            // Validate that the member entityType is the
                            // same as that of the group/cluster.
                            // So far, the only known case of
                            // entityType!=entitypesOfMembers is in
                            // VCenter Clusters where a VDC is part of
                            // the Cluster along with the PhysicalMachine
                            // entities. After checking with Dmitry
                            // Illichev, this was done in legacy to
                            // accomodate license feature - to model vdcs as
                            // folders - and he thinks that this is now obsolete.
                            // So the assumption that
                            // GroupEntityType==EntityTypesOfItsMembers should be
                            // fine as this is also the assumption made in
                            // the UI. Even the Group protobuf message is defined
                            // with this assumption.
                            if (entity.get().getEntityType() == groupDTO.getEntityType()) {
                                staticMemberBldr.addStaticMemberOids(discoveredEntityId);
                            } else {
                                logger.warn("EntityType: {} and groupType: {} doesn't match for oid: {}"
                                        + ". Not adding to the group/cluster members list for groupId: {}, " +
                                        " groupDisplayName: {}.",
                                    entity.get().getEntityType(), groupDTO.getEntityType(), discoveredEntityId,
                                    groupId, groupDTO.getDisplayName());
                            }
                        }
                    } else {
                        // If the uuid is not an entity ID, it may refer to another group discovered
                        // by the target. In that case, we expand the group into the list of "leaf"
                        // entities, and add those entities to the group definition.
                        //
                        // Note (roman, March 25 2019): In the future we will want to preserve
                        // the group hierarchies.
                        final Optional<List<Long>> groupMembers = tryExpandMemberGroup(uuid, groupDTO, context);
                        if (groupMembers.isPresent()) {
                            // The uuid referred to another one of the discovered groups.
                            staticMemberBldr.addAllStaticMemberOids(groupMembers.get());
                        } else {
                            // This may happen if the probe sends members that aren't
                            // discovered (i.e. a bug in the probe) or if the Topology
                            // Processor doesn't record the entities before processing
                            // groups (i.e. a bug in the TP).
                            missingMemberCount.incrementAndGet();
                        }
                    }
                });
            if (missingMemberCount.get() > 0) {
                Metrics.MISSING_MEMBER_COUNT.increment((double)missingMemberCount.get());
                logger.warn("{} members could not be mapped to OIDs when processing group {} (uuid: {})",
                    missingMemberCount.get(), groupDTO.getDisplayName(), groupId);
            }
            return Optional.of(staticMemberBldr.build());
        }
    }

    @Nonnull
    private InterpretedGroup interpretGroup(@Nonnull final GroupDTO group,
                                            @Nonnull GroupInterpretationContext context) {
        if (isCluster(group)) {
            Metrics.DISCOVERED_GROUP_COUNT.labels(Group.Type.CLUSTER.name()).increment();
            return new InterpretedGroup(group, Optional.empty(), sdkToCluster(group, context));
        } else {
            Metrics.DISCOVERED_GROUP_COUNT.labels(Group.Type.GROUP.name()).increment();
            return new InterpretedGroup(group, sdkToGroup(group, context), Optional.empty());
        }
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
    private Optional<List<Long>> tryExpandMemberGroup(
        @Nonnull final String memberId,
        @Nonnull final GroupDTO groupDto,
        @Nonnull final GroupInterpretationContext context) {
        final GroupDTO discoveredGroup = context.getGroupsByUuid().get(memberId);
        if (discoveredGroup == null) {
            return Optional.empty();
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
                context.computeInterpretedGroup(memberId, () -> interpretGroup(discoveredGroup, context));
            // We may have detected a cycle while interpreting the member group.
            // If so, we shouldn't include any of that member group's members in this group.
            if (context.inCycle()) {
                return Optional.empty();
            } else {
                final GroupDTO childGroup = interpretedGroup.getOriginalSdkGroup();
                if (groupDto.getEntityType() != childGroup.getEntityType()) {
                    Metrics.ERROR_COUNT.labels("entity_type_mismatch").increment();
                    logger.error("Group {} (uuid: {}, entity type: {}) has child group " +
                            "{} (uuid: {}) with a different entity type: {}. Removing child group.",
                        groupDto.getDisplayName(), groupId, groupDto.getEntityType(),
                        childGroup.getDisplayName(), GroupProtoUtil.extractId(childGroup), childGroup.getEntityType());
                    return Optional.empty();
                } else {
                    if (childGroup.getMembersCase() != MembersCase.MEMBER_LIST) {
                        Metrics.ERROR_COUNT.labels("non_static_child").increment();
                        logger.warn("Group {} (uuid: {}) has non-static child group {} (uuid: {}). " +
                                "Not currently supported.",
                            groupDto.getDisplayName(), groupId,
                            childGroup.getDisplayName(), GroupProtoUtil.extractId(childGroup));
                    }
                    return Optional.of(interpretedGroup.getStaticMembers());
                }
            }
        } finally {
            context.popVisitedGroup();
        }
    }

    private boolean isCluster(@Nonnull final CommonDTO.GroupDTO sdkDTO) {
        // A cluster must have a specific constraint type.
        return sdkDTO.hasConstraintInfo() &&
            sdkDTO.getConstraintInfo().getConstraintType().equals(ConstraintType.CLUSTER);
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

        /**
         * When we detect a cycle, this variable is set to true until all the groups in the
         * cycle are "popped" off the chain. This is so every group in the cycle knows not to
         * expand a particular member.
         */
        private boolean cycleUnwindingInProgress = false;

        @VisibleForTesting
        GroupInterpretationContext(final long targetId,
                                   @Nonnull final List<GroupDTO> dtoList) {
            this.targetId = targetId;
            this.groupsByUuid = Collections.unmodifiableMap(dtoList.stream()
                .collect(Collectors.toMap(GroupProtoUtil::extractId, Function.identity())));
            this.interpretedGroupByUuid = new HashMap<>(this.groupsByUuid.size());
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
}
