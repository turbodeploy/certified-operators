package com.vmturbo.topology.processor.group.discovery;

import static com.vmturbo.topology.processor.group.discovery.DiscoveredGroupUploader.VC_FOLDER_KEYWORD;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;

import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo.Type;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.SearchParametersCollection;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticGroupMembers;
import com.vmturbo.common.protobuf.search.Search.ComparisonOperator;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.NumericFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.StringFilter;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.ConstraintType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.MembersCase;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.SelectionSpec;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.SelectionSpec.ExpressionType;
import com.vmturbo.topology.processor.entity.Entity;
import com.vmturbo.topology.processor.entity.EntityStore;

/**
 * The {@link DiscoveredGroupInterpreter} is responsible for providing conversion functions
 * from {@link CommonDTO.GroupDTO} to all possible representations of that group in the
 * XL system - most notably groups, clusters, and policies.
 */
class DiscoveredGroupInterpreter {

    private final Logger logger = LogManager.getLogger();

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
        return dtoList.stream()
            // Filter out folders, because we're currently not supporting mapping
            // folders to groups in XL. In VC, folders can be identified by the group name.
            // Filter out also groups with empty displayName, because they will not be visible in
            // the UI anyway, and can cause discrepancies
            .filter(groupDTO -> groupDTO.hasDisplayName() && (!groupDTO.hasGroupName() ||
                    !groupDTO.getGroupName().contains(VC_FOLDER_KEYWORD)))
            .map(dto -> {
                if (isCluster(dto)) {
                    return new InterpretedGroup(dto, Optional.empty(), sdkToCluster(dto, targetId));
                } else {
                    return new InterpretedGroup(dto, sdkToGroup(dto, targetId), Optional.empty());
                }
            })
            .collect(Collectors.toList());
    }

    private boolean isCluster(@Nonnull final CommonDTO.GroupDTO sdkDTO) {
        // A cluster must have a specific constraint type.
        return sdkDTO.hasConstraintInfo() &&
                sdkDTO.getConstraintInfo().getConstraintType().equals(ConstraintType.CLUSTER);
    }

    /**
     * Attempt to convert a {@link CommonDTO.GroupDTO} to a {@link ClusterInfo}, if it meets the
     * required criteria to be treated as a cluster in the XL system.
     *
     * @param sdkDTO The {@link CommonDTO.GroupDTO}.
     * @param targetId The ID of the target that discovered the {@link CommonDTO.GroupDTO}.
     * @return An optional containing the {@link ClusterInfo}, or an empty optional if the
     *         {@link CommonDTO.GroupDTO} does not represent a cluster.
     */
    @VisibleForTesting
    @Nonnull
    Optional<ClusterInfo.Builder> sdkToCluster(@Nonnull final CommonDTO.GroupDTO sdkDTO,
                                               final long targetId) {
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
        builder.setName(sdkDTO.getDisplayName());
        final Optional<StaticGroupMembers> parsedMembersOpt =
                parseMemberList(sdkDTO, targetId);
        if (parsedMembersOpt.isPresent()) {
            // There may not be any members, but we allow empty clusters.
            builder.setMembers(parsedMembersOpt.get());
        } else {
            logger.warn("Unable to parse cluster member list: {}", sdkDTO.getMemberList());
            return Optional.empty();
        }
        return Optional.of(builder);
    }

    /**
     * Attempt to convert a {@link CommonDTO.GroupDTO} to a {@link GroupInfo} describing a group
     * in the XL system. This should happen after an attempt for
     * {@link DiscoveredGroupInterpreter#sdkToCluster(GroupDTO, long)}.
     *
     * @param sdkDTO The {@link CommonDTO.GroupDTO} discovered by the target.
     * @param targetId The ID of the target that discovered the group.
     * @return An optional containing the {@link GroupInfo}, or an empty optional if the conversion
     *         is not successful.
     */
    @VisibleForTesting
    @Nonnull
    Optional<GroupInfo.Builder> sdkToGroup(@Nonnull final CommonDTO.GroupDTO sdkDTO,
                                           final long targetId) {
        final GroupInfo.Builder builder = GroupInfo.newBuilder();
        builder.setEntityType(sdkDTO.getEntityType().getNumber());
        builder.setName(sdkDTO.hasGroupName()
                        ? sdkDTO.getGroupName()
                        : sdkDTO.getConstraintInfo().getConstraintName());

        switch (sdkDTO.getInfoCase()) {
            case GROUP_NAME:
                // What to do with the group name? Is it important?
                logger.info("Got GroupDTO with group name {}", sdkDTO.getGroupName());
                break;
            case CONSTRAINT_INFO:
                // Mapping constraints to policies is done in DiscoveredGroupUploader
                break;
            default:
                logger.error("Unhandled SDK group information case: {}", sdkDTO.getInfoCase());
                return Optional.empty();
        }

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
                        parseMemberList(sdkDTO, targetId);
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

        // TODO: What to do with entityProperties?
        logger.info("Skipping entity properties: {}", sdkDTO.getEntityPropertiesList());

        return Optional.of(builder);
    }

    /**
     * Converts an SDK {@link CommonDTO.GroupDTO.MembersList} to the equivalent
     * {@link StaticGroupMembers}. The most important part of that is looking up entity
     * identifiers, since the input members list has UUID's as reported by the probe, and
     * the output members list has OIDs.
     *
     * @param groupDTO The {@link CommonDTO.GroupDTO}
     * @param targetId The target that discovered the group containing the members.
     * @return An optional containing the {@link StaticGroupMembers} equivalent of the input
     *         members list. If the input is empty, the output should be an optional containing
     *         an empty {@link StaticGroupMembers}.
     *         <p>
     *         Returns an empty optional if there are any errors looking up entity ID information.
     */
    private Optional<StaticGroupMembers> parseMemberList(
            @Nonnull final CommonDTO.GroupDTO groupDTO,
            final long targetId) {

        final Optional<Map<String, Long>> idMapOpt =
                entityStore.getTargetEntityIdMap(targetId);
        StaticGroupMembers retMembers = null;
        if (!idMapOpt.isPresent()) {
            logger.warn("No entity ID map available for target {}", targetId);
        } else {
            Map<String, Long> idMap = idMapOpt.get();
            final StaticGroupMembers.Builder staticMemberBldr =
                    StaticGroupMembers.newBuilder();
            final AtomicInteger missingMemberCount = new AtomicInteger(0);
            groupDTO.getMemberList().getMemberList()
                    .stream()
                    // Filter out folders, because we're currently not supporting mapping
                    // folders to groups in XL. Some groups have folder members - we
                    // don't want to take those into account.
                    .filter(uuid -> !uuid.contains(VC_FOLDER_KEYWORD))
                    .map(idMap::get)
                    .map(Optional::ofNullable)
                    .forEach(optOid -> {
                        if (optOid.isPresent()) {
                            Optional<Entity> entity = entityStore.getEntity(optOid.get());
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
                                    staticMemberBldr.addStaticMemberOids(optOid.get());
                                } else {
                                    logger.warn("EntityType: {} and groupType: {} doesn't match for oid: {}"
                                        + ". Not adding to the group/cluster members list for groupName: {}, " +
                                        " groupDisplayName: {}.",
                                        entity.get().getEntityType(), groupDTO.getEntityType(), optOid.get(),
                                        groupDTO.hasGroupName() ?
                                            groupDTO.getGroupName() :
                                            groupDTO.getConstraintInfo().getConstraintName(),
                                        groupDTO.getDisplayName());
                                }
                            }
                         } else {
                            // This may happen if the probe sends members that aren't
                            // discovered (i.e. a bug in the probe) or if the Topology
                            // Processor doesn't record the entities before processing
                            // groups (i.e. a bug in the TP).
                            missingMemberCount.incrementAndGet();
                        }
                    });
            if (missingMemberCount.get() == 0) {
                retMembers = staticMemberBldr.build();
            } else {
                logger.warn("Failed to find static group members in member list: {}",
                        groupDTO.getMemberList().getMemberList());
            }
        }
        return Optional.ofNullable(retMembers);
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
                                    .setMatch(false));
                            break;
                        case CONTAINS:
                            builder.setStringFilter(StringFilter.newBuilder()
                                    .setStringPropertyRegex(".*" + selectionSpec.getPropertyValueString() + ".*"));
                            break;
                        case NOT_CONTAINS:
                            builder.setStringFilter(StringFilter.newBuilder()
                                    .setStringPropertyRegex(".*" + selectionSpec.getPropertyValueString() + ".*")
                                    .setMatch(false));
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
}
