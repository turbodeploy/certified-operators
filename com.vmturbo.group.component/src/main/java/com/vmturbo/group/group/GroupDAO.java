package com.vmturbo.group.group;

import static com.vmturbo.group.db.tables.GroupDiscoverTargets.GROUP_DISCOVER_TARGETS;
import static com.vmturbo.group.db.tables.GroupExpectedMembersEntities.GROUP_EXPECTED_MEMBERS_ENTITIES;
import static com.vmturbo.group.db.tables.GroupExpectedMembersGroups.GROUP_EXPECTED_MEMBERS_GROUPS;
import static com.vmturbo.group.db.tables.GroupStaticMembersEntities.GROUP_STATIC_MEMBERS_ENTITIES;
import static com.vmturbo.group.db.tables.GroupStaticMembersGroups.GROUP_STATIC_MEMBERS_GROUPS;
import static com.vmturbo.group.db.tables.GroupTags.GROUP_TAGS;
import static com.vmturbo.group.db.tables.Grouping.GROUPING;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.gson.reflect.TypeToken;
import com.google.protobuf.InvalidProtocolBufferException;

import io.grpc.Status;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Query;
import org.jooq.Record1;
import org.jooq.Record2;
import org.jooq.Record3;
import org.jooq.Result;
import org.jooq.TableRecord;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.EntityFilters;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.GroupFilters;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.OptimizationMetadata;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.SelectionCriteriaCase;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType.TypeCase;
import com.vmturbo.common.protobuf.group.GroupDTO.Origin;
import com.vmturbo.common.protobuf.group.GroupDTO.Origin.Type;
import com.vmturbo.common.protobuf.group.GroupDTO.OriginFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers.StaticMembersByType;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.MapFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.StringFilter;
import com.vmturbo.common.protobuf.search.SearchableProperties;
import com.vmturbo.common.protobuf.tag.Tag.TagValuesDTO;
import com.vmturbo.common.protobuf.tag.Tag.Tags;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.common.diagnostics.Diagnosable;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.group.common.DuplicateNameException;
import com.vmturbo.group.db.Tables;
import com.vmturbo.group.db.tables.pojos.GroupDiscoverTargets;
import com.vmturbo.group.db.tables.pojos.GroupExpectedMembersEntities;
import com.vmturbo.group.db.tables.pojos.GroupExpectedMembersGroups;
import com.vmturbo.group.db.tables.pojos.GroupStaticMembersEntities;
import com.vmturbo.group.db.tables.pojos.GroupStaticMembersGroups;
import com.vmturbo.group.db.tables.pojos.GroupTags;
import com.vmturbo.group.db.tables.pojos.Grouping;
import com.vmturbo.group.db.tables.records.GroupDiscoverTargetsRecord;
import com.vmturbo.group.db.tables.records.GroupingRecord;
import com.vmturbo.group.identity.IdentityProvider;
import com.vmturbo.group.service.StoreOperationException;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.platform.sdk.common.util.Pair;
import com.vmturbo.proactivesupport.DataMetricCounter;

/**
 * DAO implementing {@link IGroupStore} - CRUD operations with groups.
 */
public class GroupDAO implements IGroupStore, Diagnosable {

    private static final String GET_LABEL = "get";

    private static final String CREATE_LABEL = "create";

    private static final String UPDATE_DISCOVERED_LABEL = "update_discovered";

    private static final String UPDATE_LABEL = "update";

    private static final String DELETE_LABEL = "delete";

    private static final DataMetricCounter GROUP_STORE_ERROR_COUNT = DataMetricCounter.builder()
            .withName("group_store_error_count")
            .withHelp("Number of errors encountered in operating the group store.")
            .withLabelNames("operation")
            .build()
            .register();

    private static final DataMetricCounter GROUP_STORE_DUPLICATE_NAME_COUNT =
            DataMetricCounter.builder()
                    .withName("group_store_duplicate_name_count")
                    .withHelp("Number of duplicate name attempts in operating the group store.")
                    .build()
                    .register();

    private static final Logger logger = LogManager.getLogger();

    private final IdentityProvider identityProvider;

    private final DSLContext dslContext;

    private final Collection<Consumer<Long>> deleteCallbacks = new CopyOnWriteArrayList<>();

    /**
     * Constructs group DAO.
     *
     * @param dslContext DB context to execute SQL operations on
     * @param identityProvider identity provider to fetch groups' new OIDs.
     */
    public GroupDAO(@Nonnull final DSLContext dslContext,
            @Nonnull final IdentityProvider identityProvider) {
        this.dslContext = Objects.requireNonNull(dslContext);
        this.identityProvider = Objects.requireNonNull(identityProvider);
    }

    @Override
    public long createGroup(@Nonnull Origin origin,
            @Nonnull GroupDefinition groupDefinition, @Nonnull Set<MemberType> expectedMemberTypes,
            boolean supportReverseLookup) throws StoreOperationException {
        final Grouping pojo = createPojoForNewGroup(origin, groupDefinition, supportReverseLookup);
        try {
            createGroup(dslContext, pojo, groupDefinition, expectedMemberTypes);
        } catch (DataAccessException e) {
            GROUP_STORE_ERROR_COUNT.labels(CREATE_LABEL).increment();
            if (e.getCause() instanceof DuplicateNameException) {
                GROUP_STORE_DUPLICATE_NAME_COUNT.increment();
            }
            throw e;
        }
        return pojo.getId();
    }

    private void createGroup(@Nonnull DSLContext context, @Nonnull Grouping groupPojo,
            @Nonnull GroupDefinition groupDefinition, @Nonnull Set<MemberType> expectedMembers)
            throws StoreOperationException {
        validateStaticMembers(context,
                Collections.singleton(groupDefinition.getStaticGroupMembers()),
                Collections.singletonMap(groupPojo.getId(), groupPojo.getGroupType()));
        final Collection<Long> sameNameGroups = context.select(GROUPING.ID)
                .from(GROUPING)
                .where(GROUPING.ORIGIN_DISCOVERED_SRC_ID.isNull())
                .and(GROUPING.DISPLAY_NAME.eq(groupPojo.getDisplayName()))
                .fetch()
                .stream()
                .map(Record1::value1)
                .collect(Collectors.toList());
        if (!sameNameGroups.isEmpty()) {
            throw new StoreOperationException(Status.ALREADY_EXISTS,
                    "Cannot create object with name " + groupPojo.getDisplayName() +
                            " because an object with the same name and type (id: " +
                            sameNameGroups + ") already exists.");
        }
        context.newRecord(GROUPING, groupPojo).store();
        final Collection<TableRecord<?>> inserts = new ArrayList<>();
        inserts.addAll(
                insertGroupDefinitionDependencies(context, groupPojo.getId(), groupDefinition));
        inserts.addAll(
                insertExpectedMembers(context, groupPojo.getId(), new HashSet<>(expectedMembers),
                        groupDefinition.getStaticGroupMembers()));
        context.batchInsert(inserts).execute();
    }

    @Nonnull
    private Grouping createPojoForNewGroup(@Nonnull Origin origin,
            @Nonnull GroupDefinition groupDefinition, boolean supportReverseLookup)
            throws StoreOperationException {
        final Grouping pojo = createGroupFromDefinition(groupDefinition);
        pojo.setId(identityProvider.next());
        pojo.setSupportsMemberReverseLookup(supportReverseLookup);
        switch (origin.getCreationOriginCase()) {
            case SYSTEM:
                requireTrue(origin.getSystem().hasDescription(),
                        "Description must be specified for system group " + pojo.getDisplayName());
                pojo.setOriginSystemDescription(origin.getSystem().getDescription());
                break;
            case USER:
                requireTrue(origin.getUser().hasUsername(),
                        "User name must be specified for system group " + pojo.getDisplayName());
                pojo.setOriginUserCreator(origin.getUser().getUsername());
                break;
            default:
                throw new StoreOperationException(Status.INVALID_ARGUMENT,
                        "Invalid origin " + origin.getCreationOriginCase() +
                                " passed to create a group");
        }
        return pojo;
    }

    private Grouping createGroupFromDefinition(@Nonnull GroupDefinition groupDefinition)
            throws StoreOperationException {
        final Grouping groupPojo = new Grouping();
        groupPojo.setGroupType(groupDefinition.getType());
        requireTrue(groupDefinition.hasDisplayName(), "Group display name not set");
        groupPojo.setDisplayName(groupDefinition.getDisplayName());
        groupPojo.setIsHidden(groupDefinition.getIsHidden());
        if (groupDefinition.hasOwner()) {
            groupPojo.setOwnerId(groupDefinition.getOwner());
        }
        switch (groupDefinition.getSelectionCriteriaCase()) {
            case ENTITY_FILTERS:
                groupPojo.setEntityFilters(groupDefinition.getEntityFilters().toByteArray());
                break;
            case GROUP_FILTERS:
                groupPojo.setGroupFilters(groupDefinition.getGroupFilters().toByteArray());
                break;
            case STATIC_GROUP_MEMBERS:
                break;
            default:
                throw new StoreOperationException(Status.INVALID_ARGUMENT,
                        "Group " + groupDefinition.getDisplayName() +
                                " does not have any recognized selection criteria (" +
                                groupDefinition.getSelectionCriteriaCase() + ")");
        }
        if (groupDefinition.hasOptimizationMetadata()) {
            final OptimizationMetadata metadata = groupDefinition.getOptimizationMetadata();
            groupPojo.setOptimizationEnvironmentType(metadata.getEnvironmentType());
            groupPojo.setOptimizationIsGlobalScope(metadata.getIsGlobalScope());
        }
        return groupPojo;
    }

    private Collection<TableRecord<?>> insertGroupDefinitionDependencies(
            @Nonnull DSLContext context, long groupId, @Nonnull GroupDefinition groupDefinition) {
        final Collection<TableRecord<?>> records = new ArrayList<>();
        if (groupDefinition.getSelectionCriteriaCase() ==
                SelectionCriteriaCase.STATIC_GROUP_MEMBERS) {
            records.addAll(insertGroupStaticMembers(context, groupId,
                    groupDefinition.getStaticGroupMembers()));
        }
        records.addAll(insertTags(context, groupDefinition.getTags(), groupId));
        return records;
    }

    private Collection<TableRecord<?>> insertTags(@Nonnull DSLContext context, @Nonnull Tags tags,
            long groupId) {
        final Collection<TableRecord<?>> result = new ArrayList<>();
        for (Entry<String, TagValuesDTO> entry : tags.getTagsMap().entrySet()) {
            for (String tagValue : entry.getValue().getValuesList()) {
                final GroupTags tag = new GroupTags(groupId, entry.getKey(), tagValue);
                result.add(
                        context.newRecord(com.vmturbo.group.db.tables.GroupTags.GROUP_TAGS, tag));
            }
        }
        return result;
    }

    private Collection<TableRecord<?>> insertGroupStaticMembers(@Nonnull DSLContext context,
            long groupId, @Nonnull StaticMembers staticMembers) {
        final Collection<TableRecord<?>> records = new ArrayList<>();
        for (StaticMembersByType staticMember : staticMembers.getMembersByTypeList()) {
            if (staticMember.getType().getTypeCase() == TypeCase.GROUP) {
                for (Long memberId : staticMember.getMembersList()) {
                    final GroupStaticMembersGroups groupChild =
                            new GroupStaticMembersGroups(groupId, memberId);
                    records.add(context.newRecord(GROUP_STATIC_MEMBERS_GROUPS, groupChild));
                }
            } else {
                for (Long memberId : staticMember.getMembersList()) {
                    final GroupStaticMembersEntities entityChild =
                            new GroupStaticMembersEntities(groupId,
                                    staticMember.getType().getEntity(), memberId);
                    records.add(context.newRecord(GROUP_STATIC_MEMBERS_ENTITIES, entityChild));
                }
            }
        }
        return records;
    }

    private Collection<TableRecord<?>> insertExpectedMembers(@Nonnull DSLContext context,
            long groupId, @Nonnull Set<MemberType> memberTypes,
            @Nullable StaticMembers staticMembers) throws StoreOperationException {
        final Collection<TableRecord<?>> records = new ArrayList<>();
        final Set<MemberType> directMembers;
        if (staticMembers != null) {
            directMembers = staticMembers.getMembersByTypeList()
                    .stream()
                    .map(StaticMembersByType::getType)
                    .collect(Collectors.toSet());
        } else {
            directMembers = Collections.emptySet();
        }
        if (!memberTypes.containsAll(directMembers)) {
            throw new StoreOperationException(Status.INVALID_ARGUMENT,
                    "Group " + groupId + " declared expected members  " + memberTypes.stream()
                            .map(memberType -> memberType.hasGroup() ?
                                    memberType.getGroup().toString() :
                                    Integer.toString(memberType.getEntity()))
                            .collect(Collectors.joining(",", "[", "]")) +
                            " does not contain all the direct members: " +
                            staticMembers.getMembersByTypeList()
                                    .stream()
                                    .map(StaticMembersByType::getType)
                                    .map(memberType -> memberType.hasGroup() ?
                                            memberType.getGroup().toString() :
                                            Integer.toString(memberType.getEntity()))
                                    .collect(Collectors.joining(",", "[", "]")));
        }
        for (MemberType memberType : memberTypes) {
            final boolean directMember = directMembers.contains(memberType);
            if (memberType.getTypeCase() == TypeCase.GROUP) {
                final GroupExpectedMembersGroups groupMember =
                        new GroupExpectedMembersGroups(groupId, memberType.getGroup(),
                                directMember);
                records.add(context.newRecord(GROUP_EXPECTED_MEMBERS_GROUPS, groupMember));
            } else {
                final GroupExpectedMembersEntities entityMember =
                        new GroupExpectedMembersEntities(groupId, memberType.getEntity(),
                                directMember);
                records.add(context.newRecord(GROUP_EXPECTED_MEMBERS_ENTITIES, entityMember));
            }
        }
        return records;
    }

    private Collection<GroupDiscoverTargetsRecord> createTargetForGroupRecords(
            @Nonnull DSLContext context, long groupId, @Nonnull Set<Long> targets) {
        final Collection<GroupDiscoverTargetsRecord> result = new ArrayList<>();
        for (Long sharedTarget : targets) {
            final GroupDiscoverTargets otherTargetLink =
                    new GroupDiscoverTargets(groupId, sharedTarget);
            result.add(context.newRecord(
                    com.vmturbo.group.db.tables.GroupDiscoverTargets.GROUP_DISCOVER_TARGETS,
                    otherTargetLink));
        }
        return result;
    }

    /**
     * Get the existing groups in DB and create a mapping from group identifying key to group oid.
     * The identifying key is created as that defined in
     * {@link GroupProtoUtil#createIdentifyingKey(GroupType, String)}.
     *
     * @param context the DB context to use
     * @return map from group identifying key to group oid.
     */
    @Nonnull
    private Map<String, Long> getExistingGroupKeys(@Nonnull DSLContext context) {
        final Result<Record3<Long, String, GroupType>> result =
                context.select(GROUPING.ID, GROUPING.ORIGIN_DISCOVERED_SRC_ID, GROUPING.GROUP_TYPE)
                        .from(GROUPING)
                        .where(GROUPING.ORIGIN_DISCOVERED_SRC_ID.isNotNull())
                        .fetch();
        return result.stream()
                .collect(Collectors.toMap(record ->
                        GroupProtoUtil.createIdentifyingKey(record.value3(), record.value2()),
                        Record3::value1));
    }

    /**
     * Method performs validation of static members.
     *
     * @param context DB context to use
     * @param members static members collection to validate
     * @param newGroups new or updated group types.
     * @throws StoreOperationException if one of static member references is invalid
     */
    private void validateStaticMembers(@Nonnull DSLContext context,
            @Nonnull Collection<StaticMembers> members, @Nonnull Map<Long, GroupType> newGroups)
            throws StoreOperationException {
        final Set<Long> referencedIds = members.stream()
                .map(StaticMembers::getMembersByTypeList)
                .flatMap(Collection::stream)
                .filter(member -> member.hasType() && member.getType().hasGroup())
                .map(StaticMembersByType::getMembersList)
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
        final Set<Long> searchOids = new HashSet<>(referencedIds);
        searchOids.removeAll(newGroups.keySet());
        final Map<Long, GroupType> groupTypes = new HashMap<>(newGroups);
        context.select(GROUPING.ID, GROUPING.GROUP_TYPE)
                .from(GROUPING)
                .where(GROUPING.ID.in(searchOids))
                .fetch()
                .forEach(record -> groupTypes.put(record.value1(), record.value2()));
        for (StaticMembers staticMember : members) {
            for (StaticMembersByType membersByType : staticMember.getMembersByTypeList()) {
                if (membersByType.getType().hasGroup()) {
                    final GroupType requested = membersByType.getType().getGroup();
                    for (Long groupOid : membersByType.getMembersList()) {
                        final GroupType realType = groupTypes.get(groupOid);
                        if (realType == null) {
                            throw new StoreOperationException(Status.INVALID_ARGUMENT,
                                    "Wrong reference to an absent group from static members oid=" +
                                            groupOid);
                        }
                        if (requested != realType) {
                            throw new StoreOperationException(Status.INVALID_ARGUMENT,
                                    "Group definition contains reference to group " + groupOid +
                                            " of type " + requested + " while its real type is " +
                                            realType);
                        }
                    }
                }
            }
        }
    }

    @Nonnull
    @Override
    public Optional<GroupDTO.Grouping> getGroup(long groupId) {
        try {
            return dslContext.transactionResult(configuration -> {
                final DSLContext transactionContext = DSL.using(configuration);
                return getGroupInternal(transactionContext, groupId);
            });
        } catch (DataAccessException e) {
            GROUP_STORE_ERROR_COUNT.labels(GET_LABEL).increment();
            throw e;
        }
    }

    @Nonnull
    private Optional<GroupDTO.Grouping> getGroupInternal(@Nonnull DSLContext context, long groupId) {
        final List<Grouping> groupings = context.selectFrom(GROUPING)
                .where(GROUPING.ID.eq(groupId))
                .fetchInto(Grouping.class);
        if (groupings.isEmpty()) {
            return Optional.empty();
        }
        if (groupings.size() > 1) {
            throw new RuntimeException("Unexpected duplicated groups with the same ID " + groupId);
        }
        final Grouping grouping = groupings.iterator().next();
        final GroupDTO.Grouping.Builder builder = GroupDTO.Grouping.newBuilder();
        final Map<MemberType, Boolean> expectedMembers = getExpectedMemberTypes(context, groupId);
        builder.setId(groupId);
        builder.addAllExpectedTypes(expectedMembers.keySet());
        builder.setSupportsMemberReverseLookup(grouping.getSupportsMemberReverseLookup());
        builder.setOrigin(getGroupOrigin(context, grouping));
        final GroupDefinition.Builder defBuilder = GroupDefinition.newBuilder();
        defBuilder.setType(grouping.getGroupType());
        defBuilder.setDisplayName(grouping.getDisplayName());
        defBuilder.setIsHidden(grouping.getIsHidden());
        if (grouping.getOwnerId() != null) {
            defBuilder.setOwner(grouping.getOwnerId());
        }
        getOptimizationMetadata(grouping).ifPresent(defBuilder::setOptimizationMetadata);
        // optimization metadata
        // selection criteria
        defBuilder.setTags(getGroupTags(context, groupId));
        try {
            if (grouping.getEntityFilters() != null) {
                defBuilder.setEntityFilters(EntityFilters.parseFrom(grouping.getEntityFilters()));
            } else if (grouping.getGroupFilters() != null) {
                defBuilder.setGroupFilters(GroupFilters.parseFrom(grouping.getGroupFilters()));
            } else {
                // If a group does not have any members, we still fill the StaticMembers
                // field in order to show that the group is a static one
                final StaticMembers staticMembers =
                        getStaticMembersMessage(context, groupId, expectedMembers).orElse(
                                StaticMembers.getDefaultInstance());
                defBuilder.setStaticGroupMembers(staticMembers);
            }
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException(
                    "Failed to parse dynamic selection criteria for group " + groupId, e);
        }
        builder.setDefinition(defBuilder);
        return Optional.of(builder.build());
    }

    @Nonnull
    private Optional<OptimizationMetadata> getOptimizationMetadata(@Nonnull Grouping grouping) {
        if (grouping.getOptimizationIsGlobalScope() != null &&
                grouping.getOptimizationEnvironmentType() != null) {
            final OptimizationMetadata.Builder metadataBuilder = OptimizationMetadata.newBuilder();
            if (grouping.getOptimizationEnvironmentType() != null) {
                metadataBuilder.setEnvironmentType(grouping.getOptimizationEnvironmentType());
            }
            if (grouping.getOptimizationIsGlobalScope() != null) {
                metadataBuilder.setIsGlobalScope(grouping.getOptimizationIsGlobalScope());
            }
            return Optional.of(metadataBuilder.build());
        } else {
            return Optional.empty();
        }
    }

    @Nonnull
    private Map<MemberType, Boolean> getExpectedMemberTypes(@Nonnull DSLContext context,
            long groupId) {
        final List<Record2<Integer, Boolean>> expectedMembersEntities =
                context.select(GROUP_EXPECTED_MEMBERS_ENTITIES.ENTITY_TYPE,
                        GROUP_EXPECTED_MEMBERS_ENTITIES.DIRECT_MEMBER)
                        .from(GROUP_EXPECTED_MEMBERS_ENTITIES)
                        .where(GROUP_EXPECTED_MEMBERS_ENTITIES.GROUP_ID.eq(groupId))
                        .fetch();
        final List<Record2<GroupType, Boolean>> expectedMembersGroups =
                context.select(GROUP_EXPECTED_MEMBERS_GROUPS.GROUP_TYPE,
                        GROUP_EXPECTED_MEMBERS_GROUPS.DIRECT_MEMBER)
                        .from(GROUP_EXPECTED_MEMBERS_GROUPS)
                        .where(GROUP_EXPECTED_MEMBERS_GROUPS.GROUP_ID.eq(groupId))
                        .fetch();
        final Map<MemberType, Boolean> result = new HashMap<>();
        for (Record2<Integer, Boolean> member : expectedMembersEntities) {
            final MemberType memberType =
                    MemberType.newBuilder().setEntity(member.value1()).build();
            result.put(memberType, member.value2());
        }
        for (Record2<GroupType, Boolean> member : expectedMembersGroups) {
            final MemberType memberType =
                    MemberType.newBuilder().setGroup(member.value1()).build();
            result.put(memberType, member.value2());
        }
        return result;
    }

    @Nonnull
    private Origin getGroupOrigin(@Nonnull DSLContext context, @Nonnull Grouping group) {
        if (group.getOriginSystemDescription() != null) {
            return Origin.newBuilder()
                    .setSystem(Origin.System.newBuilder()
                            .setDescription(group.getOriginSystemDescription()))
                    .build();
        } else if (group.getOriginUserCreator() != null) {
            return Origin.newBuilder()
                    .setUser(Origin.User.newBuilder().setUsername(group.getOriginUserCreator()))
                    .build();
        } else if (group.getDisplayName() != null) {
            final List<Record1<Long>> targets = context.select(GROUP_DISCOVER_TARGETS.TARGET_ID)
                    .from(GROUP_DISCOVER_TARGETS)
                    .where(GROUP_DISCOVER_TARGETS.GROUP_ID.eq(group.getId()))
                    .fetch();
            final Origin.Discovered.Builder builder = Origin.Discovered.newBuilder();
            targets.stream().map(Record1::value1).forEach(builder::addDiscoveringTargetId);
            builder.setSourceIdentifier(group.getOriginDiscoveredSrcId());
            return Origin.newBuilder().setDiscovered(builder).build();
        } else {
            throw new RuntimeException("Unknown origin for the group " + group.getId());
        }
    }

    @Nonnull
    @Override
    public Pair<Set<Long>, Set<Long>> getStaticMembers(long groupId) {
        return getStaticMembers(dslContext, groupId);
    }

    @Nonnull
    private Pair<Set<Long>, Set<Long>> getStaticMembers(@Nonnull DSLContext context, long groupId) {
        final List<Record1<Long>> staticMembersEntities =
                context.select(GROUP_STATIC_MEMBERS_ENTITIES.ENTITY_ID)
                        .from(GROUP_STATIC_MEMBERS_ENTITIES)
                        .where(GROUP_STATIC_MEMBERS_ENTITIES.GROUP_ID.eq(groupId))
                        .fetch();
        final List<Record1<Long>> staticMembersGroups =
                context.select(GROUP_STATIC_MEMBERS_GROUPS.CHILD_GROUP_ID)
                        .from(GROUP_STATIC_MEMBERS_GROUPS)
                        .where(GROUP_STATIC_MEMBERS_GROUPS.PARENT_GROUP_ID.eq(groupId))
                        .fetch();
        final Set<Long> entitiesMembers =
                staticMembersEntities.stream().map(Record1::value1).collect(Collectors.toSet());
        final Set<Long> groupMembers =
                staticMembersGroups.stream().map(Record1::value1).collect(Collectors.toSet());
        return Pair.create(entitiesMembers, groupMembers);
    }

    @Nonnull
    private Optional<StaticMembers> getStaticMembersMessage(@Nonnull DSLContext context,
            long groupId, @Nonnull Map<MemberType, Boolean> membersTypes) {
        final Set<MemberType> expectedDirectTypes = membersTypes.entrySet()
                .stream()
                .filter(Entry::getValue)
                .map(Entry::getKey)
                .collect(Collectors.toSet());
        final List<Record2<Integer, Long>> staticMembersEntities =
                context.select(GROUP_STATIC_MEMBERS_ENTITIES.ENTITY_TYPE,
                        GROUP_STATIC_MEMBERS_ENTITIES.ENTITY_ID)
                        .from(GROUP_STATIC_MEMBERS_ENTITIES)
                        .where(GROUP_STATIC_MEMBERS_ENTITIES.GROUP_ID.eq(groupId))
                        .fetch();
        final List<Record2<Long, GroupType>> staticMembersGroups =
                context.select(GROUP_STATIC_MEMBERS_GROUPS.CHILD_GROUP_ID, GROUPING.GROUP_TYPE)
                        .from(GROUP_STATIC_MEMBERS_GROUPS)
                        .join(GROUPING)
                        .on(GROUP_STATIC_MEMBERS_GROUPS.CHILD_GROUP_ID.eq(GROUPING.ID))
                        .where(GROUP_STATIC_MEMBERS_GROUPS.PARENT_GROUP_ID.eq(groupId))
                        .fetch();
        final Map<MemberType, Set<Long>> staticMembers = new HashMap<>();
        for (Record2<Integer, Long> record : staticMembersEntities) {
            final MemberType type = MemberType.newBuilder().setEntity(record.value1()).build();
            final long member = record.value2();
            staticMembers.computeIfAbsent(type, key -> new HashSet<>()).add(member);
        }
        for (Record2<Long, GroupType> record : staticMembersGroups) {
            final MemberType type = MemberType.newBuilder().setGroup(record.value2()).build();
            final long member = record.value1();
            staticMembers.computeIfAbsent(type, key -> new HashSet<>()).add(member);
        }
        // We fill in the empty collections to create a StaticMembersByType record for every
        // expected direct member.
        for (MemberType memberType: expectedDirectTypes) {
            if (!staticMembers.containsKey(memberType)) {
                staticMembers.put(memberType, Collections.emptySet());
            }
        }
        final StaticMembers.Builder resultBuilder = StaticMembers.newBuilder();
        for (Entry<MemberType, Set<Long>> entry : staticMembers.entrySet()) {
            resultBuilder.addMembersByType(StaticMembersByType.newBuilder()
                    .setType(entry.getKey())
                    .addAllMembers(entry.getValue())
                    .build());
        }
        return Optional.of(resultBuilder.build());
    }

    @Nonnull
    private Tags getGroupTags(@Nonnull DSLContext context, long groupId) {
        final List<Record2<String, String>> tags =
                context.select(GROUP_TAGS.TAG_KEY, GROUP_TAGS.TAG_VALUE)
                        .from(GROUP_TAGS)
                        .where(GROUP_TAGS.GROUP_ID.eq(groupId))
                        .fetch();
        final Map<String, List<Record2<String, String>>> grouppedTags =
                tags.stream().collect(Collectors.groupingBy(Record2::value1));
        final Tags.Builder builder = Tags.newBuilder();
        for (Entry<String, List<Record2<String, String>>> entry : grouppedTags.entrySet()) {
            final String tagKey = entry.getKey();
            final TagValuesDTO.Builder tagValue = TagValuesDTO.newBuilder();
            entry.getValue().stream().map(Record2::value2).forEach(tagValue::addValues);
            builder.putTags(tagKey, tagValue.build());
        }
        return builder.build();
    }

    @Nonnull
    @Override
    public GroupDTO.Grouping updateGroup(long groupId, @Nonnull GroupDefinition groupDefinition,
            @Nonnull Set<MemberType> supportedMemberTypes, boolean supportReverseLookups)
            throws StoreOperationException {
        final Grouping pojo = createGroupFromDefinition(groupDefinition);
        pojo.setId(groupId);
        pojo.setSupportsMemberReverseLookup(supportReverseLookups);
        try {
            updateGroup(dslContext, pojo, groupDefinition, supportedMemberTypes);
            return getGroupInternal(dslContext, groupId)
                .orElseThrow(() -> new StoreOperationException(Status.INTERNAL, "Cannot find the " +
                    "updated group."));
        } catch (DataAccessException e) {
            GROUP_STORE_ERROR_COUNT.labels(UPDATE_LABEL).increment();
            throw e;
        }
    }

    private void updateGroup(@Nonnull DSLContext context, @Nonnull Grouping group,
            @Nonnull GroupDefinition groupDefinition, @Nonnull Set<MemberType> expectedMemberTypes)
            throws StoreOperationException {
        final long groupId = group.getId();
        final List<Record3<String, String, String>> result =
                context.select(GROUPING.ORIGIN_SYSTEM_DESCRIPTION, GROUPING.ORIGIN_USER_CREATOR,
                        GROUPING.ORIGIN_DISCOVERED_SRC_ID)
                        .from(GROUPING)
                        .where(GROUPING.ID.eq(groupId))
                        .fetch();
        if (result.isEmpty()) {
            throw new StoreOperationException(Status.NOT_FOUND, "Group " + groupId + " not found");
        }
        if (result.size() > 1) {
            throw new RuntimeException(
                    "Unexpected query result size " + result.size() + " for group with id " +
                            groupId + ". Must be PK violation");
        }
        final Record3<String, String, String> record = result.get(0);
        if (record.value3() != null) {
            throw new StoreOperationException(Status.INVALID_ARGUMENT,
                    "Attempt to modify immutable discovered group " + groupId + " display name " +
                            groupDefinition.getDisplayName());
        }
        final int countWithTheSameName = context.selectCount()
                .from(GROUPING)
                .where(GROUPING.ORIGIN_DISCOVERED_SRC_ID.isNull()
                        .and(GROUPING.DISPLAY_NAME.eq(groupDefinition.getDisplayName()))
                        .and(GROUPING.ID.ne(groupId)))
                .fetchOne()
                .value1();
        if (countWithTheSameName > 0) {
            throw new StoreOperationException(Status.ALREADY_EXISTS,
                    "Cannot create object with name " + groupDefinition.getDisplayName() +
                            " because an object with the same name and type (id: " + groupId +
                            ") already exists.");
        }
        validateStaticMembers(context,
                Collections.singleton(groupDefinition.getStaticGroupMembers()),
                Collections.singletonMap(group.getId(), group.getGroupType()));

        cleanGroupChildTables(context, groupId);
        final Collection<TableRecord<?>> children = new ArrayList<>();
        children.addAll(insertGroupDefinitionDependencies(context, groupId, groupDefinition));
        children.addAll(insertExpectedMembers(context, groupId, expectedMemberTypes,
                groupDefinition.getStaticGroupMembers()));

        // Set the values that don't get updated as a part of update
        group.setOriginSystemDescription(record.value1());
        group.setOriginUserCreator(record.value2());

        createGroupUpdate(context, group).execute();
        context.batchInsert(children).execute();
    }

    @Nonnull
    @Override
    public Collection<GroupDTO.Grouping> getGroups(@Nonnull GroupDTO.GroupFilter filter) {
        final Condition sqlCondition = createGroupCondition(filter);
        final Set<Long> groupingIds = dslContext.select(GROUPING.ID)
                .from(GROUPING)
                .where(sqlCondition)
                .fetch()
                .stream()
                .map(Record1::value1)
                .collect(Collectors.toSet());
        final Collection<Long> postSqlFiltered = filterPostSQL(dslContext, groupingIds, filter);
        return postSqlFiltered.stream()
                .map(id -> getGroupInternal(dslContext, id))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toSet());
    }

    /**
     * This method creates SQL conditions for the group filter. The only condition that could not
     * be applied in SQL is DISPLAY_NAME case-sensitive regexp search.
     *
     * @param filter filter to convert to SQL condition
     * @return SQL condition
     */
    @Nonnull
    private Condition createGroupCondition(@Nonnull GroupDTO.GroupFilter filter) {
        final Collection<Condition> conditions = new ArrayList<>();
        conditions.addAll(createPropertyFiltersCondition(filter.getPropertyFiltersList()));
        if (filter.hasGroupType()) {
            conditions.add(GROUPING.GROUP_TYPE.eq(filter.getGroupType()));
        }
        if (!filter.getIncludeHidden()) {
            conditions.add(GROUPING.IS_HIDDEN.ne(true));
        }
        if (filter.hasOriginFilter()) {
            final OriginFilter originFilter = filter.getOriginFilter();
            if (originFilter.getOriginCount() == 0) {
                throw new IllegalArgumentException(
                        "Origin filters without any origins specified: " + filter);
            }
            createOriginFilter(originFilter).ifPresent(conditions::add);
        }
        if (filter.getIdCount() > 0) {
            conditions.add(createIdFilter(filter.getIdList()));
        }
        final Optional<Condition> resultCondition = combineConditions(conditions, Condition::and);
        return resultCondition.orElse(DSL.noCondition());
    }

    /**
     * This method performs post-SQL filtering. The only filter to apply here is a case-sensitive
     * regexp search for DISPLAY_NAME. See {@link #createGroupCondition(GroupDTO.GroupFilter)}.
     * Both methods together must implement all the search criterias.
     *
     * @param context transactional context
     * @param groupIds group ids to check
     * @param filter filter to apply
     * @return filtered group OIDs. If there is nothing to apply, just return {@code groupIds}
     */
    @Nonnull
    private Collection<Long> filterPostSQL(@Nonnull DSLContext context, @Nonnull Set<Long> groupIds,
            @Nonnull GroupDTO.GroupFilter filter) {
        if (filter.getPropertyFiltersCount() == 0) {
            return groupIds;
        }
        final Optional<PropertyFilter> propDisplayName = filter.getPropertyFiltersList()
                .stream()
                .filter(flt -> flt.getPropertyName().equals(SearchableProperties.DISPLAY_NAME))
                .findAny();
        if (!propDisplayName.isPresent()) {
            return groupIds;
        }
        if (!propDisplayName.get().hasStringFilter()) {
            throw new IllegalArgumentException(
                    "StringFilter must be present for DisplayName search: " + filter);
        }
        final StringFilter stringFilter = propDisplayName.get().getStringFilter();
        if (!stringFilter.hasStringPropertyRegex()) {
            return groupIds;
        }
        if (!stringFilter.hasCaseSensitive() || !stringFilter.getCaseSensitive()) {
            return groupIds;
        }
        final Pattern pattern = Pattern.compile(stringFilter.getStringPropertyRegex());
        final Collection<Record2<Long, String>> records =
                context.select(GROUPING.ID, GROUPING.DISPLAY_NAME)
                        .from(GROUPING)
                        .where(GROUPING.ID.in(groupIds))
                        .fetch();
        final Set<Long> result = new HashSet<>();
        for (Record2<Long, String> record : records) {
            final String displayName = record.value2();
            if (pattern.matcher(displayName).matches()) {
                result.add(record.value1());
            }
        }
        return result;
    }

    @Nonnull
    private Collection<Condition> createPropertyFiltersCondition(
            @Nonnull Collection<PropertyFilter> propertyFilters) {
        if (propertyFilters.isEmpty()) {
            return Collections.emptyList();
        }
        final Collection<Condition> allConditions = new ArrayList<>();
        for (PropertyFilter propertyFilter: propertyFilters) {
            if (propertyFilter.getPropertyName().equals(SearchableProperties.DISPLAY_NAME)) {
                createDisplayNameSearchCondition(propertyFilter).ifPresent(allConditions::add);
            } else if (propertyFilter.getPropertyName().equals(StringConstants.TAGS_ATTR)) {
                allConditions.add(createTagsSearchCondition(propertyFilter));
            } else if (propertyFilter.getPropertyName().equals(StringConstants.OID)) {
                allConditions.add(createOidCondition(propertyFilter, GROUPING.ID));
            } else if (propertyFilter.getPropertyName().equals(StringConstants.ACCOUNTID)) {
                allConditions.add(createOidCondition(propertyFilter, GROUPING.OWNER_ID));
            }
        }
        return allConditions;
    }

    @Nonnull
    private Optional<Condition> createDisplayNameSearchCondition(
            @Nonnull PropertyFilter propertyFilter) {
        if (!propertyFilter.hasStringFilter()) {
            throw new IllegalArgumentException("Filter for display name must have StringFilter");
        }
        final StringFilter filter = propertyFilter.getStringFilter();
        if (filter.hasStringPropertyRegex()) {
            if (!filter.hasCaseSensitive() || !filter.getCaseSensitive()) {
                if (filter.getPositiveMatch()) {
                    return Optional.of(GROUPING.DISPLAY_NAME.likeRegex(filter.getStringPropertyRegex()));
                } else {
                    return Optional.of(GROUPING.DISPLAY_NAME.notLikeRegex(filter.getStringPropertyRegex()));
                }
            } else {
                return Optional.empty();
            }
        } else if (filter.getOptionsCount() != 0) {
            final Optional<Condition> condition;
            if (filter.hasCaseSensitive() && filter.getCaseSensitive()) {
                condition = combineConditions(filter.getOptionsList()
                        .stream()
                        .map(GROUPING.DISPLAY_NAME::contains)
                        .collect(Collectors.toSet()), Condition::or);
            } else {
                condition = combineConditions(filter.getOptionsList()
                        .stream()
                        .map(GROUPING.DISPLAY_NAME::containsIgnoreCase)
                        .collect(Collectors.toSet()), Condition::or);
            }
            if (!filter.hasPositiveMatch() || filter.getPositiveMatch()) {
                return condition;
            } else {
                return condition.map(Condition::not);
            }
        } else {
            throw new IllegalArgumentException(
                    "Neither regexp nor options specified in the string filter: " + propertyFilter);
        }
    }

    @Nonnull
    private Condition createTagsSearchCondition(@Nonnull PropertyFilter propertyFilter) {
        if (!propertyFilter.hasMapFilter()) {
            throw new IllegalArgumentException(
                    "MapFilter is expected for " + StringConstants.TAGS_ATTR + " filter: " +
                            propertyFilter);
        }
        final MapFilter filter = propertyFilter.getMapFilter();
        final String key = filter.getKey();
        if (key == null) {
            throw new IllegalArgumentException(
                    "Property filter has no key specified: " + propertyFilter);
        }
        if (filter.getValuesCount() == 0 && !filter.hasRegex()) {
            throw new IllegalArgumentException(
                    "Either regexp or values must be specified in MapFilter: " + propertyFilter);
        }
        if (filter.getValuesCount() > 0 && filter.hasRegex()) {
            throw new IllegalArgumentException(
                    "Only one of regexp or values could be specified in MapFilter: " +
                            propertyFilter);
        }
        final Condition tagValuesCondition;
        if (filter.hasRegex()) {
            final String regexp = filter.getRegex();
            if (filter.getPositiveMatch()) {
                tagValuesCondition = GROUP_TAGS.TAG_VALUE.likeRegex(regexp);
            } else {
                tagValuesCondition = GROUP_TAGS.TAG_VALUE.notLikeRegex(regexp);
            }
        } else {
            final Collection<String> values = filter.getValuesList();
            if (filter.getPositiveMatch()) {
                tagValuesCondition = GROUP_TAGS.TAG_VALUE.in(values);
            } else {
                tagValuesCondition = GROUP_TAGS.TAG_VALUE.notIn(values);
            }
        }
        return GROUPING.ID.in(DSL.select(GROUP_TAGS.GROUP_ID)
                .from(GROUP_TAGS)
                .where(GROUP_TAGS.TAG_KEY.eq(key).and(tagValuesCondition)));
    }

    @Nonnull
    private Condition createOidCondition(@Nonnull PropertyFilter filter,
            @Nonnull Field<Long> fieldToCheck) {
        if (!filter.hasStringFilter()) {
            throw new IllegalArgumentException(
                    "String filter is expected for property " + filter.getPropertyName() + ": " +
                            filter);
        }
        final StringFilter stringFilter = filter.getStringFilter();
        if (stringFilter.getOptionsCount() == 0) {
            throw new IllegalArgumentException(
                    "No options found for filter of " + filter.getPropertyName() + ": " + filter);
        }
        final Set<Long> ids = new HashSet<>(stringFilter.getOptionsCount());
        for (String option: stringFilter.getOptionsList()) {
            if (!NumberUtils.isDigits(option)) {
                throw new IllegalArgumentException(
                        "Illegal " + filter.getPropertyName() + " format \"" + option +
                                "\" in filter: " + filter);
            }
            ids.add(Long.valueOf(option));
        }
        if (!stringFilter.hasPositiveMatch() || stringFilter.getPositiveMatch()) {
            return fieldToCheck.in(ids);
        } else {
            return fieldToCheck.notIn(ids);
        }
    }

    @Nonnull
    private Optional<Condition> createOriginFilter(@Nonnull OriginFilter originFilter) {
        final Collection<Condition> conditions = new ArrayList<>();
        for (Origin.Type originType : EnumSet.copyOf(originFilter.getOriginList())) {
            switch (originType) {
                case USER:
                    conditions.add(GROUPING.ORIGIN_USER_CREATOR.isNotNull());
                    break;
                case SYSTEM:
                    conditions.add(GROUPING.ORIGIN_SYSTEM_DESCRIPTION.isNotNull());
                    break;
                case DISCOVERED:
                    conditions.add(GROUPING.ORIGIN_DISCOVERED_SRC_ID.isNotNull());
                    break;
                default:
                    throw new UnsupportedOperationException(
                            "Origin type " + originType + " is not supported");
            }
        }
        return combineConditions(conditions, Condition::or);
    }

    @Nonnull
    private Condition createIdFilter(@Nonnull List<Long> groupIds) {
        return GROUPING.ID.in(groupIds);
    }

    @Nonnull
    private Optional<Condition> combineConditions(@Nonnull Collection<Condition> conditions,
            @Nonnull BiFunction<Condition, Condition, Condition> combineFunction) {
        Condition result = null;
        for (Condition condition : conditions) {
            if (result == null) {
                result = condition;
            } else {
                result = combineFunction.apply(result, condition);
            }
        }
        return Optional.ofNullable(result);
    }

    @Override
    public void deleteGroup(long groupId) throws StoreOperationException {
        try {
            deleteGroup(dslContext, groupId);
        } catch (DataAccessException e) {
            GROUP_STORE_ERROR_COUNT.labels(DELETE_LABEL).increment();
            throw e;
        }
        deleteCallbacks.forEach(callback -> callback.accept(groupId));
    }

    private void deleteGroup(@Nonnull DSLContext context, long groupId)
            throws StoreOperationException {
        final Collection<Record1<String>> discoveredSrcIds =
                context.select(GROUPING.ORIGIN_DISCOVERED_SRC_ID)
                        .from(GROUPING)
                        .where(GROUPING.ID.eq(groupId))
                        .fetch();
        if (discoveredSrcIds.isEmpty()) {
            throw new StoreOperationException(Status.NOT_FOUND,
                    "Group with id " + groupId + " not found. Cannot delete");
        }
        final String discoveredSrcId = discoveredSrcIds.iterator().next().value1();
        if (discoveredSrcId != null) {
            throw new StoreOperationException(Status.INVALID_ARGUMENT,
                    "Discovered group " + groupId + " attempted to be removed");
        }
        cleanGroupChildTables(context, groupId);
        context.deleteFrom(GROUPING).where(GROUPING.ID.eq(groupId)).execute();
    }

    @Nonnull
    @Override
    public Map<String, Long> updateDiscoveredGroups(@Nonnull Collection<DiscoveredGroup> groups)
            throws StoreOperationException {
        return updateDiscoveredGroups(dslContext, groups, srcId -> identityProvider.next());
    }

    /**
     * Adds or updates discovered groups in the appliance.
     *
     * @param context DB connection context
     * @param groups groups to be a new set of discovered groups in the store.
     * @param idGenerator function to assign OIDs to the groups. It receives
     *         discoveredSourceId and produces a unique OID
     * @return map of discovered source id to OID for the new groups.
     * @throws StoreOperationException if group configuration is incorrect
     */
    @Nonnull
    private Map<String, Long> updateDiscoveredGroups(@Nonnull DSLContext context,
            @Nonnull Collection<DiscoveredGroup> groups,
            @Nonnull Function<String, Long> idGenerator) throws StoreOperationException {
        final Map<String, Long> existingGroups = getExistingGroupKeys(context);
        validateDiscoveredGroups(context, existingGroups, groups);

        context.deleteFrom(GROUPING)
                .where(GROUPING.ORIGIN_DISCOVERED_SRC_ID.isNotNull()
                        .and(GROUPING.ID.notIn(existingGroups.values())))
                .execute();
        cleanDiscoveredGroupsChildTables(context);
        final Map<String, Long> result = new HashMap<>();
        final Collection<TableRecord<?>> inserts = new ArrayList<>();
        final Collection<GroupingRecord> newGroups = new ArrayList<>();
        final Collection<Query> updates = new ArrayList<>();
        for (DiscoveredGroup group : groups) {
            requireTrue(!group.getSourceIdentifier().isEmpty(), "Source identifier must be set");
            final String sourceIdentifier = group.getSourceIdentifier();
            final GroupDefinition def = group.getDefinition();
            final String identifyingKey = GroupProtoUtil.createIdentifyingKey(def.getType(),
                    sourceIdentifier);
            final Long existingId = existingGroups.get(identifyingKey);
            final long effectiveId;
            if (existingId == null) {
                effectiveId = idGenerator.apply(sourceIdentifier);
            } else {
                effectiveId = existingId;
            }
            final Grouping groupPojo = createGroupFromDefinition(def);
            groupPojo.setId(effectiveId);
            groupPojo.setSupportsMemberReverseLookup(group.isReverseLookupSupported());
            groupPojo.setOriginDiscoveredSrcId(sourceIdentifier);
            if (existingId == null) {
                newGroups.add(context.newRecord(GROUPING, groupPojo));
            } else {
                updates.add(createGroupUpdate(context, groupPojo));
            }
            inserts.addAll(insertGroupDefinitionDependencies(context, effectiveId, def));
            inserts.addAll(insertExpectedMembers(context, groupPojo.getId(),
                    new HashSet<>(group.getExpectedMembers()),
                    group.getDefinition().getStaticGroupMembers()));
            inserts.addAll(createTargetForGroupRecords(context, effectiveId, group.getTargetIds()));
            result.put(identifyingKey, effectiveId);
        }
        context.batch(updates).execute();
        context.batchInsert(newGroups).execute();
        context.batchInsert(inserts).execute();
        return Collections.unmodifiableMap(result);
    }

    private void validateDiscoveredGroups(@Nonnull DSLContext context,
            @Nonnull Map<String, Long> existingGroups,
            @Nonnull Collection<DiscoveredGroup> discoveredGroups) throws StoreOperationException {
        final List<StaticMembers> members = discoveredGroups.stream()
                .map(DiscoveredGroup::getDefinition)
                .map(GroupDefinition::getStaticGroupMembers)
                .collect(Collectors.toList());

        final Map<Long, GroupType> groupTypes = new HashMap<>();
        for (DiscoveredGroup discoveredGroup : discoveredGroups) {
            final GroupType groupType = discoveredGroup.getDefinition().getType();
            final Long oid = existingGroups.get(GroupProtoUtil.createIdentifyingKey(groupType,
                    discoveredGroup.getSourceIdentifier()));
            if (oid != null) {
                groupTypes.put(oid, groupType);
            }
        }
        validateStaticMembers(context, members, groupTypes);
    }

    /**
     * Method cleans all the child records for all the discovered groups in the database.
     *
     * @param context DB context to use.
     */
    private void cleanDiscoveredGroupsChildTables(@Nonnull DSLContext context) {
        context.deleteFrom(GROUP_STATIC_MEMBERS_GROUPS)
                .where(GROUP_STATIC_MEMBERS_GROUPS.PARENT_GROUP_ID.in(context.select(GROUPING.ID)
                        .from(GROUPING)
                        .where(GROUPING.ORIGIN_DISCOVERED_SRC_ID.isNotNull())))
                .execute();
        context.deleteFrom(GROUP_STATIC_MEMBERS_ENTITIES)
                .where(GROUP_STATIC_MEMBERS_ENTITIES.GROUP_ID.in(context.select(GROUPING.ID)
                        .from(GROUPING)
                        .where(GROUPING.ORIGIN_DISCOVERED_SRC_ID.isNotNull())))
                .execute();
        context.deleteFrom(GROUP_DISCOVER_TARGETS).execute();
        context.deleteFrom(GROUP_EXPECTED_MEMBERS_ENTITIES)
                .where(GROUP_EXPECTED_MEMBERS_ENTITIES.GROUP_ID.in(context.select(GROUPING.ID)
                        .from(GROUPING)
                        .where(GROUPING.ORIGIN_DISCOVERED_SRC_ID.isNotNull())))
                .execute();
        context.deleteFrom(GROUP_EXPECTED_MEMBERS_GROUPS)
                .where(GROUP_EXPECTED_MEMBERS_GROUPS.GROUP_ID.in(context.select(GROUPING.ID)
                        .from(GROUPING)
                        .where(GROUPING.ORIGIN_DISCOVERED_SRC_ID.isNotNull())))
                .execute();
        context.deleteFrom(GROUP_TAGS)
                .where(GROUP_TAGS.GROUP_ID.in(context.select(GROUPING.ID)
                        .from(GROUPING)
                        .where(GROUPING.ORIGIN_DISCOVERED_SRC_ID.isNotNull())))
                .execute();
    }

    /**
     * Method cleans all the child records for all the discovered groups in the database.
     *
     * @param context DB context to use.
     * @param groupId group ID to remove child records for
     */
    private void cleanGroupChildTables(@Nonnull DSLContext context, long groupId) {
        context.deleteFrom(GROUP_STATIC_MEMBERS_GROUPS)
                .where(GROUP_STATIC_MEMBERS_GROUPS.PARENT_GROUP_ID.eq(groupId))
                .execute();
        context.deleteFrom(GROUP_STATIC_MEMBERS_ENTITIES)
                .where(GROUP_STATIC_MEMBERS_ENTITIES.GROUP_ID.eq(groupId))
                .execute();
        context.deleteFrom(GROUP_EXPECTED_MEMBERS_ENTITIES)
                .where(GROUP_EXPECTED_MEMBERS_ENTITIES.GROUP_ID.eq(groupId))
                .execute();
        context.deleteFrom(GROUP_EXPECTED_MEMBERS_GROUPS)
                .where(GROUP_EXPECTED_MEMBERS_GROUPS.GROUP_ID.eq(groupId))
                .execute();
        context.deleteFrom(GROUP_TAGS).where(GROUP_TAGS.GROUP_ID.eq(groupId)).execute();
    }

    @Nonnull
    private Query createGroupUpdate(@Nonnull DSLContext context, @Nonnull Grouping groupPojo) {
        final long groupId = groupPojo.getId();
        return context.update(GROUPING)
                .set(GROUPING.GROUP_TYPE, groupPojo.getGroupType())
                .set(GROUPING.DISPLAY_NAME, groupPojo.getDisplayName())
                .set(GROUPING.IS_HIDDEN, groupPojo.getIsHidden())
                .set(GROUPING.OWNER_ID, groupPojo.getOwnerId())
                .set(GROUPING.SUPPORTS_MEMBER_REVERSE_LOOKUP,
                        groupPojo.getSupportsMemberReverseLookup())
                .set(GROUPING.ORIGIN_DISCOVERED_SRC_ID, groupPojo.getOriginDiscoveredSrcId())
                .set(GROUPING.ORIGIN_SYSTEM_DESCRIPTION, groupPojo.getOriginSystemDescription())
                .set(GROUPING.ORIGIN_USER_CREATOR, groupPojo.getOriginUserCreator())
                .set(GROUPING.ENTITY_FILTERS, groupPojo.getEntityFilters())
                .set(GROUPING.GROUP_FILTERS, groupPojo.getGroupFilters())
                .set(GROUPING.OPTIMIZATION_ENVIRONMENT_TYPE,
                        groupPojo.getOptimizationEnvironmentType())
                .set(GROUPING.OPTIMIZATION_IS_GLOBAL_SCOPE,
                        groupPojo.getOptimizationIsGlobalScope())
                .where(GROUPING.ID.eq(groupId));
    }

    @Nonnull
    @Override
    public Map<String, Set<String>> getTags() {
        final Map<String, Set<String>> result = new HashMap<>();
        final Map<String, List<Record2<String, String>>> records =
                dslContext.selectDistinct(GROUP_TAGS.TAG_KEY, GROUP_TAGS.TAG_VALUE)
                        .from(GROUP_TAGS)
                        .fetch()
                        .stream()
                        .collect(Collectors.groupingBy(Record2::value1));
        for (Entry<String, List<Record2<String, String>>> entry : records.entrySet()) {
            final Set<String> tagsValues =
                    entry.getValue().stream().map(Record2::value2).collect(Collectors.toSet());
            result.put(entry.getKey(), Collections.unmodifiableSet(tagsValues));
        }
        return Collections.unmodifiableMap(result);
    }

    @Nonnull
    @Override
    public Set<GroupDTO.Grouping> getStaticGroupsForEntity(long entityId) {
        try {
            return getStaticGroupsForEntity(dslContext, entityId);
        } catch (DataAccessException e) {
            GROUP_STORE_ERROR_COUNT.labels(GET_LABEL).increment();
            throw e;
        }
    }

    @Nonnull
    private Set<GroupDTO.Grouping> getStaticGroupsForEntity(@Nonnull DSLContext context,
            long entityId) {
        final Set<Long> parentGroups = context.select(GROUP_STATIC_MEMBERS_ENTITIES.GROUP_ID)
                .from(GROUP_STATIC_MEMBERS_ENTITIES)
                .where(GROUP_STATIC_MEMBERS_ENTITIES.ENTITY_ID.eq(entityId))
                .fetch()
                .stream()
                .map(Record1::value1)
                .collect(Collectors.toSet());
        final Set<GroupDTO.Grouping> groupings = new HashSet<>();
        for (Long oid : parentGroups) {
            final Optional<GroupDTO.Grouping> group = getGroupInternal(context, oid);
            if (!group.isPresent()) {
                throw new IllegalStateException("Group with id " + oid +
                        " is absent but is referenced as static owner of entity " + entityId);
            }
            groupings.add(group.get());
        }
        return Collections.unmodifiableSet(groupings);
    }

    private static void requireTrue(boolean condition, @Nonnull String message)
            throws StoreOperationException {
        if (!condition) {
            throw new StoreOperationException(Status.INVALID_ARGUMENT, message);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public List<String> collectDiags() throws DiagnosticsException {
        try {
            final Collection<GroupDTO.Grouping> discovered = getGroups(
                    GroupDTO.GroupFilter.newBuilder()
                            .setOriginFilter(
                                    OriginFilter.newBuilder().addOrigin(Origin.Type.DISCOVERED))
                            .build());
            final List<GroupDTO.Grouping> notDiscovered = sortGroupsForCreation(getGroups(
                    GroupDTO.GroupFilter.newBuilder()
                            .setOriginFilter(OriginFilter.newBuilder()
                                    .addOrigin(Type.USER)
                                    .addOrigin(Type.SYSTEM))
                            .build()));

            logger.info("Collected diags for {} discovered groups and {} created groups.",
                    discovered.size(), notDiscovered.size());

            return Arrays.asList(ComponentGsonFactory.createGsonNoPrettyPrint().toJson(discovered),
                    ComponentGsonFactory.createGsonNoPrettyPrint().toJson(notDiscovered));
        } catch (DataAccessException e) {
            throw new DiagnosticsException(e);
        }
    }

    @Nonnull
    private List<GroupDTO.Grouping> sortGroupsForCreation(@Nonnull Collection<GroupDTO.Grouping> sourceCollection) {
        final Set<Long> addedGroups = new HashSet<>();
        final List<GroupDTO.Grouping> source = new ArrayList<>(sourceCollection);
        final List<GroupDTO.Grouping> dst = new ArrayList<>(sourceCollection.size());
        while (!source.isEmpty()) {
            final Iterator<GroupDTO.Grouping> iter = source.iterator();
            while (iter.hasNext()) {
                final GroupDTO.Grouping group = iter.next();
                final Set<Long> subgroups = getGroupStaticSubGroups(group.getDefinition());
                if (subgroups.isEmpty() || addedGroups.containsAll(subgroups)) {
                    dst.add(group);
                    addedGroups.add(group.getId());
                    iter.remove();
                }
            }
        }
        return dst;
    }

    private Set<Long> getGroupStaticSubGroups(@Nonnull GroupDefinition group) {
        if (!group.hasStaticGroupMembers()) {
            return Collections.emptySet();
        } else {
            final Set<Long> children = new HashSet<>();
            for (StaticMembersByType membersByType: group.getStaticGroupMembers().getMembersByTypeList()) {
                if (membersByType.getType().hasGroup()) {
                    children.addAll(membersByType.getMembersList());
                }
            }
            return children;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void restoreDiags(@Nonnull List<String> collectedDiags) throws DiagnosticsException {
        // Replace all existing groups with the ones in the collected diags.
        final Collection<GroupDTO.Grouping> discoveredGroups =
                ComponentGsonFactory.createGsonNoPrettyPrint()
                        .fromJson(collectedDiags.get(0),
                                new TypeToken<Collection<GroupDTO.Grouping>>() {}.getType());
        final Collection<GroupDTO.Grouping> nonDiscoveredGroups =
                ComponentGsonFactory.createGsonNoPrettyPrint()
                        .fromJson(collectedDiags.get(1),
                                new TypeToken<Collection<GroupDTO.Grouping>>() {}.getType());
        logger.info(
                "Attempting to restore {} discovered groups and {} non-discovered groups from diagnostics.",
                discoveredGroups.size(), nonDiscoveredGroups.size());

        try {
            dslContext.transaction(configuration -> {
                final DSLContext context = DSL.using(configuration);
                final int affectedRows = context.deleteFrom(Tables.GROUPING).execute();
                logger.info("Removed {} groups.", affectedRows);
                final Collection<DiscoveredGroup> discoveredGroupsConverted =
                        new ArrayList<>(discoveredGroups.size());
                final Map<String, Long> srcId2oid = new HashMap<>(discoveredGroups.size());
                for (GroupDTO.Grouping group: discoveredGroups) {
                    final Origin.Discovered origin = group.getOrigin().getDiscovered();
                    final DiscoveredGroup discoveredGroup = new DiscoveredGroup(
                            group.getDefinition(), origin.getSourceIdentifier(),
                            new HashSet<>(origin.getDiscoveringTargetIdList()),
                            group.getExpectedTypesList(),
                            group.getSupportsMemberReverseLookup());
                    srcId2oid.put(origin.getSourceIdentifier(), group.getId());
                    discoveredGroupsConverted.add(discoveredGroup);
                }
                updateDiscoveredGroups(context, discoveredGroupsConverted, srcId2oid::get);
                for (GroupDTO.Grouping group: nonDiscoveredGroups) {
                    final Grouping pojo =
                            createPojoForNewGroup(group.getOrigin(), group.getDefinition(),
                                    group.getSupportsMemberReverseLookup());
                    pojo.setId(group.getId());
                    createGroup(context, pojo, group.getDefinition(),
                            new HashSet<>(group.getExpectedTypesList()));
                }
            });
        } catch (DataAccessException e) {
            throw new DiagnosticsException(Collections.singletonList(e.getMessage() + ": " +
                    ExceptionUtils.getStackTrace(e)));
        }
    }

    @Override
    public void subscribeUserGroupRemoved(@Nonnull Consumer<Long> consumer) {
        deleteCallbacks.add(Objects.requireNonNull(consumer));
    }
}
