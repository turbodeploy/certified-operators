package com.vmturbo.group.group;

import static com.vmturbo.group.db.tables.GroupDiscoverTargets.GROUP_DISCOVER_TARGETS;
import static com.vmturbo.group.db.tables.GroupExpectedMembersEntities.GROUP_EXPECTED_MEMBERS_ENTITIES;
import static com.vmturbo.group.db.tables.GroupExpectedMembersGroups.GROUP_EXPECTED_MEMBERS_GROUPS;
import static com.vmturbo.group.db.tables.GroupStaticMembersEntities.GROUP_STATIC_MEMBERS_ENTITIES;
import static com.vmturbo.group.db.tables.GroupStaticMembersGroups.GROUP_STATIC_MEMBERS_GROUPS;
import static com.vmturbo.group.db.tables.GroupSupplementaryInfo.GROUP_SUPPLEMENTARY_INFO;
import static com.vmturbo.group.db.tables.GroupTags.GROUP_TAGS;
import static com.vmturbo.group.db.tables.Grouping.GROUPING;

import java.util.ArrayList;
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
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.google.common.collect.Table.Cell;
import com.google.protobuf.InvalidProtocolBufferException;

import io.grpc.Status;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.OrderField;
import org.jooq.Query;
import org.jooq.Record1;
import org.jooq.Record2;
import org.jooq.Record3;
import org.jooq.Record6;
import org.jooq.Result;
import org.jooq.Select;
import org.jooq.SelectConditionStep;
import org.jooq.SelectJoinStep;
import org.jooq.TableField;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.springframework.util.StopWatch;

import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.common.protobuf.common.Pagination.PaginationResponse;
import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.GetPaginatedGroupsResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.EntityFilters;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.GroupFilters;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.OptimizationMetadata;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.SelectionCriteriaCase;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter.StaticOrDynamicFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType.TypeCase;
import com.vmturbo.common.protobuf.group.GroupDTO.Origin;
import com.vmturbo.common.protobuf.group.GroupDTO.OriginFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers.StaticMembersByType;
import com.vmturbo.common.protobuf.search.Search.LogicalOperator;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.MapFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.StringFilter;
import com.vmturbo.common.protobuf.search.SearchableProperties;
import com.vmturbo.common.protobuf.tag.Tag.TagValuesDTO;
import com.vmturbo.common.protobuf.tag.Tag.Tags;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.group.DiscoveredObjectVersionIdentity;
import com.vmturbo.group.common.DuplicateNameException;
import com.vmturbo.group.common.Truncator;
import com.vmturbo.group.db.Tables;
import com.vmturbo.group.db.tables.pojos.GroupDiscoverTargets;
import com.vmturbo.group.db.tables.pojos.GroupExpectedMembersEntities;
import com.vmturbo.group.db.tables.pojos.GroupExpectedMembersGroups;
import com.vmturbo.group.db.tables.pojos.GroupStaticMembersEntities;
import com.vmturbo.group.db.tables.pojos.GroupStaticMembersGroups;
import com.vmturbo.group.db.tables.pojos.GroupSupplementaryInfo;
import com.vmturbo.group.db.tables.pojos.GroupTags;
import com.vmturbo.group.db.tables.pojos.Grouping;
import com.vmturbo.group.group.pagination.GroupPaginationParams;
import com.vmturbo.group.service.StoreOperationException;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.proactivesupport.DataMetricCounter;
import com.vmturbo.proactivesupport.DataMetricHistogram;

/**
 * DAO implementing {@link IGroupStore} - CRUD operations with groups.
 * This class does not implement any transaction processing. In order to use transactions,
 * you have to create a DSL context with transaction opened and pass the context to a
 * constructor.
 */
public class GroupDAO implements IGroupStore {

    private static final String GET_LABEL = "get";

    private static final String CREATE_LABEL = "create";

    private static final String UPDATE_LABEL = "update";

    private static final String DELETE_LABEL = "delete";

    // The display_name field in GROUPING table is stored with a case insensitive collation by
    // default (utfmb4_unicode_ci). Thus, queries asking for case sensitive filtering by display
    // name cannot be done. To bypass this, we use a case sensitive collation on the fly for case
    // sensitive queries.
    // For more details see https://dev.mysql.com/doc/refman/8.0/en/adding-collation.html
    private static final String CASE_SENSITIVE_COLLATION = "utf8mb4_bin";

    private final GroupPaginationParams groupPaginationParams;

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

    private static final DataMetricHistogram GROUPS_BY_ID_SIZE_COUNTER =
            DataMetricHistogram.builder()
                    .withName("group_store_groups_by_id_request_size")
                    .withHelp("Number of groups requested to be loaded from GroupStore within one request.")
                    .withBuckets(1, 10, 100, 1000, 10000, 100000, 1000000)
                    .build()
                    .register();

    private static final Logger logger = LogManager.getLogger();

    private static final Map<String, Function<PropertyFilter, Optional<Condition>>>
            PROPETY_FILTER_CONDITION_CREATORS;

    private final DSLContext dslContext;

    private final Set<GroupUpdateListener> groupUpdateListeners = Collections.synchronizedSet(new HashSet<>());

    static {
        PROPETY_FILTER_CONDITION_CREATORS =
                ImmutableMap.<String, Function<PropertyFilter, Optional<Condition>>>builder().put(
                        SearchableProperties.DISPLAY_NAME,
                        GroupDAO::createDisplayNameSearchCondition)
                        .put(StringConstants.TAGS_ATTR, propertyFilter -> Optional.of(
                                createTagsSearchCondition(propertyFilter)))
                        .put(StringConstants.OID, propertyFilter -> Optional.of(
                                createOidCondition(propertyFilter, GROUPING.ID)))
                        .put(StringConstants.ACCOUNTID, propertyFilter -> Optional.of(
                                createOidCondition(propertyFilter, GROUPING.OWNER_ID)))
                        .build();
    }

    /**
     * Constructs group DAO.
     *
     * @param dslContext DB context to execute SQL operations on
     * @param groupPaginationParams group component's internal parameters for paginated calls for
     *                              groups.
     */
    public GroupDAO(@Nonnull final DSLContext dslContext,
            @Nonnull final GroupPaginationParams groupPaginationParams) {
        this.dslContext = Objects.requireNonNull(dslContext);
        this.groupPaginationParams = Objects.requireNonNull(groupPaginationParams);
    }

    /**
     * Add a listener for group change events.
     *
     * @param groupUpdateListener The {@link GroupUpdateListener}.
     */
    public void addUpdateListener(@Nonnull final GroupUpdateListener groupUpdateListener) {
        groupUpdateListeners.add(groupUpdateListener);
    }

    /**
     * Wrapper class that contains all the necessary information for the next page of paginated
     * group results.
     */
    private class NextGroupPageInfo {

        /**
         * The group ids of current page.
         */
        private final List<Long> groupIds;

        /**
         * The cursor for the next page. Empty if that was the last page.
         *
         * <p>In the future we could consider replacing the numeric cursor with seek
         * pagination, if it proves to have a significant performance difference.</p>
         */
        private final Optional<String> nextCursor;

        /**
         * Total Record Count available for current paginated query.
         */
        private final Optional<Integer> totalRecordCount;

        NextGroupPageInfo(List<Long> groupIds,
                @Nullable final String paginationCursor,
                @Nullable final Integer totalRecordCount) {
            this.groupIds = Objects.requireNonNull(groupIds);
            this.nextCursor = Optional.ofNullable(paginationCursor);
            this.totalRecordCount = Optional.ofNullable(totalRecordCount);
        }
    }

    /**
     * {@inheritDoc}
     *
     * <p>Those data are not stored in grouping table but in a separate one.
     */
    @Override
    public void createGroupSupplementaryInfo(long groupId, boolean isEmpty,
            @Nonnull GroupEnvironment groupEnvironment, @Nonnull Severity severity) {
        dslContext.insertInto(GROUP_SUPPLEMENTARY_INFO)
                .set(dslContext.newRecord(GROUP_SUPPLEMENTARY_INFO,
                        new GroupSupplementaryInfo(groupId,
                                isEmpty,
                                groupEnvironment.getEnvironmentType().getNumber(),
                                groupEnvironment.getCloudType().getNumber(),
                                severity.getNumber())))
                .execute();
    }

    @Override
    public void updateBulkGroupSupplementaryInfo(Map<Long, GroupSupplementaryInfo> groups) {
        // read records to ensure that they still exist in the database and place locks
        Collection<Long> existingGroupIds = dslContext.select(GROUPING.ID)
                .from(GROUPING)
                .leftJoin(GROUP_SUPPLEMENTARY_INFO)
                .on(GROUPING.ID.eq(GROUP_SUPPLEMENTARY_INFO.GROUP_ID))
                .where(GROUPING.ID.in(groups.keySet()))
                .forUpdate()
                .fetch()
                .stream()
                .map(Record1::value1)
                .collect(Collectors.toList());
        // filter out groups that might have been deleted between previous calculations and
        // ingestion
        if (existingGroupIds.size() < groups.size()) {
            Set<Long> skippedGroups = new HashSet<>(groups.keySet());
            skippedGroups.removeAll(existingGroupIds);
            logger.info("Skipping {} groups during bulk supplementary info update since they "
                    + "were not found in the database.", skippedGroups.size());
            logger.debug("Uuids of the groups that were skipped: {}", () -> skippedGroups);
        }
        // create upsert statements only for the groups that exist in the database
        final Collection<Query> upserts = new ArrayList<>();
        existingGroupIds.forEach(groupId -> {
            GroupSupplementaryInfo gsi = groups.get(groupId);
            upserts.add(dslContext.insertInto(GROUP_SUPPLEMENTARY_INFO)
                    .set(dslContext.newRecord(GROUP_SUPPLEMENTARY_INFO, gsi))
                    .onDuplicateKeyUpdate()
                    .set(GROUP_SUPPLEMENTARY_INFO.EMPTY, gsi.getEmpty())
                    .set(GROUP_SUPPLEMENTARY_INFO.ENVIRONMENT_TYPE, gsi.getEnvironmentType())
                    .set(GROUP_SUPPLEMENTARY_INFO.CLOUD_TYPE, gsi.getCloudType())
                    .set(GROUP_SUPPLEMENTARY_INFO.SEVERITY, gsi.getSeverity()));
        });
        // update records
        dslContext.batch(upserts).execute();
    }

    /**
     * {@inheritDoc}
     *
     * <p>If a group doesn't have a record in GROUP_SUPPLEMENTARY_INFO table already, we skip it;
     * it will be updated after next topology broadcast.
     */
    @Override
    public int updateBulkGroupsSeverity(Collection<GroupSupplementaryInfo> groups) {
        // create update statements
        final Collection<Query> updates = new ArrayList<>();
        groups.forEach(group -> {
            updates.add(dslContext.update(GROUP_SUPPLEMENTARY_INFO)
                    .set(GROUP_SUPPLEMENTARY_INFO.SEVERITY, group.getSeverity())
                    .where(GROUP_SUPPLEMENTARY_INFO.GROUP_ID.eq(group.getGroupId())));
        });
        // update records
        final int[] updatedRows = dslContext.batch(updates).execute();
        int result = 0;
        for (int updatedRow : updatedRows) {
            if (updatedRow > 0) {
                result++;
            }
        }
        return result;
    }

    @Override
    public void createGroup(long oid, @Nonnull Origin origin,
            @Nonnull GroupDefinition groupDefinition, @Nonnull Set<MemberType> expectedMemberTypes,
            boolean supportReverseLookup) throws StoreOperationException {
        final Grouping pojo =
                createPojoForNewGroup(oid, origin, groupDefinition, supportReverseLookup);
        try {
            createGroup(dslContext, pojo, groupDefinition, expectedMemberTypes);
            groupUpdateListeners.forEach(l -> l.onUserGroupCreated(oid, groupDefinition));
        } catch (DataAccessException e) {
            GROUP_STORE_ERROR_COUNT.labels(CREATE_LABEL).increment();
            if (e.getCause() instanceof DuplicateNameException) {
                GROUP_STORE_DUPLICATE_NAME_COUNT.increment();
            }
            throw e;
        }
    }

    private void createGroup(@Nonnull DSLContext context, @Nonnull Grouping groupPojo,
            @Nonnull GroupDefinition groupDefinition, @Nonnull Set<MemberType> expectedMembers)
            throws StoreOperationException {
        validateStaticMembers(context,
                Collections.singleton(groupDefinition.getStaticGroupMembers()),
                Collections.singletonMap(groupPojo.getId(), groupPojo.getGroupType()));
        validatePropertyFilters(groupDefinition.getGroupFilters());
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
                    "Cannot create object with name " + groupPojo.getDisplayName()
                            + " because an object with the same name and type (id: "
                            + sameNameGroups + ") already exists.");
        }
        final Collection<Query> inserts = new ArrayList<>();
        inserts.add(context.insertInto(GROUPING).set(context.newRecord(GROUPING, groupPojo)));
        inserts.addAll(
                insertGroupDefinitionDependencies(context, groupPojo.getId(), groupDefinition));
        inserts.addAll(
                insertExpectedMembers(context, groupPojo.getId(), new HashSet<>(expectedMembers),
                        groupDefinition.getStaticGroupMembers()));
        context.batch(inserts).execute();
    }

    private void validatePropertyFilters(@Nullable GroupFilters groupFilters) {
        for (GroupFilter filter : groupFilters.getGroupFilterList()) {
            for (PropertyFilter propertyFilter : filter.getPropertyFiltersList()) {
                if (!PROPETY_FILTER_CONDITION_CREATORS.containsKey(
                        propertyFilter.getPropertyName())) {
                    throw new IllegalArgumentException(
                            "Property filter " + propertyFilter.getPropertyName()
                                    + " is not supported");
                }
                final Function<PropertyFilter, Optional<Condition>> conditionCreator =
                    PROPETY_FILTER_CONDITION_CREATORS.get(propertyFilter.getPropertyName());
                // try to apply the filter and check if it can be translated into real
                // conditions, if not it throws an exception
                conditionCreator.apply(propertyFilter);
            }
        }
    }

    @Nonnull
    private Grouping createPojoForNewGroup(long oid, @Nonnull Origin origin,
            @Nonnull GroupDefinition groupDefinition, boolean supportReverseLookup)
            throws StoreOperationException {
        final Grouping pojo = createGroupFromDefinition(groupDefinition);
        pojo.setId(oid);
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
                        "Invalid origin " + origin.getCreationOriginCase()
                                + " passed to create a group");
        }
        return pojo;
    }

    private Grouping createGroupFromDefinition(@Nonnull GroupDefinition groupDefinition)
            throws StoreOperationException {
        final Grouping groupPojo = new Grouping();
        groupPojo.setGroupType(groupDefinition.getType());
        requireTrue(groupDefinition.hasDisplayName(), "Group display name not set");
        final String groupDisplayName =
            Truncator.truncateGroupDisplayName(groupDefinition.getDisplayName(), true);
        groupPojo.setDisplayName(groupDisplayName);
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
                        "Group " + groupDefinition.getDisplayName()
                                + " does not have any recognized selection criteria ("
                                + groupDefinition.getSelectionCriteriaCase() + ")");
        }
        if (groupDefinition.hasOptimizationMetadata()) {
            final OptimizationMetadata metadata = groupDefinition.getOptimizationMetadata();
            groupPojo.setOptimizationEnvironmentType(metadata.getEnvironmentType());
            groupPojo.setOptimizationIsGlobalScope(metadata.getIsGlobalScope());
        }
        return groupPojo;
    }

    private Collection<Query> insertGroupDefinitionDependencies(
            @Nonnull DSLContext context, long groupId, @Nonnull GroupDefinition groupDefinition) {
        final Collection<Query> records = new ArrayList<>();
        if (groupDefinition.getSelectionCriteriaCase()
                == SelectionCriteriaCase.STATIC_GROUP_MEMBERS) {
            records.addAll(insertGroupStaticMembers(context, groupId,
                    groupDefinition.getStaticGroupMembers()));
        }
        records.addAll(insertTags(context, groupDefinition.getTags(), groupId));
        return records;
    }

    private Collection<Query> insertTags(@Nonnull DSLContext context, @Nonnull Tags tags,
            long groupId) {
        final Collection<Query> result = new ArrayList<>();
        for (Entry<String, TagValuesDTO> entry : tags.getTagsMap().entrySet()) {
            final String tagKey = Truncator.truncateTagKey(entry.getKey(), true);
            for (String tagValue : entry.getValue().getValuesList()) {
                final String truncateTagValue = Truncator.truncateTagValue(tagValue, true);
                final GroupTags tag = new GroupTags(groupId, tagKey, truncateTagValue);
                result.add(
                    context.insertInto(com.vmturbo.group.db.tables.GroupTags.GROUP_TAGS)
                        .set(context.newRecord(com.vmturbo.group.db.tables.GroupTags.GROUP_TAGS, tag))
                        .onDuplicateKeyIgnore()
                );
            }
        }
        return result;
    }

    private Collection<Query> insertGroupStaticMembers(@Nonnull DSLContext context,
            long groupId, @Nonnull StaticMembers staticMembers) {
        final Collection<Query> records = new ArrayList<>();
        for (StaticMembersByType staticMember : staticMembers.getMembersByTypeList()) {
            if (staticMember.getType().getTypeCase() == TypeCase.GROUP) {
                for (Long memberId : staticMember.getMembersList()) {
                    final GroupStaticMembersGroups groupChild =
                            new GroupStaticMembersGroups(groupId, memberId);
                    records.add(context.insertInto(GROUP_STATIC_MEMBERS_GROUPS)
                        .set(context.newRecord(GROUP_STATIC_MEMBERS_GROUPS, groupChild)));
                }
            } else {
                for (Long memberId : staticMember.getMembersList()) {
                    final GroupStaticMembersEntities entityChild =
                            new GroupStaticMembersEntities(groupId,
                                    staticMember.getType().getEntity(), memberId);
                    records.add(context.insertInto(GROUP_STATIC_MEMBERS_ENTITIES)
                        .set(context.newRecord(GROUP_STATIC_MEMBERS_ENTITIES, entityChild)));
                }
            }
        }
        return records;
    }

    private Collection<Query> insertExpectedMembers(@Nonnull DSLContext context,
            long groupId, @Nonnull Set<MemberType> memberTypes,
            @Nullable StaticMembers staticMembers) throws StoreOperationException {
        final Collection<Query> records = new ArrayList<>();
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
                            .map(memberType -> memberType.hasGroup() ? memberType.getGroup()
                                    .toString() : Integer.toString(memberType.getEntity()))
                            .collect(Collectors.joining(",", "[", "]"))
                            + " does not contain all the direct members: "
                            + staticMembers.getMembersByTypeList()
                            .stream()
                            .map(StaticMembersByType::getType)
                            .map(memberType -> memberType.hasGroup()
                                    ? memberType.getGroup().toString()
                                    : Integer.toString(memberType.getEntity()))
                            .collect(Collectors.joining(",", "[", "]")));
        }
        for (MemberType memberType : memberTypes) {
            final boolean directMember = directMembers.contains(memberType);
            if (memberType.getTypeCase() == TypeCase.GROUP) {
                final GroupExpectedMembersGroups groupMember =
                        new GroupExpectedMembersGroups(groupId, memberType.getGroup(),
                                directMember);
                records.add(context.insertInto(GROUP_EXPECTED_MEMBERS_GROUPS)
                    .set(context.newRecord(GROUP_EXPECTED_MEMBERS_GROUPS, groupMember)));
            } else {
                final GroupExpectedMembersEntities entityMember =
                        new GroupExpectedMembersEntities(groupId, memberType.getEntity(),
                                directMember);
                records.add(context.insertInto(GROUP_EXPECTED_MEMBERS_ENTITIES)
                    .set(context.newRecord(GROUP_EXPECTED_MEMBERS_ENTITIES, entityMember)));
            }
        }
        return records;
    }

    private Collection<Query> createTargetForGroupRecords(
            @Nonnull DSLContext context, long groupId, @Nonnull Set<Long> targets) {
        final Collection<Query> result = new ArrayList<>();
        for (Long sharedTarget : targets) {
            final GroupDiscoverTargets otherTargetLink =
                    new GroupDiscoverTargets(groupId, sharedTarget);
            result.add(context.insertInto(com.vmturbo.group.db.tables.GroupDiscoverTargets.GROUP_DISCOVER_TARGETS)
                .set(context.newRecord(
                    com.vmturbo.group.db.tables.GroupDiscoverTargets.GROUP_DISCOVER_TARGETS,
                    otherTargetLink)));
        }
        return result;
    }

    /**
     * Get the existing discovered groups in DB and create a collection of identifying fields
     * for each groups.
     *
     * @return collection of discovered group id
     */
    @Nonnull
    @Override
    public Collection<DiscoveredGroupId> getDiscoveredGroupsIds() {
        // This grouping is aimed to fetch groups with a count of associated targets
        final Result<Record6<Long, String, GroupType, byte[], Integer, Long>> records =
                dslContext.select(GROUPING.ID, DSL.max(GROUPING.ORIGIN_DISCOVERED_SRC_ID),
                        DSL.max(GROUPING.GROUP_TYPE), DSL.max(GROUPING.HASH),
                        DSL.count(GROUP_DISCOVER_TARGETS.TARGET_ID),
                        DSL.max(GROUP_DISCOVER_TARGETS.TARGET_ID))
                        .from(GROUPING)
                        .leftJoin(GROUP_DISCOVER_TARGETS)
                        .on(GROUPING.ID.eq(GROUP_DISCOVER_TARGETS.GROUP_ID))
                        .where(GROUPING.ORIGIN_DISCOVERED_SRC_ID.isNotNull())
                        .groupBy(GROUPING.ID)
                        .fetch();
        final Collection<DiscoveredGroupId> result = new ArrayList<>(records.size());
        for (Record6<Long, String, GroupType, byte[], Integer, Long> record: records) {
            final Long targetId = record.value5() != 1 ? null : record.value6();
            final DiscoveredObjectVersionIdentity identity = new DiscoveredObjectVersionIdentity(record.value1(), record.value4());
            final DiscoveredGroupId id = new DiscoveredGroupIdImpl(identity, targetId,
                    record.value2(), record.value3());
            result.add(id);
        }
        return Collections.unmodifiableCollection(result);
    }

    @Nonnull
    @Override
    public Multimap<Long, Long> getDiscoveredGroupsWithTargets() {
        final Result<Record2<Long, Long>> records =
                dslContext.select(GROUPING.ID,
                        GROUP_DISCOVER_TARGETS.TARGET_ID)
                        .from(GROUPING)
                        .leftJoin(GROUP_DISCOVER_TARGETS)
                        .on(GROUPING.ID.eq(GROUP_DISCOVER_TARGETS.GROUP_ID))
                        .where(GROUPING.ORIGIN_DISCOVERED_SRC_ID.isNotNull())
                        .groupBy(GROUPING.ID)
                        .fetch();
        final Multimap<Long, Long> result = ArrayListMultimap.create();
        records.forEach(record -> result.put(record.value1(), record.value2()));
        return result;
    }

    @Nonnull
    @Override
    public Set<Long> getGroupsByTargets(@Nonnull Collection<Long> targets) {
        if (targets.isEmpty()) {
            return Collections.emptySet();
        }
        return dslContext.selectDistinct(GROUP_DISCOVER_TARGETS.GROUP_ID)
                .from(GROUP_DISCOVER_TARGETS)
                .where(GROUP_DISCOVER_TARGETS.TARGET_ID.in(targets))
                .stream()
                .map(Record1::value1)
                .collect(Collectors.toSet());
    }

    /**
     * Method performs validation of static members. It validates that group type in {@link
     * GroupDefinition} is the same as real group has.
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
                                    "Wrong reference to an absent group from static members oid="
                                            + groupOid);
                        }
                        if (requested != realType) {
                            throw new StoreOperationException(Status.INVALID_ARGUMENT,
                                    "Group definition contains reference to group " + groupOid
                                            + " of type " + requested + " while its real type is "
                                            + realType);
                        }
                    }
                }
            }
        }
    }

    @Nonnull
    @Override
    public Collection<GroupDTO.Grouping> getGroupsById(@Nonnull Collection<Long> groupIds) {
        try {
            return getGroupInternal(new FilteredIds(groupIds, false));
        } catch (DataAccessException e) {
            GROUP_STORE_ERROR_COUNT.labels(GET_LABEL).increment();
            throw e;
        }
    }

    @Nonnull
    private List<GroupDTO.Grouping> getGroupInternal(@Nonnull FilteredIds groupIds) {
        GROUPS_BY_ID_SIZE_COUNTER.observe((double)groupIds.size());
        if (groupIds.isEmpty()) {
            return Collections.emptyList();
        }
        final StopWatch stopWatch = new StopWatch("Retrieving " + groupIds.size() + " groups");
        stopWatch.start("grouping table");
        final Map<Long, Grouping> groupings = dslContext.selectFrom(GROUPING)
                .where(groupIds.idCondition(GROUPING.ID))
                .fetchInto(Grouping.class)
                .stream()
                .collect(Collectors.toMap(Grouping::getId, Function.identity()));
        if (groupings.isEmpty()) {
            return Collections.emptyList();
        }
        // Preserve the ordering of the input group ids. This is necessary for paginated requests.
        final ArrayList<Grouping> sortedGroupings = new ArrayList<>();
        groupIds.groupIds().stream().map(groupings::get).filter(Objects::nonNull)
                .forEach(sortedGroupings::add);
        stopWatch.stop();
        stopWatch.start("expected member types");
        final Table<Long, MemberType, Boolean> expectedMembers = internalGetExpectedMemberTypes(dslContext, groupIds);
        stopWatch.stop();
        stopWatch.start("origins");
        final Map<Long, Origin> groupsOrigins = getGroupOrigin(sortedGroupings);
        stopWatch.stop();
        stopWatch.start("tags");
        final Map<Long, Tags> groupTags = getGroupTags(groupIds);
        stopWatch.stop();
        stopWatch.start("static members");
        final Map<Long, StaticMembers> staticMembers =
                getStaticMembersMessage(groupIds, expectedMembers);
        stopWatch.stop();
        stopWatch.start("calculation");
        final List<GroupDTO.Grouping> result = new ArrayList<>(groupIds.size());
        for (Grouping grouping: sortedGroupings) {
            final long groupId = grouping.getId();
            final GroupDTO.Grouping.Builder builder = GroupDTO.Grouping.newBuilder();
            builder.setId(groupId);
            builder.addAllExpectedTypes(expectedMembers.row(groupId).keySet());
            builder.setSupportsMemberReverseLookup(grouping.getSupportsMemberReverseLookup());
            builder.setOrigin(groupsOrigins.get(groupId));
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
            Optional.ofNullable(groupTags.get(groupId)).ifPresent(defBuilder::setTags);
            try {
                if (grouping.getEntityFilters() != null) {
                    defBuilder.setEntityFilters(
                            EntityFilters.parseFrom(grouping.getEntityFilters()));
                } else if (grouping.getGroupFilters() != null) {
                    defBuilder.setGroupFilters(GroupFilters.parseFrom(grouping.getGroupFilters()));
                } else {
                    // If a group does not have any members, we still fill the StaticMembers
                    // field in order to show that the group is a static one
                    final StaticMembers groupStaticMembers = staticMembers.get(groupId);
                    if (groupStaticMembers != null) {
                        defBuilder.setStaticGroupMembers(groupStaticMembers);
                    }
                }
            } catch (InvalidProtocolBufferException e) {
                logger.error("Failed to parse dynamic selection criteria for group: {} (id: {})",
                        grouping.getDisplayName(), groupId, e);
            }
            builder.setDefinition(defBuilder);
            result.add(builder.build());
        }
        stopWatch.stop();
        logger.debug(stopWatch::prettyPrint);
        return result;
    }

    @Nonnull
    private Optional<OptimizationMetadata> getOptimizationMetadata(@Nonnull Grouping grouping) {
        if (grouping.getOptimizationIsGlobalScope() != null
                && grouping.getOptimizationEnvironmentType() != null) {
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

    /**
     * Returns the group type for given group id.
     *
     * @param groupId the group to query for.
     * @return the group's type, or null if there is no record in the database for the group uuid
     *         provided.
     */
    @Nullable
    public GroupType getGroupType(long groupId) {
        Record1<GroupType> result = dslContext.select(GROUPING.GROUP_TYPE)
                .from(GROUPING)
                .where(GROUPING.ID.eq(groupId))
                .fetchOne();
        if (result != null) {
            return result.value1();
        }
        return null;
    }

    @Nonnull
    @Override
    public Table<Long, MemberType, Boolean> getExpectedMemberTypesForGroup(long groupId) {
        return internalGetExpectedMemberTypes(dslContext, new FilteredIds(groupId));
    }

    /**
     * Get the expected member types for a collection of groups. These are the entity (or group)
     * types that the members of the group conform to.
     *
     * @param context Transaction context.
     * @param groupId The groups to fetch
     * @return A table of (group) -> (member type) -> (boolean). The boolean indicates whether the
     *         type is a direct member, or an indirect member (in case of nested groups).
     */
    @Nonnull
    public Table<Long, MemberType, Boolean> getExpectedMemberTypes(DSLContext context, Collection<Long> groupId) {
        return internalGetExpectedMemberTypes(context, new FilteredIds(groupId, false));
    }

    private Table<Long, MemberType, Boolean> internalGetExpectedMemberTypes(DSLContext context, FilteredIds filteredIds) {
        if (filteredIds.isEmpty()) {
            return HashBasedTable.create();
        }

        final List<Record3<Long, Integer, Boolean>> expectedMembersEntities =
                context.select(GROUP_EXPECTED_MEMBERS_ENTITIES.GROUP_ID,
                        GROUP_EXPECTED_MEMBERS_ENTITIES.ENTITY_TYPE,
                        GROUP_EXPECTED_MEMBERS_ENTITIES.DIRECT_MEMBER)
                        .from(GROUP_EXPECTED_MEMBERS_ENTITIES)
                        .where(filteredIds.idCondition(GROUP_EXPECTED_MEMBERS_ENTITIES.GROUP_ID))
                        .fetch();
        final List<Record3<Long, GroupType, Boolean>> expectedMembersGroups =
                context.select(GROUP_EXPECTED_MEMBERS_GROUPS.GROUP_ID,
                        GROUP_EXPECTED_MEMBERS_GROUPS.GROUP_TYPE,
                        GROUP_EXPECTED_MEMBERS_GROUPS.DIRECT_MEMBER)
                        .from(GROUP_EXPECTED_MEMBERS_GROUPS)
                        .where(filteredIds.idCondition(GROUP_EXPECTED_MEMBERS_GROUPS.GROUP_ID))
                        .fetch();
        final Table<Long, MemberType, Boolean> result = HashBasedTable.create();
        for (Record3<Long, Integer, Boolean> member : expectedMembersEntities) {
            final MemberType memberType =
                    MemberType.newBuilder().setEntity(member.value2()).build();
            result.put(member.value1(), memberType, member.value3());
        }
        for (Record3<Long, GroupType, Boolean> member : expectedMembersGroups) {
            final MemberType memberType =
                    MemberType.newBuilder().setGroup(member.value2()).build();
            result.put(member.value1(), memberType, member.value3());
        }
        return result;
    }

    @Nonnull
    private Map<Long, Origin> getGroupOrigin(@Nonnull Collection<Grouping> groups) {
        final Set<Long> discoveredGroups = groups.stream()
                .filter(group -> group.getOriginSystemDescription() == null
                        && group.getOriginUserCreator() == null)
                .map(Grouping::getId)
                .collect(Collectors.toSet());
        final Multimap<Long, Long> groupTargets = HashMultimap.create();
        dslContext.select(GROUP_DISCOVER_TARGETS.GROUP_ID, GROUP_DISCOVER_TARGETS.TARGET_ID)
                .from(GROUP_DISCOVER_TARGETS)
                .where(GROUP_DISCOVER_TARGETS.GROUP_ID.in(discoveredGroups))
                .fetch()
                .forEach(record -> groupTargets.put(record.value1(), record.value2()));
        final Map<Long, Origin> origins = new HashMap<>(groups.size());
        for (Grouping group: groups) {
            final Origin origin;
            if (group.getOriginSystemDescription() != null) {
                origin = Origin.newBuilder()
                        .setSystem(Origin.System.newBuilder()
                                .setDescription(group.getOriginSystemDescription()))
                        .build();
            } else if (group.getOriginUserCreator() != null) {
                origin = Origin.newBuilder()
                        .setUser(Origin.User.newBuilder().setUsername(group.getOriginUserCreator()))
                        .build();
            } else if (group.getDisplayName() != null) {
                final Collection<Long> targets = groupTargets.get(group.getId());
                origin = Origin.newBuilder()
                        .setDiscovered(Origin.Discovered.newBuilder()
                                .addAllDiscoveringTargetId(targets)
                                .setSourceIdentifier(group.getOriginDiscoveredSrcId()))
                        .build();
            } else {
                throw new RuntimeException("Unknown origin for the group " + group.getId());
            }
            origins.put(group.getId(), origin);
        }
        return origins;
    }

    @Nonnull
    @Override
    public GroupMembersPlain getMembers(@Nonnull Collection<Long> groupId,
            boolean expandNestedGroups) throws StoreOperationException {
        final GroupMembersPlain members = getDirectMembers(groupId);
        if (expandNestedGroups) {
            Set<Long> newGroups = members.getGroupIds();
            while (!newGroups.isEmpty()) {
                final GroupMembersPlain subMembers = getDirectMembers(newGroups);
                newGroups = members.mergeMembers(subMembers);
            }
        }
        return members.unmodifiable();
    }

    @Nonnull
    private GroupMembersPlain getDirectMembers(@Nonnull Collection<Long> groupId)
            throws StoreOperationException {
        final List<Record1<Long>> staticMembersEntities =
                dslContext.select(GROUP_STATIC_MEMBERS_ENTITIES.ENTITY_ID)
                        .from(GROUP_STATIC_MEMBERS_ENTITIES)
                        .where(GROUP_STATIC_MEMBERS_ENTITIES.GROUP_ID.in(groupId))
                        .fetch();
        final List<Record1<Long>> staticMembersGroups =
                dslContext.select(GROUP_STATIC_MEMBERS_GROUPS.CHILD_GROUP_ID)
                        .from(GROUP_STATIC_MEMBERS_GROUPS)
                        .where(GROUP_STATIC_MEMBERS_GROUPS.PARENT_GROUP_ID.in(groupId))
                        .fetch();
        final Set<Long> entitiesMembers =
                staticMembersEntities.stream().map(Record1::value1).collect(Collectors.toSet());
        final Set<Long> groupMembers = staticMembersGroups.stream()
                .map(Record1::value1)
                .collect(Collectors.toCollection(HashSet::new));
        final Collection<Record2<byte[], byte[]>> groups =
                dslContext.select(GROUPING.ENTITY_FILTERS, GROUPING.GROUP_FILTERS)
                        .from(GROUPING)
                        .where(GROUPING.ID.in(groupId))
                        .fetch();
        final Set<EntityFilters> entityFilters = new HashSet<>();
        final Set<GroupFilters> groupFilters = new HashSet<>();
        for (Record2<byte[], byte[]> record: groups) {
            try {
                if (record.value1() != null) {
                    final EntityFilters entityFilter = EntityFilters.parseFrom(record.value1());
                    entityFilters.add(entityFilter);
                }
                if (record.value2() != null) {
                    final GroupFilters groupFilter = GroupFilters.parseFrom(record.value2());
                    groupFilters.add(groupFilter);
                }
            } catch (InvalidProtocolBufferException e) {
                throw new StoreOperationException(Status.INTERNAL,
                        "Failed deserializing filters from group " + groupId, e);
            }
        }
        for (GroupFilters groupFilter: groupFilters) {
            final Set<Long> subgroups = getGroupIds(groupFilter);
            groupMembers.addAll(subgroups);
        }
        return new GroupMembersPlain(entitiesMembers, groupMembers, entityFilters);
    }

    @Nonnull
    private Map<Long, Map<MemberType, Set<Long>>> getStaticMembers(@Nonnull StopWatch stopWatch,
            @Nonnull FilteredIds groupIds,
            @Nonnull SetMultimap<Long, MemberType> expectedDirectTypes) {
        stopWatch.start("fetch static members entities");
        final List<Record3<Long, Integer, Long>> staticMembersEntities =
                dslContext.select(GROUP_STATIC_MEMBERS_ENTITIES.GROUP_ID,
                        GROUP_STATIC_MEMBERS_ENTITIES.ENTITY_TYPE,
                        GROUP_STATIC_MEMBERS_ENTITIES.ENTITY_ID)
                        .from(GROUP_STATIC_MEMBERS_ENTITIES)
                        .where(groupIds.idCondition(GROUP_STATIC_MEMBERS_ENTITIES.GROUP_ID))
                        .fetch();
        stopWatch.stop();
        stopWatch.start("fetch static members groups");
        final List<Record3<Long, Long, GroupType>> staticMembersGroups =
                dslContext.select(GROUP_STATIC_MEMBERS_GROUPS.PARENT_GROUP_ID,
                        GROUP_STATIC_MEMBERS_GROUPS.CHILD_GROUP_ID, GROUPING.GROUP_TYPE)
                        .from(GROUP_STATIC_MEMBERS_GROUPS)
                        .join(GROUPING)
                        .on(GROUP_STATIC_MEMBERS_GROUPS.CHILD_GROUP_ID.eq(GROUPING.ID))
                        .where(groupIds.idCondition(GROUP_STATIC_MEMBERS_GROUPS.PARENT_GROUP_ID))
                        .fetch();
        stopWatch.stop();
        stopWatch.start("apply expected members");
        final Map<Long, Map<MemberType, Set<Long>>> staticMembers = new HashMap<>();
        expectedDirectTypes.entries()
                .forEach(entry -> staticMembers.computeIfAbsent(entry.getKey(),
                        key -> new HashMap<>()).put(entry.getValue(), new HashSet<>()));
        stopWatch.stop();
        stopWatch.start("calculate static members entities");
        for (Record3<Long, Integer, Long> record : staticMembersEntities) {
            final MemberType type = MemberType.newBuilder().setEntity(record.value2()).build();
            final long member = record.value3();
            final Map<MemberType, Set<Long>> members =
                    staticMembers.computeIfAbsent(record.value1(), key -> new HashMap<>());
            members.computeIfAbsent(type, key -> new HashSet<>()).add(member);
        }
        stopWatch.stop();
        stopWatch.start("calculate static members groups");
        for (Record3<Long, Long, GroupType> record : staticMembersGroups) {
            final MemberType type = MemberType.newBuilder().setGroup(record.value3()).build();
            final long member = record.value2();
            final Map<MemberType, Set<Long>> members =
                    staticMembers.computeIfAbsent(record.value1(), key -> new HashMap<>());
            members.computeIfAbsent(type, key -> new HashSet<>()).add(member);
        }
        stopWatch.stop();
        return staticMembers;
    }

    @Nonnull
    private Map<Long, StaticMembers> getStaticMembersMessage(@Nonnull FilteredIds groupIds,
            @Nonnull Table<Long, MemberType, Boolean> membersTypes) {
        final StopWatch stopWatch =
                new StopWatch("Get static members for " + groupIds.size() + " groups");
        stopWatch.start("calc expected types");
        final SetMultimap<Long, MemberType> expectedDirectTypes = HashMultimap.create();
        membersTypes.cellSet()
                .stream()
                .filter(Cell::getValue)
                .forEach(cell -> expectedDirectTypes.put(cell.getRowKey(), cell.getColumnKey()));
        stopWatch.stop();
        final Map<Long, Map<MemberType, Set<Long>>> staticMembers =
                getStaticMembers(stopWatch, groupIds, expectedDirectTypes);
        stopWatch.start("fill static members");
        final Map<Long, StaticMembers> result = new HashMap<>(groupIds.size());
        for (Long groupId : groupIds.groupIds()) {
            // We fill in the empty collections to create a StaticMembersByType record for every
            // expected direct member.
            final Map<MemberType, Set<Long>> groupStaticMembers =
                    staticMembers.getOrDefault(groupId, new HashMap<>());
            final StaticMembers.Builder resultBuilder = StaticMembers.newBuilder();
            for (Entry<MemberType, Set<Long>> entry : groupStaticMembers.entrySet()) {
                resultBuilder.addMembersByType(StaticMembersByType.newBuilder()
                        .setType(entry.getKey())
                        .addAllMembers(entry.getValue())
                        .build());
            }
            result.put(groupId, resultBuilder.build());
        }
        stopWatch.stop();
        logger.debug(stopWatch::prettyPrint);
        return result;
    }

    @Nonnull
    private Map<Long, Tags> getGroupTags(@Nonnull FilteredIds groupIds) {
        final List<Record3<Long, String, String>> tags =
                dslContext.select(GROUP_TAGS.GROUP_ID, GROUP_TAGS.TAG_KEY, GROUP_TAGS.TAG_VALUE)
                        .from(GROUP_TAGS)
                        .where(groupIds.idCondition(GROUP_TAGS.GROUP_ID))
                        .fetch();
        final Map<Long, Multimap<String, String>> tagsMultimap = new HashMap<>(groupIds.size());
        for (Record3<Long, String, String> record : tags) {
            tagsMultimap.computeIfAbsent(record.value1(), key -> HashMultimap.create())
                    .put(record.value2(), record.value3());
        }
        final Map<Long, Tags> result = new HashMap<>(groupIds.size());
        for (Entry<Long, Multimap<String, String>> groupEntry : tagsMultimap.entrySet()) {
            final Tags.Builder tagsBuilder = Tags.newBuilder();
            for (Entry<String, Collection<String>> tagEntry : groupEntry.getValue()
                    .asMap()
                    .entrySet()) {
                final TagValuesDTO values =
                        TagValuesDTO.newBuilder().addAllValues(tagEntry.getValue()).build();
                tagsBuilder.putTags(tagEntry.getKey(), values);
            }
            result.put(groupEntry.getKey(), tagsBuilder.build());
        }
        return result;
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
            final List<GroupDTO.Grouping> groupings = getGroupInternal(new FilteredIds(groupId));
            if (groupings.isEmpty()) {
                throw new StoreOperationException(Status.INTERNAL,
                        "Cannot find the updated group by id " + groupId);
            }
            if (groupings.size() > 1) {
                throw new StoreOperationException(Status.INTERNAL,
                        "Multiple groups returned after update for group id " + groupId);
            }
            final GroupDTO.Grouping grouping = groupings.iterator().next();
            groupUpdateListeners.forEach(l -> l.onUserGroupUpdated(grouping.getId(), grouping.getDefinition()));
            return grouping;
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
                    "Unexpected query result size " + result.size() + " for group with id "
                            + groupId + ". Must be PK violation");
        }
        final Record3<String, String, String> record = result.get(0);
        if (record.value3() != null) {
            throw new StoreOperationException(Status.INVALID_ARGUMENT,
                    "Attempt to modify immutable discovered group " + groupId + " display name "
                            + groupDefinition.getDisplayName());
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
                    "Cannot create object with name " + groupDefinition.getDisplayName()
                            + " because an object with the same name and type (id: " + groupId
                            + ") already exists.");
        }
        validateStaticMembers(context,
                Collections.singleton(groupDefinition.getStaticGroupMembers()),
                Collections.singletonMap(group.getId(), group.getGroupType()));

        cleanGroupChildTables(context, groupId);
        final Collection<Query> children = new ArrayList<>();
        children.addAll(insertGroupDefinitionDependencies(context, groupId, groupDefinition));
        children.addAll(insertExpectedMembers(context, groupId, expectedMemberTypes,
                groupDefinition.getStaticGroupMembers()));

        // Set the values that don't get updated as a part of update
        group.setOriginSystemDescription(record.value1());
        group.setOriginUserCreator(record.value2());

        createGroupUpdate(context, group).execute();
        context.batch(children).execute();
    }

    @Override
    public void updateSingleGroupSupplementaryInfo(final long groupId,
            final boolean isEmpty,
            final GroupEnvironment groupEnvironment,
            final Severity groupSeverity) {
        dslContext.update(GROUP_SUPPLEMENTARY_INFO)
                .set(GROUP_SUPPLEMENTARY_INFO.EMPTY, isEmpty)
                .set(GROUP_SUPPLEMENTARY_INFO.ENVIRONMENT_TYPE,
                        groupEnvironment.getEnvironmentType().getNumber())
                .set(GROUP_SUPPLEMENTARY_INFO.CLOUD_TYPE,
                        groupEnvironment.getCloudType().getNumber())
                .set(GROUP_SUPPLEMENTARY_INFO.SEVERITY, groupSeverity.getNumber())
                .where(GROUP_SUPPLEMENTARY_INFO.GROUP_ID.eq(groupId))
                .execute();
    }

    @Nonnull
    @Override
    public Collection<GroupDTO.Grouping> getGroups(@Nonnull GroupDTO.GroupFilter filter) {
        final FilteredIds groupingIds = getGroupIds(filter);
        return getGroupInternal(groupingIds);
    }

    @Nonnull
    @Override
    public GroupDTO.GetPaginatedGroupsResponse getPaginatedGroups(
            @Nonnull GroupDTO.GetPaginatedGroupsRequest paginatedGroupsRequest) {
        // fetch next page
        NextGroupPageInfo nextPage = getNextPage(paginatedGroupsRequest.getGroupFilter(),
                paginatedGroupsRequest.getPaginationParameters());

        PaginationResponse.Builder paginationResponse = PaginationResponse.newBuilder();
        nextPage.nextCursor.ifPresent(paginationResponse::setNextCursor);
        nextPage.totalRecordCount.ifPresent(paginationResponse::setTotalRecordCount);
        return GetPaginatedGroupsResponse.newBuilder()
                .addAllGroups(getGroupInternal(new FilteredIds(nextPage.groupIds, false)))
                .setPaginationResponse(paginationResponse.build())
                .build();
    }

    /**
     * Returns the ids of the groups for the next page in this request.
     *
     * @param filter the filter for groups.
     * @param paginationParams the parameters for pagination.
     * @return the ids of the groups, along with info for the next page of the pagination.
     */
    private NextGroupPageInfo getNextPage(@Nullable GroupDTO.GroupFilter filter,
            @Nullable PaginationParameters paginationParams) {
        final Optional<Condition> sqlCondition = filter != null
                ? createGroupCondition(filter)
                : Optional.empty();
        // get ascending value from input, or default to true
        final boolean ascendingOrder = paginationParams == null || paginationParams.getAscending();
        final int cursorValue = getCursorValue(paginationParams);
        final int paginationLimit = getPaginationLimit(paginationParams);
        // Get the ids of the next page
        List<Long> result = dslContext.select(GROUPING.ID)
                .from(GROUPING)
                .leftJoin(GROUP_SUPPLEMENTARY_INFO)
                .on(GROUPING.ID.eq(GROUP_SUPPLEMENTARY_INFO.GROUP_ID))
                .where(sqlCondition.orElse(DSL.noCondition()))
                .orderBy(createOrderByClause(paginationParams, ascendingOrder),
                        // add secondary sorting to catch edge cases of duplicate values in primary
                        // sorting
                        ascendingOrder ? GROUPING.ID.asc() : GROUPING.ID.desc())
                // apply pagination
                .offset(cursorValue)
                .limit(paginationLimit + 1)
                .fetch()
                .getValues(GROUPING.ID);
        // check if we have more results than page size, which indicates that there are more pages
        // to be returned.
        boolean nextPageExists = false;
        if (result.size() > paginationLimit) {
            result = result.subList(0, paginationLimit);
            nextPageExists = true;
        }
        final int totalRecordCount = getTotalRecordCount(sqlCondition);
        return new NextGroupPageInfo(result,
                nextPageExists ? String.valueOf(cursorValue + result.size()) : null,
                totalRecordCount);
    }

    /**
     * Returns the pagination limit for the query. If the input limit exceeds max pagination limit,
     * returns the max pagination limit. If there is no pagination limit provided, it returns a
     * default value.
     *
     * @param paginationParams the pagination parameters provided by the user.
     * @return the pagination limit for the query.
     */
    private int getPaginationLimit(final PaginationParameters paginationParams) {
        if (paginationParams == null || !paginationParams.hasLimit()) {
            return groupPaginationParams.getGroupPaginationDefaultLimit();
        }
        int paginationLimit = paginationParams.getLimit();
        if (paginationLimit <= 0) {
            throw new IllegalArgumentException("Invalid limit value provided: '" + paginationLimit
                    + "'. Limit must be a positive integer.");
        }
        final int maxPaginationLimit = groupPaginationParams.getGroupPaginationMaxLimit();
        if (paginationLimit > maxPaginationLimit) {
            logger.warn("Client limit " + paginationParams.getLimit() + " exceeds max limit "
                    + maxPaginationLimit + ". Page size will be reduced to " + maxPaginationLimit
                    + ".");
            paginationLimit = maxPaginationLimit;
        }
        return paginationLimit;
    }

    /**
     * Returns the database field that the query should order by, based on the pagination
     * parameters provided in the input.
     *
     * @param paginationParams the pagination parameters provided by the user.
     * @param ascendingOrder whether to return the results in ascending or descending order.
     * @return The appropriate database field to order by. Defaults to group id if input is empty.
     */
    private OrderField<?> createOrderByClause(PaginationParameters paginationParams,
            final boolean ascendingOrder) {
        // Ordering by COST not supported yet (will be added at a later stage of pagination work;
        // see OM-63107)
        // default is name
        if (paginationParams == null || !paginationParams.hasOrderBy()
                || !paginationParams.getOrderBy().hasGroupSearch()) {
            return ascendingOrder
                    ? GROUPING.DISPLAY_NAME.asc()
                    : GROUPING.DISPLAY_NAME.desc();
        }
        switch (paginationParams.getOrderBy().getGroupSearch()) {
            case GROUP_SEVERITY:
                return ascendingOrder
                        ? GROUP_SUPPLEMENTARY_INFO.SEVERITY.asc()
                        : GROUP_SUPPLEMENTARY_INFO.SEVERITY.desc();
            case GROUP_NAME:
            default:
                return ascendingOrder
                        ? GROUPING.DISPLAY_NAME.asc()
                        : GROUPING.DISPLAY_NAME.desc();
        }
    }

    /**
     * Extracts the cursor from the pagination parameters, doing some sanity checking.
     *
     * @param params the pagination parameters provided by the user.
     * @return the cursor value if it's a valid one, otherwise 0.
     */
    private int getCursorValue(PaginationParameters params) {
        int cursorValue;
        if (params != null && params.hasCursor()) {
            try {
                cursorValue = Integer.parseInt(params.getCursor());
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid cursor value provided: '"
                        + params.getCursor() + "'.");
            }
        } else {
            cursorValue = 0;
        }
        return cursorValue;
    }

    /**
     * Returns the total record count for the query.
     *
     * @param sqlCondition the 'where' condition for the query.
     * @return the total record count
     */
    private int getTotalRecordCount(Optional<Condition> sqlCondition) {
        int groupCount = dslContext.selectCount()
                .from(GROUPING)
                .leftJoin(GROUP_SUPPLEMENTARY_INFO)
                .on(GROUPING.ID.eq(GROUP_SUPPLEMENTARY_INFO.GROUP_ID))
                .where(sqlCondition.orElse(DSL.noCondition()))
                .fetchOne(0, int.class);
        return groupCount;
    }

    @Nonnull
    private FilteredIds getGroupIds(@Nonnull GroupDTO.GroupFilter filter) {
        final Optional<Condition> sqlCondition = createGroupCondition(filter);
        final Set<Long> groupingIds = dslContext.select(GROUPING.ID)
                .from(GROUPING)
                .leftJoin(GROUP_SUPPLEMENTARY_INFO)
                .on(GROUPING.ID.eq(GROUP_SUPPLEMENTARY_INFO.GROUP_ID))
                .where(sqlCondition.orElse(DSL.noCondition()))
                .fetch()
                .stream()
                .map(Record1::value1)
                .collect(Collectors.toSet());
        return new FilteredIds(groupingIds, !sqlCondition.isPresent());
    }

    /**
     * Tuple for all group ids retrieved from the database.
     */
    private static class FilteredIds {
        private final Collection<Long> groupIds;
        private final boolean allGroups;

        private FilteredIds(Collection<Long> groupIds, boolean allGroups) {
            this.groupIds = Collections.unmodifiableCollection(groupIds);
            this.allGroups = allGroups;
        }

        private FilteredIds(long groupId) {
            this.groupIds = Collections.singleton(groupId);
            this.allGroups = false;
        }

        public int size() {
            return groupIds.size();
        }

        public boolean isEmpty() {
            return groupIds.isEmpty();
        }

        public Condition idCondition(TableField<?, Long> idField) {
            return allGroups ? DSL.noCondition() : idField.in(groupIds);
        }

        public Collection<Long> groupIds() {
            return groupIds;
        }
    }

    @Nonnull
    @Override
    public Set<Long> getGroupIds(@Nonnull GroupFilters filters) {
        if (filters.getGroupFilterCount() == 0) {
            return Collections.unmodifiableSet(dslContext.select(GROUPING.ID)
                    .from(GROUPING)
                    .fetch()
                    .stream()
                    .map(Record1::value1)
                    .collect(Collectors.toSet()));
        }
        final Iterator<GroupFilter> iterator = filters.getGroupFilterList().iterator();
        final Set<Long> initialSet = new HashSet<>(getGroupIds(iterator.next()).groupIds());
        while (iterator.hasNext()) {
            final GroupFilter additionalFilter = iterator.next();
            final Collection<Long> anotherSet = getGroupIds(additionalFilter).groupIds();
            initialSet.retainAll(anotherSet);
        }
        return Collections.unmodifiableSet(initialSet);
    }

    @Nonnull
    @Override
    public Set<Long> getExistingGroupIds(@Nonnull Collection<Long> groupIds) {
        final List<Long> records = dslContext.select(GROUPING.ID)
                .from(GROUPING)
                .where(GROUPING.ID.in(groupIds))
                .fetch()
                .map(Record1::value1);
        return ImmutableSet.copyOf(records);
    }

    /**
     * This method creates SQL conditions for the group filter.
     *
     * @param filter filter to convert to SQL condition
     * @return SQL condition
     */
    @Nonnull
    private Optional<Condition> createGroupCondition(@Nonnull GroupDTO.GroupFilter filter) {
        final Collection<Condition> conditions = new ArrayList<>();
        createPropertyFiltersCondition(filter.getPropertyFiltersList(), filter.getLogicalOperator())
            .ifPresent(conditions::add);
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
        if (filter.getStaticOrDynamic().equals(StaticOrDynamicFilter.STATIC)) {
            conditions.add(GROUPING.GROUP_FILTERS.isNull().and(GROUPING.ENTITY_FILTERS.isNull()));
        }
        if (filter.getStaticOrDynamic().equals(StaticOrDynamicFilter.DYNAMIC)) {
            conditions.add(GROUPING.GROUP_FILTERS.isNotNull()
                    .or(GROUPING.ENTITY_FILTERS.isNotNull()));
        }
        if (!filter.getDirectMemberTypesList().isEmpty()) {
            filter.getDirectMemberTypesList().forEach(directMemberType ->
                    createExpectedMembersCondition(directMemberType, conditions, true));
        }
        if (!filter.getIndirectMemberTypesList().isEmpty()) {
            filter.getIndirectMemberTypesList().forEach(indirectMemberType ->
                    createExpectedMembersCondition(indirectMemberType, conditions, false));
        }
        if (filter.getExcludeEmpty()) {
            conditions.add(GROUP_SUPPLEMENTARY_INFO.EMPTY.eq(false));
        }
        if (filter.hasEnvironmentType()) {
            conditions.add(GROUP_SUPPLEMENTARY_INFO.ENVIRONMENT_TYPE.eq(
                            filter.getEnvironmentType().getNumber()));
        }
        if (filter.hasCloudType()) {
            conditions.add(GROUP_SUPPLEMENTARY_INFO.ENVIRONMENT_TYPE.eq(EnvironmentType.CLOUD_VALUE)
                            .and(GROUP_SUPPLEMENTARY_INFO.CLOUD_TYPE.eq(
                                    filter.getCloudType().getNumber())));
        }
        if (filter.hasSeverity()) {
            conditions.add(GROUP_SUPPLEMENTARY_INFO.SEVERITY.eq(filter.getSeverity().getNumber()));
        }
        return combineConditions(conditions, Condition::and);
    }

    private void createExpectedMembersCondition(@Nonnull GroupDTO.MemberType memberType,
            final Collection<Condition> conditions,
            final boolean isDirectMember) {
        if (memberType.hasEntity()) {
            conditions.add(GROUPING.ID.in(DSL.select(GROUP_EXPECTED_MEMBERS_ENTITIES.GROUP_ID)
                    .from(GROUP_EXPECTED_MEMBERS_ENTITIES)
                    .where(GROUP_EXPECTED_MEMBERS_ENTITIES.ENTITY_TYPE.eq(memberType.getEntity())
                            .and(GROUP_EXPECTED_MEMBERS_ENTITIES.DIRECT_MEMBER.eq(
                                    isDirectMember)))));
        } else if (memberType.hasGroup()) {
            conditions.add(GROUPING.ID.in(DSL.select(GROUP_EXPECTED_MEMBERS_GROUPS.GROUP_ID)
                    .from(GROUP_EXPECTED_MEMBERS_GROUPS)
                    .where(GROUP_EXPECTED_MEMBERS_GROUPS.GROUP_TYPE.eq(memberType.getGroup())
                            .and(GROUP_EXPECTED_MEMBERS_GROUPS.DIRECT_MEMBER.eq(isDirectMember)))));
        }
    }

    @Nonnull
    private Optional<Condition> createPropertyFiltersCondition(
            @Nonnull Collection<PropertyFilter> propertyFilters,
            LogicalOperator logicalOperator) {
        if (propertyFilters.isEmpty()) {
            return Optional.empty();
        }
        final Collection<Condition> allConditions = new ArrayList<>();
        for (PropertyFilter propertyFilter: propertyFilters) {
            final Function<PropertyFilter, Optional<Condition>> conditionCreator =
                    PROPETY_FILTER_CONDITION_CREATORS.get(propertyFilter.getPropertyName());
            if (conditionCreator == null) {
                throw new IllegalArgumentException(
                        "Unsupported property filter found: " + propertyFilter.getPropertyName());
            }
            conditionCreator.apply(propertyFilter).ifPresent(allConditions::add);
        }

        final BiFunction<Condition, Condition, Condition> combination;
        if (logicalOperator == LogicalOperator.XOR) {
            // Shouldn't throw an exception because we have at least one condition.
            combination = (condition1, condition2) -> condition1.or(condition2).andNot(condition1.and(condition2));
        } else if (logicalOperator == LogicalOperator.OR) {
            combination = Condition::or;
        } else {
            combination = Condition::and;
        }
        return combineConditions(allConditions, combination);
    }

    @Nonnull
    private static Optional<Condition> createDisplayNameSearchCondition(
            @Nonnull PropertyFilter propertyFilter) {
        if (!propertyFilter.hasStringFilter()) {
            throw new IllegalArgumentException("Filter for display name must have StringFilter");
        }
        final StringFilter filter = propertyFilter.getStringFilter();
        if (filter.hasStringPropertyRegex()) {
            if (filter.getPositiveMatch()) {
                return filter.getCaseSensitive()
                        ? Optional.of(GROUPING.DISPLAY_NAME.collate(CASE_SENSITIVE_COLLATION)
                        .likeRegex(filter.getStringPropertyRegex()))
                        : Optional.of(GROUPING.DISPLAY_NAME
                        .likeRegex(filter.getStringPropertyRegex()));
            } else {
                // if filter has the positive_match flag set to false, negate the regex matching
                return filter.getCaseSensitive()
                        ? Optional.of(GROUPING.DISPLAY_NAME.collate(CASE_SENSITIVE_COLLATION)
                        .notLikeRegex(filter.getStringPropertyRegex()))
                        : Optional.of(GROUPING.DISPLAY_NAME
                        .notLikeRegex(filter.getStringPropertyRegex()));
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
    private static Condition createTagsSearchCondition(@Nonnull PropertyFilter propertyFilter) {
        if (!propertyFilter.hasMapFilter()) {
            throw new IllegalArgumentException(
                    "MapFilter is expected for " + StringConstants.TAGS_ATTR + " filter: "
                            + propertyFilter);
        }
        final MapFilter filter = propertyFilter.getMapFilter();
        final Condition tagCondition;
        if (StringUtils.isEmpty(filter.getKey())) {
            // key is not present in the filter
            // string key=value must match the regex
            final Field<String> stringToMatch =
                    DSL.concat(GROUP_TAGS.TAG_KEY, DSL.val("="), GROUP_TAGS.TAG_VALUE);
            tagCondition = filter.getPositiveMatch()
                                    ? stringToMatch.likeRegex(filter.getRegex())
                                    : stringToMatch.notLikeRegex(filter.getRegex());
        } else {
            // key is present in the filter
            // key must match and value must satisfy a specific predicate
            final Condition tagKeyCondition = GROUP_TAGS.TAG_KEY.eq(filter.getKey());
            final Condition tagValueCondition;
            if (!StringUtils.isEmpty(filter.getRegex())) {
                // value must match regex
                tagValueCondition = GROUP_TAGS.TAG_VALUE.likeRegex(filter.getRegex());
            } else if (!filter.getValuesList().isEmpty()) {
                // value must be equal to one of the options
                tagValueCondition = GROUP_TAGS.TAG_VALUE.in(filter.getValuesList());
            } else {
                // no restriction on the value
                tagValueCondition = DSL.trueCondition();
            }
            if (filter.getPositiveMatch()) {
                tagCondition = tagKeyCondition.and(tagValueCondition);
            } else {
                tagCondition = tagKeyCondition.and(tagValueCondition.not());
            }
        }
        return GROUPING.ID.in(DSL.select(GROUP_TAGS.GROUP_ID)
                                 .from(GROUP_TAGS)
                                 .where(tagCondition));
    }

    @Nonnull
    private static Condition createOidCondition(@Nonnull PropertyFilter filter,
            @Nonnull Field<Long> fieldToCheck) {
        if (!filter.hasStringFilter()) {
            throw new IllegalArgumentException(
                    "String filter is expected for property " + filter.getPropertyName() + ": "
                            + filter);
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
                        "Illegal " + filter.getPropertyName() + " format \"" + option
                                + "\" in filter: " + filter);
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
    private static Optional<Condition> createOriginFilter(@Nonnull OriginFilter originFilter) {
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
    private static Condition createIdFilter(@Nonnull List<Long> groupIds) {
        return GROUPING.ID.in(groupIds);
    }

    @Nonnull
    private static Optional<Condition> combineConditions(@Nonnull Collection<Condition> conditions,
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
            groupUpdateListeners.forEach(l -> l.onUserGroupDeleted(groupId));
        } catch (DataAccessException e) {
            GROUP_STORE_ERROR_COUNT.labels(DELETE_LABEL).increment();
            throw e;
        }
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

    @Override
    public void updateDiscoveredGroups(@Nonnull Collection<DiscoveredGroup> groupsToAdd,
            @Nonnull Collection<DiscoveredGroup> groupsToUpdate, @Nonnull Set<Long> groupsToDelete)
            throws StoreOperationException {
        logger.debug("Is about to add the following groups: {}", groupsToAdd);
        logger.debug("Is about to update the following groups: {}", groupsToUpdate);
        logger.debug("Is about to delete the following groups: {}", groupsToDelete);
        updateDiscoveredGroups(dslContext, groupsToAdd, groupsToUpdate, groupsToDelete);
    }

    /**
     * Adds or updates discovered groups in the appliance. Discovered groups that are not listed in
     * either collection are not touched.
     *
     * @param context DB connection context
     * @param groupsToAdd groups to add to the store.
     * @param groupsToUpdate groups to be updated in the store.
     * @param groupsToDelete groups to delete from the store.
     * @throws StoreOperationException if group configuration is incorrect
     */
    private void updateDiscoveredGroups(@Nonnull DSLContext context,
            @Nonnull Collection<DiscoveredGroup> groupsToAdd,
            @Nonnull Collection<DiscoveredGroup> groupsToUpdate, @Nonnull Set<Long> groupsToDelete)
            throws StoreOperationException {
        final Set<Long> newGroupOids = Stream.of(groupsToAdd, groupsToUpdate)
                .flatMap(Collection::stream)
                .map(DiscoveredGroup::getOid)
                .collect(Collectors.toSet());
        context.deleteFrom(GROUPING)
                .where(GROUPING.ORIGIN_DISCOVERED_SRC_ID.isNotNull()
                        .and(GROUPING.ID.in(groupsToDelete)))
                .execute();
        final Set<Long> groupsToIgnore =
                getIgnoredDiscoveredGroups(context, Sets.union(newGroupOids, groupsToDelete));
        if (!groupsToIgnore.isEmpty()) {
            logger.info("Following groups are ignored by update, as the related target has not been"
                    + " discovered yet: " + groupsToIgnore);
        }
        cleanDiscoveredGroupsChildTables(context, groupsToIgnore);
        final Collection<Query> inserts = new ArrayList<>();
        final Collection<Query> newGroups = new ArrayList<>();
        final Collection<Query> updates = new ArrayList<>();
        createGroupStatements(context, inserts, newGroups,
                groupPojo -> context.insertInto(GROUPING).set(context.newRecord(GROUPING,
                    groupPojo)), groupsToAdd);
        createGroupStatements(context, inserts, updates,
                groupPojo -> createGroupUpdate(context, groupPojo), groupsToUpdate);

        context.batch(updates).execute();
        context.batch(newGroups).execute();
        context.batch(inserts).execute();
    }

    private <T> void createGroupStatements(@Nonnull DSLContext context,
            @Nonnull Collection<Query> insertsToAppend,
            @Nonnull Collection<T> queriesToAppend, Function<Grouping, T> createFunction,
            @Nonnull Collection<DiscoveredGroup> groups) throws StoreOperationException {
        for (DiscoveredGroup group : groups) {
            requireTrue(!group.getSourceIdentifier().isEmpty(), "Source identifier must be set");
            final String sourceIdentifier = group.getSourceIdentifier();
            final GroupDefinition def = group.getDefinition();
            final long effectiveId = group.getOid();
            final Grouping groupPojo = createGroupFromDefinition(def);
            groupPojo.setId(effectiveId);
            groupPojo.setSupportsMemberReverseLookup(group.isReverseLookupSupported());
            groupPojo.setOriginDiscoveredSrcId(sourceIdentifier);
            groupPojo.setHash(DiscoveredGroupHash.hash(group));
            queriesToAppend.add(createFunction.apply(groupPojo));
            insertsToAppend.addAll(insertGroupDefinitionDependencies(context, effectiveId, def));
            insertsToAppend.addAll(insertExpectedMembers(context, groupPojo.getId(),
                    new HashSet<>(group.getExpectedMembers()),
                    group.getDefinition().getStaticGroupMembers()));
            insertsToAppend.addAll(
                    createTargetForGroupRecords(context, effectiveId, group.getTargetIds()));
        }
    }

    @Nonnull
    private Set<Long> getIgnoredDiscoveredGroups(@Nonnull DSLContext context, @Nonnull Set<Long> groupsToUpdate) {
        final Set<Long> allDiscoveredGroupsInDb = context.select(GROUPING.ID)
                .from(GROUPING)
                .where(GROUPING.ORIGIN_DISCOVERED_SRC_ID.isNotNull())
                .fetch()
                .stream()
                .map(Record1::value1)
                .collect(Collectors.toSet());
        final Set<Long> ignoredGroups = new HashSet<>(allDiscoveredGroupsInDb);
        ignoredGroups.removeAll(groupsToUpdate);
        return ignoredGroups;
    }

    /**
     * Method cleans all the child records for all the discovered groups in the database.
     *
     * @param context DB context to use.
     * @param groupsToIgnore groups not to touch while cleaning the data
     */
    private void cleanDiscoveredGroupsChildTables(@Nonnull DSLContext context,
            @Nonnull Set<Long> groupsToIgnore) {
        final Condition additionalCondition =
                groupsToIgnore.isEmpty() ? DSL.noCondition() : GROUPING.ID.notIn(groupsToIgnore);
        final Select<Record1<Long>> groupIds = context.select(GROUPING.ID)
                .from(GROUPING)
                .where(GROUPING.ORIGIN_DISCOVERED_SRC_ID.isNotNull())
                .and(additionalCondition);
        context.deleteFrom(GROUP_STATIC_MEMBERS_GROUPS)
                .where(GROUP_STATIC_MEMBERS_GROUPS.PARENT_GROUP_ID.in(groupIds))
                .execute();
        context.deleteFrom(GROUP_STATIC_MEMBERS_ENTITIES)
                .where(GROUP_STATIC_MEMBERS_ENTITIES.GROUP_ID.in(groupIds))
                .execute();
        context.deleteFrom(GROUP_EXPECTED_MEMBERS_ENTITIES)
                .where(GROUP_EXPECTED_MEMBERS_ENTITIES.GROUP_ID.in(groupIds))
                .execute();
        context.deleteFrom(GROUP_EXPECTED_MEMBERS_GROUPS)
                .where(GROUP_EXPECTED_MEMBERS_GROUPS.GROUP_ID.in(groupIds))
                .execute();
        context.deleteFrom(GROUP_TAGS).where(GROUP_TAGS.GROUP_ID.in(groupIds)).execute();
        final Condition targetCondition = groupsToIgnore.isEmpty() ? DSL.noCondition()
                : GROUP_DISCOVER_TARGETS.GROUP_ID.notIn(groupsToIgnore);
        context.deleteFrom(GROUP_DISCOVER_TARGETS).where(targetCondition).execute();
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
                .set(GROUPING.HASH, groupPojo.getHash())
                .where(GROUPING.ID.eq(groupId));
    }

    @Nonnull
    @Override
    public Map<Long, Map<String, Set<String>>> getTags(@Nonnull final Collection<Long> groupIds) {
        final Map<Long, Map<String, Set<String>>> tagsMap = new HashMap<>();
        SelectJoinStep<Record3<Long, String, String>> query =
                dslContext.select(GROUP_TAGS.GROUP_ID, GROUP_TAGS.TAG_KEY, GROUP_TAGS.TAG_VALUE)
                        .from(GROUP_TAGS);
        if (!groupIds.isEmpty()) {
            query.where(GROUP_TAGS.GROUP_ID.in(groupIds));
        }
        final Result<Record3<Long, String, String>> groupsTags = query.fetch();
        for (Record3<Long, String, String> record : groupsTags) {
            final Long groupId = record.value1();
            final String tagName = record.value2();
            final String tagValue = record.value3();
            final Map<String, Set<String>> tags = new HashMap<>();
            tags.put(tagName, Sets.newHashSet(tagValue));
            tagsMap.merge(groupId, tags, (stringSetMap, stringSetMap2) -> {
                stringSetMap2.forEach(
                        (key, value) -> stringSetMap.merge(key, value, (strings, strings2) -> {
                            strings.addAll(strings2);
                            return strings;
                        }));
                return stringSetMap;
            });
        }
        return tagsMap;
    }

    @Nonnull
    @Override
    public Map<Long, Set<Long>> getStaticGroupsForEntities(
            @Nonnull Collection<Long> entityIds, @Nonnull Collection<GroupType> groupTypes) {
        try {
            return getStaticGroupsForEntityInternal(entityIds, groupTypes);
        } catch (DataAccessException e) {
            GROUP_STORE_ERROR_COUNT.labels(GET_LABEL).increment();
            throw e;
        }
    }

    @Nonnull
    private Map<Long, Set<Long>> getStaticGroupsForEntityInternal(
            @Nonnull Collection<Long> entityId, @Nonnull Collection<GroupType> groupTypes) {
        final SelectConditionStep<Record2<Long, Long>> query = groupTypes.isEmpty()
                ? dslContext.select(GROUP_STATIC_MEMBERS_ENTITIES.ENTITY_ID,
                        GROUP_STATIC_MEMBERS_ENTITIES.GROUP_ID)
                        .from(GROUP_STATIC_MEMBERS_ENTITIES)
                        .where(GROUP_STATIC_MEMBERS_ENTITIES.ENTITY_ID.in(entityId))
                : dslContext.select(GROUP_STATIC_MEMBERS_ENTITIES.ENTITY_ID,
                        GROUP_STATIC_MEMBERS_ENTITIES.GROUP_ID)
                        .from(GROUP_STATIC_MEMBERS_ENTITIES)
                        .join(GROUPING)
                        .on(GROUPING.ID.eq(GROUP_STATIC_MEMBERS_ENTITIES.GROUP_ID)
                                .and(GROUPING.GROUP_TYPE.in(groupTypes)))
                        .where(GROUP_STATIC_MEMBERS_ENTITIES.ENTITY_ID.in(entityId));
        final Map<Long, Set<Long>> entityToGroupMap = new HashMap<>();
        query.fetch()
                .forEach(record -> entityToGroupMap.computeIfAbsent(record.value1(),
                        key -> new HashSet<>()).add(record.value2()));
        return Collections.unmodifiableMap(entityToGroupMap);
    }

    private static void requireTrue(boolean condition, @Nonnull String message)
            throws StoreOperationException {
        if (!condition) {
            throw new StoreOperationException(Status.INVALID_ARGUMENT, message);
        }
    }

    @Override
    public void deleteAllGroups() {
        dslContext.deleteFrom(Tables.POLICY_GROUP).execute();
        dslContext.deleteFrom(GROUPING).execute();
    }

    @Nonnull
    @Override
    public Set<Long> getOwnersOfGroups(@Nonnull Collection<Long> groupIds,
            @Nullable GroupType groupType) {
        if (groupIds.isEmpty()) {
            return Collections.emptySet();
        }
        final SelectJoinStep<Record1<Long>> query =
                dslContext.select(GROUPING.OWNER_ID).from(GROUPING);
        if (groupType == null) {
            query.where(GROUPING.ID.in(groupIds));
        } else {
            query.where(GROUPING.GROUP_TYPE.eq(groupType).and(GROUPING.ID.in(groupIds)));
        }
        return new HashSet<>(query.fetchInto(Long.class));
    }

    /**
     * Immutable discovered group id implementation. Just an immutable POJO.
     */
    @Immutable
    public static class DiscoveredGroupIdImpl implements DiscoveredGroupId {
        private final Long targetId;
        private final DiscoveredObjectVersionIdentity identity;
        private final String sourceId;
        private final GroupType groupType;

        /**
         * Constructs discovered group id.
         *
         * @param identity identity of the group (OID + hash)
         * @param targetId target the group is reporeted
         * @param sourceId source id
         * @param groupType group type
         */
        public DiscoveredGroupIdImpl(@Nonnull DiscoveredObjectVersionIdentity identity,
                @Nullable Long targetId, @Nonnull String sourceId, @Nonnull GroupType groupType) {
            this.targetId = targetId;
            this.identity = Objects.requireNonNull(identity);
            this.sourceId = Objects.requireNonNull(sourceId);
            this.groupType = Objects.requireNonNull(groupType);
        }

        @Nullable
        @Override
        public Long getTarget() {
            return targetId;
        }

        @Override
        public DiscoveredObjectVersionIdentity getIdentity() {
            return identity;
        }

        @Nonnull
        @Override
        public String getSourceId() {
            return sourceId;
        }

        @Nonnull
        @Override
        public GroupType getGroupType() {
            return groupType;
        }
    }
}
