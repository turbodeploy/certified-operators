package com.vmturbo.group.group;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import com.google.common.base.Preconditions;
import com.google.gson.reflect.TypeToken;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;

import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.Group.Origin;
import com.vmturbo.common.protobuf.group.GroupDTO.Group.Type;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.TempGroupInfo;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.common.diagnostics.Diagnosable;
import com.vmturbo.group.common.DuplicateNameException;
import com.vmturbo.group.common.ImmutableUpdateException.ImmutableGroupUpdateException;
import com.vmturbo.group.common.ItemNotFoundException.GroupNotFoundException;
import com.vmturbo.group.common.TargetCollectionUpdate.TargetClusterUpdate;
import com.vmturbo.group.common.TargetCollectionUpdate.TargetGroupUpdate;
import com.vmturbo.group.db.Tables;
import com.vmturbo.group.db.tables.pojos.Grouping;
import com.vmturbo.group.db.tables.records.GroupingRecord;
import com.vmturbo.group.identity.IdentityProvider;
import com.vmturbo.group.policy.PolicyStore;
import com.vmturbo.group.policy.PolicyStore.PolicyDeleteException;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.proactivesupport.DataMetricCounter;

/**
 * The {@link GroupStore} class is used for CRUD operations on groups, to abstract away the
 * persistence details from the rest of the component.
 */
public class GroupStore implements Diagnosable {

    private static final String GET_LABEL = "get";

    private static final String CREATE_LABEL = "create";

    private static final String UPDATE_LABEL = "update";

    private static final String DELETE_LABEL = "delete";

    private static final DataMetricCounter GROUP_STORE_ERROR_COUNT = DataMetricCounter.builder()
            .withName("group_store_error_count")
            .withHelp("Number of errors encountered in operating the group store.")
            .withLabelNames("operation")
            .build()
            .register();

    private static final DataMetricCounter GROUP_STORE_DUPLICATE_NAME_COUNT = DataMetricCounter.builder()
            .withName("group_store_duplicate_name_count")
            .withHelp("Number of duplicate name attempts in operating the group store.")
            .build()
            .register();

    private final static Logger logger = LogManager.getLogger();

    private final IdentityProvider identityProvider;

    private final DSLContext dslContext;

    private final PolicyStore policyStore;

    public GroupStore(final DSLContext dslContext,
                      final PolicyStore policyStore,
                      final IdentityProvider identityProvider) {
        this.dslContext = Objects.requireNonNull(dslContext);
        this.policyStore = Objects.requireNonNull(policyStore);
        this.identityProvider = Objects.requireNonNull(identityProvider);
    }

    /**
     * Update the set of groups discovered by a particular target.
     * The new set of groups will completely replace the old, even if the new set is empty.
     *
     * <p>See {@link TargetGroupUpdate} for details on the update behaviour.
     *
     * @param context The context to use to do the updates.
     * @param targetId The ID of the target that discovered the groups.
     * @param newGroups The new set of discovered {@link GroupInfo}s.
     * @return a mapping of group key (name/SE type) to group OID
     * @throws DataAccessException If there is an error interacting with the database.
     */
    public Map<String, Long> updateTargetGroups(@Nonnull final DSLContext context,
                                                final long targetId,
                                                @Nonnull final List<GroupInfo> newGroups,
                                                @Nonnull final List<ClusterInfo> newClusters)
            throws DataAccessException {
        logger.info("Updating groups discovered by {}. Got {} new groups and {} clusters.",
                targetId, newGroups.size(), newClusters.size());
        final Map<Group.Type, List<Group>> groupsByType = internalGet(context,
                GroupFilter.newBuilder().addDiscoveredBy(targetId).build())
                        .collect(Collectors.groupingBy(Group::getType));
        final TargetGroupUpdate groupUpdate = new TargetGroupUpdate(targetId, identityProvider,
                newGroups, groupsByType.getOrDefault(Group.Type.GROUP, Collections.emptyList()));
        final TargetClusterUpdate clusterUpdate = new TargetClusterUpdate(targetId, identityProvider,
                newClusters, groupsByType.getOrDefault(Group.Type.CLUSTER, Collections.emptyList()));
        final Map<String, Long> nameToOidMap = groupUpdate.apply(
                (group) -> internalInsert(context, group),
                (group) -> internalUpdate(context, group),
                (group) -> internalDelete(context, group));
        nameToOidMap.putAll(clusterUpdate.apply(
                (cluster) -> internalInsert(context, cluster),
                (cluster) -> internalUpdate(context, cluster),
                (cluster) -> internalDelete(context, cluster)));

        logger.info("Finished updating discovered groups.");
        return nameToOidMap;
    }


    /**
     * Retrieve a {@link Group} by it's ID.
     *
     * @param id The ID of the group to look for.
     * @return The {@link Group} associated with that ID.
     * @throws DataAccessException If there is an error interacting with the database.
     */
    @Nonnull
    public Optional<Group> get(final long id) throws DataAccessException {
        try {
            return internalGet(dslContext, id);
        } catch (DataAccessException e) {
            GROUP_STORE_ERROR_COUNT.labels(GET_LABEL).increment();
            throw e;
        }
    }

    /**
     * Get groups with IDs in a set from the store.
     *
     * @param ids The IDs to look for.
     * @return A map with an entry for each input ID. The value is an optional containing the
     *         {@link Group} with that ID, or an empty optional if that group was not found.
     * @throws DataAccessException If there is an error interacting with the database.
     */
    @Nonnull
    public Map<Long, Optional<Group>> getGroups(@Nonnull final Collection<Long> ids)
            throws DataAccessException {
        final Map<Long, Optional<Group>> result = ids.stream()
                .collect(Collectors.toMap(Function.identity(),
                        (k) -> Optional.empty(), (id1, id2) -> id1));
        try {
            internalGet(dslContext, GroupFilter.newBuilder()
                    .addIds(ids)
                    .build())
                .forEach(group -> result.put(group.getId(), Optional.of(group)));
        } catch (DataAccessException e) {
            GROUP_STORE_ERROR_COUNT.labels(GET_LABEL).increment();
            throw e;
        }
        return result;
    }

    /**
     * Get all groups in the store.
     *
     * @return A collection of {@link Group}s for every group the store knows about.
     *         Each group will only appear once.
     * @throws DataAccessException If there is an error interacting with the database.
     */
    @Nonnull
    public Collection<Group> getAll() throws DataAccessException {
        try {
            return internalGet(dslContext, GroupFilter.newBuilder().build())
                    .collect(Collectors.toList());
        } catch (DataAccessException e) {
            GROUP_STORE_ERROR_COUNT.labels(GET_LABEL).increment();
            throw e;
        }
    }

    /**
     * Create a new group from a {@link GroupInfo} provided by a user of the group component.
     * Group names are unique, so the name in {@link GroupInfo} should not already be assigned
     * to another group.
     *
     * @param groupInfo The {@link GroupInfo} defining the group.
     * @return The {@link Group} describing the newly created group.
     * @throws DuplicateNameException If a group with the same name already exists.
     */
    @Nonnull
    public Group newUserGroup(@Nonnull final GroupInfo groupInfo)
            throws DuplicateNameException, DataAccessException {
        final long oid = identityProvider.next();
        try {
            return dslContext.transactionResult(configuration -> {
                final DSLContext transactionContext = DSL.using(configuration);

                final Group group = Group.newBuilder()
                        .setId(oid)
                        .setOrigin(Origin.USER)
                        .setType(Type.GROUP)
                        .setGroup(groupInfo)
                        .build();
                internalInsert(transactionContext, group);
                return group;
            });
        } catch (DataAccessException e) {
            GROUP_STORE_ERROR_COUNT.labels(CREATE_LABEL).increment();
            if (e.getCause() instanceof DuplicateNameException) {
                throw (DuplicateNameException)e.getCause();
            } else {
                throw e;
            }
        }
    }

    /**
     * Update an existing group created via {@link GroupStore#newUserGroup(GroupInfo)}.
     *
     * @param id The id of the group to update.
     * @param newInfo The new {@link GroupInfo} for the group.
     * @return The updated {@link Group} object.
     * @throws ImmutableGroupUpdateException If the ID identifies a discovered (i.e. non-user) group.
     * @throws GroupNotFoundException If a group associated with the ID is not found.
     * @throws DuplicateNameException If there is already a different group with the same name.
     */
    @Nonnull
    public Group updateUserGroup(final long id,
                                 @Nonnull final GroupInfo newInfo)
            throws ImmutableGroupUpdateException, GroupNotFoundException, DuplicateNameException, DataAccessException {
        try {
            return dslContext.transactionResult(configuration -> {
                final DSLContext transactionDsl = DSL.using(configuration);
                final Group existingGroup = internalGet(transactionDsl, id)
                        .orElseThrow(() -> new GroupNotFoundException(id));
                if (existingGroup.getOrigin().equals(Origin.DISCOVERED)) {
                    throw new ImmutableGroupUpdateException(existingGroup);
                }

                final Group newGroup = existingGroup.toBuilder().setGroup(newInfo).build();
                return internalUpdate(dslContext, newGroup);
            });
        } catch (DataAccessException e) {
            GROUP_STORE_ERROR_COUNT.labels(UPDATE_LABEL).increment();
            if (e.getCause() instanceof ImmutableGroupUpdateException) {
                throw (ImmutableGroupUpdateException)e.getCause();
            } else if (e.getCause() instanceof GroupNotFoundException) {
                throw (GroupNotFoundException)e.getCause();
            } else if (e.getCause() instanceof DuplicateNameException) {
                throw (DuplicateNameException)e.getCause();
            } else {
                throw e;
            }
        }
    }

    /**
     * Updates the cluster headroom template ID of a given cluster.
     *
     * @param groupId The Group ID of the cluster.
     * @param clusterHeadroomTemplateId The cluster headroom template ID.
     * @return The updated {@link Group} object.
     * @throws GroupNotFoundException If the group is not found.
     * @throws GroupNotClusterException IOf the group is not a cluster.
     * @throws DataAccessException If there is an error reading from or writing to the database.
     */
    @Nonnull
    public Group updateClusterHeadroomTemplate(final long groupId,
                                               final long clusterHeadroomTemplateId)
            throws GroupNotFoundException, GroupNotClusterException, DataAccessException {
        try {
            return dslContext.transactionResult(configuration -> {
                final DSLContext transactionDsl = DSL.using(configuration);
                final Group existingGroup = internalGet(transactionDsl, groupId)
                        .orElseThrow(() -> new GroupNotFoundException(groupId));
                if (!existingGroup.hasCluster()) {
                    throw new GroupNotClusterException(existingGroup.getId());
                }
                final ClusterInfo newClusterInfo = existingGroup.getCluster().toBuilder()
                        .setClusterHeadroomTemplateId(clusterHeadroomTemplateId)
                        .build();
                final Group newGroup = existingGroup.toBuilder()
                        .setCluster(newClusterInfo)
                        .build();
                return internalUpdate(transactionDsl, newGroup);
            });
        } catch (DataAccessException e) {
            GROUP_STORE_ERROR_COUNT.labels(UPDATE_LABEL).increment();
            if (e.getCause() instanceof GroupNotFoundException) {
                throw (GroupNotFoundException)e.getCause();
            } else if (e.getCause() instanceof GroupNotClusterException) {
                throw (GroupNotClusterException)e.getCause();
            } else {
                throw e;
            }
        }
    }

    /**
     * Delete an existing group created via {@link GroupStore#newUserGroup(GroupInfo)}.
     * This method also deletes any policies associated with the group.
     *
     * @param id The id of the group to delete.
     * @throws ImmutableGroupUpdateException If the ID identifies a discovered (i.e. non-user) group.
     * @throws GroupNotFoundException If a group associated with the ID is not found.
     * @throws DataAccessException If there is an error reading from or writing to the database.
     * @throws PolicyDeleteException If there is an error deleting related policies.
     */
    public Group deleteUserGroup(final long id)
            throws ImmutableGroupUpdateException, GroupNotFoundException, DataAccessException, PolicyDeleteException {
        try {
            return dslContext.transactionResult(configuration -> {
                final DSLContext transactionDsl = DSL.using(configuration);
                final Group existingGroup = internalGet(transactionDsl, id)
                        .orElseThrow(() -> new GroupNotFoundException(id));
                if (existingGroup.getOrigin().equals(Origin.DISCOVERED)) {
                    throw new ImmutableGroupUpdateException(existingGroup);
                }

                policyStore.deletePoliciesForGroup(transactionDsl, id);

                // The group exists, and is non-discovered. It's safe to delete.
                internalDelete(transactionDsl, id);
                return existingGroup;
            });
        } catch (DataAccessException e) {
            GROUP_STORE_ERROR_COUNT.labels(DELETE_LABEL).increment();
            if (e.getCause() instanceof ImmutableGroupUpdateException) {
                throw (ImmutableGroupUpdateException) e.getCause();
            } else if (e.getCause() instanceof GroupNotFoundException) {
                throw (GroupNotFoundException) e.getCause();
            } else if (e.getCause() instanceof PolicyDeleteException) {
                throw (PolicyDeleteException)e.getCause();
            } else {
                throw e;
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public List<String> collectDiags() throws DiagnosticsException {
        try {
            final Collection<Group> groups = getAll();
            logger.info("Collected diags for {} groups.", groups.size());

            return Collections.singletonList(ComponentGsonFactory
                    .createGsonNoPrettyPrint().toJson(groups));
        } catch (DataAccessException e) {
            throw new DiagnosticsException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void restoreDiags(@Nonnull List<String> collectedDiags) throws DiagnosticsException {
        // Replace all existing groups with the ones in the collected diags.
        Collection<Group> groups = ComponentGsonFactory.createGsonNoPrettyPrint()
                .fromJson(collectedDiags.get(0), new TypeToken<Collection<Group>>() { }.getType());
        logger.info("Attempting to restore {} groups from diagnostics.", groups.size());

        try {
            dslContext.transaction(configuration -> {
                final DSLContext transactionContext = DSL.using(configuration);
                final int affectedRows = transactionContext.deleteFrom(Tables.GROUPING).execute();
                logger.info("Removed {} groups.", affectedRows);
                groups.forEach(group -> {
                    try {
                        internalInsert(transactionContext, group);
                    } catch (DuplicateNameException | RuntimeException e) {
                        logger.error("Failed to restore group " + group.getId() +
                                        " due to exception.", e);
                    }
                });
            });
        } catch (DataAccessException e) {
            throw new DiagnosticsException(Collections.singletonList(e.getMessage() + ": " +
                    ExceptionUtils.getStackTrace(e)));
        }
    }

    /**
     * Check if there is a duplicate group.
     * A duplicate group has the same name as the groupInfo but a different id.
     *
     * @param context The {@link DSLContext} for the current transaction.
     * @param id The id of the new group.
     * @param name The name for the new group.
     * @throws DuplicateNameException If there is a group with the same name but a different ID.
     */
    private void checkForDuplicates(@Nonnull final DSLContext context,
                           final long id,
                           @Nonnull final String name,
                           final int type)
            throws DuplicateNameException {
        final List<Long> sameNameDiffId = context.select(Tables.GROUPING.ID)
                .from(Tables.GROUPING)
                .where(Tables.GROUPING.NAME.eq(name).and(Tables.GROUPING.TYPE.eq(type)))
                .and(Tables.GROUPING.ID.ne(id))
                .fetch()
                .getValues(Tables.GROUPING.ID);
        if (!sameNameDiffId.isEmpty()) {
            if (sameNameDiffId.size() > 1) {
                // This shouldn't happen, because there is a constraint on the name.
                logger.error("Multiple groups ({}) exist with name {} and type {}. " +
                                "This should never happen because the name column is unique.",
                        sameNameDiffId, name, type);
            }
            GROUP_STORE_DUPLICATE_NAME_COUNT.increment();
            throw new DuplicateNameException(sameNameDiffId.get(0), name, type);
        }
    }

    private Optional<Group> internalGet(@Nonnull final DSLContext context, final long groupId) {
        return internalGet(context, GroupFilter.newBuilder().addId(groupId).build()).findFirst();
    }

    private byte[] getGroupData(@Nonnull final Group group) {
        switch (group.getType()) {
            case GROUP:
                return group.getGroup().toByteArray();
            case CLUSTER:
                return group.getCluster().toByteArray();
            case TEMP_GROUP:
                return group.getTempGroup().toByteArray();
            default:
                throw new IllegalArgumentException("Unsupported group type: " + group.getType());
        }
    }

    private void internalDelete(@Nonnull final DSLContext context,
                                final long groupId) {
        // The entry from the POLICY_GROUP table should be deleted automatically
        // because of the foreign key constaint.
        context.deleteFrom(Tables.GROUPING)
                .where(Tables.GROUPING.ID.eq(groupId))
                .execute();
    }

    @Nonnull
    private Group internalInsert(@Nonnull final DSLContext context,
                                 @Nonnull final Group group)
            throws DuplicateNameException {
        final String groupName = GroupProtoUtil.getGroupName(group);
        final int groupEntityType = GroupProtoUtil.getEntityType(group);
        // Explicitly search for an existing group with the same name, so that we
        // know when to throw a DuplicateNameException as opposed to a generic
        // DataIntegrityException.
        checkForDuplicates(context, group.getId(), groupName, groupEntityType);

        final Grouping grouping = new Grouping(group.getId(),
                groupName,
                group.getOrigin().getNumber(),
                group.getType().getNumber(),
                groupEntityType,
                group.getOrigin().equals(Group.Origin.DISCOVERED) ? group.getTargetId() : null,
                getGroupData(group));

        context.newRecord(Tables.GROUPING, grouping).store();

        return toGroup(grouping).orElseThrow(() -> new IllegalArgumentException(
                "Failed to map grouping for group " + group.getId() + " (" + groupName + ") back to group."));
    }

    @Nonnull
    private Group internalUpdate(@Nonnull final DSLContext context,
                                 @Nonnull final Group group)
            throws GroupNotFoundException, DuplicateNameException {
        final String groupName = GroupProtoUtil.getGroupName(group);
        final int groupEntityType = GroupProtoUtil.getEntityType(group);
        final GroupingRecord groupingRecord =
                context.fetchOne(Tables.GROUPING, Tables.GROUPING.ID.eq(group.getId()));
        if (groupingRecord == null) {
            throw new GroupNotFoundException(group.getId());
        }

        checkForDuplicates(context, group.getId(), groupName, groupEntityType);

        if (group.hasTargetId()) {
            groupingRecord.setDiscoveredById(group.getTargetId());
        }

        groupingRecord.setGroupData(getGroupData(group));
        groupingRecord.setName(groupName);
        groupingRecord.setOrigin(group.getOrigin().getNumber());
        groupingRecord.setType(group.getType().getNumber());
        groupingRecord.setEntityType(groupEntityType);
        final int modifiedRecords = groupingRecord.update();
        if (modifiedRecords == 0) {
            // This should never happen, because we overwrote fields in the record,
            // and update() should always execute an UPDATE statement if some fields
            // got overwritten.
            throw new IllegalStateException("Failed to update record.");
        }

        return toGroup(groupingRecord.into(Grouping.class)).orElseThrow(() ->
                new IllegalArgumentException("Failed to map grouping for group " + group.getId() +
                        " (" + groupName + ") back to group."));
    }

    @Nonnull
    private Stream<Group> internalGet(@Nonnull final DSLContext context,
                                      @Nonnull final GroupFilter filter) {
        return context.selectFrom(Tables.GROUPING)
                .where(filter.getConditions())
                .fetch()
                .into(Grouping.class)
                .stream()
                .map(this::toGroup)
                .filter(Optional::isPresent)
                .map(Optional::get);
    }

    @Nonnull
    private Optional<Group> toGroup(@Nonnull final Grouping grouping) {
        final Group.Builder groupBuilder = Group.newBuilder()
            .setId(grouping.getId());

        final Group.Origin groupOrigin = Group.Origin.forNumber(grouping.getOrigin());
        if (groupOrigin == null) {
            // Shouldn't happen, except in the case of a failed migration.
            return Optional.empty();
        } else {
            groupBuilder.setOrigin(groupOrigin);
        }

        final Group.Type groupType = Group.Type.forNumber(grouping.getType());
        if (groupType == null) {
            // Shouldn't happen, except in the case of a failed migration.
            return Optional.empty();
        } else {
            groupBuilder.setType(groupType);
        }

        if (grouping.getDiscoveredById() != null) {
            groupBuilder.setTargetId(grouping.getDiscoveredById());
        }

        // Database has NOT NULL and default row.
        Preconditions.checkArgument(grouping.getEntityType() != null);
        final int entityTypeCol = grouping.getEntityType();

        try {
            switch (groupType) {
                case GROUP:
                    final GroupInfo.Builder group =
                            GroupInfo.parseFrom(grouping.getGroupData()).toBuilder();
                    if (!StringUtils.equals(group.getName(), grouping.getName())) {
                        logger.error("Inconsistent group name - column: {}, blob: {}. " +
                            "Keeping column value.", grouping.getName(), group.getName());
                        group.setName(grouping.getName());
                    }

                    if (group.getEntityType() != entityTypeCol) {
                        if (grouping.getEntityType() == -1) {
                            logger.warn("Group record has invalid entity type {}. " +
                                "This should only happen during migration.", entityTypeCol);
                        } else {
                            logger.error("Inconsistent group entity types - column: {}, blob: {}." +
                                " Keeping blob value.", entityTypeCol, group.getEntityType());
                        }
                    }
                    groupBuilder.setGroup(group);
                    break;
                case CLUSTER:
                    final ClusterInfo.Builder cluster =
                            ClusterInfo.parseFrom(grouping.getGroupData()).toBuilder();
                    if (!StringUtils.equals(cluster.getName(), grouping.getName())) {
                        logger.error("Inconsistent cluster name - column: {}, blob: {}. " +
                            "Keeping column value.", grouping.getName(), cluster.getName());
                        cluster.setName(grouping.getName());
                    }

                    final int entityType = cluster.getClusterType() == ClusterInfo.Type.COMPUTE ?
                            EntityType.PHYSICAL_MACHINE_VALUE : EntityType.STORAGE_VALUE;
                    if (entityType != entityTypeCol) {
                        if (entityTypeCol == -1) {
                            logger.warn("Cluster record has invalid entity type {}. " +
                                "This should only happen during migration.", entityTypeCol);
                        } else {
                            logger.error("Inconsistent cluster entity types - column: {}, blob: {}." +
                                " Keeping blob value.", entityTypeCol, entityType);
                        }
                    }
                    groupBuilder.setCluster(cluster);
                    break;
                case TEMP_GROUP:
                    // This shouldn't happen at the time of this writing (May 2018) but
                    // it's not fatal, so we allow it.
                    final TempGroupInfo.Builder tempGroup =
                            TempGroupInfo.parseFrom(grouping.getGroupData()).toBuilder();
                    if (!StringUtils.equals(tempGroup.getName(), grouping.getName())) {
                        logger.error("Inconsistent temp group name - column: {}, blob: {}. " +
                            "Keeping column value.", grouping.getName(), tempGroup.getName());
                        tempGroup.setName(grouping.getName());
                    }

                    if (tempGroup.getEntityType() != entityTypeCol) {
                        if (grouping.getEntityType() == -1) {
                            logger.warn("Temp group record has invalid entity type {}. " +
                                "This should only happen during migration.", entityTypeCol);
                        } else {
                            logger.error("Inconsistent temp groupentity types - column: {}, blob: {}." +
                                " Keeping blob value.", entityTypeCol, tempGroup.getEntityType());
                        }
                    }

                    groupBuilder.setTempGroup(tempGroup);
                    logger.warn("Temp group somehow made it into database: {}",
                            TextFormat.printToString(groupBuilder.getTempGroup()));
                    break;
                default:
                    return Optional.empty();
            }
            return Optional.of(groupBuilder.build());
        } catch (InvalidProtocolBufferException e) {
            return Optional.empty();
        }
    }

    public static class GroupNotClusterException extends Exception {
        public GroupNotClusterException(final long groupId) {
            super("Group " + groupId
                    + " is not a cluster. Cannot update cluster headroom template for this group.");
        }
    }
}
