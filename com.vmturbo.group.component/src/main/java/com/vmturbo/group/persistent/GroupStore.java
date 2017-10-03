package com.vmturbo.group.persistent;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.arangodb.ArangoCollection;
import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDBException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.Group.Origin;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupInfo;
import com.vmturbo.group.ArangoDriverFactory;
import com.vmturbo.group.GroupDBDefinition;
import com.vmturbo.group.identity.IdentityProvider;
import com.vmturbo.group.persistent.TargetCollectionUpdate.TargetGroupUpdate;

public class GroupStore {

    private final static Logger logger = LogManager.getLogger();

    @VisibleForTesting
    final static String ALL_GROUPS_QUERY = "FOR g IN @@group_collection RETURN g";

    @VisibleForTesting
    final static String GROUPS_BY_TARGET_QUERY = "FOR g in @@group_collection " +
            "FILTER g.targetId == @targetId " +
            "RETURN g";

    @VisibleForTesting
    final static String GROUPS_BY_NAME_QUERY = "FOR g in @@group_collection " +
            "FILTER g.displayName == @displayName " +
            "RETURN g";

    private final ArangoDriverFactory arangoDriverFactory;
    private final GroupDBDefinition groupDBDefinition;
    private final IdentityProvider identityProvider;

    public GroupStore(final ArangoDriverFactory arangoDriverFactory,
                      final GroupDBDefinition groupDBDefinitionArg,
                      final IdentityProvider identityProvider) {
        this.arangoDriverFactory = Objects.requireNonNull(arangoDriverFactory);
        this.identityProvider = Objects.requireNonNull(identityProvider);
        this.groupDBDefinition = Objects.requireNonNull(groupDBDefinitionArg);
    }

    /**
     * Update the set of groups discovered by a particular target.
     * The new set of groups will completely replace the old, even if the new set is empty.
     *
     * <p>See {@link TargetGroupUpdate} for details on the update behaviour.
     *
     * @param targetId The ID of the target that discovered the groups.
     * @param newGroups The new set of discovered {@link GroupInfo}s.
     * @return a mapping of group key (name/SE type) to group OID
     * @throws DatabaseException If there is an error interacting with the database.
     */
    public Map<String, Long>  updateTargetGroups(final long targetId,
                                   @Nonnull final List<GroupInfo> newGroups)
            throws DatabaseException {
        logger.info("Updating groups discovered by {}. Got {} new groups.", targetId, newGroups.size());
        final TargetGroupUpdate update = new TargetGroupUpdate(targetId, identityProvider,
                newGroups, getDiscoveredByTarget(targetId));
        // (roman, August 1 2017): These should happen in a single transaction, but
        // since we are migrating from ArangoDB to MySQL I'm not taking the time to figure
        // out how to do transactional updates in ArangoDB, and just using the current GroupStore.
        Map<String, Long> map = update.apply(this::store, this::delete);
        logger.info("Finished updating discovered groups.");
        return map;
    }


    /**
     * Retrieve a {@link Group} by it's ID.
     *
     * @param id The ID of the group to look for.
     * @return The {@link Group} associated with that ID.
     * @throws DatabaseException If there is an error interacting with the database.
     */
    @Nonnull
    public Optional<Group> get(final long id) throws DatabaseException {
        return getGroups(Collections.singleton(id)).get(id);
    }

    /**
     * Get groups with IDs in a set from the store.
     *
     * @param ids The IDs to look for.
     * @return A map with an entry for each input ID. The value is an optional containing the
     *         {@link Group} with that ID, or an empty optional if that group was not found.
     * @throws DatabaseException If there is an error interacting with the database.
     */
    @Nonnull
    public Map<Long, Optional<Group>> getGroups(@Nonnull final Collection<Long> ids)
            throws DatabaseException {
        // (roman, Oct 2 2017): This is a temporary implementation - when building on top
        // of MySQL this filtering should be done by the database.
        final Map<Long, Optional<Group>> requestedIds = ids.stream()
            .collect(Collectors.toMap(Function.identity(), x -> Optional.empty()));
        getAll().stream()
            .filter(group -> requestedIds.containsKey(group.getId()))
            .forEach(group -> requestedIds.put(group.getId(), Optional.of(group)));
        return requestedIds;
    }

    /**
     * Get all groups in the store.
     *
     * @return A collection of {@link Group}s for every group the store knows about.
     *         Each group will only appear once.
     * @throws DatabaseException If there is an error interacting with the database.
     */
    @Nonnull
    public Collection<Group> getAll() throws DatabaseException {
        ArangoDB arangoDB = arangoDriverFactory.getDriver();

        try {
            final Map<String, Object> bindVars = ImmutableMap.of(
                    "@group_collection", groupDBDefinition.groupCollection());

            final ArangoCursor<Group> cursor = arangoDB
                    .db(groupDBDefinition.databaseName())
                    .query(ALL_GROUPS_QUERY, bindVars, null, Group.class);

            return cursor.asListRemaining();
        } catch (ArangoDBException e) {
            throw new DatabaseException("Failed to get all groups.", e);
        } finally {
            arangoDB.shutdown();
        }
    }

    /**
     * Create a new group from a {@link GroupInfo} provided by a user of the group component.
     *
     * @param groupInfo The {@link GroupInfo} defining the group.
     * @return The {@link Group} describing the newly created group.
     * @throws DatabaseException If there is an error saving the group to the database.
     * @throws DuplicateGroupException If a group with the same name already exists.
     */
    @Nonnull
    public Group newUserGroup(@Nonnull final GroupInfo groupInfo) throws DatabaseException, DuplicateGroupException {
        final long oid = identityProvider.next();
        if (checkForDuplicates(oid, groupInfo)) {
            throw new DuplicateGroupException(groupInfo);
        }

        final Group group = Group.newBuilder()
                .setId(oid)
                .setOrigin(Origin.USER)
                .setInfo(groupInfo)
                .build();
        store(group);
        return group;
    }

    /**
     * Update an existing group created via {@link GroupStore#newUserGroup(GroupInfo)}.
     *
     * @param id The id of the group to update.
     * @param newInfo The new {@link GroupInfo} for the group.
     * @return The updated {@link Group} object.
     * @throws ImmutableUpdateException If the ID identifies a discovered (i.e. non-user) group.
     * @throws GroupNotFoundException If a group associated with the ID is not found.
     * @throws DatabaseException If there is an error reading from or writing to the database.
     * @throws DuplicateGroupException If there is already a different group with the same name.
     */
    @Nonnull
    public Group updateUserGroup(final long id,
                                 @Nonnull final GroupInfo newInfo)
            throws ImmutableUpdateException, GroupNotFoundException, DatabaseException, DuplicateGroupException {
        final Optional<Group> existingGroupOpt = get(id);
        if (existingGroupOpt.isPresent()) {
            final Group existingGroup = existingGroupOpt.get();
            if (existingGroup.getOrigin().equals(Origin.DISCOVERED)) {
                throw new ImmutableUpdateException(existingGroup);
            }
            if (checkForDuplicates(id, newInfo)) {
                throw new DuplicateGroupException(newInfo);
            }

            final Group newGroup = existingGroup.toBuilder().setInfo(newInfo).build();
            store(newGroup);
            return newGroup;
        } else {
            throw new GroupNotFoundException(id);
        }
    }

    /**
     * Delete an existing group created via {@link GroupStore#newUserGroup(GroupInfo)}.
     *
     * @param id The id of the group to delete.
     * @throws ImmutableUpdateException If the ID identifies a discovered (i.e. non-user) group.
     * @throws GroupNotFoundException If a group associated with the ID is not found.
     * @throws DatabaseException If there is an error reading from or writing to the database.
     */
    public void deleteUserGroup(final long id)
            throws ImmutableUpdateException, GroupNotFoundException, DatabaseException {
        final Group existingGroup = get(id).orElseThrow(() -> new GroupNotFoundException(id));
        if (existingGroup.getOrigin().equals(Origin.DISCOVERED)) {
            throw new ImmutableUpdateException(existingGroup);
        }
        // The group exists, and is non-discovered. It's safe to delete.
        delete(id);
    }

    /**
     * Check if there is a duplicate group.
     * A duplicate group has the same name as the groupInfo but a different id.
     * If multiple groups already have the same name, the group will be considered a duplicate
     * if its id is not contained in the list of all groups with that name.
     * TODO: This code is very inefficient. When moving to SQL, use a unique constraint on the name field instead.
     *
     * @param id The id of the new group.
     * @param groupInfo The definition for the new group.
     * @return true if the groupInfo duplicates the name of an existing, persisted group
     *         but has a different oid than that group, false otherwise.
     */
    @VisibleForTesting
    boolean checkForDuplicates(final long id, @Nonnull final GroupInfo groupInfo)
            throws DatabaseException {
        ArangoDB arangoDB = arangoDriverFactory.getDriver();

        try {
            final Map<String, Object> bindVars = ImmutableMap.of(
                "@group_collection", groupDBDefinition.groupCollection(),
                "displayName", groupInfo.getName());

            final ArangoCursor<Group> cursor = arangoDB
                .db(groupDBDefinition.databaseName())
                .query(GROUPS_BY_NAME_QUERY, bindVars, null, Group.class);
            final List<Group> groups = cursor.asListRemaining();
            if (groups.isEmpty()) {
                return false;
            } else if (groups.size() > 1) {
                logger.error("Multiple groups with the same name ({}) exist: {}", groupInfo.getName(), groups);
            }

            // If the group being created/edited is not in the list of groups with the given name,
            // we consider it to be a duplicate. If we had a database constraint this is a case
            // we would not really have to consider.
            return !(groups.isEmpty() || groups.stream()
                .map(Group::getId)
                .anyMatch(otherId -> otherId == id));
        } catch (ArangoDBException e) {
            throw new DatabaseException("Failed to get query for duplicate groups on group with name "
                + groupInfo.getName(), e);
        } finally {
            arangoDB.shutdown();
        }
    }

    @Nonnull
    private Collection<Group> getDiscoveredByTarget(final long targetId) throws DatabaseException {
        ArangoDB arangoDB = arangoDriverFactory.getDriver();

        try {
            final Map<String, Object> bindVars = ImmutableMap.of(
                    "@group_collection", groupDBDefinition.groupCollection(),
                    "targetId", targetId);

            final ArangoCursor<Group> cursor = arangoDB
                    .db(groupDBDefinition.databaseName())
                    .query(GROUPS_BY_TARGET_QUERY, bindVars, null, Group.class);

            return cursor.asListRemaining();
        } catch (ArangoDBException e) {
            throw new DatabaseException("Failed to get groups discovered by target " + targetId, e);
        } finally {
            arangoDB.shutdown();
        }
    }

    private void store(@Nonnull final Group group) throws DatabaseException {
        ArangoDB arangoDB = arangoDriverFactory.getDriver();

        try {
            final ArangoCollection groupCollection = arangoDB
                .db(groupDBDefinition.databaseName())
                .collection(groupDBDefinition.groupCollection());

            final String groupID = Long.toString(group.getId());

            if (groupCollection.documentExists(groupID)) {
                groupCollection.replaceDocument(groupID, group);
            } else {
                groupCollection.insertDocument(group);
            }
        } catch (ArangoDBException e) {
            throw new DatabaseException("Failed to save group: " + group, e);
        } finally {
            arangoDB.shutdown();
        }
    }

    private void delete(final Long id) throws DatabaseException {
        ArangoDB arangoDB = arangoDriverFactory.getDriver();

        try {
            arangoDB.db(groupDBDefinition.databaseName())
                    .collection(groupDBDefinition.groupCollection())
                    .deleteDocument(Long.toString(id));
        } catch (ArangoDBException e) {
            throw new DatabaseException("Failed to delete group " + id, e);
        } finally {
            arangoDB.shutdown();
        }
    }

    public static class GroupNotFoundException extends Exception {
        private GroupNotFoundException(final long id) {
            super("Group " + id + " not found.");
        }
    }

    public static class ImmutableUpdateException extends Exception {
        private ImmutableUpdateException(Group group) {
            super("Attempt to update immutable discovered group " + group.getInfo().getName());
        }
    }

    public static class DuplicateGroupException extends Exception {
        private DuplicateGroupException(@Nonnull final GroupInfo duplicateGroup) {
            super("Cannot create group with name " + duplicateGroup.getName()
                + " because a group with the same name already exists.");
        }
    }
}
