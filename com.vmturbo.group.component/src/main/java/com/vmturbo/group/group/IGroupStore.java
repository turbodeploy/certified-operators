package com.vmturbo.group.group;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import com.google.common.collect.Multimap;
import com.google.common.collect.Table;

import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.GetTagValuesRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.GroupFilters;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.group.GroupDTO.Origin;
import com.vmturbo.common.protobuf.tag.Tag.Tags;
import com.vmturbo.group.DiscoveredObjectVersionIdentity;
import com.vmturbo.group.db.tables.pojos.GroupSupplementaryInfo;
import com.vmturbo.group.service.StoreOperationException;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;

/**
 * Store to operate with groups. It is responsible to create, save and query groups.
 */
public interface IGroupStore {

    /**
     * Create a new group based on group definition. Discovered groups are not supported by this
     * call. Discovered groups should be created with {@link #updateDiscoveredGroups(Collection,
     * Collection, Set)}*
     *
     * @param oid oid for the new group
     * @param origin origin of this group
     * @param groupDefinition group definition
     * @param expecMemberTypes expected members types of this group
     * @param supportReverseLookup whether the group supports reverse lookups
     * @throws StoreOperationException if operation failed
     * @see #updateDiscoveredGroups(Collection, Collection, Set) #updateDiscoveredGroups(Collection,
     *         Collection, Set)
     */
    void createGroup(long oid, @Nonnull Origin origin, @Nonnull GroupDefinition groupDefinition,
            @Nonnull Set<MemberType> expecMemberTypes, boolean supportReverseLookup)
            throws StoreOperationException;

    /**
     * Creates a new entry with the supplementary characteristics of the group, for the group
     * provided. These are data that are not known when we create the group (they derive from
     * calculations after the definition & other information have been created), so a different call
     * is required.
     * Currently they include emptiness, environment and cloud type.
     *
     * @param groupId group's uuid.
     * @param isEmpty whether the group is empty.
     * @param groupEnvironment group's environment (environment + cloud type)
     * @param severity group's severity.
     */
    void createGroupSupplementaryInfo(long groupId, boolean isEmpty,
            @Nonnull GroupEnvironment groupEnvironment, @Nonnull Severity severity);

    /**
     * Returns the group type for the given group.
     *
     * @param groupId the group whose type to return.
     * @return the group's type, or null if the group was not found in the database.
     */
    @Nullable
    GroupType getGroupType(long groupId);

    /**
     * Get the expected member types for a single group. These are the entity (or group)  types that
     * the members of the group conform to.
     *
     * @param groupId The groups to fetch
     * @return A table of (group) -> (member type) -> (boolean). The boolean indicates whether the
     *               type is:         - a direct member (true)         - an indirect member (in case
     *         of nested groups) (false).
     */
    Table<Long, MemberType, Boolean> getExpectedMemberTypesForGroup(long groupId);

    /**
     * Retrieves groups by id.
     *
     * @param groupId id of the group
     * @return group or empty collection if none found.
     */
    @Nonnull
    Collection<Grouping> getGroupsById(@Nonnull Collection<Long> groupId);

    /**
     * Method returns Ids for the groups from {@code groupId} collection filtering only
     * existing groups. This method should be used as a very lightweight way to check whether
     * group exists in the DAO.
     *
     * @param groupIds group IDs to check for existence
     * @return a subset of {@code groupIds} where every Id exists in the DAO.
     */
    @Nonnull
    Set<Long> getExistingGroupIds(@Nonnull Collection<Long> groupIds);

    /**
     * Updates group using new definition.
     *
     * @param groupId group id to update
     * @param groupDefinition new data to set
     * @param expectedMemberTypes expected members types for this group
     * @param supportReverseLookups whether this group supports reverse lookups
     * @return group object
     * @throws StoreOperationException if operation failed
     */
    @Nonnull
    Grouping updateGroup(long groupId, @Nonnull GroupDefinition groupDefinition,
            @Nonnull Set<MemberType> expectedMemberTypes, boolean supportReverseLookups)
            throws StoreOperationException;

    /**
     * Updates supplementary characteristics of the group. These are data that are not known when we
     * update the group (they derive from calculations after the definition & other information have
     * been updated), so a different call is required.
     * Currently they include emptiness, environment and cloud type.
     *
     * @param groupId group's uuid
     * @param isEmpty whether the group is currently empty or not
     * @param groupEnvironment wrapper for environment and cloud type of the group
     * @param groupSeverity group's severity
     */
    void updateSingleGroupSupplementaryInfo(long groupId, boolean isEmpty,
            GroupEnvironment groupEnvironment, Severity groupSeverity);

    /**
     * Updates GroupSupplementaryInfo data in bulk.
     * Any group whose uuid cannot be found in the database is skipped.
     *
     * @param groups a map with information for each group to be updated.
     */
    void updateBulkGroupSupplementaryInfo(Map<Long, GroupSupplementaryInfo> groups);

    /**
     * Updates groups' severity data in bulk.
     *
     * @param groups a collection with the severity for each group to be updated.
     * @return the number of updated groups
     */
    int updateBulkGroupsSeverity(Collection<GroupSupplementaryInfo> groups);

    /**
     * Returns the next page of groups, conforming to the request specified.
     *
     * @param paginatedGroupsRequest request for groups with necessary filters & pagination
     *         parameters
     * @return the next page of groups, along with the corresponding pagination info
     */
    @Nonnull
    GroupDTO.GetPaginatedGroupsResponse getPaginatedGroups(
            @Nonnull GroupDTO.GetPaginatedGroupsRequest paginatedGroupsRequest);

    /**
     * Returns collection of groups, conforming to the request specified.
     *
     * @param groupFilter request to query
     * @return collection of groups
     */
    @Nonnull
    Collection<Grouping> getGroups(@Nonnull GroupDTO.GroupFilter groupFilter);

    /**
     * Returns collection of group ids, conforming to the request specified.
     *
     * @param groupFilter request to query. If the filter is empty, request will return all
     *         the group ids existing in the component
     * @return collection of groups
     */
    @Nonnull
    Collection<Long> getGroupIds(@Nonnull GroupFilters groupFilter);

    /**
     * Returns an ordered collection of group ids, conforming to the filter and ordering provided.
     *
     * @param groupFilter requested filters to filter groups by. If the filter is null or
     *         empty, the                    response will contain all the group ids existing in the
     *         component.
     * @param paginationParameters parameters that contain oderBy & ascending values. If
     *         null or                             empty, defaults will be used.
     * @return a collection of groups
     */
    @Nonnull
    Collection<Long> getOrderedGroupIds(@Nonnull GroupDTO.GroupFilter groupFilter,
            @Nonnull PaginationParameters paginationParameters);

    /**
     * Returns the groups that are empty.
     *
     * @return a list with the ids of the empty groups.
     */
    @Nonnull
    Collection<Long> getEmptyGroupIds();

    /**
     * Deletes the group specified by id.
     *
     * @param groupId group id
     * @throws StoreOperationException if operation failed         discovered group)
     */
    void deleteGroup(long groupId) throws StoreOperationException;

    /**
     * Updates all the discovered group. Operation could be treated as removing of
     * all the discovered groups and recreating them (preserving OIDs).
     *
     * @param groupsToAdd discovered groups to add (new groups)
     * @param groupsToUpdate discovered groups to update (existing groups)
     * @param groupsToDelete groups to delete (they are no longer present it the
     *         environment)
     * @throws StoreOperationException if operation failed
     */
    void updateDiscoveredGroups(@Nonnull Collection<DiscoveredGroup> groupsToAdd,
            @Nonnull Collection<DiscoveredGroup> groupsToUpdate, @Nonnull Set<Long> groupsToDelete)
            throws StoreOperationException;

    /**
     * Returns discovered groups identifiers. The identifiers are used for groups matching in order
     * to understand whether OIDs should be reused for some groups (updating existing groups) or
     * new OIDs should be assigned instead (creating new groups)
     *
     * @return collection of discovered groups ids.
     */
    @Nonnull
    Collection<DiscoveredGroupId> getDiscoveredGroupsIds();

    /**
     * Returns discovered groups with the discovering target ids for each group.
     *
     * @return a multimap from group uuid to targets' uuids.
     */
    @Nonnull
    Multimap<Long, Long> getDiscoveredGroupsWithTargets();

    /**
     * Returns a set of groups discovered by the specified targets.
     *
     * @param targets targets to search for
     * @return set of group OIDs
     */
    @Nonnull
    Set<Long> getGroupsByTargets(@Nonnull Collection<Long> targets);

    /**
     * Return the tags present in group component for required groups.
     * If the are no requested groups, return tags for all exited groups in group component.
     *
     * @param groupIds ids of requested groups
     * @return map with tags related to groups
     */
    @Nonnull
    Map<Long, Map<String, Set<String>>> getTags(@Nonnull Collection<Long> groupIds);

    /**
     * Return the aggregated tags across all groups that match the request.
     *
     * @param request The request specifying the groups to match.
     * @return Map with distinct tags.
     */
    @Nonnull
    Map<String, Set<String>> getTagValues(GetTagValuesRequest request);

    /**
     * Create a list of user defined group tags. Discovered tags are not supported by this
     * call.
     *
     * @param groupId is the entity oid to attach the tags.
     * @param tags is the list of tag entries to store.
     * @return the number of tags inserted.
     * @throws StoreOperationException if a tag already exists.
     */
    int insertTags(long groupId, @Nonnull Tags tags) throws StoreOperationException;

    /**
     * Delete a user defined tag for a group.
     *
     * @param groupId is the group oid.
     * @param tagKey is the tag key to delete.
     * @return the affected rows after the database operation.
     * @throws StoreOperationException if the tag with key could not be deleted.
     */
    int deleteTag(long groupId, @Nonnull String tagKey) throws StoreOperationException;

    /**
     * Delete all user defined group tags for a group.
     *
     * @param groupId is the group oid to attach the tags.
     * @throws StoreOperationException if the tags for the group could not to be deleted.
     */
    void deleteTags(long groupId) throws StoreOperationException;

    /**
     * Delete a list of user created tags for a group. Note that it will first check if requested
     * tag for delete exists, and if it doesn't it will fail. If not it will proceed to the
     * deletion, but a double delete can still happen, if in the meantime of the "check" another
     * delete happens.
     *
     * @param groupId is the group oid.
     * @param tagKeys is the list of tag keys to delete.
     * @return the affected rows after the database operations. This should match the length of the
     *         tag key list.
     * @throws StoreOperationException if the tags for the group could not to be deleted.
     */
    int deleteTagList(long groupId, Collection<String> tagKeys) throws StoreOperationException;

    /**
     * Returns direct static members of the specified group. Method does not perform any
     * recursion. Only direct static members are returned. If group is a dynamic group or
     * it does not have any members, this method returns empty set.
     *
     * @param groupIds ids of groups to get members for
     * @param expandNestedGroups whether to expand nested groups. If this value is {@code
     *         false} only direct members will be returned.
     * @return collection of members: oids and entity filters
     * @throws StoreOperationException if error occurred during data reading operations.
     */
    @Nonnull
    GroupMembersPlain getMembers(Collection<Long> groupIds, boolean expandNestedGroups)
            throws StoreOperationException;

    /**
     * Returns static groups containing the specified entity. No recursion will be performed
     * in this method. Only direct parents of the specified entity will be returned
     *
     * @param entityIds entity ids to query
     * @param groupTypes group types to query. If this collection is empty, all groups are
     *         queried
     * @return map of groups by requested entity
     */
    @Nonnull
    Map<Long, Set<Long>> getStaticGroupsForEntities(@Nonnull Collection<Long> entityIds,
            @Nonnull Collection<GroupType> groupTypes);

    /**
     * Method deletes all the groups in the store. Should be used only when the whole set
     * of groups has to be replaced with a new one. There are no checks for immutable groups
     * implied here. All groups are just deleted.
     */
    void deleteAllGroups();

    /**
     * Method returns owners of groups (resource groups owned by businessAccounts).
     *
     * @param groupIds group ids to query
     * @param groupType group type to query
     * @return set of owners
     */
    @Nonnull
    Set<Long> getOwnersOfGroups(@Nonnull Collection<Long> groupIds,
            @Nullable GroupType groupType);

    /**
     * Class to hold discovered group information.
     */
    @Immutable
    class DiscoveredGroup {
        private final long oid;
        private final GroupDefinition groupDefinition;
        private final String sourceIdentifier;
        private final boolean stitchAcrossTargets;
        private final Set<Long> targetIds;
        private final Collection<MemberType> expectedMembers;
        private final boolean isReverseLookupSupported;

        /**
         * Constructs discovered group.
         *
         * @param oid oid for the group
         * @param groupDefinition group definition
         * @param sourceIdentifier source id from the probe
         * @param stitchAcrossTargets The stitch across targets flag from the probe.
         * @param targetIds all targets which discovers this group
         * @param expectedMembers expected member types of the group
         * @param isReverseLookupSupported whether reverse lookup is supported for this
         *         group
         */
        public DiscoveredGroup(long oid, @Nonnull GroupDefinition groupDefinition,
                               @Nonnull String sourceIdentifier, boolean stitchAcrossTargets,
                               @Nonnull Set<Long> targetIds, @Nonnull Collection<MemberType> expectedMembers,
                               boolean isReverseLookupSupported) {
            this.targetIds = Objects.requireNonNull(targetIds);
            if (targetIds.isEmpty()) {
                throw new IllegalArgumentException(
                        "Target ids must not be empty for group " + groupDefinition
                                + " with source id " + sourceIdentifier);
            }
            this.groupDefinition = Objects.requireNonNull(groupDefinition);
            this.sourceIdentifier = Objects.requireNonNull(sourceIdentifier);
            this.stitchAcrossTargets = stitchAcrossTargets;
            this.expectedMembers = Objects.requireNonNull(expectedMembers);
            this.isReverseLookupSupported = isReverseLookupSupported;
            this.oid = oid;
        }

        /**
         * Gets definition.
         *
         * @return the definition
         */
        public GroupDefinition getDefinition() {
            return groupDefinition;
        }

        /**
         * Gets source identifier.
         *
         * @return the source identifier
         */
        public String getSourceIdentifier() {
            return sourceIdentifier;
        }

        /**
         * Gets expected members.
         *
         * @return the expected members
         */
        @Nonnull
        public Collection<MemberType> getExpectedMembers() {
            return expectedMembers;
        }

        /**
         * Is reverse lookup supported boolean.
         *
         * @return the boolean
         */
        public boolean isReverseLookupSupported() {
            return isReverseLookupSupported;
        }

        /**
         * Gets target ids.
         *
         * @return the target ids
         */
        @Nonnull
        public Set<Long> getTargetIds() {
            return targetIds;
        }

        /**
         * Gets oid.
         *
         * @return the oid
         */
        public long getOid() {
            return oid;
        }

        /**
         * Stitch across targets boolean.
         *
         * @return the boolean
         */
        public boolean stitchAcrossTargets() {
            return stitchAcrossTargets;
        }

        /**
         * To string string.
         *
         * @return the string
         */
        @Override
        public String toString() {
            return Long.toString(oid) + '-' + sourceIdentifier;
        }
    }

    /**
     * Discovered group id. This interface represents all the fields that could be used for
     * group matching.
     */
    interface DiscoveredGroupId {

        /**
         * Returns group type.
         *
         * @return group type
         */
        @Nonnull
        GroupType getGroupType();

        /**
         * Returns source identifier of the group.
         *
         * @return source id
         */
        @Nonnull
        String getSourceId();

        /**
         * Returns target this group is reported for. Only return non-null value if there is
         * exactly one target. For multiple targets, this value will be {@code null}.
         *
         * @return target id
         */
        @Nullable
        Long getTarget();

        /**
         * Identity of the discovered group that does already exist in the DB.
         *
         * @return discovered identity
         */
        @Nonnull
        DiscoveredObjectVersionIdentity getIdentity();
    }
}
