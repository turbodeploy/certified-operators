package com.vmturbo.group.group;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.GroupFilters;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.group.GroupDTO.Origin;
import com.vmturbo.group.DiscoveredObjectVersionIdentity;
import com.vmturbo.group.service.StoreOperationException;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;

/**
 * Store to operate with groups. It is responsible to create, save and query groups.
 */
public interface IGroupStore {

    /**
     * Create a new group based on group definition. Discovered groups are not supported by this
     * call. Discovered groups should be created with {@link #updateDiscoveredGroups(Collection,
     * Collection, Set)}
     *
     * @param oid oid for the new group
     * @param origin origin of this group
     * @param groupDefinition group definition
     * @param expecMemberTypes expected members types of this group
     * @param supportReverseLookup whether the group supports reverse lookups
     * @throws StoreOperationException if operation failed
     * @see #updateDiscoveredGroups(Collection, Collection, Set)
     */
    void createGroup(long oid, @Nonnull Origin origin, @Nonnull GroupDefinition groupDefinition,
            @Nonnull Set<MemberType> expecMemberTypes, boolean supportReverseLookup)
            throws StoreOperationException;

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
     * Deletes the group specified by id.
     *
     * @param groupId group id
     * @throws StoreOperationException if operation failed
     *         discovered group)
     */
    void deleteGroup(long groupId) throws StoreOperationException;

    /**
     * Updates all the discovered group. Operation could be treated as removing of
     * all the discovered groups and recreating them (preserving OIDs).
     *
     * @param groupsToAdd discovered groups to add (new groups)
     * @param groupsToUpdate discovered groups to update (existing groups)
     * @param groupsToDelete groups to delete (they are no longer present it the environment)
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
        private final Set<Long> targetIds;
        private final Collection<MemberType> expectedMembers;
        private final boolean isReverseLookupSupported;

        /**
         * Constructs discovered group.
         *
         * @param oid oid for the group
         * @param groupDefinition group definition
         * @param sourceIdentifier source id from the probe
         * @param targetIds all targets which discovers this group
         * @param expectedMembers expected member types of the group
         * @param isReverseLookupSupported whether reverse lookup is supported for this group
         */
        public DiscoveredGroup(long oid, @Nonnull GroupDefinition groupDefinition,
                @Nonnull String sourceIdentifier, @Nonnull Set<Long> targetIds,
                @Nonnull Collection<MemberType> expectedMembers, boolean isReverseLookupSupported) {
            this.targetIds = Objects.requireNonNull(targetIds);
            if (targetIds.isEmpty()) {
                throw new IllegalArgumentException(
                        "Target ids must not be empty for group " + groupDefinition
                                + " with source id " + sourceIdentifier);
            }
            this.groupDefinition = Objects.requireNonNull(groupDefinition);
            this.sourceIdentifier = Objects.requireNonNull(sourceIdentifier);
            this.expectedMembers = Objects.requireNonNull(expectedMembers);
            this.isReverseLookupSupported = isReverseLookupSupported;
            this.oid = oid;
        }

        public GroupDefinition getDefinition() {
            return groupDefinition;
        }

        public String getSourceIdentifier() {
            return sourceIdentifier;
        }

        @Nonnull
        public Collection<MemberType> getExpectedMembers() {
            return expectedMembers;
        }

        public boolean isReverseLookupSupported() {
            return isReverseLookupSupported;
        }

        @Nonnull
        public Set<Long> getTargetIds() {
            return targetIds;
        }

        public long getOid() {
            return oid;
        }

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
