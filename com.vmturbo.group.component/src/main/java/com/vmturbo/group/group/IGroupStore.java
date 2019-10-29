package com.vmturbo.group.group;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.group.GroupDTO.Origin;
import com.vmturbo.group.common.DuplicateNameException;
import com.vmturbo.group.common.ImmutableUpdateException.ImmutableGroupUpdateException;
import com.vmturbo.group.common.ItemNotFoundException.GroupNotFoundException;
import com.vmturbo.platform.sdk.common.util.Pair;

/**
 * Store to operate with groups. It is responsible to create, save and query groups.
 */
public interface IGroupStore {

    /**
     * Create a new group based on group definition. Discovered groups are not supported by this
     * call. Discovered groups should be created with {@link #updateDiscoveredGroups(Collection)}
     *
     * @param origin origin of this group
     * @param groupDefinition group definition
     * @param expecMemberTypes expected members types of this group
     * @param supportReverseLookup whether the group supports reverse lookups
     * @return OID of the newly create group
     * @throws InvalidGroupException if group is malformed (including some key violations)
     * @throws DuplicateNameException if group with the same name already exists
     * @see #updateDiscoveredGroups(Collection)
     */
    long createGroup(@Nonnull Origin origin, @Nonnull GroupDefinition groupDefinition,
            @Nonnull Set<MemberType> expecMemberTypes, boolean supportReverseLookup)
            throws InvalidGroupException, DuplicateNameException;

    /**
     * Retrieves group by id.
     *
     * @param groupId id of the group
     * @return group or {@link Optional#empty} if none found.
     */
    @Nonnull
    Optional<Grouping> getGroup(long groupId);

    /**
     * Updates group using new definition.
     *
     * @param groupId group id to update
     * @param groupDefinition new data to set
     * @param expectedMemberTypes expected mebers types for this group
     * @param supportReverseLookups whether this group supports reverse lookups
     * @return group object
     * @throws InvalidGroupException if new group definition is malformed
     * @throws ImmutableGroupUpdateException If the ID identifies a discovered (i.e.
     *         non-user) group.
     * @throws GroupNotFoundException If a group associated with the ID is not found.
     * @throws DuplicateNameException If there is already a different group with the same
     *         name.
     */
    @Nonnull
    Grouping updateGroup(long groupId, @Nonnull GroupDefinition groupDefinition,
            @Nonnull Set<MemberType> expectedMemberTypes, boolean supportReverseLookups)
            throws InvalidGroupException, ImmutableGroupUpdateException, GroupNotFoundException,
            DuplicateNameException;

    /**
     * Returns collection of groups, conforming to the request specified.
     *
     * @param groupFilter request to query
     * @return collection of groups
     */
    @Nonnull
    Collection<Grouping> getGroups(@Nonnull GroupDTO.GroupFilter groupFilter);

    /**
     * Deletes the group specified by id.
     *
     * @param groupId group id
     * @throws GroupNotFoundException if group is absent.
     * @throws ImmutableGroupUpdateException if group could not be deleted (it is a
     *         discovered group)
     */
    void deleteGroup(long groupId) throws GroupNotFoundException, ImmutableGroupUpdateException;

    /**
     * Updates all the discovered group. Operation could be treated as removing of
     * all the discovered groups and recreating them (preserving OIDs).
     *
     * @param groups new set of discovered groups
     * @return map of group string id (reported by probe's discovery process) to groups' oids.
     * @throws InvalidGroupException if one of the group definitions specified is invalid.
     *         Nothing will be written to the DB as a result
     */
    @Nonnull
    Map<String, Long> updateDiscoveredGroups(@Nonnull Collection<DiscoveredGroup> groups)
            throws InvalidGroupException;

    /**
     * Returns all the tags present in the group component.
     *
     * @return tags multi-map
     */
    @Nonnull
    Map<String, Set<String>> getTags();

    /**
     * Returns direct static members of the specified group. Method does not perform any
     * recursion. Only direct static members are returned. If group is a dynamic group or
     * it does not have any members, this method returns empty set.
     *
     * @param groupId group id to search
     * @return set of member entities and set of member groups. If group does not exist or it
     *         does not have any static members empty collections will be returned.
     */
    @Nonnull
    Pair<Set<Long>, Set<Long>> getStaticMembers(long groupId);

    /**
     * Returns static groups containing the specified entity. No recursion will be performed
     * in this method. Only direct parents of the specified entity will be returned
     *
     * @param entityId entity id to query
     * @return set of groups
     */
    @Nonnull
    Set<Grouping> getStaticGroupsForEntity(long entityId);

    /**
     * Subscrtibes to deletion of user or system group. The callback will not be called for any
     * discovered groups. Callback receives a OID of the group after the deletion.
     *
     * @param consumer callback
     */
    void subscribeUserGroupRemoved(@Nonnull Consumer<Long> consumer);

    /**
     * Class to hold discovered group information.
     */
    @Immutable
    class DiscoveredGroup {
        private final GroupDefinition groupDefinition;
        private final String sourceIdentifier;
        private final Set<Long> targetIds;
        private final Collection<MemberType> expectedMembers;
        private final boolean isReverseLookupSupported;

        /**
         * Constructs discovered group.
         *
         * @param groupDefinition group definition
         * @param sourceIdentifier source id from the probe
         * @param targetIds all targets which discovers this group
         * @param expectedMembers expected member types of the group
         * @param isReverseLookupSupported whether reverse lookup is supported for this group
         */
        public DiscoveredGroup(@Nonnull GroupDefinition groupDefinition,
                               @Nonnull String sourceIdentifier,
                               @Nonnull Set<Long> targetIds,
                               @Nonnull Collection<MemberType> expectedMembers,
                               boolean isReverseLookupSupported) {
            this.targetIds = Objects.requireNonNull(targetIds);
            if (targetIds.isEmpty()) {
                throw new IllegalArgumentException(
                        "Target ids must not be empty for group " + groupDefinition +
                                " with source id " + sourceIdentifier);
            }
            this.groupDefinition = Objects.requireNonNull(groupDefinition);
            this.sourceIdentifier = Objects.requireNonNull(sourceIdentifier);
            this.expectedMembers = Objects.requireNonNull(expectedMembers);
            this.isReverseLookupSupported = isReverseLookupSupported;
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
    }
}
