package com.vmturbo.group.group;

import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.EntityFilters;

/**
 * Object to hold groups' members search results. It containers group ids, static entity ids and
 * entity filters to perform search for.
 */
public class GroupMembersPlain {
    private final Set<Long> entityIds;
    private final Set<Long> groupIds;
    private final Set<EntityFilters> entityFilters;

    /**
     * Constructs groups members plan.
     *
     * @param entityIds found static members OIDs.
     * @param groupIds found group members OIDs
     * @param entityFilters found dynamic entity members.
     */
    public GroupMembersPlain(@Nonnull Set<Long> entityIds, @Nonnull Set<Long> groupIds,
            @Nonnull Set<EntityFilters> entityFilters) {
        this.entityIds = Objects.requireNonNull(entityIds);
        this.groupIds = Objects.requireNonNull(groupIds);
        this.entityFilters = Objects.requireNonNull(entityFilters);
    }

    public Set<Long> getEntityIds() {
        return entityIds;
    }

    public Set<Long> getGroupIds() {
        return groupIds;
    }

    public Set<EntityFilters> getEntityFilters() {
        return entityFilters;
    }

    /**
     * Creates an unmodifiable view of this object.
     *
     * @return unmodifiable view.
     */
    @Nonnull
    public GroupMembersPlain unmodifiable() {
        return new GroupMembersPlain(Collections.unmodifiableSet(entityIds),
                Collections.unmodifiableSet(groupIds), Collections.unmodifiableSet(entityFilters));
    }

    /**
     * Merges an additional members information into the current group members.
     * This is an optional operation.
     *
     * @param additional additional entities to add
     * @return new group IDs added to current group members from {@code additional}
     * @throws UnsupportedOperationException if current group members are unmodifiable
     */
    @Nonnull
    public Set<Long> mergeMembers(@Nonnull GroupMembersPlain additional) {
        entityFilters.addAll(additional.getEntityFilters());
        entityIds.addAll(additional.getEntityIds());
        final Set<Long> newGroups = new HashSet<>();
        for (Long groupId: additional.getGroupIds()) {
            if (groupIds.add(groupId)) {
                newGroups.add(groupId);
            }
        }
        return newGroups;
    }

}
