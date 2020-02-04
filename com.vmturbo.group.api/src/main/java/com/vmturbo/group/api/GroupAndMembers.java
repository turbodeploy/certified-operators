package com.vmturbo.group.api;

import java.util.Collection;

import org.immutables.value.Value;

import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;

/**
 * A utility object to represent a group and its members.
 * TODO (roman, Mar 29 2019): Edit the GetMembers call to return the leaf entity IDs, as well as
 * the Group definition.
 */
@Value.Immutable
public interface GroupAndMembers {
    /**
     * The {@link Grouping} definition retrieved from the group component.
     *
     * @return the group.
     */
    Grouping group();

    /**
     * The members of the group.
     *
     * @return members of the group.
     */
    Collection<Long> members();

    /**
     * The entities in the group. In a non-nested group, this will be the same collection
     * as the {@link GroupAndMembers#members()}. In a nested group, the members will be
     * the immediate groups inside {@link GroupAndMembers#group()}, and the entities will be the
     * leaf entities.
     *
     * @return leaf members of the group. For a non-nested group, this will be the same as members.
     */
    Collection<Long> entities();
}
