package com.vmturbo.group.stitching;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers.StaticMembersByType;

/**
 * A group capable of being stitched.
 *
 * <p>A {@link StitchingGroup} wrapped the {@link GroupDefinition.Builder} and other info related
 * to this group, which may be merged with other {@link StitchingGroup}s or removed after stitching.
 */
public class StitchingGroup {

    private final GroupDefinition.Builder groupDefinition;

    private final String sourceId;

    private final long targetId;

    private final Set<Long> mergedFromTargets = new HashSet<>();

    /**
     * Create a new {@link StitchingGroup}.
     *
     * @param groupDefinition the group definition
     * @param sourceId source identifier
     * @param targetId id of the target which discovers this group
     */
    StitchingGroup(final GroupDefinition.Builder groupDefinition, final String sourceId,
            final long targetId) {
        this.groupDefinition = Objects.requireNonNull(groupDefinition);
        this.sourceId = Objects.requireNonNull(sourceId);
        this.targetId = targetId;
    }

    /**
     * Get the group definition.
     *
     * @return {@link GroupDefinition.Builder}
     */
    public GroupDefinition.Builder getGroupDefinition() {
        return groupDefinition;
    }

    /**
     * Build a {@link GroupDefinition} based on the builder.
     *
     * @return {@link GroupDefinition}
     */
    public GroupDefinition buildGroupDefinition() {
        return groupDefinition.build();
    }

    /**
     * Get the source identifier of this group from probe.
     *
     * @return source identifier
     */
    public String getSourceId() {
        return sourceId;
    }

    /**
     * Get the id of the target which originally discovers this group.
     *
     * @return source target id
     */
    public long getSourceTargetId() {
        return targetId;
    }

    /**
     * Get all the targets which discovers this group.
     *
     * @return set of target ids
     */
    public Set<Long> getAllTargetIds() {
        return Stream.concat(Stream.of(targetId), mergedFromTargets.stream())
                .collect(Collectors.toSet());
    }

    /**
     * Mark the given target as one of the targets where this group is discovered from.
     *
     * @param targetId id of target which also discovers this group
     */
    public void mergedFromTarget(long targetId) {
        mergedFromTargets.add(targetId);
    }

    /**
     * Set members of this group. Old members will be replaced.
     *
     * @param membersByType new members by type
     */
    public void setMembers(@Nonnull Map<MemberType, Set<Long>> membersByType) {
        final StaticMembers.Builder membersBuilder = groupDefinition.getStaticGroupMembersBuilder();
        // clear existing members and set new members
        membersBuilder.clear();
        membersByType.forEach(((memberType, members) ->
                membersBuilder.addMembersByType(StaticMembersByType.newBuilder()
                        .setType(memberType)
                        .addAllMembers(members))));
    }

    @Override
    public String toString() {
        return sourceId + '-' + targetId;
    }
}