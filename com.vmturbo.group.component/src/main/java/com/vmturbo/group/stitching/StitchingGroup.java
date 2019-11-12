package com.vmturbo.group.stitching;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers.StaticMembersByType;

/**
 * A group capable of being stitched.
 *
 * <p>A {@link StitchingGroup} wrapped the {@link GroupDefinition.Builder} and other info related
 * to this group, which may be merged with other {@link StitchingGroup}s or removed after stitching.
 */
public class StitchingGroup {

    private final long oid;
    private final GroupDefinition.Builder groupDefinition;
    private final String sourceId;
    private final Set<Long> targets;
    private final boolean newGroup;
    private final Map<MemberType, Set<Long>> membersByType = new HashMap<>();

    /**
     * Create a new {@link StitchingGroup}.
     *
     * @param oid OID of the group
     * @param groupDefinition the group definition
     * @param sourceId source identifier
     * @param targetId id of the target which discovers this group
     * @param newGroup whether this group is a new one ({@code true}) or it is an update of
     *         the existing group (otherwise)
     */
    StitchingGroup(final long oid, final GroupDefinition groupDefinition, final String sourceId,
            final long targetId, final boolean newGroup) {
        this.oid = oid;
        this.groupDefinition = GroupDefinition.newBuilder(Objects.requireNonNull(groupDefinition));
        this.sourceId = Objects.requireNonNull(sourceId);
        this.targets = new HashSet<>();
        this.targets.add(targetId);
        this.newGroup = newGroup;
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
        if (!membersByType.isEmpty()) {
            final Set<MemberType> mergedTypes = new HashSet<>();
            final Map<MemberType, Set<Long>> unmergedMembers = new HashMap<>(membersByType);
            for (StaticMembersByType.Builder builder : groupDefinition.getStaticGroupMembersBuilder()
                    .getMembersByTypeBuilderList()) {
                final MemberType memberType = builder.getType();
                final Set<Long> members = membersByType.get(memberType);
                if (members != null) {
                    members.addAll(builder.getMembersList());
                    // Clear members to ensure uniqueness
                    builder.clearMembers();
                    builder.addAllMembers(members);
                    unmergedMembers.remove(memberType);
                }
            }
            for (Entry<MemberType, Set<Long>> entry : unmergedMembers.entrySet()) {
                groupDefinition.getStaticGroupMembersBuilder()
                        .addMembersByType(StaticMembersByType.newBuilder()
                                .setType(entry.getKey())
                                .addAllMembers(entry.getValue())
                                .build());
            }
        }
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
     * Get all the targets which discovers this group.
     *
     * @return set of target ids
     */
    public Set<Long> getTargetIds() {
        return targets;
    }

    /**
     * Mark the given target as one of the targets where this group is discovered from.
     *
     * @param groupDefinition group definition to stitch
     * @param targetId id of target which also discovers this group
     */
    public void mergedGroup(@Nonnull GroupDefinition groupDefinition, long targetId) {
        if (!targets.add(targetId)) {
            throw new IllegalArgumentException(
                    "Target " + targetId + " is already registered in the stitching group " +
                            sourceId);
        }
        for (StaticMembersByType members : groupDefinition.getStaticGroupMembers()
                .getMembersByTypeList()) {
            membersByType.computeIfAbsent(members.getType(), key -> new HashSet<>())
                    .addAll(members.getMembersList());
        }
    }

    /**
     * Teturnes the group OID.
     *
     * @return group OID
     */
    public long getOid() {
        return oid;
    }

    /**
     * Returns whether this group is a new one.
     *
     * @return whether this group is a new one
     */
    public boolean isNewGroup() {
        return newGroup;
    }

    @Override
    public String toString() {
        return sourceId + "->" + oid;
    }
}