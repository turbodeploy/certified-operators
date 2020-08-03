package com.vmturbo.group.stitching;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
    private static final Logger logger = LogManager.getLogger();

    private final long oid;
    private final GroupDefinition.Builder groupDefinition;
    private final String sourceId;
    private final Set<Long> targets;
    private final boolean newGroup;
    private final byte[] existingHash;
    private final Map<MemberType, Set<Long>> membersByType = new HashMap<>();

    /**
     * Create a new {@link StitchingGroup}.
     *
     * @param oid OID of the group
     * @param groupDefinition the group definition
     * @param sourceId source identifier
     * @param targetId id of the target which discovers this group
     * @param existingHash has of the matched existing group. If this is a new group, this
     *         value will be {@code null}. It still may be {@code null} for existing groups
     * @param newGroup whether this group is a new one ({@code true}) or it is an update of
     *         the existing group (otherwise)
     */
    StitchingGroup(final long oid, final GroupDefinition groupDefinition, final String sourceId,
            final long targetId, boolean newGroup, final @Nullable byte[] existingHash) {
        this.oid = oid;
        this.groupDefinition = GroupDefinition.newBuilder(Objects.requireNonNull(groupDefinition));
        this.sourceId = Objects.requireNonNull(sourceId);
        this.targets = new HashSet<>();
        this.newGroup = newGroup;
        this.existingHash = existingHash;
        mergedGroup(groupDefinition, targetId);
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
            // This is not a normal case. It most likely means the Topology Processor uploaded the
            // same group multiple times from the same target - which could be due to an internal
            // error, or due to bad probe results.

            // We will treat this as an error or warning depending on whether the new group
            // definition contains additional members.
            final Map<MemberType, Set<Long>> newMembersByType = new HashMap<>();
            for (StaticMembersByType members : groupDefinition.getStaticGroupMembers()
                    .getMembersByTypeList()) {
                final Set<Long> existingMembers = membersByType.getOrDefault(members.getType(), Collections.emptySet());
                final Set<Long> newMembers = members.getMembersList().stream()
                    .filter(member -> !existingMembers.contains(member))
                    .collect(Collectors.toSet());
                if (!newMembers.isEmpty()) {
                    newMembersByType.put(members.getType(), newMembers);
                }
            }
            if (newMembersByType.isEmpty()) {
                // If the new group is the same, it's a more benign warning.
                logger.warn("Target {} is already registered in " + "the stitching group {}.",
                        targetId, sourceId);
            } else {
                // If the new group is different, it's a bigger issue - the target uploaded two
                // groups with different members! To play it safe we just keep the first.
                logger.error("Target {} is already registered in "
                        + "the stitching group {} with different members. Ignoring the new target. "
                        + "Ignored members: {}", targetId, sourceId, newMembersByType);
                // Return so that we don't modify the membersByType.
                return;
            }
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
     * Returns a hash of discovered group existing in the DB.
     *
     * @return hash, if group already exists and has a hash calculated
     */
    @Nullable
    public byte[] getExistingHash() {
        return existingHash;
    }

    /**
     * Returns whether this is a new group to create ({@code trye}) or an existing one ({@code
     * false}) tp update.
     *
     * @return whether this is a new group
     */
    public boolean isNewGroup() {
        return newGroup;
    }

    @Override
    public String toString() {
        return sourceId + "->" + oid;
    }
}