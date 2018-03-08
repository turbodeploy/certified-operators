package com.vmturbo.topology.processor.group.discovery;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;

import com.vmturbo.stitching.StitchingMergeInformation;

/**
 * A cache that permits the lookup of groups containing a given
 * {@link com.vmturbo.topology.processor.stitching.TopologyStitchingEntity}.
 *
 * Related to the {@link ComputeClusterMemberCache} but different in that it handles lookups
 * for all types of discovered groups and not just compute clusters.
 */
public class DiscoveredGroupMemberCache {
    private final Map<Long, List<DiscoveredGroupMembers>> targetDiscoveredGroups;

    /**
     * Create a new {@link DiscoveredGroupMemberCache}.
     *
     * @param latestGroupsByTargets A map of {@link InterpretedGroup}s associated with the targets that
     *                              discovered those groups.
     */
    public DiscoveredGroupMemberCache(@Nonnull final Map<Long, List<InterpretedGroup>> latestGroupsByTargets) {
        targetDiscoveredGroups = latestGroupsByTargets.entrySet().stream()
            .collect(Collectors.toMap(Entry::getKey, entry -> entry.getValue().stream()
                .map(DiscoveredGroupMembers::new)
                .collect(Collectors.toList())));
    }

    /**
     * Returns groups for all targets.
     *
     * @return all discovered groups members
     */
    public Stream<DiscoveredGroupMembers> getAllDiscoveredGroupsMembers() {
        return targetDiscoveredGroups.values().stream().flatMap(List::stream);
    }

    /**
     * Look up the discovered groups for the target/oid pair in the merge information.
     *
     * @param mergeInfo The merge information containing the target/oid pair for which discovered
     *                  groups should be looked up.
     * @return The discovered groups discovered by the target in the mergeInfo containing the OID
     *         present in the mergeInfo.
     */
    public Stream<DiscoveredGroupMembers> groupsContaining(@Nonnull final StitchingMergeInformation mergeInfo) {
        final List<DiscoveredGroupMembers> targetGroups = targetDiscoveredGroups.get(mergeInfo.getTargetId());
        if (targetGroups == null) {
            return Stream.empty();
        } else {
            return targetGroups.stream()
                .filter(groupMembers -> groupMembers.hasMember(mergeInfo.getOid()));
        }
    }

    /**
     * A helper class that contains the members of a group as a set along with the associated
     * {@link InterpretedGroup} definition.
     */
    public static class DiscoveredGroupMembers {
        private final Set<Long> memberOids;
        private InterpretedGroup associatedGroup;

        @VisibleForTesting
        DiscoveredGroupMembers(@Nonnull final InterpretedGroup associatedGroup) {
            this.associatedGroup = Objects.requireNonNull(associatedGroup);

            final List<Long> members = associatedGroup.getDtoAsCluster()
                .map(cluster -> cluster.getMembers().getStaticMemberOidsList())
                .orElseGet(() -> associatedGroup.getDtoAsGroup()
                    .map(group -> group.getStaticGroupMembers().getStaticMemberOidsList())
                    .orElse(Collections.<Long>emptyList()));
            this.memberOids = new HashSet<>(members);
        }

        /**
         * Determine whether this discovered group has a member with the given OID.
         *
         * @param memberOid The oid of the member we should look up.
         * @return True if the discovered group contains the member, false otherwise.
         */
        public boolean hasMember(final long memberOid) {
            return memberOids.contains(memberOid);
        }

        /**
         * Swap out an existing member with a new member. If the existing member is not present,
         * no update is performed.
         *
         * If the replacementOid is already present, the existing member is removed and re-adding
         * the replacement results in no additional change.
         *
         * @param existingMemberOid The OID of an existing member in the {@link DiscoveredGroupMembers} to be
         *                          removed in favor of its replacement.
         * @param replacementOid The OID to replace the existing member.
         * @return true if the existing member existed and was replaced, false if the existing member was not
         *         present and could not be replaced.
         */
        public boolean swapMember(final long existingMemberOid, final long replacementOid) {
            if (memberOids.remove(existingMemberOid)) {
                memberOids.add(replacementOid);
                return true;
            }
            return false;
        }

        /**
         * Get a set containing the OID members of the associated group.
         *
         * @return A set containing the OID members of the associated group.
         */
        public Set<Long> getMemberOids() {
            return memberOids;
        }

        /**
         * Get the associated {@link InterpretedGroup}.
         *
         * @return The associated {@link InterpretedGroup}.
         */
        @Nonnull
        public InterpretedGroup getAssociatedGroup() {
            return associatedGroup;
        }
    }
}
