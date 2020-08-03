package com.vmturbo.topology.processor.stitching;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers.StaticMembersByType;
import com.vmturbo.stitching.StitchingMergeInformation;
import com.vmturbo.topology.processor.group.discovery.DiscoveredGroupMemberCache;
import com.vmturbo.topology.processor.group.discovery.DiscoveredGroupMemberCache.DiscoveredGroupMembers;
import com.vmturbo.topology.processor.group.discovery.InterpretedGroup;

/**
 * One type of topological change that can be performed during stitching is that multiple entities
 * with different OIDs may be merged into a single entity with a single OID. A side effect of this
 * is that references to the entity by one of the aliases it lost due to these merges during
 * stitching are invalidated. For further details on merges, see
 * {@link TopologyStitchingChanges.MergeEntitiesChange} and/or
 * {@link com.vmturbo.stitching.utilities.MergeEntities.MergeEntitiesDetails}.
 *
 * Now consider two Storages, one with OID 1234, and the other with OID 5678. During stitching,
 * suppose that Storage-5678 is merged onto Storage-1234 (as a result, Storage-5678 is removed and
 * Storage-1234 receives some of the properties of Storage-5678 but not its OID). Now suppose
 * that the target that discovered Storage-5678 included this entity as a member of a storage
 * cluster. As a result of this merge, the storage cluster now contains an invalid group member!
 * In order to fix the invalid reference, we should replace the OID 5678 with its new identifier
 * 1234.
 *
 * The {@link StitchingGroupFixer} scans the {@link TopologyStitchingGraph} for merged entities
 * that belonged to discovered groups, and fixes up those discovered groups to reference
 * the post-stitched identities of those entities.
 */
public class StitchingGroupFixer {
    public StitchingGroupFixer() {

    }

    /**
     * Fixup groups by replacing references to entities whose identity changed as a result of stitching
     * with a reference to their new post-stitched identity.
     *
     * @param stitchingGraph The topology graph that has been stitched. Entities should contain
     *                       {@link StitchingMergeInformation} describing the merges that affected
     *                       them.
     * @param groupCache A cache of the discovered groups to be fixed up.
     * @return Number of fixed up groups.
     */
    public int fixupGroups(@Nonnull final TopologyStitchingGraph stitchingGraph,
                            @Nonnull final DiscoveredGroupMemberCache groupCache) {
        // The set of all groups modified by the fixup operation.
        final Set<DiscoveredGroupMembers> modifiedGroups = new HashSet<>();

        // Swap membership in the cache and note which groups were modified.
        stitchingGraph.entities()
            .filter(TopologyStitchingEntity::hasMergeInformation)
            .forEach(entity -> fixupGroupsFor(entity, groupCache, modifiedGroups));

        // Replace the group members based on the cache modifications.
        modifiedGroups.forEach(modifiedGroup -> replaceGroupMembers(
                modifiedGroup.getAssociatedGroup(), modifiedGroup.getMembers()));
        return modifiedGroups.size();
    }

    /**
     * Fixup groups for a given entity by replacing references to entities whose identity changed as a
     * result of stitching with a reference to their new post-stitched identity.
     *
     * @param entity The entity whose discovered group memberships should be fixed up.
     * @param groupCache A cache of the discovered groups to be fixed up.
     * @param modifiedGroups A set containing the groups
     */
    private void fixupGroupsFor(@Nonnull final TopologyStitchingEntity entity,
                                @Nonnull final DiscoveredGroupMemberCache groupCache,
                                @Nonnull final Set<DiscoveredGroupMembers> modifiedGroups) {
        entity.getMergeInformation().forEach(mergeInfo -> groupCache.groupsContaining(mergeInfo)
            .forEach(group -> {
                // Swap the merge OID with the OID of the entity that was merged "onto".
                group.swapMember(mergeInfo.getOid(), entity.getOid());

                // Add the group to the list of those modified so that we can fix up memberships at the end.
                modifiedGroups.add(group);
            }));
    }

    private void replaceGroupMembers(@Nonnull final InterpretedGroup interpretedGroup,
                                     @Nonnull final List<StaticMembersByType> newMembersByType) {
        final StaticMembers.Builder memberBuilder = interpretedGroup.getGroupDefinition()
                .map(GroupDefinition.Builder::getStaticGroupMembersBuilder)
                .orElseThrow(() -> new IllegalStateException(
                        "Interpreted group must contain interpreted group definition."));
        memberBuilder.clear();
        memberBuilder.addAllMembersByType(newMembersByType);
    }
}
