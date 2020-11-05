package com.vmturbo.topology.processor.stitching;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
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
 * <p>Another example is the discovered virtual machine groups sent by kubeturbo. Assume a virtual
 * machine group VM-Group-1 contains one proxy virtual machine with OID VM-1234. After stitching,
 * the proxy virtual machine is stitched with the real virtual machine with OID VM-5678, and is then
 * removed. Because of this, we need the {@link StitchingGroupFixer} to replace the OID VM-1234 with
 * VM-5678 for the virtual machine group VM-Group-1.
 *
 * <p>The {@link StitchingGroupFixer} is shared by two pipeline stages:
 * The StitchingGroupAnalyzerStage uses the {@link StitchingGroupFixer} to scan the
 * {@link TopologyStitchingGraph} for merged entities that belonged to discovered groups, and cache
 * those groups whose members need to reference the post-stitched identities of those entities.
 * The UploadGroupsStage, in an atomic fashion, makes a deep copy of each discovered group, and
 * replaces the membership of those copied groups that need modification based on the cache stored
 * in {@link StitchingGroupFixer}, before uploading them to the group component.
 */
public class StitchingGroupFixer {
    public StitchingGroupFixer() {

    }

    /**
     * The logger.
     */
    private final Logger logger = LogManager.getLogger();
    /**
     * The cache of modified group memberships after the StitchingGroupAnalyzerStage.
     */
    private final Map<String, DiscoveredGroupMembers> modifiedGroups = new HashMap<>();

    /**
     * Analyze and cache groups whose member ids are replaced as a result of stitching with
     * references to their new post-stitched ids.
     *
     * @param stitchingGraph The topology graph that has been stitched. Entities should contain
     *                       {@link StitchingMergeInformation} describing the merges that affected
     *                       them.
     * @param groupCache A cache of the discovered groups to be fixed up.
     * @return Number of analyzed groups.
     */
    public int analyzeStitchingGroups(@Nonnull final TopologyStitchingGraph stitchingGraph,
                                      @Nonnull final DiscoveredGroupMemberCache groupCache) {
        modifiedGroups.clear();
        // Swap membership in the cache and note which groups were modified.
        stitchingGraph.entities()
            .filter(TopologyStitchingEntity::hasMergeInformation)
            .forEach(entity -> cacheModifiedGroupFor(entity, groupCache));
        return modifiedGroups.size();
    }

    /**
     * Replace group members whose identities have changed as a result of stitching with references
     * to their new post-stitched identities.
     *
     * @param groups The set of groups whose memberships may be replaced.
     */
    public void replaceGroupMembers(@Nonnull final List<InterpretedGroup> groups) {
        // Replace the group members based on the cache modifications.
        groups.forEach(this::replaceGroupMembersFor);
    }

    /**
     * Cache groups whose member ids are replaced as a result of stitching with references to
     * their new post-stitched ids.
     *
     * @param entity The entity whose discovered group memberships should be fixed up.
     * @param groupCache A cache of the discovered groups to be fixed up.
     */
    private void cacheModifiedGroupFor(@Nonnull final TopologyStitchingEntity entity,
                                       @Nonnull final DiscoveredGroupMemberCache groupCache) {
        entity.getMergeInformation().forEach(mergeInfo -> groupCache.groupsContaining(mergeInfo)
            .forEach(group -> {
                // Swap the merge OID with the OID of the entity that was merged "onto".
                group.swapMember(mergeInfo.getOid(), entity.getOid());
                if (logger.isDebugEnabled()) {
                    logger.debug("Cache modified member for group {}: {} -> {}.",
                            group.getAssociatedGroup().getSourceId(), mergeInfo.getOid(), entity.getOid());
                }
                // Add the group to the modified groups map so that we can fix up memberships at the end.
                modifiedGroups.put(group.getAssociatedGroup().getSourceId(), group);
            }));
    }

    /**
     * Replace group members whose identities have changed as a result of stitching with references
     * to their new post-stitched identities.
     *
     * @param group The group whose membership may be replaced.
     */
    private void replaceGroupMembersFor(@Nonnull final InterpretedGroup group) {
        Optional.ofNullable(modifiedGroups.get(group.getSourceId()))
                .ifPresent(modifiedGroup -> group.getGroupDefinition()
                        .map(GroupDefinition.Builder::getStaticGroupMembersBuilder)
                        .ifPresent(memberBuilder -> {
                            if (logger.isDebugEnabled()) {
                                logger.debug("Replace members for group {}.",
                                        group.getSourceId());
                            }
                            memberBuilder.clear();
                            memberBuilder.addAllMembersByType(modifiedGroup.getMembers());
                        }));
    }
}
