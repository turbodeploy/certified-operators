package com.vmturbo.group.stitching;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;

/**
 * An operation which takes resource groups discovered from different probes, combine their
 * members, keep only one resource group for one target and discarding same RGs from other targets.
 */
public class ResourceGroupStitchingOperation implements GroupStitchingOperation {

    /**
     * Set of probes which discover resource groups. This is listed in the descending order by
     * importance.
     * For example: The RG from Azure probe is considered of higher priority than others, so it
     * will be used as a basis and persisted, RGs from other probes will be discarded.
     */
    private static final Set<String> ORDERED_RESOURCE_GROUP_PROBE_TYPES;

    static {
        LinkedHashSet<String> resourceGroupProbeTypesInOrder = new LinkedHashSet<>();
        resourceGroupProbeTypesInOrder.add(SDKProbeType.AZURE.toString());
        resourceGroupProbeTypesInOrder.add(SDKProbeType.AZURE_STORAGE_BROWSE.toString());
        resourceGroupProbeTypesInOrder.add(SDKProbeType.APPINSIGHTS.toString());
        ORDERED_RESOURCE_GROUP_PROBE_TYPES = Collections.unmodifiableSet(
                resourceGroupProbeTypesInOrder);
    }

    @Override
    public Collection<StitchingGroup> getScope(@Nonnull GroupStitchingContext groupStitchingContext) {
        return groupStitchingContext.getGroupsOfProbeTypesAndGroupType(
                ORDERED_RESOURCE_GROUP_PROBE_TYPES, GroupType.RESOURCE);
    }

    @Override
    public GroupStitchingContext stitch(@Nonnull Collection<StitchingGroup> groups,
                                        @Nonnull GroupStitchingContext groupStitchingContext) {
        // group RG by id first and do not do stitching if only ONE RG is discovered for same id
        final Map<String, List<StitchingGroup>> resourceGroupsById = new HashMap<>();
        groups.forEach(resourceGroup -> resourceGroupsById.computeIfAbsent(
                resourceGroup.getSourceId(), k -> new ArrayList<>()).add(resourceGroup));

        // find groups to stitch and organize by id and probe type
        final Map<String, Map<String, StitchingGroup>> resourceGroupsToStitch = new HashMap<>();
        resourceGroupsById.entrySet().stream()
                // only try to stitch if there are multiple RGs for same id
                .filter(entry -> entry.getValue().size() > 1)
                .forEach(entry -> {
                    entry.getValue().forEach(resourceGroup -> {
                        final Map<String, StitchingGroup> groupByProbeType =
                                resourceGroupsToStitch.computeIfAbsent(entry.getKey(),
                                        k -> new HashMap<>());
                        groupByProbeType.put(groupStitchingContext.getProbeType(resourceGroup),
                                resourceGroup);
                    });
                });

        final List<StitchingGroup> groupsToRemove = new ArrayList<>();
        // merge members of RGs with same id
        for (Entry<String, Map<String, StitchingGroup>> entry : resourceGroupsToStitch.entrySet()) {
            final Map<String, StitchingGroup> groupByProbeType = entry.getValue();
            // find the main group which will be based on and saved to db
            final Optional<StitchingGroup> mainGroupOpt =
                    ORDERED_RESOURCE_GROUP_PROBE_TYPES.stream()
                            .filter(groupByProbeType::containsKey)
                            .map(groupByProbeType::get)
                            .findFirst();
            if (!mainGroupOpt.isPresent()) {
                // this should not happen, since there will always be at least one ResourceGroup
                // if we get this point
                throw new IllegalStateException(
                        "No resource groups found for id " + entry.getKey());
            }

            final StitchingGroup mainGroup = mainGroupOpt.get();
            final Map<MemberType, Set<Long>> combinedMembers = new HashMap<>();
            groupByProbeType.values().forEach(group -> {
                group.getGroupDefinition()
                        .getStaticGroupMembers()
                        .getMembersByTypeList()
                        .forEach(membersByType -> combinedMembers.computeIfAbsent(
                                membersByType.getType(), k -> new HashSet<>())
                                .addAll(membersByType.getMembersList()));
                if (group != mainGroup) {
                    // this group should be removed
                    groupsToRemove.add(group);
                    // add the merged from target id
                    mainGroup.mergedFromTarget(group.getSourceTargetId());
                }
            });

            // set all members after merge
            mainGroup.setMembers(combinedMembers);
        }

        // remove RGs involved in stitching, so we only upload ONE RG to group component
        groupStitchingContext.removeGroups(groupsToRemove);

        return groupStitchingContext;
    }
}