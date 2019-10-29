package com.vmturbo.group.stitching;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredGroupsPoliciesSettings.UploadedGroup;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;

/**
 * A context object that contains the data necessary to perform group stitching.
 *
 * <p>A {@link GroupStitchingContext} contains acceleration structures that permit rapid lookup
 * of groups by {@link GroupType} and also by the probe type that discovered that group.
 */
public class GroupStitchingContext {

    private static final Logger logger = LogManager.getLogger();

    /**
     * A map which contains groups grouped by probe type and group type.
     */
    private final Map<String, Map<GroupType, List<StitchingGroup>>> groupsByProbeTypeAndGroupType =
            new HashMap<>();

    private final Map<Long, String> targetIdToProbeType = new HashMap<>();

    /**
     * Store all the groups for the given target and probe type.
     *
     * @param targetId target id
     * @param probeType probe type of the target
     * @param uploadedGroups list of discovered groups for the given target
     */
    public void setTargetGroups(long targetId, @Nonnull String probeType,
            @Nonnull List<UploadedGroup> uploadedGroups) {
        final Map<GroupType, List<StitchingGroup>> groupsByType =
                groupsByProbeTypeAndGroupType.computeIfAbsent(probeType, k -> new HashMap<>());
        uploadedGroups.forEach(uploadedGroup -> {
            final GroupDefinition.Builder groupBuilder =
                    uploadedGroup.toBuilder().getDefinitionBuilder();
            final StitchingGroup stitchingGroup = new StitchingGroup(groupBuilder,
                    uploadedGroup.getSourceIdentifier(), targetId);
            groupsByType.computeIfAbsent(groupBuilder.getType(), k -> new ArrayList<>())
                    .add(stitchingGroup);
        });
        targetIdToProbeType.put(targetId, probeType);
    }

    /**
     * Remove the given list of groups from this context.
     *
     * @param groupsToRemove list of groups to remove
     */
    public void removeGroups(@Nonnull List<StitchingGroup> groupsToRemove) {
        groupsToRemove.forEach(this::removeGroup);
    }

    /**
     * Remove the given group from this context.
     *
     * @param groupToRemove the group to remove
     */
    private void removeGroup(@Nonnull StitchingGroup groupToRemove) {
        String probeType = getProbeType(groupToRemove);
        if (probeType == null) {
            logger.error("Probe type not found for group {}", groupToRemove.getGroupDefinition());
        }
        final Map<GroupType, List<StitchingGroup>> groupsByType =
                groupsByProbeTypeAndGroupType.get(probeType);
        if (groupsByType != null) {
            final List<StitchingGroup> interpretedGroups =
                    groupsByType.get(groupToRemove.getGroupDefinition().getType());
            if (interpretedGroups != null) {
                interpretedGroups.remove(groupToRemove);
            }
        }
    }

    /**
     * Get all the stitching groups available in this stitching context. After stitching, some
     * groups are merged into one group, so some may have been deleted.
     *
     * @return collection of all stitching groups.
     */
    public Collection<StitchingGroup> getAllStitchingGroups() {
        return groupsByProbeTypeAndGroupType.values().stream()
                .flatMap(groupsByType -> groupsByType.values().stream())
                .flatMap(List::stream)
                .collect(Collectors.toList());
    }

    /**
     * Get all stitching groups matching the given probe types and group types.
     *
     * @param probesTypes set of probe types
     * @param groupType type of the group
     * @return collection of {@link StitchingGroup}
     */
    public Collection<StitchingGroup> getGroupsOfProbeTypesAndGroupType(
            @Nonnull Set<String> probesTypes, @Nonnull GroupType groupType) {
        return groupsByProbeTypeAndGroupType.entrySet()
                .stream()
                .filter(entry -> probesTypes.contains(entry.getKey()))
                .map(entry -> entry.getValue().get(groupType))
                .filter(Objects::nonNull)
                .flatMap(List::stream)
                .collect(Collectors.toList());
    }

    /**
     * Get the probe type of the given group.
     *
     * @param group group to get probe type for
     * @return optional probe of the group
     */
    public String getProbeType(@Nonnull StitchingGroup group) {
        return targetIdToProbeType.get(group.getSourceTargetId());
    }
}