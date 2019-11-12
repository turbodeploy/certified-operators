package com.vmturbo.group.stitching;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredGroupsPoliciesSettings.UploadedGroup;

/**
 * A context object that contains the data necessary to perform group stitching.
 *
 * <p>A {@link GroupStitchingContext} contains acceleration structures that permit rapid lookup
 * of groups.
 */
public class GroupStitchingContext {
    /**
     * A map which contains groups grouped by probe type and target Id.
     */
    private final Map<Long, Collection<UploadedGroup>> uploadedGroupsMap = new HashMap<>();

    private final Map<Long, String> targetIdToProbeType = new HashMap<>();
    private final Set<Long> undiscoveredTargets = new HashSet<>();

    /**
     * Store all the groups for the given target and probe type.
     *
     * @param targetId target id
     * @param probeType probe type of the target
     * @param uploadedGroups list of discovered groups for the given target
     */
    public void setTargetGroups(long targetId, @Nonnull String probeType,
            @Nonnull Collection<UploadedGroup> uploadedGroups) {
        ensureNewTarget(targetId);
        uploadedGroupsMap.put(targetId, uploadedGroups);
        targetIdToProbeType.put(targetId, probeType);
    }

    private void ensureNewTarget(long targetId) {
        if (undiscoveredTargets.contains(targetId) && uploadedGroupsMap.containsKey(targetId)) {
            throw new IllegalArgumentException(
                    "Target " + targetId + " is already registered in stitching context");
        }
    }

    /**
     * Register a target that have not been discovered yet. Groups from this targets will not be
     * updated or deleted.
     *
     * @param targetId target that lacks any data
     */
    public void addUndiscoveredTarget(long targetId) {
        ensureNewTarget(targetId);
        undiscoveredTargets.add(targetId);
    }

    public Map<Long, Collection<UploadedGroup>> getUploadedGroupsMap() {
        return uploadedGroupsMap;
    }

    public Map<Long, String> getTargetIdToProbeType() {
        return targetIdToProbeType;
    }

    /**
     * Returns a set of targets that have not finished discovery. I.e., groups from these targets
     * are not present temporarily.
     *
     * @return set of undiscovered targets
     */
    @Nonnull
    public Set<Long> getUndiscoveredTargets() {
        return undiscoveredTargets;
    }
}
