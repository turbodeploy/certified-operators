package com.vmturbo.group.stitching;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

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

    /**
     * Store all the groups for the given target and probe type.
     *
     * @param targetId target id
     * @param probeType probe type of the target
     * @param uploadedGroups list of discovered groups for the given target
     */
    public void setTargetGroups(long targetId, @Nonnull String probeType,
            @Nonnull Collection<UploadedGroup> uploadedGroups) {
        uploadedGroupsMap.put(targetId, uploadedGroups);
        targetIdToProbeType.put(targetId, probeType);
    }

    public Map<Long, Collection<UploadedGroup>> getUploadedGroupsMap() {
        return uploadedGroupsMap;
    }

    public Map<Long, String> getTargetIdToProbeType() {
        return targetIdToProbeType;
    }
}
