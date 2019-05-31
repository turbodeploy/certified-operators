package com.vmturbo.plan.orchestrator.project.headroom;

import java.util.Map;

import com.google.common.collect.Maps;

import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.stats.Stats.SystemLoadInfoResponse;
import com.vmturbo.plan.orchestrator.project.headroom.SystemLoadCalculatedProfile.Operation;

/**
 * Initiates creation of system load profile for : AVG and MAX operations.
 */
public class SystemLoadProfileCreator {

    private Group cluster = null;
    private SystemLoadInfoResponse records = null;
    private String profileNamePostfix = null;
    private String profileDisplayNamePostfix = null;

    /**
     * Initializes SystemLoadProfileCreator to generate system load profile.
     *
     * @param cluster for which profile needs to be created.
     * @param records to process for profile creation.
     * @param loopbackDays for which history is considered for profile creation.
     */
    public SystemLoadProfileCreator(Group cluster, SystemLoadInfoResponse records, int loopbackDays) {
        this.cluster = cluster;
        this.records = records;
        profileNamePostfix = String.format("%s_HEADROOM", cluster.getCluster().getDisplayName());
        this.profileDisplayNamePostfix = String.format("%s for last %s days", cluster.getCluster().getDisplayName(), loopbackDays);
    }

    /**
     * Creates profile for all operation types (AVG and MAX).
     *
     * @return a map with operation type as key and created profile as value.
     */
    public Map<Operation, SystemLoadCalculatedProfile> createAllProfiles() {
        Map<Operation, SystemLoadCalculatedProfile> profileMap = Maps.newHashMap();
        for (Operation op : Operation.values()) {
            SystemLoadCalculatedProfile profile = new SystemLoadCalculatedProfile(op, cluster, records, profileNamePostfix,
                            profileDisplayNamePostfix);
            profile.createVirtualMachineProfile();
            profileMap.put(op, profile);
        }
        return profileMap;
    }
}
