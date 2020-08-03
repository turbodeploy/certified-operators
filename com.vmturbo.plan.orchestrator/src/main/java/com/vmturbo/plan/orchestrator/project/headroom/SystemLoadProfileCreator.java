package com.vmturbo.plan.orchestrator.project.headroom;

import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import com.google.common.collect.Maps;

import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.stats.Stats.SystemLoadRecord;
import com.vmturbo.plan.orchestrator.project.headroom.SystemLoadCalculatedProfile.Operation;

/**
 * Initiates creation of system load profile for : AVG and MAX operations.
 */
public class SystemLoadProfileCreator {

    private Grouping cluster;
    private List<SystemLoadRecord> systemLoadRecordList;
    private String profileNamePostfix;
    private String profileDisplayNamePostfix;
    private final Map<Long, String> targetOidToTargetName;

    /**
     * Initializes SystemLoadProfileCreator to generate system load profile.
     *
     * @param cluster for which profile needs to be created.
     * @param systemLoadRecordList to process for profile creation.
     * @param loopbackDays for which history is considered for profile creation.
     * @param targetOidToTargetName targetOid to targetName map.
     */
    public SystemLoadProfileCreator(@Nonnull final Grouping cluster,
                                    @Nonnull final List<SystemLoadRecord> systemLoadRecordList,
                                    final int loopbackDays,
                                    @Nonnull final Map<Long, String> targetOidToTargetName) {
        this.cluster = cluster;
        this.systemLoadRecordList = systemLoadRecordList;
        this.profileNamePostfix = String.format("%s_HEADROOM", cluster.getDefinition().getDisplayName());
        this.profileDisplayNamePostfix = String.format("%s for last %s days",
            cluster.getDefinition().getDisplayName(), loopbackDays);
        this.targetOidToTargetName = targetOidToTargetName;
    }

    /**
     * Creates profile for all operation types (AVG and MAX).
     *
     * @return a map with operation type as key and created profile as value.
     */
    public Map<Operation, SystemLoadCalculatedProfile> createAllProfiles() {
        Map<Operation, SystemLoadCalculatedProfile> profileMap = Maps.newHashMap();
        for (Operation op : Operation.values()) {
            SystemLoadCalculatedProfile profile = new SystemLoadCalculatedProfile(
                op, cluster, systemLoadRecordList, profileNamePostfix, profileDisplayNamePostfix,
                targetOidToTargetName);
            profile.createVirtualMachineProfile();
            profileMap.put(op, profile);
        }
        return profileMap;
    }
}
