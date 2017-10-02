package com.vmturbo.topology.processor.group.policy;

import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.SearchParametersCollection;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticGroupMembers;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyGrouping;

/**
 * Helper class to create instances of {@link PolicyGrouping} fopr unit tests.
 */
public class PolicyGroupingHelper {

    private PolicyGroupingHelper() {}

    public static PolicyGrouping policyGrouping(SearchParametersCollection searchParameters, int entityType, long oid) {
        return PolicyGrouping.newBuilder()
                        .setGroup(Group.newBuilder()
                            .setInfo(GroupInfo.newBuilder()
                                .setSearchParametersCollection(searchParameters)
                                .setEntityType(entityType)
                                .build())
                            .setId(oid)
                            .build())
                        .build();
    }

    public static PolicyGrouping policyGrouping(StaticGroupMembers members, int entityType, long oid) {
        return PolicyGrouping.newBuilder()
                        .setGroup(Group.newBuilder()
                            .setInfo(GroupInfo.newBuilder()
                                .setStaticGroupMembers(members)
                                .setEntityType(entityType)
                                .build())
                            .setId(oid)
                            .build())
                        .build();
    }

    public static PolicyGrouping policyGrouping(String name, int entityType, long oid) {
        return PolicyGrouping.newBuilder()
                        .setGroup(Group.newBuilder()
                            .setInfo(GroupInfo.newBuilder()
                                .setName(name)
                                .setEntityType(entityType)
                                .build())
                            .setId(oid)
                            .build())
                        .build();
    }
}
