package com.vmturbo.topology.processor.group.policy;

import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.SearchParametersCollection;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticGroupMembers;

public class PolicyGroupingHelper {

    private PolicyGroupingHelper() {}

    public static Group policyGrouping(SearchParametersCollection searchParameters,
                                                int entityType, long oid) {
        return Group.newBuilder()
            .setGroup(GroupInfo.newBuilder()
                .setSearchParametersCollection(searchParameters)
                .setEntityType(entityType)
                .build())
            .setId(oid)
            .build();
    }

    public static Group policyGrouping(StaticGroupMembers members,
                                                int entityType, long oid) {
        return Group.newBuilder()
            .setGroup(GroupInfo.newBuilder()
                .setStaticGroupMembers(members)
                .setEntityType(entityType)
                .build())
            .setId(oid)
            .build();
    }

    public static Group policyGrouping(String name, int entityType, long oid) {
        return Group.newBuilder()
            .setGroup(GroupInfo.newBuilder()
                .setName(name)
                .setEntityType(entityType)
                .build())
            .setId(oid)
            .build();
    }
}
