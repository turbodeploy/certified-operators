package com.vmturbo.common.protobuf.group;

import com.vmturbo.common.protobuf.group.GroupDTO.Cluster;
import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo.Type;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.Group.Origin;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticGroupMembers;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyGrouping;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyGroupingID;

public class GroupTestUtil {

    public static final Long id1 = 4815162342L;
    private static final Long id2 = 31415926L;
    private static final Long id3 = 1123581321L;

    public static final Group group1 = makeTestGroup("group1", id1);
    public static final Group group2 = makeTestGroup("group2", id2);
    public static final Group group3 = makeTestGroup("group3", id3);

    public static final PolicyGroupingID groupId1 = makeTestGroupingId(id1, true);
    public static final PolicyGroupingID groupId2 = makeTestGroupingId(id2, true);
    public static final PolicyGroupingID groupId3 = makeTestGroupingId(id3, true);

    public static final PolicyGrouping groupGrouping1 = makeTestGrouping(group1, true);
    public static final PolicyGrouping groupGrouping2 = makeTestGrouping(group2, true);
    public static final PolicyGrouping groupGrouping3 = makeTestGrouping(group3, true);

    public static final Cluster cluster1 = makeTestCluster("cluster1", id1);
    public static final Cluster cluster2 = makeTestCluster("cluster2", id2);
    public static final Cluster cluster3 = makeTestCluster("cluster3", id3);

    public static final PolicyGroupingID clusterId1 = makeTestGroupingId(id1, false);
    public static final PolicyGroupingID clusterId2 = makeTestGroupingId(id2, false);
    public static final PolicyGroupingID clusterId3 = makeTestGroupingId(id3, false);

    public static final PolicyGrouping clusterGrouping1 = makeTestGrouping(cluster1, false);
    public static final PolicyGrouping clusterGrouping2 = makeTestGrouping(cluster2, false);
    public static final PolicyGrouping clusterGrouping3 = makeTestGrouping(cluster3, false);

    private static Cluster makeTestCluster(String name, Long id) {
        return Cluster.newBuilder()
                .setInfo(ClusterInfo.newBuilder()
                        .setName(name)
                        .setClusterType(Type.COMPUTE)
                        .setMembers(StaticGroupMembers.newBuilder()
                                .addStaticMemberOids(id)
                                .build())
                        .build())
                .setId(id)
                .setTargetId(id)
                .build();
    }

    private static Group makeTestGroup(String name, Long id) {
        return Group.newBuilder()
                .setInfo(GroupInfo.newBuilder()
                        .setEntityType(0)
                        .setName(name)
                        .setStaticGroupMembers(StaticGroupMembers.newBuilder()
                                .addStaticMemberOids(id)
                                .build())
                        .build())
                .setOrigin(Origin.USER)
                .setId(id)
                .setTargetId(id)
                .build();
    }

    private static PolicyGroupingID makeTestGroupingId(Long id, boolean isGroup){
        PolicyGroupingID.Builder b = PolicyGroupingID.newBuilder();
        return isGroup ? b.setGroupId(id).build() : b.setClusterId(id).build();
    }

    private static PolicyGrouping makeTestGrouping(Object grouping, boolean isGroup){
        PolicyGrouping.Builder b = PolicyGrouping.newBuilder();
        return isGroup ? b.setGroup((Group)grouping).build()
                : b.setCluster((Cluster)grouping).build();
    }
}
