package com.vmturbo.group.group;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo.Type;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticGroupMembers;

public class EntityToClusterMappingTest {

    private EntityToClusterMapping entityToClusterMapping;

    private static final ClusterInfo HOST_CLUSTER =
            ClusterInfo.newBuilder()
                    .setName("HostCluster1")
                    .setClusterType(Type.COMPUTE)
                    .setMembers(StaticGroupMembers.newBuilder()
                            .addStaticMemberOids(100L)
                            .addStaticMemberOids(101L)
                            .build())
                    .build();

    private static final ClusterInfo STORAGE_CLUSTER =
            ClusterInfo.newBuilder()
                    .setName("StorageCluster1")
                    .setClusterType(Type.STORAGE)
                    .setMembers(StaticGroupMembers.newBuilder()
                            .addStaticMemberOids(200L)
                            .addStaticMemberOids(201L)
                            .build())
                    .build();

    @Before
    public void setup() throws Exception{

        entityToClusterMapping = new EntityToClusterMapping();
    }

    @Test
    public void testUpdateAndGetEntityClusterMapping() {
        Map<Long, ClusterInfo> updatedClusters = new HashMap<>();
        updatedClusters.put(111L, HOST_CLUSTER);
        updatedClusters.put(222L, STORAGE_CLUSTER);

        entityToClusterMapping.updateEntityClusterMapping(updatedClusters);

        assertThat(entityToClusterMapping.getClusterForEntity(
                HOST_CLUSTER.getMembers().getStaticMemberOids(0)),
                is(111L));
        assertThat(entityToClusterMapping.getClusterForEntity(
                STORAGE_CLUSTER.getMembers().getStaticMemberOids(0)),
                is(222L));
    }

    @Test
    public void testGetClusterForEntityNoMapping() {
        assertThat(entityToClusterMapping.getClusterForEntity(1L), is(0L));
    }

    @Test
    public void testEntityClusterMapCleared() {

        Map<Long, ClusterInfo> updatedClusters = new HashMap<>();
        updatedClusters.put(111L, HOST_CLUSTER);
        entityToClusterMapping.updateEntityClusterMapping(updatedClusters);

        assertThat(entityToClusterMapping.getClusterForEntity(
                HOST_CLUSTER.getMembers().getStaticMemberOids(0)),
                is(111L));

        entityToClusterMapping.updateEntityClusterMapping(Collections.emptyMap());
        // now the index should be cleared and we should not get a valid clusterId.
        assertThat(entityToClusterMapping.getClusterForEntity(
                HOST_CLUSTER.getMembers().getStaticMemberOids(0)),
                is(0L));
    }

}
