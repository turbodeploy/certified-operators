package com.vmturbo.common.protobuf.group;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.Sets;

import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.group.ClusterServiceGrpc.ClusterServiceImplBase;
import com.vmturbo.common.protobuf.group.GroupDTO.Cluster;
import com.vmturbo.common.protobuf.group.GroupDTO.GetClusterRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetClusterResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetClustersRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupID;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceImplBase;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyGrouping;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyGroupingID;
import com.vmturbo.components.api.test.GrpcTestServer;

public class GroupFetcherTest {

    private GroupFetcher groupFetcher;

    private GroupServiceMock groupServiceBackend = Mockito.spy(new GroupServiceMock());
    private ClusterServiceMock clusterServiceBackend = Mockito.spy(new ClusterServiceMock());


    @Before
    public void setup() throws IOException{

        GrpcTestServer grpcTestServer =
                GrpcTestServer.withServices(groupServiceBackend, clusterServiceBackend);
        groupFetcher = new GroupFetcher(grpcTestServer.getChannel(), Duration.ofSeconds(60L));

    }

    @Test
    public void testGetGroups() throws Exception{
        groupServiceBackend.addGroup(GroupTestUtil.group2);
        groupServiceBackend.addGroup(GroupTestUtil.group1);
        groupServiceBackend.addGroup(GroupTestUtil.group3);
        Map<PolicyGroupingID, PolicyGrouping> result1 =
                groupFetcher.getGroupings(Sets.newHashSet(GroupTestUtil.groupId1, GroupTestUtil.groupId2));
        Map<PolicyGroupingID, PolicyGrouping> expected1 = 
                new HashMap<PolicyGroupingID, PolicyGrouping>(){{
                    put(GroupTestUtil.groupId1, GroupTestUtil.groupGrouping1);
                    put(GroupTestUtil.groupId2, GroupTestUtil.groupGrouping2);
                }};
        assertEquals(result1, expected1);

        Map<PolicyGroupingID, PolicyGrouping> result2 =
                groupFetcher.getGroupings(Sets.newHashSet(GroupTestUtil.groupId1, GroupTestUtil.groupId3));
        Map<PolicyGroupingID, PolicyGrouping> expected2 = 
                new HashMap<PolicyGroupingID, PolicyGrouping>(){{
                    put(GroupTestUtil.groupId1, GroupTestUtil.groupGrouping1);
                    put(GroupTestUtil.groupId3, GroupTestUtil.groupGrouping3);
                }};
        assertEquals(result2, expected2);

        Map<PolicyGroupingID, PolicyGrouping> result3 =
                groupFetcher.getGroupings(Sets.newHashSet(GroupTestUtil.groupId2, GroupTestUtil.groupId3));
        Map<PolicyGroupingID, PolicyGrouping> expected3 =
                new HashMap<PolicyGroupingID, PolicyGrouping>(){{
                    put(GroupTestUtil.groupId3, GroupTestUtil.groupGrouping3);
                    put(GroupTestUtil.groupId2, GroupTestUtil.groupGrouping2);
                }};
        assertEquals(result3, expected3);
    }

    @Test
    public void testGetNonexistentGroups() throws Exception {
        Map<PolicyGroupingID, PolicyGrouping> result =
                groupFetcher.getGroupings(Sets.newHashSet(GroupTestUtil.groupId1, GroupTestUtil.groupId2));
        assertTrue(result.isEmpty());
    }

    @Test
    public void testGetGroupsIncludingNonexistent() throws Exception {
        groupServiceBackend.addGroup(GroupTestUtil.group1);
        Map<PolicyGroupingID, PolicyGrouping> result =
                groupFetcher.getGroupings(Sets.newHashSet(GroupTestUtil.groupId1, GroupTestUtil.groupId2));
        Map<PolicyGroupingID, PolicyGrouping> expected = 
                new HashMap<PolicyGroupingID, PolicyGrouping>(){{
                    put(GroupTestUtil.groupId1, GroupTestUtil.groupGrouping1);
                }};
        assertEquals(result, expected);
    }

    @Test
    public void testGetGroupsSingle() throws Exception {
        groupServiceBackend.addGroup(GroupTestUtil.group2);
        groupServiceBackend.addGroup(GroupTestUtil.group1);
        Map<PolicyGroupingID, PolicyGrouping> result =
                groupFetcher.getGroupings(Collections.singleton(GroupTestUtil.groupId1));
        Map<PolicyGroupingID, PolicyGrouping> expected =
				new HashMap<PolicyGroupingID, PolicyGrouping>(){{
					put(GroupTestUtil.groupId1, GroupTestUtil.groupGrouping1);
				}};
        assertEquals(result, expected);

    }

    @Test
    public void testGetClusters() throws Exception {
        clusterServiceBackend.addCluster(GroupTestUtil.cluster2);
        clusterServiceBackend.addCluster(GroupTestUtil.cluster1);
        clusterServiceBackend.addCluster(GroupTestUtil.cluster3);

        Map<PolicyGroupingID, PolicyGrouping> result1 =
		        groupFetcher.getGroupings(Sets.newHashSet(GroupTestUtil.clusterId1, GroupTestUtil.clusterId2));
        Map<PolicyGroupingID, PolicyGrouping> expected1 =
				new HashMap<PolicyGroupingID, PolicyGrouping>(){{
					put(GroupTestUtil.clusterId1, GroupTestUtil.clusterGrouping1);
                    put(GroupTestUtil.clusterId2, GroupTestUtil.clusterGrouping2);
				}};
        assertEquals(result1, expected1);

        Map<PolicyGroupingID, PolicyGrouping> result2 =
			    groupFetcher.getGroupings(Sets.newHashSet(GroupTestUtil.clusterId1, GroupTestUtil.clusterId3));
        Map<PolicyGroupingID, PolicyGrouping> expected2 =
				new HashMap<PolicyGroupingID, PolicyGrouping>(){{
					put(GroupTestUtil.clusterId1, GroupTestUtil.clusterGrouping1);
                    put(GroupTestUtil.clusterId3, GroupTestUtil.clusterGrouping3);
				}};
        assertEquals(result2, expected2);

        Map<PolicyGroupingID, PolicyGrouping> result3 =
				groupFetcher.getGroupings(Sets.newHashSet(GroupTestUtil.clusterId2, GroupTestUtil.clusterId3));
        Map<PolicyGroupingID, PolicyGrouping> expected3 =
				new HashMap<PolicyGroupingID, PolicyGrouping>(){{
					put(GroupTestUtil.clusterId3, GroupTestUtil.clusterGrouping3);
                    put(GroupTestUtil.clusterId2, GroupTestUtil.clusterGrouping2);
				}};
        assertEquals(result3, expected3);


    }

    @Test
    public void testGetNonexistentClusters() throws Exception {
        Map<PolicyGroupingID, PolicyGrouping> result =
				groupFetcher.getGroupings(Sets.newHashSet(GroupTestUtil.clusterId1, GroupTestUtil.clusterId2));
        assertTrue(result.isEmpty());
    }

    @Test
    public void testGetClustersIncludingNonexistent() throws Exception {
        clusterServiceBackend.addCluster(GroupTestUtil.cluster1);
        Map<PolicyGroupingID, PolicyGrouping> result =
				groupFetcher.getGroupings(Sets.newHashSet(GroupTestUtil.clusterId1, GroupTestUtil.clusterId2));
        Map<PolicyGroupingID, PolicyGrouping> expected =
				new HashMap<PolicyGroupingID, PolicyGrouping>(){{
					put(GroupTestUtil.clusterId1, GroupTestUtil.clusterGrouping1);
				}};
        assertEquals(result, expected);

    }

    @Test
    public void testGetClustersSingle() throws Exception {
        clusterServiceBackend.addCluster(GroupTestUtil.cluster2);
        clusterServiceBackend.addCluster(GroupTestUtil.cluster1);
        Map<PolicyGroupingID, PolicyGrouping> result =
				groupFetcher.getGroupings(Collections.singleton(GroupTestUtil.clusterId1));
        Map<PolicyGroupingID, PolicyGrouping> expected =
				new HashMap<PolicyGroupingID, PolicyGrouping>(){{
					put(GroupTestUtil.clusterId1, GroupTestUtil.clusterGrouping1);
				}};
        assertEquals(result, expected);

    }

    @Test
    public void testGetGroupsAndClusters() throws Exception {
        clusterServiceBackend.addCluster(GroupTestUtil.cluster2);
        clusterServiceBackend.addCluster(GroupTestUtil.cluster1);
        clusterServiceBackend.addCluster(GroupTestUtil.cluster3);
        groupServiceBackend.addGroup(GroupTestUtil.group2);
        groupServiceBackend.addGroup(GroupTestUtil.group1);
        groupServiceBackend.addGroup(GroupTestUtil.group3);

        Map<PolicyGroupingID, PolicyGrouping> result1 =
				groupFetcher.getGroupings(Sets.newHashSet(GroupTestUtil.clusterId1, GroupTestUtil.clusterId2, GroupTestUtil.groupId1, GroupTestUtil.groupId2));
        Map<PolicyGroupingID, PolicyGrouping> expected1 =
                new HashMap<PolicyGroupingID, PolicyGrouping>(){{
                    put(GroupTestUtil.clusterId1, GroupTestUtil.clusterGrouping1);
                    put(GroupTestUtil.clusterId2, GroupTestUtil.clusterGrouping2);
                    put(GroupTestUtil.groupId1, GroupTestUtil.groupGrouping1);
                    put(GroupTestUtil.groupId2, GroupTestUtil.groupGrouping2);
                }};
        assertEquals(result1, expected1);

        Map<PolicyGroupingID, PolicyGrouping> result2 =
				groupFetcher.getGroupings(Sets.newHashSet(GroupTestUtil.clusterId1, GroupTestUtil.clusterId3, GroupTestUtil.groupId1, GroupTestUtil.groupId3));
        Map<PolicyGroupingID, PolicyGrouping> expected2 =
                new HashMap<PolicyGroupingID, PolicyGrouping>(){{
                    put(GroupTestUtil.clusterId1, GroupTestUtil.clusterGrouping1);
                    put(GroupTestUtil.clusterId3, GroupTestUtil.clusterGrouping3);
                    put(GroupTestUtil.groupId1, GroupTestUtil.groupGrouping1);
                    put(GroupTestUtil.groupId3, GroupTestUtil.groupGrouping3);
                }};
        assertEquals(result2, expected2);

        Map<PolicyGroupingID, PolicyGrouping> result3 =
				groupFetcher.getGroupings(Sets.newHashSet(GroupTestUtil.clusterId3, GroupTestUtil.clusterId2, GroupTestUtil.groupId3, GroupTestUtil.groupId2));
        Map<PolicyGroupingID, PolicyGrouping> expected3 =
                new HashMap<PolicyGroupingID, PolicyGrouping>(){{
                    put(GroupTestUtil.clusterId3, GroupTestUtil.clusterGrouping3);
                    put(GroupTestUtil.clusterId2, GroupTestUtil.clusterGrouping2);
                    put(GroupTestUtil.groupId3, GroupTestUtil.groupGrouping3);
                    put(GroupTestUtil.groupId2, GroupTestUtil.groupGrouping2);
                }};
        assertEquals(result3, expected3);

    }

    public static class GroupServiceMock extends GroupServiceImplBase {
        private final List<Group> groups = new ArrayList<>();

        void addGroup(@Nonnull final Group group){
            groups.add(Objects.requireNonNull(group));
        }

        @Override
        public void getGroup(GroupID id, StreamObserver<GetGroupResponse> observer){
            groups.stream()
                    .filter(g -> g.getId() == id.getId())
                    .map(g -> GetGroupResponse.newBuilder().setGroup(g).build())
                    .forEach(observer::onNext);
            observer.onCompleted();
        }

        @Override
        public void getGroups(GetGroupsRequest request, StreamObserver<Group> observer){
            groups.stream()
                    .filter(g -> request.getIdList().contains(g.getId()))
                    .forEach(observer::onNext);
            observer.onCompleted();
        }
    }

    private static class ClusterServiceMock extends ClusterServiceImplBase {
        private final List<Cluster> clusters = new ArrayList<>();

        void addCluster(@Nonnull final Cluster cluster){
            clusters.add(Objects.requireNonNull(cluster));
        }

        @Override
        public void getCluster(GetClusterRequest request,
                               StreamObserver<GetClusterResponse> observer){
            clusters.stream()
                    .filter(cluster -> cluster.getId() == request.getClusterId())
                    .map(c -> GetClusterResponse.newBuilder().setCluster(c).build())
                    .forEach(observer::onNext);
            observer.onCompleted();
        }

        @Override
        public void getClusters(GetClustersRequest request, StreamObserver<Cluster> observer){
            clusters.stream()
                    .filter(c -> request.getIdList().contains(c.getId()))
                    .forEach(observer::onNext);
            observer.onCompleted();
        }
    }
}
