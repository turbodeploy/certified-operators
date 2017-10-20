package com.vmturbo.api.component.external.api.service;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.assertj.core.util.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.mapper.ActionSpecMapper;
import com.vmturbo.api.component.external.api.mapper.GroupMapper;
import com.vmturbo.api.dto.FilterApiDTO;
import com.vmturbo.api.dto.GroupApiDTO;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc;
import com.vmturbo.common.protobuf.group.ClusterServiceGrpc;
import com.vmturbo.common.protobuf.group.ClusterServiceGrpc.ClusterServiceImplBase;
import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.Cluster;
import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.GetClusterRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetClusterResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetClustersRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupID;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticGroupMembers;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceImplBase;
import com.vmturbo.components.api.test.GrpcTestServer;

public class GroupsServiceTest {

    public static final String NE_MATCH_TYPE = "NE";
    public static final String EQ_MATCH_TYPE = "EQ";
    public static final String GROUP_FILTER_TYPE = "groupsByName";
    public static final String CLUSTER_FILTER_TYPE = "clustersByName";
    private GroupsService groupsService;

    private static final String GROUP_TEST_PATTERN = "groupTestString";
    private static final String CLUSTER_TEST_PATTERN = "clusterTestString";

    @Mock
    private ActionSpecMapper actionSpecMapper;

    @Mock
    private GroupMapper groupMapper;

    @Mock
    private RepositoryApi repositoryApi;

    @Mock
    private Channel channelMock;

    private GroupServiceSpyRecruit groupServiceSpy = spy(new GroupServiceSpyRecruit());

    private ClusterServiceSpyRecruit clusterServiceSpy = spy(new ClusterServiceSpyRecruit());

    private FilterApiDTO groupFilterApiDTO = new FilterApiDTO();
    private FilterApiDTO clusterFilterApiDTO = new FilterApiDTO();

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(groupServiceSpy, clusterServiceSpy);

    @Before
    public void init() throws Exception {
        MockitoAnnotations.initMocks(this);

        channelMock = Mockito.mock(Channel.class);
        ActionsServiceGrpc.ActionsServiceBlockingStub actionsRpcService = ActionsServiceGrpc.newBlockingStub(channelMock);

        long realtimeTopologyContextId = 7777777;
        groupsService =  new GroupsService(
                actionsRpcService,
                GroupServiceGrpc.newBlockingStub(grpcServer.getChannel()),
                ClusterServiceGrpc.newBlockingStub(grpcServer.getChannel()),
                actionSpecMapper,
                groupMapper,
                repositoryApi,
                realtimeTopologyContextId);

        groupFilterApiDTO.setFilterType(GROUP_FILTER_TYPE);
        groupFilterApiDTO.setExpVal(GROUP_TEST_PATTERN);
        groupFilterApiDTO.setExpType(EQ_MATCH_TYPE);

        clusterFilterApiDTO.setFilterType(CLUSTER_FILTER_TYPE);
        clusterFilterApiDTO.setExpVal(CLUSTER_TEST_PATTERN);
        clusterFilterApiDTO.setExpType(EQ_MATCH_TYPE);
    }

    /**
     * Test generating group request with name pattern match.
     */
    @Test
    public void testGetGroupsWithFilterEq() throws Exception {
        // Arrange

        // Act
        List<FilterApiDTO> filterList = Lists.newArrayList(
                groupFilterApiDTO,
                clusterFilterApiDTO
        );
        GroupDTO.GetGroupsRequest groupsRequest = groupsService.getGroupsRequestForFilters(filterList);

        // Assert
        assertThat(groupsRequest.getNameFilter().getNameRegex(), is(GROUP_TEST_PATTERN));
        assertThat(groupsRequest.getNameFilter().getNegateMatch(), is(false));
    }

    /**
     * Test generating group request with negation on the name pattern match.
     */
    @Test
    public void testGetGroupsWithFilterNe() throws Exception {
        // Arrange
        groupFilterApiDTO.setExpType(NE_MATCH_TYPE);
        // Act
        List<FilterApiDTO> filterList = Lists.newArrayList(
                groupFilterApiDTO,
                clusterFilterApiDTO
        );
        GroupDTO.GetGroupsRequest groupsRequest = groupsService.getGroupsRequestForFilters(filterList);

        // Assert
        assertThat(groupsRequest.getNameFilter().getNameRegex(), is(GROUP_TEST_PATTERN));
        assertThat(groupsRequest.getNameFilter().getNegateMatch(), is(true));
    }

    /**
     * Test generating cluster request with negation on the name pattern match.
     */
    @Test
    public void testGetClustersWithFilterNe() throws Exception {
        // Arrange
        clusterFilterApiDTO.setExpType(NE_MATCH_TYPE);
        // Act
        List<FilterApiDTO> filterList = Lists.newArrayList(
                groupFilterApiDTO,
                clusterFilterApiDTO
        );
        GetClustersRequest.Builder groupsRequest = groupsService.getClustersFilterRequest(filterList);

        // Assert
        assertThat(groupsRequest.getNameFilter().getNameRegex(), is(CLUSTER_TEST_PATTERN));
        assertThat(groupsRequest.getNameFilter().getNegateMatch(), is(true));
    }

    @Test
    public void getGroupByUuid() throws Exception {
        final GroupApiDTO apiGroup = new GroupApiDTO();
        apiGroup.setDisplayName("minion");
        final Group xlGroup = Group.newBuilder().setId(1).build();
        when(groupMapper.toGroupApiDto(xlGroup)).thenReturn(apiGroup);
        doReturn(Optional.of(xlGroup)).when(groupServiceSpy).getGroup(eq(1L));

        final GroupApiDTO retGroup = groupsService.getGroupByUuid("1", false);
        assertEquals(apiGroup.getDisplayName(), retGroup.getDisplayName());
    }

    @Test
    public void getClusterByUuid() throws Exception {
        final GroupApiDTO apiCluster = new GroupApiDTO();
        apiCluster.setDisplayName("minion");
        final Cluster cluster = Cluster.newBuilder().setId(1).build();
        when(groupMapper.toGroupApiDto(cluster)).thenReturn(apiCluster);
        doReturn(Optional.of(cluster)).when(clusterServiceSpy).getCluster(eq(1L));

        final GroupApiDTO retCluster = groupsService.getGroupByUuid("1", false);
        assertEquals(apiCluster.getDisplayName(), retCluster.getDisplayName());
    }

    @Test(expected = UnknownObjectException.class)
    public void testGroupAndClusterNotFound() throws Exception {
        groupsService.getGroupByUuid("1", false);
    }

    @Test
    public void getGroupMembersByUuid() throws Exception {
        final long memberId = 7;
        doReturn(Collections.singletonList(memberId)).when(groupServiceSpy).getGroupMembers(eq(1L));
        Optional<Collection<Long>> retIds = groupsService.getMemberIds("1");
        assertTrue(retIds.isPresent());
        assertEquals(1, retIds.get().size());
        assertEquals(memberId, retIds.get().iterator().next().longValue());
    }

    @Test
    public void getClusterMembersByUuid() throws Exception {
        final long clusterId = 1;
        final long memberId = 7;
        doThrow(UnknownObjectException.class).when(groupServiceSpy).getGroupMembers(eq(clusterId));
        doReturn(Optional.of(Cluster.newBuilder()
                .setInfo(ClusterInfo.newBuilder()
                        .setMembers(StaticGroupMembers.newBuilder()
                                .addStaticMemberOids(memberId)))
                .build())).when(clusterServiceSpy).getCluster(clusterId);
        Optional<Collection<Long>> retIds = groupsService.getMemberIds("1");
        assertTrue(retIds.isPresent());
        assertEquals(1, retIds.get().size());
        assertEquals(memberId, retIds.get().iterator().next().longValue());
    }

    @Test(expected = UnknownObjectException.class)
    public void testGetMemberIdsInvalidUuid() throws Exception {
        final long groupId = 1;
        doThrow(UnknownObjectException.class).when(groupServiceSpy).getGroupMembers(eq(groupId));
        groupsService.getMemberIds("1");
    }

    /**
     * An implementation of GroupService, the behaviour of which can be modified using spies.
     * It's a spy recruit because it can become a spy :)
     */
    private class GroupServiceSpyRecruit extends GroupServiceImplBase {

        Optional<Group> getGroup(final long id) {
            return Optional.empty();
        }

        List<Long> getGroupMembers(final long groupId) throws UnknownObjectException {
            return Collections.emptyList();
        }

        @Override
        public void getGroup(GroupID request,
                             StreamObserver<GetGroupResponse> responseObserver) {
            final Optional<Group> group = getGroup(request.getId());
            final GetGroupResponse.Builder builder = GetGroupResponse.newBuilder();
            group.ifPresent(builder::setGroup);
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        }

        @Override
        public void getMembers(GetMembersRequest request,
                               StreamObserver<GetMembersResponse> responseObserver) {
            try {
                responseObserver.onNext(GetMembersResponse.newBuilder()
                        .addAllMemberId(getGroupMembers(request.getId()))
                        .build());
                responseObserver.onCompleted();
            } catch (UnknownObjectException e) {
                responseObserver.onError(Status.NOT_FOUND.asException());
            }
        }
    }

    /**
     * An implementation of ClusterService, the behaviour of which can be modified using spies.
     * It's a spy recruit because it can become a spy :)
     */
    private static class ClusterServiceSpyRecruit extends ClusterServiceImplBase {

        Optional<Cluster> getCluster(final long id) {
            return Optional.empty();
        }

        @Override
        public void getCluster(GetClusterRequest request,
                               StreamObserver<GetClusterResponse> responseObserver) {
            final Optional<Cluster> cluster = getCluster(request.getClusterId());
            final GetClusterResponse.Builder builder = GetClusterResponse.newBuilder();
            cluster.ifPresent(builder::setCluster);
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        }
    }
}