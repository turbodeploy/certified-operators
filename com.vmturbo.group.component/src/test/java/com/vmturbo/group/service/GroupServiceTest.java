package com.vmturbo.group.service;

import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.UpdateGroupRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.SearchParametersCollection;
import com.vmturbo.common.protobuf.group.PolicyDTO;
import com.vmturbo.common.protobuf.group.PolicyDTO.InputGroup;
import com.vmturbo.common.protobuf.group.PolicyDTO.InputPolicy.BindToGroupPolicy;
import com.vmturbo.common.protobuf.search.Search;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.group.persistent.DatabaseException;
import com.vmturbo.group.persistent.GroupStore;
import com.vmturbo.group.persistent.PolicyStore;
import com.vmturbo.group.persistent.PolicyStore.PolicyDeleteException;

import io.grpc.stub.StreamObserver;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.google.common.collect.Lists;

@SuppressWarnings("unchecked")
@RunWith(MockitoJUnitRunner.class)
public class GroupServiceTest {
    private GrpcTestServer testServer;

    private AtomicReference<List<Long>> mockDataReference = new AtomicReference<>(Collections.emptyList());

    private SearchServiceHandler searchServiceHandler = new SearchServiceHandler(mockDataReference);

    @Mock
    private GroupStore groupStore;

    @Mock
    private PolicyStore policyStore;

    private GroupService groupService;

    @Before
    public void setUp() throws Exception {
        testServer = GrpcTestServer.withServices(searchServiceHandler);
        SearchServiceBlockingStub searchServiceRpc = SearchServiceGrpc.newBlockingStub(testServer.getChannel());
        groupService = new GroupService(groupStore, policyStore, searchServiceRpc);
    }

    @Test
    public void testCreate() throws Exception {
        final long id = 1234L;

        final GroupDTO.Group group = GroupDTO.Group.newBuilder()
                .setId(id)
                .setInfo(GroupInfo.newBuilder()
                    .setName("group-foo"))
                .build();
        final StreamObserver<GroupDTO.CreateGroupResponse> mockObserver =
                Mockito.mock(StreamObserver.class);

        given(groupStore.newUserGroup(group.getInfo())).willReturn(group);

        groupService.createGroup(group.getInfo(), mockObserver);

        verify(groupStore).newUserGroup(eq(group.getInfo()));
        verify(mockObserver).onNext(GroupDTO.CreateGroupResponse.newBuilder()
                .setGroup(group)
                .build());
        verify(mockObserver).onCompleted();
    }

    @Test
    public void testCreateGroupFail() throws Exception {
        final long id = 1234L;
        final GroupDTO.Group group = GroupDTO.Group.newBuilder()
                .setId(id)
                .setInfo(GroupInfo.newBuilder()
                    .setName("group-foo"))
                .build();
        final StreamObserver<GroupDTO.CreateGroupResponse> mockObserver =
                Mockito.mock(StreamObserver.class);

        given(groupStore.newUserGroup(group.getInfo())).willThrow(DatabaseException.class);

        groupService.createGroup(group.getInfo(), mockObserver);

        verify(groupStore).newUserGroup(group.getInfo());
        verify(mockObserver).onError(any(IllegalStateException.class));
    }

    @Test
    public void testDeleteEmptyReq() {
        final GroupDTO.GroupID gid = GroupDTO.GroupID.getDefaultInstance();

        final StreamObserver<GroupDTO.DeleteGroupResponse> mockObserver =
                Mockito.mock(StreamObserver.class);

        groupService.deleteGroup(gid, mockObserver);

        verify(mockObserver).onError(any(IllegalArgumentException.class));
        verify(mockObserver, never()).onCompleted();
    }

    @Test
    public void testDelete() throws PolicyDeleteException {
        final long groupIdToDelete = 1234L;
        final long policyIdToDelete = 5678L;

        final GroupDTO.GroupID gid = GroupDTO.GroupID.newBuilder()
                .setId(groupIdToDelete)
                .build();

        final PolicyDTO.InputPolicy testPolicy = PolicyDTO.InputPolicy.newBuilder()
            .setId(policyIdToDelete)
            .setBindToGroup(BindToGroupPolicy.newBuilder()
                .setConsumerGroup(InputGroup.newBuilder()
                    .setGroupId(groupIdToDelete))
                .setProviderGroup(InputGroup.newBuilder()
                    .setGroupId(2345L)))
            .build();

        final StreamObserver<GroupDTO.DeleteGroupResponse> mockObserver =
                Mockito.mock(StreamObserver.class);

        given(policyStore.getAll()).willReturn(Lists.newArrayList(testPolicy));

        groupService.deleteGroup(gid, mockObserver);

        verify(policyStore).deletePolicies(Lists.newArrayList(policyIdToDelete));
        verify(mockObserver).onNext(
                GroupDTO.DeleteGroupResponse.newBuilder().setDeleted(true).build());
        verify(mockObserver).onCompleted();
        verify(mockObserver, never()).onError(any());
    }

    @Test
    public void testDeleteGroupFail() throws Exception {
        final long idToDelete = 1234L;
        final GroupDTO.GroupID gid = GroupDTO.GroupID.newBuilder()
                .setId(idToDelete)
                .build();


        final StreamObserver<GroupDTO.DeleteGroupResponse> mockObserver =
                Mockito.mock(StreamObserver.class);
        given(policyStore.getAll()).willReturn(new ArrayList<>());
        doThrow(DatabaseException.class).when(groupStore).deleteUserGroup(eq(idToDelete));

        groupService.deleteGroup(gid, mockObserver);

        verify(groupStore).deleteUserGroup(idToDelete);
        verify(mockObserver).onError(any(IllegalStateException.class));
    }

    @Test
    public void testGetEmptyReq() {
        final GroupDTO.GroupID gid = GroupDTO.GroupID.getDefaultInstance();

        final StreamObserver<GroupDTO.GetGroupResponse> mockObserver =
                Mockito.mock(StreamObserver.class);

        groupService.getGroup(gid, mockObserver);

        verify(mockObserver).onError(any(IllegalArgumentException.class));
        verify(mockObserver, never()).onCompleted();
    }

    @Test
    public void testGet() throws Exception {
        final long id = 1234L;

        final GroupDTO.GroupID gid = GroupDTO.GroupID.newBuilder()
                .setId(id)
                .build();
        final StreamObserver<GroupDTO.GetGroupResponse> mockObserver =
                Mockito.mock(StreamObserver.class);

        final GroupDTO.Group group = GroupDTO.Group.newBuilder()
                        .setId(id)
                        .build();
        given(groupStore.get(id)).willReturn(Optional.of(group));

        groupService.getGroup(gid, mockObserver);

        verify(groupStore).get(id);
        verify(mockObserver).onNext(GroupDTO.GetGroupResponse.newBuilder().setGroup(group).build());
        verify(mockObserver).onCompleted();
    }

    @Test
    public void testGetAll() throws Exception {
        final long id1 = 1234L;
        final long id2 = 5678L;

        final GroupDTO.GetGroupsRequest getGroupsRequest =
                GroupDTO.GetGroupsRequest.getDefaultInstance();
        final StreamObserver<GroupDTO.Group> mockObserver =
                Mockito.mock(StreamObserver.class);

        final GroupDTO.Group g1 = GroupDTO.Group.newBuilder()
                        .setId(id1)
                        .build();

        final GroupDTO.Group g2 = GroupDTO.Group.newBuilder()
                        .setId(id2)
                        .build();

        given(groupStore.getAll()).willReturn(Arrays.asList(g1, g2));

        groupService.getGroups(getGroupsRequest, mockObserver);

        verify(groupStore).getAll();
        verify(mockObserver).onNext(g1);
        verify(mockObserver).onNext(g2);
        verify(mockObserver).onCompleted();
    }

    @Test
    public void testUpdateWithoutID() {
        final StreamObserver<GroupDTO.UpdateGroupResponse> mockObserver =
                Mockito.mock(StreamObserver.class);

        groupService.updateGroup(UpdateGroupRequest.getDefaultInstance(), mockObserver);

        verify(mockObserver).onError(any(IllegalArgumentException.class));
        verify(mockObserver, never()).onCompleted();
    }

    @Test
    public void testUpdateWithoutNewInfo() {
        final StreamObserver<GroupDTO.UpdateGroupResponse> mockObserver =
                Mockito.mock(StreamObserver.class);

        groupService.updateGroup(UpdateGroupRequest.newBuilder()
                .setId(1)
                .build(), mockObserver);

        verify(mockObserver).onError(any(IllegalArgumentException.class));
        verify(mockObserver, never()).onCompleted();
    }

    @Test
    public void testUpdate() throws Exception {
        final long id = 1234L;

        final GroupDTO.Group group = GroupDTO.Group.newBuilder()
                .setId(id)
                .setInfo(GroupInfo.newBuilder()
                        .setName("new"))
                .build();
        final StreamObserver<GroupDTO.UpdateGroupResponse> mockObserver =
                Mockito.mock(StreamObserver.class);

        given(groupStore.updateUserGroup(eq(id), eq(group.getInfo()))).willReturn(group);

        groupService.updateGroup(UpdateGroupRequest.newBuilder()
            .setId(id)
            .setNewInfo(group.getInfo())
            .build(), mockObserver);

        verify(groupStore).updateUserGroup(eq(id), eq(group.getInfo()));
        verify(mockObserver).onNext(GroupDTO.UpdateGroupResponse.newBuilder()
                .setUpdatedGroup(group)
                .build());
        verify(mockObserver).onCompleted();
    }

    @Test
    public void testUpdateGroupFail() throws Exception {
        final long id = 1234L;

        final GroupDTO.Group group = GroupDTO.Group.newBuilder()
                .setId(id)
                .build();
        final GroupInfo newInfo = GroupInfo.newBuilder().setName("new").build();
        final StreamObserver<GroupDTO.UpdateGroupResponse> mockObserver =
                Mockito.mock(StreamObserver.class);

        given(groupStore.updateUserGroup(eq(id), eq(newInfo))).willThrow(DatabaseException.class);

        groupService.updateGroup(UpdateGroupRequest.newBuilder()
                .setId(id)
                .setNewInfo(newInfo)
                .build(), mockObserver);

        verify(groupStore).updateUserGroup(id, newInfo);
        verify(mockObserver).onError(any(IllegalStateException.class));
    }

    @Test
    public void testGetMembersMissingGroupId() {
        final GroupDTO.GetMembersRequest missingGroupIdReq = GroupDTO.GetMembersRequest.getDefaultInstance();

        final StreamObserver<GroupDTO.GetMembersResponse> mockObserver =
                (StreamObserver<GroupDTO.GetMembersResponse>) Mockito.mock(StreamObserver.class);

        groupService.getMembers(missingGroupIdReq, mockObserver);

        verify(mockObserver, never()).onNext(any(GroupDTO.GetMembersResponse.class));
        verify(mockObserver).onError(any(IllegalArgumentException.class));
    }

    @Test
    public void testDynamicGetMembers() throws Exception {
        final long groupId = 1234L;
        final List<Long> mockSearchResults = Arrays.asList(1L, 2L);

        final GroupDTO.GetMembersRequest req = GroupDTO.GetMembersRequest.newBuilder()
                .setId(groupId)
                .build();

        final GroupDTO.Group group1234 = GroupDTO.Group.newBuilder()
                .setId(groupId)
                .setInfo(GroupInfo.newBuilder()
                    .setSearchParametersCollection(SearchParametersCollection.newBuilder()
                            .addSearchParameters(SearchParameters.getDefaultInstance())))
                .build();

        final StreamObserver<GroupDTO.GetMembersResponse> mockObserver =
                (StreamObserver<GroupDTO.GetMembersResponse>) Mockito.mock(StreamObserver.class);

        given(groupStore.get(groupId)).willReturn(Optional.of(group1234));
        givenSearchHanderWillReturn(mockSearchResults);

        groupService.getMembers(req, mockObserver);

        final GroupDTO.GetMembersResponse expectedResponse = GroupDTO.GetMembersResponse.newBuilder()
                .addAllMemberId(mockSearchResults)
                .build();

        verify(mockObserver, never()).onError(any(Exception.class));
        verify(mockObserver).onNext(expectedResponse);
        verify(mockObserver).onCompleted();
    }

    @Test
    public void testStaticGetMembers() throws Exception {
        final long groupId = 1234L;
        final List<Long> staticGroupMembers = Arrays.asList(1L, 2L);

        final GroupDTO.GetMembersRequest req = GroupDTO.GetMembersRequest.newBuilder()
                .setId(groupId)
                .build();

        final GroupDTO.Group group1234 = GroupDTO.Group.newBuilder()
                .setId(groupId)
                .setInfo(GroupInfo.newBuilder()
                    .setStaticGroupMembers(GroupDTO.StaticGroupMembers.newBuilder()
                            .addAllStaticMemberOids(staticGroupMembers)))
                .build();

        final StreamObserver<GroupDTO.GetMembersResponse> mockObserver =
                (StreamObserver<GroupDTO.GetMembersResponse>) Mockito.mock(StreamObserver.class);

        given(groupStore.get(groupId)).willReturn(Optional.of(group1234));

        groupService.getMembers(req, mockObserver);

        final GroupDTO.GetMembersResponse expectedResponse = GroupDTO.GetMembersResponse.newBuilder()
                .addAllMemberId(staticGroupMembers)
                .build();

        verify(mockObserver, never()).onError(any(Exception.class));
        verify(mockObserver).onNext(expectedResponse);
        verify(mockObserver).onCompleted();
    }

    @Test
    public void testMissingGroup() throws Exception {
        final long groupId = 1234L;

        final GroupDTO.GetMembersRequest req = GroupDTO.GetMembersRequest.newBuilder()
                .setId(groupId)
                .build();

        final StreamObserver<GroupDTO.GetMembersResponse> mockObserver =
                (StreamObserver<GroupDTO.GetMembersResponse>) Mockito.mock(StreamObserver.class);

        given(groupStore.get(groupId)).willReturn(Optional.empty());

        groupService.getMembers(req, mockObserver);

        verify(mockObserver).onError(any(IllegalArgumentException.class));
        verify(mockObserver, never()).onNext(any(GroupDTO.GetMembersResponse.class));
        verify(mockObserver, never()).onCompleted();
    }

    private void givenSearchHanderWillReturn(final List<Long> oids) {
        mockDataReference.set(oids);
    }

    class SearchServiceHandler extends SearchServiceGrpc.SearchServiceImplBase {

        private final AtomicReference<List<Long>> mockDataReference;

        public SearchServiceHandler(final AtomicReference<List<Long>> mockDataReference) {
            this.mockDataReference = mockDataReference;
        }

        @Override
        public void searchEntityOids(final Search.SearchRequest request,
                                     final StreamObserver<Search.SearchResponse> responseObserver) {
            final List<Long> mockData = mockDataReference.get();

            final Search.SearchResponse searchResp = Search.SearchResponse.newBuilder()
                    .addAllEntities(mockData)
                    .build();

            responseObserver.onNext(searchResp);
            responseObserver.onCompleted();
        }
    }
}