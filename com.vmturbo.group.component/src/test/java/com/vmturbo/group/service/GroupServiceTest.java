package com.vmturbo.group.service;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.collect.Lists;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import io.grpc.Status.Code;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.CreateTempGroupRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.CreateTempGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse.Members;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.Group.Type;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupID;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.SearchParametersCollection;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticGroupMembers;
import com.vmturbo.common.protobuf.group.GroupDTO.TempGroupInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.UpdateClusterHeadroomTemplateRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.UpdateClusterHeadroomTemplateResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.UpdateGroupRequest;
import com.vmturbo.common.protobuf.group.PolicyDTO;
import com.vmturbo.common.protobuf.group.PolicyDTO.InputPolicy.BindToGroupPolicy;
import com.vmturbo.common.protobuf.search.Search;
import com.vmturbo.common.protobuf.search.Search.ClusterMembershipFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.StringFilter;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.components.api.test.GrpcExceptionMatcher;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.group.persistent.DatabaseException;
import com.vmturbo.group.persistent.GroupStore;
import com.vmturbo.group.persistent.PolicyStore;
import com.vmturbo.group.persistent.PolicyStore.PolicyDeleteException;
import com.vmturbo.group.persistent.TemporaryGroupCache;
import com.vmturbo.group.persistent.TemporaryGroupCache.InvalidTempGroupException;

@SuppressWarnings("unchecked")
public class GroupServiceTest {

    private AtomicReference<List<Long>> mockDataReference = new AtomicReference<>(Collections.emptyList());

    private SearchServiceHandler searchServiceHandler = new SearchServiceHandler(mockDataReference);

    private GroupStore groupStore = mock(GroupStore.class);

    private PolicyStore policyStore = mock(PolicyStore.class);

    private TemporaryGroupCache temporaryGroupCache = mock(TemporaryGroupCache.class);

    private GroupService groupService;

    @Rule
    public GrpcTestServer testServer = GrpcTestServer.newServer(searchServiceHandler);

    @Before
    public void setUp() throws Exception {
        SearchServiceBlockingStub searchServiceRpc = SearchServiceGrpc.newBlockingStub(testServer.getChannel());
        groupService = new GroupService(groupStore, policyStore, temporaryGroupCache, searchServiceRpc);
        when(temporaryGroupCache.get(anyLong())).thenReturn(Optional.empty());
        when(temporaryGroupCache.delete(anyLong())).thenReturn(Optional.empty());
    }

    @Test
    public void testCreate() throws Exception {
        final long id = 1234L;

        final GroupDTO.Group group = GroupDTO.Group.newBuilder()
                .setId(id)
                .setGroup(GroupInfo.newBuilder()
                    .setName("group-foo"))
                .build();
        final StreamObserver<GroupDTO.CreateGroupResponse> mockObserver =
                mock(StreamObserver.class);

        given(groupStore.newUserGroup(group.getGroup())).willReturn(group);

        groupService.createGroup(group.getGroup(), mockObserver);

        verify(groupStore).newUserGroup(eq(group.getGroup()));
        verify(mockObserver).onNext(GroupDTO.CreateGroupResponse.newBuilder()
                .setGroup(group)
                .build());
        verify(mockObserver).onCompleted();
    }

    @Test
    public void testCreateTempGroup() throws Exception {
        final TempGroupInfo tempGroupInfo = TempGroupInfo.newBuilder()
                .setName("foo")
                .build();
        when(temporaryGroupCache.create(tempGroupInfo)).thenReturn(Group.getDefaultInstance());

        final StreamObserver<GroupDTO.CreateTempGroupResponse> mockObserver =
                mock(StreamObserver.class);

        groupService.createTempGroup(CreateTempGroupRequest.newBuilder()
                .setGroupInfo(tempGroupInfo)
                .build(), mockObserver);

        verify(temporaryGroupCache).create(tempGroupInfo);
        verify(mockObserver).onNext(CreateTempGroupResponse.newBuilder()
                .setGroup(Group.getDefaultInstance())
                .build());
        verify(mockObserver).onCompleted();
    }

    @Test
    public void testCreateTempGroupNoInfo() throws Exception {
        final StreamObserver<GroupDTO.CreateTempGroupResponse> mockObserver =
                mock(StreamObserver.class);

        groupService.createTempGroup(CreateTempGroupRequest.newBuilder()
                // No group info set.
                .build(), mockObserver);

        final ArgumentCaptor<StatusException> exceptionCaptor =
                ArgumentCaptor.forClass(StatusException.class);
        verify(mockObserver).onError(exceptionCaptor.capture());

        final StatusException exception = exceptionCaptor.getValue();
        assertThat(exception, GrpcExceptionMatcher.hasCode(Code.INVALID_ARGUMENT)
                .descriptionContains("No group info"));
    }

    @Test
    public void testCreateTempGroupException() throws Exception {
        final StreamObserver<GroupDTO.CreateTempGroupResponse> mockObserver =
                mock(StreamObserver.class);

        when(temporaryGroupCache.create(TempGroupInfo.getDefaultInstance()))
            .thenThrow(new InvalidTempGroupException(Collections.singletonList("ERROR")));
        groupService.createTempGroup(CreateTempGroupRequest.newBuilder()
                .setGroupInfo(TempGroupInfo.getDefaultInstance())
                .build(), mockObserver);

        final ArgumentCaptor<StatusException> exceptionCaptor =
                ArgumentCaptor.forClass(StatusException.class);
        verify(mockObserver).onError(exceptionCaptor.capture());

        final StatusException exception = exceptionCaptor.getValue();
        assertThat(exception, GrpcExceptionMatcher.hasCode(Code.INVALID_ARGUMENT)
                .descriptionContains("ERROR"));
    }

    @Test
    public void testCreateGroupFail() throws Exception {
        final long id = 1234L;
        final GroupDTO.Group group = GroupDTO.Group.newBuilder()
                .setId(id)
                .setGroup(GroupInfo.newBuilder()
                    .setName("group-foo"))
                .build();
        final StreamObserver<GroupDTO.CreateGroupResponse> mockObserver =
                mock(StreamObserver.class);

        given(groupStore.newUserGroup(group.getGroup())).willThrow(DatabaseException.class);

        groupService.createGroup(group.getGroup(), mockObserver);

        verify(groupStore).newUserGroup(group.getGroup());
        verify(mockObserver).onError(any(IllegalStateException.class));
    }

    @Test
    public void testDeleteEmptyReq() {
        final GroupDTO.GroupID gid = GroupDTO.GroupID.getDefaultInstance();

        final StreamObserver<GroupDTO.DeleteGroupResponse> mockObserver =
                mock(StreamObserver.class);

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
                .setConsumerGroup(groupIdToDelete)
                .setProviderGroup(2345L))
            .build();

        final StreamObserver<GroupDTO.DeleteGroupResponse> mockObserver =
                mock(StreamObserver.class);

        given(policyStore.getAll()).willReturn(Lists.newArrayList(testPolicy));

        groupService.deleteGroup(gid, mockObserver);

        verify(temporaryGroupCache).delete(groupIdToDelete);
        verify(policyStore).deletePolicies(Lists.newArrayList(policyIdToDelete));
        verify(mockObserver).onNext(
                GroupDTO.DeleteGroupResponse.newBuilder().setDeleted(true).build());
        verify(mockObserver).onCompleted();
        verify(mockObserver, never()).onError(any());
    }

    @Test
    public void testDeleteTempGroup() throws Exception {
        final long id = 7;
        when(temporaryGroupCache.delete(id)).thenReturn(Optional.of(Group.getDefaultInstance()));

        final StreamObserver<GroupDTO.DeleteGroupResponse> mockObserver =
                mock(StreamObserver.class);
        groupService.deleteGroup(GroupID.newBuilder().setId(id).build(), mockObserver);

        verify(temporaryGroupCache).delete(id);
        verify(groupStore, never()).deleteUserGroup(anyLong());
        verify(policyStore, never()).deletePolicies(any());

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
                mock(StreamObserver.class);
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
                mock(StreamObserver.class);

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
                mock(StreamObserver.class);

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
    public void testGetTempGroup() throws Exception {
        final long id = 1234L;

        final StreamObserver<GroupDTO.GetGroupResponse> mockObserver =
                mock(StreamObserver.class);

        given(temporaryGroupCache.get(id)).willReturn(Optional.of(Group.getDefaultInstance()));

        groupService.getGroup(GroupID.newBuilder().setId(id).build(), mockObserver);

        verify(temporaryGroupCache).get(id);
        verify(groupStore, never()).get(anyLong());
        verify(mockObserver).onNext(GroupDTO.GetGroupResponse.newBuilder()
                .setGroup(Group.getDefaultInstance())
                .build());
        verify(mockObserver).onCompleted();
    }

    @Test
    public void testGetAll() throws Exception {
        final long id1 = 1234L;
        final long id2 = 5678L;

        final GroupDTO.GetGroupsRequest getGroupsRequest =
                GroupDTO.GetGroupsRequest.getDefaultInstance();
        final StreamObserver<GroupDTO.Group> mockObserver =
                mock(StreamObserver.class);

        final GroupDTO.Group g1 = GroupDTO.Group.newBuilder()
                        .setId(id1)
                        .build();

        final GroupDTO.Group g2 = GroupDTO.Group.newBuilder()
                        .setId(id2)
                        .build();

        given(groupStore.getAll()).willReturn(Arrays.asList(g1, g2));

        groupService.getGroups(getGroupsRequest, mockObserver);

        verify(groupStore).getAll();
        verify(temporaryGroupCache, never()).getAll();
        verify(mockObserver).onNext(g1);
        verify(mockObserver).onNext(g2);
        verify(mockObserver).onCompleted();
    }

    @Test
    public void testGetAllTempGroups() throws Exception {
        final Group group = Group.newBuilder()
                .setType(Type.TEMP_GROUP)
                .build();

        when(temporaryGroupCache.getAll())
            .thenReturn(Collections.singletonList(group));
        final StreamObserver<GroupDTO.Group> mockObserver =
                mock(StreamObserver.class);

        groupService.getGroups(GetGroupsRequest.newBuilder()
                .setTypeFilter(Type.TEMP_GROUP)
                .build(), mockObserver);

        verify(temporaryGroupCache).getAll();
        verify(groupStore, never()).getAll();
        verify(mockObserver).onNext(group);
        verify(mockObserver).onCompleted();
    }

    @Test
    public void testUpdateWithoutID() {
        final StreamObserver<GroupDTO.UpdateGroupResponse> mockObserver =
                mock(StreamObserver.class);

        groupService.updateGroup(UpdateGroupRequest.getDefaultInstance(), mockObserver);

        verify(mockObserver).onError(any(IllegalArgumentException.class));
        verify(mockObserver, never()).onCompleted();
    }

    @Test
    public void testUpdateWithoutNewInfo() {
        final StreamObserver<GroupDTO.UpdateGroupResponse> mockObserver =
                mock(StreamObserver.class);

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
                .setGroup(GroupInfo.newBuilder()
                        .setName("new"))
                .build();
        final StreamObserver<GroupDTO.UpdateGroupResponse> mockObserver =
                mock(StreamObserver.class);

        given(groupStore.updateUserGroup(eq(id), eq(group.getGroup()))).willReturn(group);

        groupService.updateGroup(UpdateGroupRequest.newBuilder()
            .setId(id)
            .setNewInfo(group.getGroup())
            .build(), mockObserver);

        verify(groupStore).updateUserGroup(eq(id), eq(group.getGroup()));
        verify(mockObserver).onNext(GroupDTO.UpdateGroupResponse.newBuilder()
                .setUpdatedGroup(group)
                .build());
        verify(mockObserver).onCompleted();
    }

    @Test
    public void testUpdateGroupFail() throws Exception {
        final long id = 1234L;

        final GroupInfo newInfo = GroupInfo.newBuilder().setName("new").build();
        final StreamObserver<GroupDTO.UpdateGroupResponse> mockObserver =
                mock(StreamObserver.class);

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
                (StreamObserver<GroupDTO.GetMembersResponse>) mock(StreamObserver.class);

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
                .setGroup(GroupInfo.newBuilder()
                    .setSearchParametersCollection(SearchParametersCollection.newBuilder()
                            .addSearchParameters(SearchParameters.getDefaultInstance())))
                .build();

        final StreamObserver<GroupDTO.GetMembersResponse> mockObserver =
                (StreamObserver<GroupDTO.GetMembersResponse>) mock(StreamObserver.class);

        given(groupStore.get(groupId)).willReturn(Optional.of(group1234));
        givenSearchHanderWillReturn(mockSearchResults);

        groupService.getMembers(req, mockObserver);

        final GroupDTO.GetMembersResponse expectedResponse = GetMembersResponse.newBuilder()
                .setMembers(Members.newBuilder().addAllIds(mockSearchResults))
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
                .setGroup(GroupInfo.newBuilder()
                    .setStaticGroupMembers(GroupDTO.StaticGroupMembers.newBuilder()
                            .addAllStaticMemberOids(staticGroupMembers)))
                .build();

        final StreamObserver<GroupDTO.GetMembersResponse> mockObserver =
                (StreamObserver<GroupDTO.GetMembersResponse>) mock(StreamObserver.class);

        given(groupStore.get(groupId)).willReturn(Optional.of(group1234));

        groupService.getMembers(req, mockObserver);

        final GroupDTO.GetMembersResponse expectedResponse = GetMembersResponse.newBuilder()
                .setMembers(Members.newBuilder().addAllIds(staticGroupMembers))
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
                (StreamObserver<GroupDTO.GetMembersResponse>) mock(StreamObserver.class);

        given(groupStore.get(groupId)).willReturn(Optional.empty());

        groupService.getMembers(req, mockObserver);

        verify(mockObserver).onError(any(IllegalArgumentException.class));
        verify(mockObserver, never()).onNext(any(GroupDTO.GetMembersResponse.class));
        verify(mockObserver, never()).onCompleted();
    }


    @Test
    public void testGetGroupWithClusterFilterResolution() throws Exception {
        final GroupDTO.Group cluster1 = GroupDTO.Group.newBuilder()
                .setType(Type.CLUSTER)
                .setId(1)
                .setCluster(ClusterInfo.newBuilder()
                        .setName("Cluster1")
                        .setMembers(StaticGroupMembers.newBuilder()
                                .addStaticMemberOids(1)
                                .addStaticMemberOids(2)))
                .build();

        // create a dynamic group based on Cluster1
        final GroupDTO.Group clusterGroup = GroupDTO.Group.newBuilder()
                .setType(Type.GROUP)
                .setId(2)
                .setGroup(GroupInfo.newBuilder()
                        .setName("clusterGroup")
                        .setSearchParametersCollection(SearchParametersCollection.newBuilder()
                                .addSearchParameters(SearchParameters.newBuilder()
                                    .addSearchFilter(SearchFilter.newBuilder()
                                        .setClusterMembershipFilter(ClusterMembershipFilter.newBuilder()
                                            .setClusterSpecifier(PropertyFilter.newBuilder()
                                                .setPropertyName("displayName")
                                                .setStringFilter(StringFilter.newBuilder()
                                                    .setStringPropertyRegex("Cluster1"))))))))
                .build();
        given(groupStore.getAll()).willReturn(Arrays.asList(cluster1,clusterGroup));

        final StreamObserver<GroupDTO.Group> mockObserver =
                mock(StreamObserver.class);

        ArgumentCaptor<Group> groupCaptor = ArgumentCaptor.forClass(Group.class);

        groupService.getGroups(GetGroupsRequest.newBuilder()
                .setResolveClusterSearchFilters(true)
                .addId(2)
                .build(), mockObserver);

        verify(mockObserver).onNext(groupCaptor.capture());
        // there should be one string filter in the group now
        GroupInfo info = groupCaptor.getValue().getGroup();
        StringFilter stringFilter = info.getSearchParametersCollection()
                .getSearchParameters(0).getSearchFilter(0)
                .getPropertyFilter().getStringFilter();
        assertEquals("/^1$|^2$/", stringFilter.getStringPropertyRegex());
    }

    @Test
    public void testUpdateClusterHeadroomTemplate() throws Exception {
        long groupId = 1111L;
        long templateId = 2222L;

        given(groupStore.updateClusterHeadroomTemplate(groupId, templateId))
                .willReturn(Group.getDefaultInstance());

        UpdateClusterHeadroomTemplateRequest request = UpdateClusterHeadroomTemplateRequest.newBuilder()
                .setGroupId(groupId)
                .setClusterHeadroomTemplateId(templateId)
                .build();
        StreamObserver<UpdateClusterHeadroomTemplateResponse> observer =
                (StreamObserver<UpdateClusterHeadroomTemplateResponse>)mock(StreamObserver.class);
        groupService.updateClusterHeadroomTemplate(request, observer);

        verify(observer).onNext(any(UpdateClusterHeadroomTemplateResponse.class));
        verify(observer).onCompleted();
    }

    private void givenSearchHanderWillReturn(final List<Long> oids) {
        mockDataReference.set(oids);
    }

    class SearchServiceHandler extends SearchServiceGrpc.SearchServiceImplBase {

        private final AtomicReference<List<Long>> mockDataReference;

        SearchServiceHandler(final AtomicReference<List<Long>> mockDataReference) {
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