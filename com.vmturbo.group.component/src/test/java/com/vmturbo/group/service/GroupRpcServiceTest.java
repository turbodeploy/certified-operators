package com.vmturbo.group.service;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import com.google.common.collect.ImmutableSet;

import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;

import com.vmturbo.auth.api.authorization.AuthorizationException.UserAccessScopeException;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.auth.api.authorization.scoping.EntityAccessScope;
import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.CreateTempGroupRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.CreateTempGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredGroupsPoliciesSettings;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse.Members;
import com.vmturbo.common.protobuf.group.GroupDTO.GetTagsResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.Group.Type;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupID;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupPropertyFilterList;
import com.vmturbo.common.protobuf.group.GroupDTO.SearchParametersCollection;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticGroupMembers;
import com.vmturbo.common.protobuf.group.GroupDTO.StoreDiscoveredGroupsPoliciesSettingsResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.TempGroupInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.UpdateClusterHeadroomTemplateRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.UpdateClusterHeadroomTemplateResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.UpdateGroupRequest;
import com.vmturbo.common.protobuf.search.Search;
import com.vmturbo.common.protobuf.search.Search.ClusterMembershipFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.MapFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.StringFilter;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.common.protobuf.tag.Tag.TagValuesDTO;
import com.vmturbo.common.protobuf.tag.Tag.Tags;
import com.vmturbo.components.api.test.GrpcExceptionMatcher;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.health.HealthStatus;
import com.vmturbo.components.common.identity.ArrayOidSet;
import com.vmturbo.components.common.identity.OidSet;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.group.common.ImmutableUpdateException.ImmutableGroupUpdateException;
import com.vmturbo.group.common.ItemNotFoundException.GroupNotFoundException;
import com.vmturbo.group.group.EntityToClusterMapping;
import com.vmturbo.group.group.GroupStore;
import com.vmturbo.group.group.TemporaryGroupCache;
import com.vmturbo.group.group.TemporaryGroupCache.InvalidTempGroupException;
import com.vmturbo.group.policy.PolicyStore;
import com.vmturbo.group.policy.PolicyStore.PolicyDeleteException;
import com.vmturbo.group.setting.SettingStore;
import com.vmturbo.sql.utils.TestSQLDatabaseConfig;

@SuppressWarnings("unchecked")
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(
        loader = AnnotationConfigContextLoader.class,
        classes = {TestSQLDatabaseConfig.class}
)
@TestPropertySource(properties = {"originalSchemaName=group_component"})
public class GroupRpcServiceTest {

    private AtomicReference<List<Long>> mockDataReference = new AtomicReference<>(Collections.emptyList());

    private SearchServiceHandler searchServiceHandler = new SearchServiceHandler(mockDataReference);

    private GroupStore groupStore = mock(GroupStore.class);

    private TemporaryGroupCache temporaryGroupCache = mock(TemporaryGroupCache.class);

    private GroupRpcService groupRpcService;

    @Autowired
    private TestSQLDatabaseConfig dbConfig;

    private PolicyStore policyStore = mock(PolicyStore.class);

    private SettingStore settingStore = mock(SettingStore.class);

    private EntityToClusterMapping entityToClusterMapping = mock(EntityToClusterMapping.class);

    private UserSessionContext userSessionContext = mock(UserSessionContext.class);

    @Rule
    public GrpcTestServer testServer = GrpcTestServer.newServer(searchServiceHandler);

    private final Group group1 = Group.newBuilder()
            .setId(1L)
            .setGroup(GroupInfo.newBuilder()
                    .setTags(Tags.newBuilder()
                            .putTags("key", TagValuesDTO.newBuilder()
                                    .addValues("value1")
                                    .addValues("value2")
                                    .build())))
            .build();
    private final Group group2 = Group.newBuilder()
            .setId(2L)
            .setGroup(GroupInfo.newBuilder()
                    .setTags(Tags.newBuilder()
                            .putTags("key", TagValuesDTO.newBuilder()
                                    .addValues("value3")
                                    .build())
                            .putTags("otherkey", TagValuesDTO.newBuilder()
                                    .addValues("value1")
                                    .addValues("value3")
                                    .build())))
            .build();

    @Before
    public void setUp() throws Exception {
        SearchServiceBlockingStub searchServiceRpc = SearchServiceGrpc.newBlockingStub(testServer.getChannel());
        groupRpcService = new GroupRpcService(groupStore, temporaryGroupCache,
                searchServiceRpc, entityToClusterMapping,
                dbConfig.dsl(), policyStore, settingStore, userSessionContext);
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

        groupRpcService.createGroup(group.getGroup(), mockObserver);

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

        groupRpcService.createTempGroup(CreateTempGroupRequest.newBuilder()
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

        groupRpcService.createTempGroup(CreateTempGroupRequest.newBuilder()
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
        groupRpcService.createTempGroup(CreateTempGroupRequest.newBuilder()
                .setGroupInfo(TempGroupInfo.getDefaultInstance())
                .build(), mockObserver);

        final ArgumentCaptor<StatusException> exceptionCaptor =
                ArgumentCaptor.forClass(StatusException.class);
        verify(mockObserver).onError(exceptionCaptor.capture());

        final StatusException exception = exceptionCaptor.getValue();
        assertThat(exception, GrpcExceptionMatcher.hasCode(Code.INVALID_ARGUMENT)
                .descriptionContains("ERROR"));
    }

    @Test(expected = UserAccessScopeException.class)
    public void testCreateTempGroupOutOfScope() throws Exception {
        // a user with access only to entity 1 should not be able to create a group containing other entities.
        when(userSessionContext.isUserScoped()).thenReturn(true);
        EntityAccessScope scope = new EntityAccessScope(null, null,
                new ArrayOidSet(Arrays.asList(1L)), null);
        when(userSessionContext.getUserAccessScope()).thenReturn(scope);

        final TempGroupInfo tempGroupInfo = TempGroupInfo.newBuilder()
                .setName("foo")
                .setIsGlobalScopeGroup(false)
                .setMembers(StaticGroupMembers.newBuilder()
                        .addStaticMemberOids(2L))
                .build();

        final StreamObserver<GroupDTO.CreateTempGroupResponse> mockObserver =
                mock(StreamObserver.class);

        groupRpcService.createTempGroup(CreateTempGroupRequest.newBuilder()
                .setGroupInfo(tempGroupInfo)
                .build(), mockObserver);
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

        given(groupStore.newUserGroup(group.getGroup())).willThrow(DataAccessException.class);

        groupRpcService.createGroup(group.getGroup(), mockObserver);

        verify(groupStore).newUserGroup(group.getGroup());
        verify(mockObserver).onError(any(IllegalStateException.class));
    }

    @Test(expected = UserAccessScopeException.class)
    public void testCreateGroupOutOfScope() {
        // a user with access only to entity 1 should not be able to create a group containing other enttiies.
        when(userSessionContext.isUserScoped()).thenReturn(true);
        EntityAccessScope scope = new EntityAccessScope(null, null,
                new ArrayOidSet(Arrays.asList(1L)), null);
        when(userSessionContext.getUserAccessScope()).thenReturn(scope);

        final GroupInfo groupInfo = GroupInfo.newBuilder()
                .setStaticGroupMembers(StaticGroupMembers.newBuilder()
                        .addStaticMemberOids(2L))
                .build();
        final StreamObserver<GroupDTO.CreateGroupResponse> mockObserver =
                mock(StreamObserver.class);

        groupRpcService.createGroup(groupInfo, mockObserver);
    }

    @Test
    public void testDeleteEmptyReq() {
        final GroupDTO.GroupID gid = GroupDTO.GroupID.getDefaultInstance();

        final StreamObserver<GroupDTO.DeleteGroupResponse> mockObserver =
                mock(StreamObserver.class);

        groupRpcService.deleteGroup(gid, mockObserver);

        verify(mockObserver).onError(any(IllegalArgumentException.class));
        verify(mockObserver, never()).onCompleted();
    }

    @Test
    public void testDelete() throws PolicyDeleteException, ImmutableGroupUpdateException, GroupNotFoundException {
        final long groupIdToDelete = 1234L;

        final GroupDTO.GroupID gid = GroupDTO.GroupID.newBuilder()
                .setId(groupIdToDelete)
                .build();

        final StreamObserver<GroupDTO.DeleteGroupResponse> mockObserver =
                mock(StreamObserver.class);

        groupRpcService.deleteGroup(gid, mockObserver);

        verify(temporaryGroupCache).delete(groupIdToDelete);
        verify(groupStore).deleteUserGroup(groupIdToDelete);
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
        groupRpcService.deleteGroup(GroupID.newBuilder().setId(id).build(), mockObserver);

        verify(temporaryGroupCache).delete(id);
        verify(groupStore, never()).deleteUserGroup(anyLong());

        verify(mockObserver).onNext(
                GroupDTO.DeleteGroupResponse.newBuilder().setDeleted(true).build());
        verify(mockObserver).onCompleted();
        verify(mockObserver, never()).onError(any());
    }

    @Test
    public void testDeleteGroupImmutableException() throws Exception {
        final long idToDelete = 1234L;
        final GroupDTO.GroupID gid = GroupDTO.GroupID.newBuilder()
                .setId(idToDelete)
                .build();

        final StreamObserver<GroupDTO.DeleteGroupResponse> mockObserver =
                mock(StreamObserver.class);
        final String name = "foo";
        when(groupStore.deleteUserGroup(idToDelete)).thenThrow(new ImmutableGroupUpdateException(Group.newBuilder()
                .setId(idToDelete)
                .setGroup(GroupInfo.newBuilder()
                        .setName(name))
                .build()));

        groupRpcService.deleteGroup(gid, mockObserver);

        verify(groupStore).deleteUserGroup(idToDelete);
        verify(mockObserver, never()).onCompleted();
        verify(mockObserver, never()).onNext(any());

        final ArgumentCaptor<StatusException> exceptionCaptor = ArgumentCaptor.forClass(StatusException.class);
        verify(mockObserver).onError(exceptionCaptor.capture());
        assertThat(exceptionCaptor.getValue(),
                GrpcExceptionMatcher.hasCode(Code.INVALID_ARGUMENT).descriptionContains(name));
    }

    @Test
    public void testDeleteGroupNotFoundException() throws Exception {
        final long idToDelete = 1234L;
        final GroupDTO.GroupID gid = GroupDTO.GroupID.newBuilder()
                .setId(idToDelete)
                .build();

        final StreamObserver<GroupDTO.DeleteGroupResponse> mockObserver =
                mock(StreamObserver.class);
        when(groupStore.deleteUserGroup(idToDelete)).thenThrow(new GroupNotFoundException(idToDelete));

        groupRpcService.deleteGroup(gid, mockObserver);

        verify(groupStore).deleteUserGroup(idToDelete);
        verify(mockObserver, never()).onCompleted();
        verify(mockObserver, never()).onNext(any());

        final ArgumentCaptor<StatusException> exceptionCaptor = ArgumentCaptor.forClass(StatusException.class);
        verify(mockObserver).onError(exceptionCaptor.capture());
        assertThat(exceptionCaptor.getValue(), GrpcExceptionMatcher.hasCode(Code.NOT_FOUND)
                .descriptionContains(Long.toString(idToDelete)));
    }

    @Test
    public void testDeleteGroupDataAccessException() throws Exception {
        final long idToDelete = 1234L;
        final GroupDTO.GroupID gid = GroupDTO.GroupID.newBuilder()
                .setId(idToDelete)
                .build();

        final StreamObserver<GroupDTO.DeleteGroupResponse> mockObserver =
                mock(StreamObserver.class);
        final String errorMsg = "Bad database access";
        when(groupStore.deleteUserGroup(idToDelete)).thenThrow(new DataAccessException(errorMsg));

        groupRpcService.deleteGroup(gid, mockObserver);

        verify(groupStore).deleteUserGroup(idToDelete);
        verify(mockObserver, never()).onCompleted();
        verify(mockObserver, never()).onNext(any());

        final ArgumentCaptor<StatusException> exceptionCaptor = ArgumentCaptor.forClass(StatusException.class);
        verify(mockObserver).onError(exceptionCaptor.capture());
        assertThat(exceptionCaptor.getValue(), GrpcExceptionMatcher.hasCode(Code.INTERNAL)
                .descriptionContains(errorMsg));
    }

    @Test
    public void testDeleteGroupDeletePolicyException() throws Exception {
        final long idToDelete = 1234L;
        final long associatedPolicyId = 1L;
        final GroupDTO.GroupID gid = GroupDTO.GroupID.newBuilder()
                .setId(idToDelete)
                .build();

        final StreamObserver<GroupDTO.DeleteGroupResponse> mockObserver =
                mock(StreamObserver.class);
        when(groupStore.deleteUserGroup(idToDelete))
                .thenThrow(new PolicyDeleteException(associatedPolicyId, idToDelete, null));

        groupRpcService.deleteGroup(gid, mockObserver);

        verify(groupStore).deleteUserGroup(idToDelete);
        verify(mockObserver, never()).onCompleted();
        verify(mockObserver, never()).onNext(any());

        final ArgumentCaptor<StatusException> exceptionCaptor = ArgumentCaptor.forClass(StatusException.class);
        verify(mockObserver).onError(exceptionCaptor.capture());
        assertThat(exceptionCaptor.getValue(), GrpcExceptionMatcher.hasCode(Code.INTERNAL)
                .descriptionContains(Long.toString(associatedPolicyId)));
    }

    @Test(expected = UserAccessScopeException.class)
    public void testDeleteGroupOutOfScope() {
        // a user with access only to entity 1 should not be able to delete a group containing entities
        // out of scope.
        long groupId = 1L;
        when(userSessionContext.isUserScoped()).thenReturn(true);
        EntityAccessScope scope = new EntityAccessScope(null, null,
                OidSet.EMPTY_OID_SET, null);
        when(userSessionContext.getUserAccessScope()).thenReturn(scope);
        given(groupStore.get(groupId)).willReturn(Optional.of(Group.getDefaultInstance()));

        final StreamObserver<GroupDTO.DeleteGroupResponse> mockObserver =
                mock(StreamObserver.class);

        groupRpcService.deleteGroup(GroupID.newBuilder().setId(groupId).build(), mockObserver);
    }


    @Test
    public void testGetEmptyReq() {
        final GroupDTO.GroupID gid = GroupDTO.GroupID.getDefaultInstance();

        final StreamObserver<GroupDTO.GetGroupResponse> mockObserver =
                mock(StreamObserver.class);

        groupRpcService.getGroup(gid, mockObserver);

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

        groupRpcService.getGroup(gid, mockObserver);

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

        groupRpcService.getGroup(GroupID.newBuilder().setId(id).build(), mockObserver);

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

        groupRpcService.getGroups(getGroupsRequest, mockObserver);

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

        groupRpcService.getGroups(GetGroupsRequest.newBuilder()
                .addTypeFilter(Type.TEMP_GROUP)
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

        groupRpcService.updateGroup(UpdateGroupRequest.getDefaultInstance(), mockObserver);

        verify(mockObserver).onError(any(IllegalArgumentException.class));
        verify(mockObserver, never()).onCompleted();
    }

    @Test
    public void testUpdateWithoutNewInfo() {
        final StreamObserver<GroupDTO.UpdateGroupResponse> mockObserver =
                mock(StreamObserver.class);

        groupRpcService.updateGroup(UpdateGroupRequest.newBuilder()
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

        groupRpcService.updateGroup(UpdateGroupRequest.newBuilder()
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

        given(groupStore.updateUserGroup(eq(id), eq(newInfo))).willThrow(DataAccessException.class);

        groupRpcService.updateGroup(UpdateGroupRequest.newBuilder()
                .setId(id)
                .setNewInfo(newInfo)
                .build(), mockObserver);

        verify(groupStore).updateUserGroup(id, newInfo);
        verify(mockObserver).onError(any(IllegalStateException.class));
    }

    @Test(expected = UserAccessScopeException.class)
    public void testUpdateGroupOutOfScope() throws Exception {
        // a scoped user should not be able to update a group containing entities out of their scope.
        when(userSessionContext.isUserScoped()).thenReturn(true);
        EntityAccessScope scope = new EntityAccessScope(null, null,
                new ArrayOidSet(Arrays.asList(1L)), null);
        when(userSessionContext.getUserAccessScope()).thenReturn(scope);

        final GroupInfo groupInfo = GroupInfo.newBuilder()
                .setStaticGroupMembers(StaticGroupMembers.newBuilder()
                        .addStaticMemberOids(2L))
                .build();

        long groupId = 10L;
        final GroupDTO.Group group = GroupDTO.Group.newBuilder()
                .setId(groupId)
                .setGroup(groupInfo)
                .build();

        given(groupStore.get(groupId)).willReturn(Optional.of(group));

        final StreamObserver<GroupDTO.UpdateGroupResponse> mockObserver =
                mock(StreamObserver.class);

        // should fail when the old group is inaccessible
        groupRpcService.updateGroup(UpdateGroupRequest.newBuilder()
                .setId(groupId)
                .setNewInfo(groupInfo)
                .build(), mockObserver);
    }

    @Test(expected = UserAccessScopeException.class)
    public void testUpdateGroupChangesOutOfScope() throws Exception {
        // a scoped user should not be able to modify a group so that it would contain entities out
        // of their scope.
        when(userSessionContext.isUserScoped()).thenReturn(true);
        EntityAccessScope scope = new EntityAccessScope(null, null,
                new ArrayOidSet(Arrays.asList(1L)), null);
        when(userSessionContext.getUserAccessScope()).thenReturn(scope);

        final GroupInfo existingGroupInfo = GroupInfo.newBuilder()
                .setStaticGroupMembers(StaticGroupMembers.newBuilder()
                        .addStaticMemberOids(1L))
                .build();

        long groupId = 10L;
        final GroupDTO.Group group = GroupDTO.Group.newBuilder()
                .setId(groupId)
                .setGroup(existingGroupInfo)
                .build();

        given(groupStore.get(groupId)).willReturn(Optional.of(group));

        final StreamObserver<GroupDTO.UpdateGroupResponse> mockObserver =
                mock(StreamObserver.class);

        final GroupInfo modifiedGroupInfo = GroupInfo.newBuilder()
                .setStaticGroupMembers(StaticGroupMembers.newBuilder()
                        .addStaticMemberOids(1L)
                        .addStaticMemberOids(2L))
                .build();


        // should fail because the changes are out of scope, even though the current group is accessible
        groupRpcService.updateGroup(UpdateGroupRequest.newBuilder()
                .setId(groupId)
                .setNewInfo(modifiedGroupInfo)
                .build(), mockObserver);
    }

    @Test
    public void testGetMembersMissingGroupId() {
        final GroupDTO.GetMembersRequest missingGroupIdReq = GroupDTO.GetMembersRequest.getDefaultInstance();

        final StreamObserver<GroupDTO.GetMembersResponse> mockObserver =
                (StreamObserver<GroupDTO.GetMembersResponse>) mock(StreamObserver.class);

        groupRpcService.getMembers(missingGroupIdReq, mockObserver);

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

        groupRpcService.getMembers(req, mockObserver);

        final GroupDTO.GetMembersResponse expectedResponse = GetMembersResponse.newBuilder()
                .setMembers(Members.newBuilder().addAllIds(mockSearchResults))
                .build();

        verify(mockObserver, never()).onError(any(Exception.class));
        verify(mockObserver).onNext(expectedResponse);
        verify(mockObserver).onCompleted();
    }

    /**
     * Tests various cases of tag filtering.
     *
     * @throws Exception should not happen.
     */
    @Test
    public void testGetWithTags1() throws Exception {
        when(groupStore.getAll()).thenReturn(ImmutableSet.of(group1, group2));
        final StreamObserver<Group> mockObserver = mock(StreamObserver.class);
        final ArgumentCaptor<Group> captor = ArgumentCaptor.forClass(Group.class);
        groupRpcService.getGroups(GetGroupsRequest.newBuilder()
                .setPropertyFilters(GroupPropertyFilterList.newBuilder()
                    .addPropertyFilters(PropertyFilter.newBuilder()
                        .setPropertyName(StringConstants.TAGS_ATTR)
                        .setMapFilter(
                            MapFilter.newBuilder()
                                .setKey("key")
                                .addValues("value4")
                                .addValues("value1"))))
                .build(),
            mockObserver);
        verify(mockObserver).onNext(captor.capture());
        verify(mockObserver).onCompleted();
        Assert.assertEquals(group1, captor.getValue());
    }

    @Test
    public void testGetWithTags2() throws Exception {
        when(groupStore.getAll()).thenReturn(ImmutableSet.of(group1, group2));
        final StreamObserver<Group> mockObserver = mock(StreamObserver.class);
        final ArgumentCaptor<Group> captor = ArgumentCaptor.forClass(Group.class);
        groupRpcService.getGroups(GetGroupsRequest.newBuilder()
                .setPropertyFilters(GroupPropertyFilterList.newBuilder()
                    .addPropertyFilters(PropertyFilter.newBuilder()
                        .setPropertyName(StringConstants.TAGS_ATTR)
                        .setMapFilter(
                            MapFilter.newBuilder()
                                .setKey("key")
                                .setRegex(".*2"))))
                .build(),
            mockObserver);
        verify(mockObserver).onNext(captor.capture());
        verify(mockObserver).onCompleted();
        Assert.assertEquals(group1, captor.getValue());
    }

    @Test
    public void testGetWithTags3() throws Exception {
        when(groupStore.getAll()).thenReturn(ImmutableSet.of(group1, group2));
        final StreamObserver<Group> mockObserver = mock(StreamObserver.class);
        final ArgumentCaptor<Group> captor = ArgumentCaptor.forClass(Group.class);
        groupRpcService.getGroups(GetGroupsRequest.newBuilder()
                .setPropertyFilters(GroupPropertyFilterList.newBuilder()
                    .addPropertyFilters(PropertyFilter.newBuilder()
                        .setPropertyName(StringConstants.TAGS_ATTR)
                        .setMapFilter(
                            MapFilter.newBuilder()
                                .setKey("otherkey")
                                .setRegex(".*")
                                .setPositiveMatch(false))))
                .build(),
            mockObserver);
        verify(mockObserver).onNext(captor.capture());
        verify(mockObserver).onCompleted();
        Assert.assertEquals(group1, captor.getValue());
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

        groupRpcService.getMembers(req, mockObserver);

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

        groupRpcService.getMembers(req, mockObserver);

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

        groupRpcService.getGroups(GetGroupsRequest.newBuilder()
                .setResolveClusterSearchFilters(true)
                .addId(2)
                .build(), mockObserver);

        verify(mockObserver).onNext(groupCaptor.capture());
        // there should be one string filter in the group now
        GroupInfo info = groupCaptor.getValue().getGroup();
        StringFilter stringFilter = info.getSearchParametersCollection()
                .getSearchParameters(0).getSearchFilter(0)
                .getPropertyFilter().getStringFilter();
        assertEquals("^1$|^2$", stringFilter.getStringPropertyRegex());
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
        groupRpcService.updateClusterHeadroomTemplate(request, observer);

        verify(observer).onNext(any(UpdateClusterHeadroomTemplateResponse.class));
        verify(observer).onCompleted();
    }


    @Test
    public void testNoTargetId() {
        StreamObserver<StoreDiscoveredGroupsPoliciesSettingsResponse> responseObserver =
                (StreamObserver<StoreDiscoveredGroupsPoliciesSettingsResponse>) mock(StreamObserver.class);
        StreamObserver<DiscoveredGroupsPoliciesSettings>  requestObserver =
                spy(groupRpcService.storeDiscoveredGroupsPoliciesSettings(responseObserver));
        requestObserver.onNext(DiscoveredGroupsPoliciesSettings.getDefaultInstance());
        requestObserver.onCompleted();

        verify(responseObserver, times(1)).onError(any(IllegalArgumentException.class));
    }

    @Test
    public void testUpdateClusters() {
        final HealthStatus status = mock(HealthStatus.class);
        when(status.isHealthy()).thenReturn(true);

        StreamObserver<StoreDiscoveredGroupsPoliciesSettingsResponse> responseObserver =
                (StreamObserver<StoreDiscoveredGroupsPoliciesSettingsResponse>) mock(StreamObserver.class);
        StreamObserver<DiscoveredGroupsPoliciesSettings>  requestObserver =
                spy(groupRpcService.storeDiscoveredGroupsPoliciesSettings(responseObserver));

        requestObserver.onNext(DiscoveredGroupsPoliciesSettings.newBuilder().setTargetId(10L)
                    .addDiscoveredGroup(GroupInfo.getDefaultInstance())
                    .addDiscoveredCluster(ClusterInfo.getDefaultInstance())
                    .build());
        requestObserver.onCompleted();

        verify(responseObserver).onCompleted();
        verify(responseObserver).onNext(StoreDiscoveredGroupsPoliciesSettingsResponse.getDefaultInstance());
        verify(groupStore).updateTargetGroups(isA(DSLContext.class), eq(10L),
                eq(Collections.singletonList(GroupInfo.getDefaultInstance())),
                eq(Collections.singletonList(ClusterInfo.getDefaultInstance())));

        verify(policyStore).updateTargetPolicies(isA(DSLContext.class),
                eq(10L), eq(Collections.emptyList()), anyMap());
        verify(settingStore).updateTargetSettingPolicies(isA(DSLContext.class),
                eq(10L), eq(Collections.emptyList()), anyMap());
    }

    /**
     * Test successful retrieval of tags.
     */
    @Test
    public void testGetTags() {
        final Tags tags = Tags.newBuilder().build();
        when(groupStore.getTags()).thenReturn(tags);
        final StreamObserver<GetTagsResponse> mockObserver = mock(StreamObserver.class);
        final ArgumentCaptor<GetTagsResponse> captor = ArgumentCaptor.forClass(GetTagsResponse.class);
        groupRpcService.getTags(GroupDTO.GetTagsRequest.newBuilder().build(), mockObserver);
        verify(mockObserver).onNext(captor.capture());
        verify(mockObserver).onCompleted();
        Assert.assertEquals(tags, captor.getValue().getTags());
    }

    /**
     * Test failed retrieval of tags.
     */
    @Test
    public void testGetTagsFailed() {
        final String errorMessage = "boom";
        when(groupStore.getTags()).thenThrow(new DataAccessException(errorMessage));
        final StreamObserver<GetTagsResponse> mockObserver = mock(StreamObserver.class);
        final ArgumentCaptor<Throwable> captor = ArgumentCaptor.forClass(Throwable.class);
        groupRpcService.getTags(GroupDTO.GetTagsRequest.newBuilder().build(), mockObserver);
        verify(mockObserver).onError(captor.capture());
        final Throwable throwable = captor.getValue();
        Assert.assertTrue(throwable instanceof StatusException);
        final StatusException statusException = (StatusException)throwable;
        Assert.assertEquals(Status.INTERNAL.getCode(), statusException.getStatus().getCode());
        Assert.assertEquals(Status.INTERNAL.getCode() + ": " + errorMessage, statusException.getMessage());
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
        public void searchEntityOids(final Search.SearchEntityOidsRequest request,
                                     final StreamObserver<Search.SearchEntityOidsResponse> responseObserver) {
            final List<Long> mockData = mockDataReference.get();

            final Search.SearchEntityOidsResponse searchResp = Search.SearchEntityOidsResponse.newBuilder()
                    .addAllEntities(mockData)
                    .build();

            responseObserver.onNext(searchResp);
            responseObserver.onCompleted();
        }
    }
}