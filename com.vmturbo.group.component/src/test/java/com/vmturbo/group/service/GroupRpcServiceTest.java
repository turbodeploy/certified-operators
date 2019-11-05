package com.vmturbo.group.service;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import org.hamcrest.CoreMatchers;
import org.jooq.exception.DataAccessException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;

import com.vmturbo.auth.api.authorization.AuthorizationException.UserAccessScopeException;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.auth.api.authorization.scoping.EntityAccessScope;
import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.CountGroupsResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.CreateGroupRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.CreateGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredGroupsPoliciesSettings;
import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredGroupsPoliciesSettings.UploadedGroup;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupForEntityRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupForEntityResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse.Members;
import com.vmturbo.common.protobuf.group.GroupDTO.GetTagsResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.EntityFilters;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.EntityFilters.EntityFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.GroupFilters;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.OptimizationMetadata;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupID;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.group.GroupDTO.Origin;
import com.vmturbo.common.protobuf.group.GroupDTO.Origin.User;
import com.vmturbo.common.protobuf.group.GroupDTO.SearchParametersCollection;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers.StaticMembersByType;
import com.vmturbo.common.protobuf.group.GroupDTO.StoreDiscoveredGroupsPoliciesSettingsResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.UpdateGroupRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.UpdateGroupResponse;
import com.vmturbo.common.protobuf.search.Search;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.components.api.test.GrpcExceptionMatcher;
import com.vmturbo.components.api.test.GrpcRuntimeExceptionMatcher;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.identity.ArrayOidSet;
import com.vmturbo.components.common.identity.OidSet;
import com.vmturbo.group.group.IGroupStore;
import com.vmturbo.group.group.IGroupStore.DiscoveredGroup;
import com.vmturbo.group.group.TemporaryGroupCache;
import com.vmturbo.group.group.TemporaryGroupCache.InvalidTempGroupException;
import com.vmturbo.group.service.GroupRpcService.InvalidGroupDefinitionException;
import com.vmturbo.group.stitching.GroupStitchingManager;
import com.vmturbo.group.stitching.GroupTestUtils;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.sql.utils.TestSQLDatabaseConfig;

/**
 * This class tests {@Link GroupRpcServiceTest}.
 */
public class GroupRpcServiceTest {

    private AtomicReference<List<Long>> mockDataReference = new AtomicReference<>(Collections.emptyList());

    private SearchServiceHandler searchServiceHandler = new SearchServiceHandler(mockDataReference);

    private TemporaryGroupCache temporaryGroupCache = mock(TemporaryGroupCache.class);

    private GroupRpcService groupRpcService;

    @Autowired
    private TestSQLDatabaseConfig dbConfig;

    private UserSessionContext userSessionContext = mock(UserSessionContext.class);

    private GroupStitchingManager groupStitchingManager = spy(GroupStitchingManager.class);

    private MockTransactionProvider transactionProvider;
    private IGroupStore groupStoreDAO;

    @Rule
    public GrpcTestServer testServer = GrpcTestServer.newServer(searchServiceHandler);

    private final GroupDefinition testGrouping = GroupDefinition.newBuilder()
                    .setType(GroupType.REGULAR)
                    .setDisplayName("TestGroup")
                    .setStaticGroupMembers(StaticMembers
                                    .newBuilder()
                                    .addMembersByType(StaticMembersByType
                                                    .newBuilder()
                                                    .setType(MemberType
                                                                .newBuilder()
                                                                .setEntity(2)
                                                            )
                                                    .addAllMembers(Arrays.asList(101L, 102L))
                                                    )
                                    )
                    .build();

    final Origin origin = Origin
                    .newBuilder()
                    .setUser(User
                                .newBuilder()
                                .setUsername("administrator")
                            )
                    .build();

    private static final MemberType VM_MEMBER_TYPE =
            MemberType.newBuilder().setEntity(EntityType.VIRTUAL_MACHINE_VALUE).build();

    @Captor
    private ArgumentCaptor<Collection<DiscoveredGroup>> groupsCaptor;

    @Before
    public void setUp() throws Exception {
        SearchServiceBlockingStub searchServiceRpc = SearchServiceGrpc.newBlockingStub(testServer.getChannel());
        transactionProvider = new MockTransactionProvider();
        groupStoreDAO = transactionProvider.getGroupStore();
        groupRpcService = new GroupRpcService(temporaryGroupCache,
                searchServiceRpc,
                userSessionContext,
                groupStitchingManager,
                transactionProvider);
        when(temporaryGroupCache.getGrouping(anyLong())).thenReturn(Optional.empty());
        when(temporaryGroupCache.deleteGrouping(anyLong())).thenReturn(Optional.empty());
        MockitoAnnotations.initMocks(this);
    }

    /**
     * Test {@link GroupRpcService#getGroups(GetGroupsRequest, StreamObserver)}.
     */
    @Test
    public void testGetGroups() {
        final long groupId = 1234L;
        final Grouping resultGroup = Grouping.newBuilder().setId(groupId).build();

        final GetGroupsRequest genericGroupsRequest = GetGroupsRequest.newBuilder()
                .setGroupFilter(GroupFilter.newBuilder().addId(groupId).build())
                .build();
        final StreamObserver<GroupDTO.Grouping> mockGroupingObserver =
                Mockito.mock(StreamObserver.class);
        Mockito.when(groupStoreDAO.getGroups(genericGroupsRequest.getGroupFilter()))
                .thenReturn(Collections.singletonList(resultGroup));
        groupRpcService.getGroups(genericGroupsRequest, mockGroupingObserver);
        Mockito.verify(mockGroupingObserver).onNext(resultGroup);
        Mockito.verify(mockGroupingObserver).onCompleted();
    }

    /**
     * Test {@link GroupRpcService#countGroups(GetGroupsRequest, StreamObserver)}
     * when GetGroupsRequest doesn't contain data about GroupFilter.
     */
    @Test
    public void testCountGenericGroups() {
        final long groupId = 1234L;
        final Grouping resultGroup = Grouping.newBuilder().setId(groupId).build();

        final GetGroupsRequest genericGroupsRequest = GetGroupsRequest.newBuilder()
                .setGroupFilter(GroupFilter.newBuilder().addId(groupId).build())
                .build();
        final StreamObserver<GroupDTO.CountGroupsResponse> mockCountGroupObserver =
                Mockito.mock(StreamObserver.class);
        Mockito.when(groupStoreDAO.getGroups(genericGroupsRequest.getGroupFilter()))
                .thenReturn(Collections.singletonList(resultGroup));
        groupRpcService.countGroups(genericGroupsRequest, mockCountGroupObserver);
        Mockito.verify(mockCountGroupObserver).onCompleted();
        Mockito.verify(mockCountGroupObserver)
                .onNext(CountGroupsResponse.newBuilder().setCount(1).build());
    }

    /**
     * Test {@link GroupRpcService#countGroups(GetGroupsRequest, StreamObserver)}
     * when GetGenericGroupsRequest doesn't contain data about GroupFilter.
     */
    @Test
    public void testCountGenericGroupsExceptionCase() {
        final ArgumentCaptor<StatusException> exceptionCaptor =
                ArgumentCaptor.forClass(StatusException.class);
        final long groupId = 1234L;
        final Grouping resultGroup = Grouping.newBuilder().setId(groupId).build();

        // without GroupFilter
        final GetGroupsRequest genericGroupsRequest = GetGroupsRequest
                .newBuilder()
                .build();

        final StreamObserver<GroupDTO.CountGroupsResponse> mockCountGroupObserver =
                Mockito.mock(StreamObserver.class);
        Mockito.when(groupStoreDAO.getGroups(genericGroupsRequest.getGroupFilter()))
                .thenReturn(Collections.singletonList(resultGroup));
        groupRpcService.countGroups(genericGroupsRequest, mockCountGroupObserver);
        verify(mockCountGroupObserver).onError(exceptionCaptor.capture());
        final StatusException exception = exceptionCaptor.getValue();
        assertThat(exception, GrpcExceptionMatcher.hasCode(Code.INVALID_ARGUMENT)
                .descriptionContains("No group filter is present"));
    }

    /**
     * Test {@link GroupRpcService#getGroupForEntity(GetGroupForEntityRequest, StreamObserver)}.
     */
    @Test
    public void testGetGroupForEntity() {
        final long entityId = 1234L;
        final GetGroupForEntityRequest groupForEntityRequest =
                GetGroupForEntityRequest.newBuilder().setEntityId(entityId).build();
        final StreamObserver<GroupDTO.GetGroupForEntityResponse> mockGroupForEntityObserver =
                Mockito.mock(StreamObserver.class);
        final Set<Grouping> listOfGroups =
                new HashSet<>(Collections.singletonList(Grouping.getDefaultInstance()));
        final GetGroupForEntityResponse entityResponse =
                GetGroupForEntityResponse.newBuilder().addAllGroup(listOfGroups).build();
        Mockito.when(groupStoreDAO.getStaticGroupsForEntity(entityId)).thenReturn(listOfGroups);
        groupRpcService.getGroupForEntity(groupForEntityRequest, mockGroupForEntityObserver);
        Mockito.verify(mockGroupForEntityObserver).onNext(entityResponse);
        Mockito.verify(mockGroupForEntityObserver).onCompleted();
    }

    /**
     * Test {@link GroupRpcService#getGroupForEntity(GetGroupForEntityRequest, StreamObserver)}
     * when GetGroupForEntityRequest doesn't contain entityId.
     */
    @Test
    public void testGetGroupForEntityExceptionCase() {
        final ArgumentCaptor<StatusException> exceptionCaptor =
                ArgumentCaptor.forClass(StatusException.class);
        // request without entityId
        final GetGroupForEntityRequest groupForEntityRequest =
                GetGroupForEntityRequest.getDefaultInstance();
        final StreamObserver<GroupDTO.GetGroupForEntityResponse> mockGroupForEntityObserver =
                Mockito.mock(StreamObserver.class);
        groupRpcService.getGroupForEntity(groupForEntityRequest, mockGroupForEntityObserver);
        verify(mockGroupForEntityObserver).onError(exceptionCaptor.capture());
        final StatusException exception = exceptionCaptor.getValue();
        assertThat(exception, GrpcExceptionMatcher.hasCode(Code.INVALID_ARGUMENT)
                .descriptionContains("EntityID is missing for the getGroupForEntityRequest"));
    }

    /**
     * Test case when user have access to requested entity.
     */
    @Test
    public void testGetGroupForEntityWhenUserHaveAccess() {
        final long entityId = 1234L;
        final GetGroupForEntityRequest groupForEntityRequest =
                GetGroupForEntityRequest.newBuilder().setEntityId(entityId).build();
        final StreamObserver<GroupDTO.GetGroupForEntityResponse> mockGroupForEntityObserver =
                Mockito.mock(StreamObserver.class);
        final Set<Grouping> listOfGroups =
                new HashSet<>(Collections.singletonList(Grouping.getDefaultInstance()));
        final GetGroupForEntityResponse entityResponse =
                GetGroupForEntityResponse.newBuilder().addAllGroup(listOfGroups).build();
        Mockito.when(groupStoreDAO.getStaticGroupsForEntity(entityId)).thenReturn(listOfGroups);
        final EntityAccessScope accessScope =
                new EntityAccessScope(null, null, new ArrayOidSet(Collections.singletonList(entityId)),
                        null);
        when(userSessionContext.isUserScoped()).thenReturn(true);
        when(userSessionContext.getUserAccessScope()).thenReturn(accessScope);
        groupRpcService.getGroupForEntity(groupForEntityRequest, mockGroupForEntityObserver);
        Mockito.verify(mockGroupForEntityObserver).onNext(entityResponse);
        Mockito.verify(mockGroupForEntityObserver).onCompleted();
    }

    /**
     * Test case when user have restrict access to entities.
     */
    @Test(expected = UserAccessScopeException.class)
    public void testGetGroupForEntityWhenUserRestrict() {
        final long requestedEntityId = 1L;
        final long allowedEntityId = 2L;
        final GetGroupForEntityRequest groupForEntityRequest =
                GetGroupForEntityRequest.newBuilder().setEntityId(requestedEntityId).build();
        final StreamObserver<GroupDTO.GetGroupForEntityResponse> mockGroupForEntityObserver =
                Mockito.mock(StreamObserver.class);
        final Set<Grouping> listOfGroups =
                new HashSet<>(Collections.singletonList(Grouping.getDefaultInstance()));
        final GetGroupForEntityResponse entityResponse =
                GetGroupForEntityResponse.newBuilder().addAllGroup(listOfGroups).build();
        Mockito.when(groupStoreDAO.getStaticGroupsForEntity(requestedEntityId)).thenReturn(listOfGroups);
        final EntityAccessScope accessScope =
                new EntityAccessScope(null, null, new ArrayOidSet(Collections.singletonList(allowedEntityId)),
                        null);
        when(userSessionContext.isUserScoped()).thenReturn(true);
        when(userSessionContext.getUserAccessScope()).thenReturn(accessScope);
        groupRpcService.getGroupForEntity(groupForEntityRequest, mockGroupForEntityObserver);
        Mockito.verify(mockGroupForEntityObserver).onNext(entityResponse);
        Mockito.verify(mockGroupForEntityObserver).onCompleted();
    }

    /**
     * Test {@link GroupRpcService#getGroupForEntity(GetGroupForEntityRequest, StreamObserver)}
     * when user doesn't have access to all group members and accordingly does not have access to
     * the group.
     */
    @Test(expected = UserAccessScopeException.class)
    public void testGetGroupForEntityWhenGroupAccessDenied() {
        final long groupId = 5L;
        final long requestedEntityId = 1L;
        final Collection<Long> allowedEntityId = Arrays.asList(1L, 2L, 3L);
        final Collection<Long> groupMembers = Arrays.asList(1L, 2L, 3L, 4L);
        final GetGroupForEntityRequest groupForEntityRequest =
                GetGroupForEntityRequest.newBuilder().setEntityId(requestedEntityId).build();
        final StreamObserver<GroupDTO.GetGroupForEntityResponse> mockGroupForEntityObserver =
                Mockito.mock(StreamObserver.class);
        final Set<Grouping> listOfGroups =
                new HashSet<>(Collections.singletonList(createGrouping(groupId, groupMembers)));
        final GetGroupForEntityResponse entityResponse =
                GetGroupForEntityResponse.newBuilder().addAllGroup(listOfGroups).build();
        Mockito.when(groupStoreDAO.getStaticGroupsForEntity(requestedEntityId))
                .thenReturn(listOfGroups);
        final EntityAccessScope accessScope =
                new EntityAccessScope(null, null, new ArrayOidSet(allowedEntityId), null);
        when(userSessionContext.isUserScoped()).thenReturn(true);
        when(userSessionContext.getUserAccessScope()).thenReturn(accessScope);
        groupRpcService.getGroupForEntity(groupForEntityRequest, mockGroupForEntityObserver);
        Mockito.verify(mockGroupForEntityObserver).onNext(entityResponse);
        Mockito.verify(mockGroupForEntityObserver).onCompleted();
    }

    /**
     * Test {@link GroupRpcService#storeDiscoveredGroupsPoliciesSettings(StreamObserver)} when
     * DiscoveredGroupsPoliciesSettings hasn't targetId.
     *
     * @throws StoreOperationException if groupDefinition is invalid
     */
    @Test
    public void testStoreDiscoveredGroupsPoliciesSettingsExceptionCase()
            throws StoreOperationException {
        final GroupDefinition groupDefinition = createGroupDefinition();
        final UploadedGroup uploadedGroup = UploadedGroup.newBuilder()
                .setDefinition(groupDefinition)
                .build();
        final DiscoveredGroupsPoliciesSettings discoveredGroup =
                DiscoveredGroupsPoliciesSettings.newBuilder()
                        .addUploadedGroups(uploadedGroup)
                        .build();
        final StreamObserver<GroupDTO.StoreDiscoveredGroupsPoliciesSettingsResponse>
                responseStreamObserver = Mockito.mock(StreamObserver.class);
        final ArgumentCaptor<StatusException> exceptionCaptor =
                ArgumentCaptor.forClass(StatusException.class);

        final StreamObserver<DiscoveredGroupsPoliciesSettings> requestObserver =
                groupRpcService.storeDiscoveredGroupsPoliciesSettings(responseStreamObserver);
        requestObserver.onNext(discoveredGroup);

        Mockito.verify(groupStoreDAO, Mockito.never())
                .updateDiscoveredGroups(Mockito.anyCollection());
        Mockito.verify(responseStreamObserver).onError(exceptionCaptor.capture());
        final StatusException exception = exceptionCaptor.getValue();
        Assert.assertThat(exception, GrpcExceptionMatcher.hasCode(Code.INVALID_ARGUMENT)
                .descriptionContains("Request must have a target ID"));
    }

    /**
     * Test when user request "all" groups (without certain groupIds) but have access only to
     * entities from one group. In this case will be filtered results and returned only
     * accessible ones.
     */
    @Test
    public void testGetGroupsWhenUserScoped() {
        final long firstGroupId = 1L;
        final long secondGroupId = 2L;
        final long firstGroupMember = 11L;
        final long secondGroupMember = 12L;
        final Collection<Long>  firstGroupMembers =  Collections.singletonList(firstGroupMember);
        final Collection<Long> secondGroupMembers = Collections.singletonList(secondGroupMember);

        final Grouping firstGrouping = createGrouping(firstGroupId, firstGroupMembers);
        final Grouping secondGrouping = createGrouping(secondGroupId, secondGroupMembers);
        final List<Grouping> groups = Arrays.asList(firstGrouping, secondGrouping);

        final EntityAccessScope accessScope =
                new EntityAccessScope(null, null,
                        new ArrayOidSet(Collections.singletonList(firstGroupMember)),
                        null);
        final StreamObserver<GroupDTO.Grouping> mockGroupingObserver =
                Mockito.mock(StreamObserver.class);
        final GetGroupsRequest genericGroupsRequest = GetGroupsRequest
                .newBuilder()
                .setGroupFilter(GroupFilter.getDefaultInstance())
                .build();
        when(userSessionContext.isUserScoped()).thenReturn(true);
        when(userSessionContext.getUserAccessScope()).thenReturn(accessScope);
        Mockito.when(groupStoreDAO.getGroup(firstGroupId)).thenReturn(Optional.of(firstGrouping));
        Mockito.when(groupStoreDAO.getGroup(secondGroupId)).thenReturn(Optional.of(secondGrouping));
        Mockito.when(groupStoreDAO.getGroups(genericGroupsRequest.getGroupFilter()))
                .thenReturn(groups);
        groupRpcService.getGroups(genericGroupsRequest, mockGroupingObserver);
        Mockito.verify(mockGroupingObserver).onNext(firstGrouping);
        Mockito.verify(mockGroupingObserver, Mockito.never()).onNext(secondGrouping);
        Mockito.verify(mockGroupingObserver).onCompleted();
    }

    /**
     * Test when user request groups with certain groupIds but have access only to
     * entities from one group. In this case will be throws {@link UserAccessScopeException}.
     */
    @Test(expected = UserAccessScopeException.class)
    public void testGetGroupsWhenUserScopedExpectedException() {
        final long firstGroupId = 1L;
        final long secondGroupId = 2L;
        final long firstGroupMember = 11L;
        final long secondGroupMember = 12L;
        final Collection<Long>  firstGroupMembers =  Collections.singletonList(firstGroupMember);
        final Collection<Long> secondGroupMembers = Collections.singletonList(secondGroupMember);

        final EntityAccessScope accessScope =
                new EntityAccessScope(null, null,
                        new ArrayOidSet(Collections.singletonList(firstGroupMember)),
                        null);

        final Grouping firstGrouping = createGrouping(firstGroupId, firstGroupMembers);
        final Grouping secondGrouping = createGrouping(secondGroupId, secondGroupMembers);
        final List<Grouping> groups = Arrays.asList(firstGrouping, secondGrouping);

        final StreamObserver<GroupDTO.Grouping> mockGroupingObserver =
                Mockito.mock(StreamObserver.class);
        final GetGroupsRequest genericGroupsRequest = GetGroupsRequest
                .newBuilder()
                .setGroupFilter(GroupFilter.newBuilder().addAllId(Arrays.asList(firstGroupId,
                        secondGroupId)))
                .build();
        when(userSessionContext.isUserScoped()).thenReturn(true);
        when(userSessionContext.getUserAccessScope()).thenReturn(accessScope);
        Mockito.when(groupStoreDAO.getGroup(firstGroupId)).thenReturn(Optional.of(firstGrouping));
        Mockito.when(groupStoreDAO.getGroup(secondGroupId)).thenReturn(Optional.of(secondGrouping));
        Mockito.when(groupStoreDAO.getGroups(genericGroupsRequest.getGroupFilter()))
                .thenReturn(groups);
        groupRpcService.getGroups(genericGroupsRequest, mockGroupingObserver);
        Mockito.verify(mockGroupingObserver).onNext(firstGrouping);
        Mockito.verify(mockGroupingObserver, Mockito.never()).onNext(secondGrouping);
        Mockito.verify(mockGroupingObserver).onCompleted();
    }

    /**
     * Test {@link GroupRpcService#getGroups(GetGroupsRequest, StreamObserver)}
     * when request doesn't contain GroupFilter.
     */
    @Test
    public void testGetGroupsExceptionCase() {
        final ArgumentCaptor<StatusException> exceptionCaptor =
                ArgumentCaptor.forClass(StatusException.class);
        final StreamObserver<GroupDTO.Grouping> mockGroupingObserver =
                Mockito.mock(StreamObserver.class);
        // request without GroupFilter
        final GetGroupsRequest genericGroupsRequest =
                GetGroupsRequest.getDefaultInstance();
        groupRpcService.getGroups(genericGroupsRequest, mockGroupingObserver);
        Mockito.verify(mockGroupingObserver).onError(exceptionCaptor.capture());
        final StatusException exception = exceptionCaptor.getValue();
        assertThat(exception, GrpcExceptionMatcher.hasCode(Code.INVALID_ARGUMENT)
                .descriptionContains("No group filter is present"));
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
    public void testDelete() throws StoreOperationException {
        final long groupIdToDelete = 1234L;

        final GroupDTO.GroupID gid = GroupDTO.GroupID.newBuilder()
                .setId(groupIdToDelete)
                .build();

        final StreamObserver<GroupDTO.DeleteGroupResponse> mockObserver =
                mock(StreamObserver.class);

        groupRpcService.deleteGroup(gid, mockObserver);

        verify(temporaryGroupCache).deleteGrouping(groupIdToDelete);
        verify(groupStoreDAO).deleteGroup(groupIdToDelete);
        verify(mockObserver).onNext(
                GroupDTO.DeleteGroupResponse.newBuilder().setDeleted(true).build());
        verify(mockObserver).onCompleted();
        verify(mockObserver, never()).onError(any());
    }

    @Test
    public void testDeleteTempGroup() throws Exception {
        final long id = 7;
        when(temporaryGroupCache.deleteGrouping(id))
            .thenReturn(Optional.of(Grouping.getDefaultInstance()));

        final StreamObserver<GroupDTO.DeleteGroupResponse> mockObserver =
                mock(StreamObserver.class);
        groupRpcService.deleteGroup(GroupID.newBuilder().setId(id).build(), mockObserver);

        verify(temporaryGroupCache).deleteGrouping(id);
        verify(groupStoreDAO, never()).deleteGroup(anyLong());

        verify(mockObserver).onNext(
                GroupDTO.DeleteGroupResponse.newBuilder().setDeleted(true).build());
        verify(mockObserver).onCompleted();
        verify(mockObserver, never()).onError(any());
    }

    @Test
    public void testDeleteGroupNotFoundException() throws Exception {
        final long idToDelete = 1234L;
        final GroupDTO.GroupID gid = GroupDTO.GroupID.newBuilder()
                .setId(idToDelete)
                .build();

        final StreamObserver<GroupDTO.DeleteGroupResponse> mockObserver =
                mock(StreamObserver.class);

        Mockito.doThrow(new StoreOperationException(Status.NOT_FOUND,
                "Updable to delete group " + idToDelete))
                .when(groupStoreDAO)
                .deleteGroup(idToDelete);

        groupRpcService.deleteGroup(gid, mockObserver);

        verify(groupStoreDAO).deleteGroup(idToDelete);
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
        Mockito.doThrow(new DataAccessException(errorMsg)).when(groupStoreDAO).deleteGroup(idToDelete);

        groupRpcService.deleteGroup(gid, mockObserver);

        verify(groupStoreDAO).deleteGroup(idToDelete);
        verify(mockObserver, never()).onCompleted();
        verify(mockObserver, never()).onNext(any());

        final ArgumentCaptor<StatusException> exceptionCaptor = ArgumentCaptor.forClass(StatusException.class);
        verify(mockObserver).onError(exceptionCaptor.capture());
        assertThat(exceptionCaptor.getValue(), GrpcExceptionMatcher.hasCode(Code.INTERNAL)
                .descriptionContains(errorMsg));
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
        given(groupStoreDAO.getGroup(groupId))
            .willReturn(Optional.of(Grouping.getDefaultInstance()));

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
    public void testGetMembersMissingGroupId() {
        final GroupDTO.GetMembersRequest missingGroupIdReq =
                        GroupDTO.GetMembersRequest.getDefaultInstance();

        final StreamObserver<GroupDTO.GetMembersResponse> mockObserver =
                mock(StreamObserver.class);

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

        final Grouping group = Grouping
                        .newBuilder()
                        .setDefinition(GroupDefinition
                            .newBuilder()
                            .setEntityFilters(
                               EntityFilters
                                  .newBuilder()
                                  .addEntityFilter(
                                      EntityFilter
                                          .newBuilder()
                                          .setSearchParametersCollection(
                                                  SearchParametersCollection
                                                  .newBuilder()
                                                  .addSearchParameters(
                                                                  SearchParameters
                                                                  .getDefaultInstance()
                                                                  )
                                                  )
                                          )
                                  )
                            )
                        .build();

        final StreamObserver<GroupDTO.GetMembersResponse> mockObserver =
                mock(StreamObserver.class);

        given(groupStoreDAO.getGroup(groupId)).willReturn(Optional.of(group));
        givenSearchHanderWillReturn(mockSearchResults);

        groupRpcService.getMembers(req, mockObserver);

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


        final Grouping grouping = Grouping
                        .newBuilder()
                        .setId(groupId)
                        .setDefinition(GroupDefinition
                                        .newBuilder()
                                        .setStaticGroupMembers(StaticMembers
                                                        .newBuilder()
                                                        .addMembersByType(StaticMembersByType
                                                            .newBuilder()
                                                            .setType(MemberType
                                                                .newBuilder()
                                                                .setEntity(5)
                                                                )
                                                            .addAllMembers(staticGroupMembers)
                                                                        )
                                                        )
                                        )
                        .build();

        final StreamObserver<GroupDTO.GetMembersResponse> mockObserver =
                mock(StreamObserver.class);

        given(groupStoreDAO.getGroup(groupId)).willReturn(Optional.of(grouping));

        groupRpcService.getMembers(req, mockObserver);

        final GroupDTO.GetMembersResponse expectedResponse = GetMembersResponse.newBuilder()
                .setMembers(Members.newBuilder().addAllIds(staticGroupMembers))
                .build();

        verify(mockObserver, never()).onError(any(Exception.class));
        verify(mockObserver).onNext(expectedResponse);
        verify(mockObserver).onCompleted();
    }

    @Test
    public void testGetMembersExpansion() {
        final long groupId = 1234L;
        final List<Long> clusterGroupMembers = Arrays.asList(1L, 2L);
        final List<Long> cluster1Members = Arrays.asList(10L, 11L);
        final List<Long> cluster2Members = Arrays.asList(20L, 21L);

        final Grouping groupOfClusters = Grouping
                        .newBuilder()
                        .setId(groupId)
                        .setDefinition(GroupDefinition
                            .newBuilder()
                            .setType(GroupType.REGULAR)
                            .setStaticGroupMembers(StaticMembers
                                .newBuilder()
                                .addMembersByType(StaticMembersByType
                                    .newBuilder()
                                    .setType(MemberType
                                        .newBuilder()
                                        .setGroup(GroupType.COMPUTE_HOST_CLUSTER)
                                        )
                                    .addAllMembers(clusterGroupMembers)
                                )
                            )
                        ).build();

        final Grouping cluster1 = Grouping
                        .newBuilder()
                        .setId(1L)
                        .setDefinition(GroupDefinition
                            .newBuilder()
                            .setType(GroupType.COMPUTE_HOST_CLUSTER)
                            .setStaticGroupMembers(StaticMembers
                                .newBuilder()
                                .addMembersByType(StaticMembersByType
                                    .newBuilder()
                                    .setType(MemberType
                                        .newBuilder()
                                        .setEntity(5)
                                        )
                                    .addAllMembers(cluster1Members)
                                )
                            )
                        ).build();

        final Grouping cluster2 = Grouping
                        .newBuilder()
                        .setId(2L)
                        .setDefinition(GroupDefinition
                            .newBuilder()
                            .setType(GroupType.COMPUTE_HOST_CLUSTER)
                            .setStaticGroupMembers(StaticMembers
                                .newBuilder()
                                .addMembersByType(StaticMembersByType
                                    .newBuilder()
                                    .setType(MemberType
                                        .newBuilder()
                                        .setEntity(5)
                                        )
                                    .addAllMembers(cluster2Members)
                                )
                            )
                        ).build();

        given(groupStoreDAO.getGroup(groupId)).willReturn(Optional.of(groupOfClusters));
        given(groupStoreDAO.getGroup(1L)).willReturn(Optional.of(cluster1));
        given(groupStoreDAO.getGroup(2L)).willReturn(Optional.of(cluster2));

        final GetGroupsRequest clusterGroupMemberRequest = GetGroupsRequest
                        .newBuilder()
                        .setGroupFilter(
                            GroupFilter
                                .newBuilder()
                                .addAllId(clusterGroupMembers)
                                .build()
                        ).build();


        given(groupStoreDAO.getGroups(clusterGroupMemberRequest.getGroupFilter())).willReturn(
                        Arrays.asList(cluster1, cluster2));

        final StreamObserver<GroupDTO.GetMembersResponse> mockObserver =
                mock(StreamObserver.class);

        // a request without expansion should get the list of clusters
        final GroupDTO.GetMembersRequest reqNoExpansion = GroupDTO.GetMembersRequest.newBuilder()
                .setId(groupId)
                .build();

        groupRpcService.getMembers(reqNoExpansion, mockObserver);

        final GroupDTO.GetMembersResponse expectedResponse = GetMembersResponse.newBuilder()
                .setMembers(Members.newBuilder().addAllIds(clusterGroupMembers))
                .build();

        verify(mockObserver).onNext(expectedResponse);

        // verify that a request WITH expansion should get all of the cluster members.
        final GroupDTO.GetMembersRequest requestExpanded = GroupDTO.GetMembersRequest.newBuilder()
                .setId(groupId)
                .setExpandNestedGroups(true)
                .build();

        final GroupDTO.GetMembersResponse expectedExpandedResponse = GetMembersResponse.newBuilder()
                .setMembers(Members.newBuilder().addAllIds(cluster2Members).addAllIds(cluster1Members))
                .build();

        groupRpcService.getMembers(requestExpanded, mockObserver);

        verify(mockObserver).onNext(expectedExpandedResponse);
    }

    @Test
    public void testGetMembersExpansionDynamic() {
        final long groupId = 1234L;
        final List<Long> cluster1Members = Arrays.asList(10L, 11L);
        final List<Long> cluster2Members = Arrays.asList(20L, 21L);

        final GroupFilter groupFilter = GroupFilter
                        .newBuilder()
                        .addDirectMemberTypes(MemberType.newBuilder()
                                        .setGroup(GroupType.COMPUTE_HOST_CLUSTER)
                                            )
                        .build();

        final Grouping dynamicGroupOfClusters = Grouping
                        .newBuilder()
                        .setId(groupId)
                        .setDefinition(
                            GroupDefinition
                            .newBuilder()
                            .setType(GroupType.REGULAR)
                            .setGroupFilters(GroupFilters
                                .newBuilder()
                                .addGroupFilter(groupFilter)
                                            )
                            ).build();


        final Grouping cluster1 = Grouping
                        .newBuilder()
                        .setId(1L)
                        .setDefinition(GroupDefinition
                            .newBuilder()
                            .setType(GroupType.COMPUTE_HOST_CLUSTER)
                            .setStaticGroupMembers(StaticMembers
                                .newBuilder()
                                .addMembersByType(StaticMembersByType
                                    .newBuilder()
                                    .setType(MemberType
                                        .newBuilder()
                                        .setEntity(5)
                                        )
                                    .addAllMembers(cluster1Members)
                                )
                            )
                        ).build();

        final Grouping cluster2 = Grouping
                        .newBuilder()
                        .setId(2L)
                        .setDefinition(GroupDefinition
                            .newBuilder()
                            .setType(GroupType.COMPUTE_HOST_CLUSTER)
                            .setStaticGroupMembers(StaticMembers
                                .newBuilder()
                                .addMembersByType(StaticMembersByType
                                    .newBuilder()
                                    .setType(MemberType
                                        .newBuilder()
                                        .setEntity(5)
                                        )
                                    .addAllMembers(cluster2Members)
                                )
                            )
                        ).build();

        given(groupStoreDAO.getGroup(groupId)).willReturn(Optional.of(dynamicGroupOfClusters));
        given(groupStoreDAO.getGroup(1L)).willReturn(Optional.of(cluster1));
        given(groupStoreDAO.getGroup(2L)).willReturn(Optional.of(cluster2));

        GetGroupsRequest request = GetGroupsRequest
                        .newBuilder()
                        .setGroupFilter(groupFilter)
                        .build();

        given(groupStoreDAO.getGroups(request.getGroupFilter())).willReturn(Arrays.asList(cluster1));


        final StreamObserver<GroupDTO.GetMembersResponse> mockObserver =
                mock(StreamObserver.class);

        // a request without expansion should get the list of clusters
        final GroupDTO.GetMembersRequest reqNoExpansion = GroupDTO.GetMembersRequest.newBuilder()
                .setId(groupId)
                .build();

        groupRpcService.getMembers(reqNoExpansion, mockObserver);

        final GroupDTO.GetMembersResponse expectedNonExpandedResponse = GetMembersResponse.newBuilder()
                .setMembers(Members.newBuilder().addAllIds(Arrays.asList(1L)))
                .build();

        verify(mockObserver).onNext(expectedNonExpandedResponse);

        // verify that a request WITH expansion should get all of the cluster members.
        final GroupDTO.GetMembersRequest requestExpanded = GroupDTO.GetMembersRequest.newBuilder()
                .setId(groupId)
                .setExpandNestedGroups(true)
                .build();

        final GroupDTO.GetMembersResponse expectedExpandedResponse = GetMembersResponse.newBuilder()
                .setMembers(Members.newBuilder().addAllIds(cluster1Members))
                .build();

        groupRpcService.getMembers(requestExpanded, mockObserver);

        verify(mockObserver).onNext(expectedExpandedResponse);
    }

    @Test
    public void testMissingGroup() throws Exception {
        final long groupId = 1234L;

        final GroupDTO.GetMembersRequest req = GroupDTO.GetMembersRequest.newBuilder()
                .setId(groupId)
                .build();

        final StreamObserver<GroupDTO.GetMembersResponse> mockObserver =
                mock(StreamObserver.class);

        given(groupStoreDAO.getGroup(groupId)).willReturn(Optional.empty());

        groupRpcService.getMembers(req, mockObserver);

        verify(mockObserver).onError(any(IllegalArgumentException.class));
        verify(mockObserver, never()).onNext(any(GroupDTO.GetMembersResponse.class));
        verify(mockObserver, never()).onCompleted();
    }


    @Test
    public void testNoTargetId() {
        StreamObserver<StoreDiscoveredGroupsPoliciesSettingsResponse> responseObserver =
                mock(StreamObserver.class);
        StreamObserver<DiscoveredGroupsPoliciesSettings>  requestObserver =
                spy(groupRpcService.storeDiscoveredGroupsPoliciesSettings(responseObserver));
        requestObserver.onNext(DiscoveredGroupsPoliciesSettings.getDefaultInstance());
        requestObserver.onCompleted();

        verify(responseObserver, times(1)).onError(any(IllegalArgumentException.class));
    }

    /**
     * Test that cluster groups/policies are invoked successfully in rpc call method.
     *
     * @throws StoreOperationException exception thrown if group is invalid
     */
    @Test
    public void testUpdateClusters() throws StoreOperationException {
        StreamObserver<StoreDiscoveredGroupsPoliciesSettingsResponse> responseObserver =
                mock(StreamObserver.class);
        StreamObserver<DiscoveredGroupsPoliciesSettings> requestObserver =
                spy(groupRpcService.storeDiscoveredGroupsPoliciesSettings(responseObserver));

        requestObserver.onNext(DiscoveredGroupsPoliciesSettings.newBuilder().setTargetId(10L)
                    .addUploadedGroups(GroupTestUtils.createUploadedGroup(
                            GroupType.COMPUTE_HOST_CLUSTER, "cluster", ImmutableMap.of(
                                    EntityType.PHYSICAL_MACHINE_VALUE, Sets.newHashSet(111L))))
                    .build());
        requestObserver.onCompleted();

        verify(responseObserver).onCompleted();
        verify(responseObserver).onNext(StoreDiscoveredGroupsPoliciesSettingsResponse.getDefaultInstance());
        // capture the group used to save to db
        final ArgumentCaptor<Collection> captor = ArgumentCaptor.forClass(Collection.class);
        verify(groupStoreDAO).updateDiscoveredGroups(captor.capture());
        Collection groups = captor.getValue();
        assertEquals(1, groups.size());
        DiscoveredGroup group = (DiscoveredGroup)groups.iterator().next();
        assertThat(group.getDefinition(), is(GroupDefinition.newBuilder()
                .setType(GroupType.COMPUTE_HOST_CLUSTER)
                .setDisplayName("cluster")
                .setStaticGroupMembers(StaticMembers.newBuilder()
                        .addMembersByType(StaticMembersByType.newBuilder()
                                .setType(MemberType.newBuilder()
                                        .setEntity(EntityType.PHYSICAL_MACHINE_VALUE))
                                .addMembers(111L)))
                .build()));
        assertThat(group.getSourceIdentifier(), is("cluster"));
        assertThat(group.getTargetIds(), contains(10L));
        assertThat(group.getExpectedMembers(), contains(MemberType.newBuilder()
                .setEntity(EntityType.PHYSICAL_MACHINE_VALUE)
                .build()));
        assertThat(group.isReverseLookupSupported(), is(true));

        verify(transactionProvider.getPlacementPolicyStore()).updateTargetPolicies(eq(10L),
                eq(Collections.emptyList()), anyMap());
        verify(transactionProvider.getSettingPolicyStore()).updateTargetSettingPolicies(eq(10L),
                eq(Collections.emptyList()), anyMap());
    }

    /**
     * Test that resource groups are uploaded and stitched successfully in rpc call method.
     *
     * @throws StoreOperationException exception thrown if group is invalid
     */
    @Test
    public void testUpdateResourceGroupsAndStitching() throws StoreOperationException {
        StreamObserver<StoreDiscoveredGroupsPoliciesSettingsResponse> responseObserver =
                mock(StreamObserver.class);
        StreamObserver<DiscoveredGroupsPoliciesSettings> requestObserver =
                spy(groupRpcService.storeDiscoveredGroupsPoliciesSettings(responseObserver));

        // create two resource groups from two different probes
        UploadedGroup rg1 = GroupTestUtils.createUploadedGroup(GroupType.RESOURCE, "rg",
                ImmutableMap.of(EntityType.VIRTUAL_MACHINE_VALUE, Sets.newHashSet(11L),
                        EntityType.DATABASE_VALUE, Sets.newHashSet(21L)));
        UploadedGroup rg2 = GroupTestUtils.createUploadedGroup(GroupType.RESOURCE, "rg",
                ImmutableMap.of(EntityType.VIRTUAL_VOLUME_VALUE, Sets.newHashSet(31L),
                        EntityType.DATABASE_VALUE, Sets.newHashSet(22L)));

        requestObserver.onNext(DiscoveredGroupsPoliciesSettings.newBuilder()
                .setTargetId(110L)
                .setProbeType(SDKProbeType.AZURE.toString())
                .addUploadedGroups(rg1)
                .build());
        requestObserver.onNext(DiscoveredGroupsPoliciesSettings.newBuilder()
                .setTargetId(111L)
                .setProbeType(SDKProbeType.APPINSIGHTS.toString())
                .addUploadedGroups(rg2)
                .build());
        requestObserver.onCompleted();

        // verify response
        verify(responseObserver).onCompleted();
        verify(responseObserver).onNext(StoreDiscoveredGroupsPoliciesSettingsResponse.getDefaultInstance());

        // capture the group used to save to db
        final ArgumentCaptor<Collection> captor = ArgumentCaptor.forClass(Collection.class);
        verify(groupStoreDAO).updateDiscoveredGroups(captor.capture());
        Collection groups = captor.getValue();

        assertEquals(1, groups.size());
        DiscoveredGroup group = (DiscoveredGroup)groups.iterator().next();
        GroupDefinition groupDefinition = group.getDefinition();

        assertEquals(GroupType.RESOURCE, groupDefinition.getType());
        Map<Integer, List<Long>> membersByType = groupDefinition.getStaticGroupMembers()
                .getMembersByTypeList()
                .stream()
                .collect(Collectors.toMap(k -> k.getType().getEntity(),
                        StaticMembersByType::getMembersList));
        // check members are merged
        assertEquals(3, groupDefinition.getStaticGroupMembers().getMembersByTypeCount());
        assertThat(membersByType.get(EntityType.VIRTUAL_MACHINE_VALUE), containsInAnyOrder(11L));
        assertThat(membersByType.get(EntityType.VIRTUAL_VOLUME_VALUE), containsInAnyOrder(31L));
        assertThat(membersByType.get(EntityType.DATABASE_VALUE), containsInAnyOrder(21L, 22L));
        // check that target ids are also merged
        assertThat(group.getTargetIds(), containsInAnyOrder(110L, 111L));
    }

    /**
     * Test failed retrieval of tags.
     */
    @Test
    public void testGetTagsFailed() {
        final String errorMessage = "boom";
        when(groupStoreDAO.getTags()).thenThrow(new DataAccessException(errorMessage));
        final StreamObserver<GetTagsResponse> mockObserver = mock(StreamObserver.class);
        final ArgumentCaptor<Throwable> captor = ArgumentCaptor.forClass(Throwable.class);
        groupRpcService.getTags(GroupDTO.GetTagsRequest.newBuilder().build(), mockObserver);
        verify(mockObserver).onError(captor.capture());
        final Throwable throwable = captor.getValue();
        Assert.assertTrue(throwable instanceof StatusException);
        final StatusException statusException = (StatusException)throwable;
        Assert.assertEquals(Status.INTERNAL.getCode(), statusException.getStatus().getCode());
        Assert.assertThat(statusException.getMessage(), CoreMatchers.containsString(errorMessage));
    }

    /**
     * Tests the case a user group is created successfully.
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testGroupCreateUserGroup() throws Exception {
        GroupDefinition group = testGrouping;

        CreateGroupRequest groupRequest = CreateGroupRequest
                        .newBuilder()
                        .setGroupDefinition(group)
                        .setOrigin(origin)
                        .build();

        final StreamObserver<GroupDTO.CreateGroupResponse> mockObserver =
                        mock(StreamObserver.class);

        long groupingOid = 5;
        given(groupStoreDAO
                        .createGroup(eq(origin), eq(group), any(),
                                        eq(true))).willReturn(groupingOid);

        groupRpcService.createGroup(groupRequest, mockObserver);
        verify(groupStoreDAO).createGroup(eq(origin), eq(group), any(),
                        eq(true));
        verify(mockObserver).onNext(CreateGroupResponse.newBuilder()
                .setGroup(Grouping
                        .newBuilder()
                        .setId(groupingOid)
                        .setDefinition(group)
                        .addExpectedTypes(MemberType
                                        .newBuilder()
                                        .setEntity(2))
                        .setSupportsMemberReverseLookup(true)
                        .build())
                .build());
        verify(mockObserver).onCompleted();
    }

    /**
     * Tests the case a temp group is created successfully.
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testCreateTempGroup() throws Exception {
        GroupDefinition group = GroupDefinition
                        .newBuilder(testGrouping)
                        .setIsTemporary(true)
                        .build();

        final Set<MemberType> expectedTypes =  new HashSet<>();

        expectedTypes.add(MemberType
                        .newBuilder()
                        .setEntity(2)
                        .build());

        final Grouping grouping = Grouping
                        .newBuilder()
                        .setId(8L)
                        .setDefinition(group)
                        .addAllExpectedTypes(expectedTypes)
                        .setSupportsMemberReverseLookup(false)
                        .build();

        when(temporaryGroupCache.create(group, origin, expectedTypes))
                                .thenReturn(grouping);

        final StreamObserver<GroupDTO.CreateGroupResponse> mockObserver =
                mock(StreamObserver.class);

        CreateGroupRequest groupRequest = CreateGroupRequest
                        .newBuilder()
                        .setGroupDefinition(group)
                        .setOrigin(origin)
                        .build();

        groupRpcService.createGroup(groupRequest, mockObserver);

        verify(temporaryGroupCache).create(group, origin, expectedTypes);
        verify(mockObserver).onNext(CreateGroupResponse.newBuilder()
                .setGroup(grouping)
                .build());
        verify(mockObserver).onCompleted();
    }

    /**
     * Tests the case a user group is tried to be created but it misses group definition.
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testCreateGroupMissingGroupDefinition() throws Exception {

        final StreamObserver<GroupDTO.CreateGroupResponse> mockObserver =
                        mock(StreamObserver.class);

        CreateGroupRequest groupRequest = CreateGroupRequest
                        .newBuilder()
                        .setOrigin(origin)
                        .build();

        groupRpcService.createGroup(groupRequest, mockObserver);

        //Verify the group was not created
        verify(groupStoreDAO, never())
            .createGroup(Mockito.anyObject(), Mockito.anyObject(),
                            Mockito.anyObject(), Mockito.anyBoolean());

        //Verify we send the error response
        final ArgumentCaptor<StatusException> exceptionCaptor =
                        ArgumentCaptor.forClass(StatusException.class);
        verify(mockObserver).onError(exceptionCaptor.capture());

        final StatusException exception = exceptionCaptor.getValue();
        assertThat(exception, GrpcExceptionMatcher.hasCode(Code.INVALID_ARGUMENT)
                .descriptionContains("No group definition"));
    }

    /**
     * Tests the case a user group is tried to be created but it misses group definition.
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testCreateGroupMissingOrigin() throws Exception {

        final StreamObserver<GroupDTO.CreateGroupResponse> mockObserver =
                        mock(StreamObserver.class);

        CreateGroupRequest groupRequest = CreateGroupRequest
                        .newBuilder()
                        .setGroupDefinition(testGrouping)
                        .build();

        groupRpcService.createGroup(groupRequest, mockObserver);

        //Verify the group was not created
        verify(groupStoreDAO, never())
            .createGroup(Mockito.anyObject(), Mockito.anyObject(),
                            Mockito.anyObject(), Mockito.anyBoolean());
        //Verify we send the error response
        final ArgumentCaptor<StatusException> exceptionCaptor =
                        ArgumentCaptor.forClass(StatusException.class);
        verify(mockObserver).onError(exceptionCaptor.capture());

        final StatusException exception = exceptionCaptor.getValue();
        assertThat(exception, GrpcExceptionMatcher.hasCode(Code.INVALID_ARGUMENT)
                .descriptionContains("No origin"));
    }

    /**
     * Tests the case a user group is tried to be created but it misses origin.
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testCreateGroupMissingDisplayName() throws Exception {
        GroupDefinition group = GroupDefinition
                        .newBuilder(testGrouping)
                        .clearDisplayName()
                        .build();
        final StreamObserver<GroupDTO.CreateGroupResponse> mockObserver =
                        mock(StreamObserver.class);

        CreateGroupRequest groupRequest = CreateGroupRequest
                        .newBuilder()
                        .setGroupDefinition(group)
                        .setOrigin(origin)
                        .build();

        groupRpcService.createGroup(groupRequest, mockObserver);

        //Verify the group was not created
        verify(groupStoreDAO, never())
                .createGroup(Mockito.anyObject(), Mockito.anyObject(),
                                Mockito.anyObject(), Mockito.anyBoolean());

        //Verify we send the error response
        final ArgumentCaptor<StatusException> exceptionCaptor =
                        ArgumentCaptor.forClass(StatusException.class);
        verify(mockObserver).onError(exceptionCaptor.capture());

        final StatusException exception = exceptionCaptor.getValue();
        assertThat(exception, GrpcExceptionMatcher.hasCode(Code.INVALID_ARGUMENT)
                .descriptionContains("Group display name is blank or not set."));
    }

    /**
     * Tests the case a scoped user group creates a group which is in their scope.
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testCreateTemporaryGroupInScope() throws Exception {
        when(userSessionContext.isUserScoped()).thenReturn(false);

        GroupDefinition group = GroupDefinition
                        .newBuilder(testGrouping)
                        .setIsTemporary(true)
                        .setOptimizationMetadata(OptimizationMetadata
                                        .newBuilder()
                                        .setIsGlobalScope(false))
                        .build();


        final Set<MemberType> expectedTypes =  new HashSet<>();

        expectedTypes.add(MemberType
                        .newBuilder()
                        .setEntity(2)
                        .build());

        final Grouping grouping = Grouping
                        .newBuilder()
                        .setId(8L)
                        .setDefinition(group)
                        .addAllExpectedTypes(expectedTypes)
                        .setSupportsMemberReverseLookup(false)
                        .build();

        when(temporaryGroupCache.create(group, origin, expectedTypes))
                                .thenReturn(grouping);

        final StreamObserver<CreateGroupResponse> mockObserver =
                mock(StreamObserver.class);

        groupRpcService.createGroup(CreateGroupRequest.newBuilder()
                .setGroupDefinition(group)
                .setOrigin(origin)
                .build(), mockObserver);

        verify(temporaryGroupCache).create(group, origin, expectedTypes);
        verify(mockObserver).onNext(CreateGroupResponse.newBuilder()
                .setGroup(grouping)
                .build());
        verify(mockObserver).onCompleted();
    }

    /**
     * Tests the case a scoped user group tries to create a group which is outside their scope.
     * @throws Exception if something goes wrong.
     */
    @Test(expected = UserAccessScopeException.class)
    public void testCreateTemporaryGroupOutOfScope() throws Exception {
        when(userSessionContext.isUserScoped()).thenReturn(true);
        EntityAccessScope scope = new EntityAccessScope(null, null,
                new ArrayOidSet(Arrays.asList(1L)), null);
        when(userSessionContext.getUserAccessScope()).thenReturn(scope);

        GroupDefinition group = GroupDefinition
                        .newBuilder(testGrouping)
                        .setIsTemporary(true)
                        .setOptimizationMetadata(OptimizationMetadata
                                        .newBuilder()
                                        .setIsGlobalScope(false))
                        .build();

        final StreamObserver<CreateGroupResponse> mockObserver =
                mock(StreamObserver.class);

        groupRpcService.createGroup(CreateGroupRequest.newBuilder()
                .setGroupDefinition(group)
                .setOrigin(origin)
                .build(), mockObserver);
    }

    /**
     * Tests the case a scoped user group tries to create a group which has invalid definition.
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testCreateInvalidTempGroup() throws Exception {
        GroupDefinition group = GroupDefinition
                        .newBuilder(testGrouping)
                        .setIsTemporary(true)
                        .build();

        final Set<MemberType> expectedTypes =  new HashSet<>();

        expectedTypes.add(MemberType
                        .newBuilder()
                        .setEntity(2)
                        .build());

        when(temporaryGroupCache.create(group, origin, expectedTypes))
                                .thenThrow(new InvalidTempGroupException(Collections
                                                .singletonList("ERR1")));

        final StreamObserver<GroupDTO.CreateGroupResponse> mockObserver =
                mock(StreamObserver.class);

        CreateGroupRequest groupRequest = CreateGroupRequest
                        .newBuilder()
                        .setGroupDefinition(group)
                        .setOrigin(origin)
                        .build();

        groupRpcService.createGroup(groupRequest, mockObserver);

        //Verify we send the error response
        final ArgumentCaptor<StatusRuntimeException> exceptionCaptor =
                        ArgumentCaptor.forClass(StatusRuntimeException.class);
        verify(mockObserver).onError(exceptionCaptor.capture());

        final StatusRuntimeException exception = exceptionCaptor.getValue();
        assertThat(exception, GrpcRuntimeExceptionMatcher.hasCode(Code.ABORTED)
                .descriptionContains("ERR1"));
    }

    /**
     * Tests the cases when we DAO object of group service throws different exception when
     * creating group.
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testGroupCreateDifferentExceptions() throws Exception {
        GroupDefinition group = testGrouping;

        CreateGroupRequest groupRequest = CreateGroupRequest
                        .newBuilder()
                        .setGroupDefinition(group)
                        .setOrigin(origin)
                        .build();

        final StreamObserver<GroupDTO.CreateGroupResponse> mockObserver =
                        mock(StreamObserver.class);
        final String message = "some error occurred";
        Mockito.doThrow(new StoreOperationException(Status.ABORTED, message))
            .when(groupStoreDAO).createGroup(Mockito.anyObject(), Mockito.anyObject(),
                            Mockito.anyObject(),
                            Mockito.anyBoolean());

        groupRpcService.createGroup(groupRequest, mockObserver);


        //Verify we send the error response
        final ArgumentCaptor<StatusException> exceptionCaptor =
                        ArgumentCaptor.forClass(StatusException.class);
        verify(mockObserver).onError(exceptionCaptor.capture());

        final StatusException exception = exceptionCaptor.getValue();
        assertThat(exception, GrpcExceptionMatcher.hasCode(Code.ABORTED)
                        .descriptionContains(message));
    }

    /**
     * Tests the case when a user successfully updates a group.
     * @throws Exception is thrown when something goes wrong.
     */
    @Test
    public void testGroupUpdateUserGroup() throws Exception {
        GroupDefinition group = testGrouping;

        final long groupingOid = 5;

        Set<MemberType> expectedTypes = new HashSet<>();
        expectedTypes.add(MemberType
                            .newBuilder()
                            .setEntity(2)
                            .build());

        Grouping grouping = Grouping
                        .newBuilder()
                        .setId(groupingOid)
                        .setDefinition(group)
                        .addAllExpectedTypes(expectedTypes)
                        .setSupportsMemberReverseLookup(true)
                        .build();

        UpdateGroupRequest groupRequest = UpdateGroupRequest
                        .newBuilder()
                        .setId(groupingOid)
                        .setNewDefinition(group)
                        .build();

        final StreamObserver<UpdateGroupResponse> mockObserver =
                        mock(StreamObserver.class);

        given(groupStoreDAO
                        .updateGroup(eq(groupingOid), eq(group), eq(expectedTypes),
                                        eq(true))).willReturn(grouping);

        groupRpcService.updateGroup(groupRequest, mockObserver);
        verify(groupStoreDAO).updateGroup(eq(groupingOid), eq(group), eq(expectedTypes),
                        eq(true));

        verify(mockObserver).onNext(UpdateGroupResponse.newBuilder()
                .setUpdatedGroup(grouping)
                .build());
        verify(mockObserver).onCompleted();
    }

    /**
     * Tests the case update group service is called but the request does not have group definition.
     * @throws Exception is thrown when something goes wrong.
     */
    @Test
    public void testUpdateGroupMissingGroupDefinition() throws Exception {

        final StreamObserver<GroupDTO.UpdateGroupResponse> mockObserver =
                        mock(StreamObserver.class);

        UpdateGroupRequest groupRequest = UpdateGroupRequest
                        .newBuilder()
                        .setId(1L)
                        .build();

        groupRpcService.updateGroup(groupRequest, mockObserver);

        //Verify the group was not created
        verify(groupStoreDAO, never())
            .updateGroup(Mockito.anyLong(), Mockito.anyObject(),
                            Mockito.anyObject(), Mockito.anyBoolean());

        //Verify we send the error response
        final ArgumentCaptor<StatusException> exceptionCaptor =
                        ArgumentCaptor.forClass(StatusException.class);
        verify(mockObserver).onError(exceptionCaptor.capture());

        final StatusException exception = exceptionCaptor.getValue();
        assertThat(exception, GrpcExceptionMatcher.hasCode(Code.INVALID_ARGUMENT)
                .descriptionContains("No group definition"));
    }

    /**
     * Tests the case update group service is called but the request does not have the id.
     * @throws Exception is thrown when something goes wrong.
     */
    @Test
    public void testUpdateGroupMissingId() throws Exception {

        final StreamObserver<GroupDTO.UpdateGroupResponse> mockObserver =
                        mock(StreamObserver.class);

        UpdateGroupRequest groupRequest = UpdateGroupRequest
                        .newBuilder()
                        .setNewDefinition(testGrouping)
                        .build();


        groupRpcService.updateGroup(groupRequest, mockObserver);

        //Verify the group was not created
        verify(groupStoreDAO, never())
            .updateGroup(Mockito.anyLong(), Mockito.anyObject(),
                            Mockito.anyObject(), Mockito.anyBoolean());

        //Verify we send the error response
        final ArgumentCaptor<StatusException> exceptionCaptor =
                        ArgumentCaptor.forClass(StatusException.class);
        verify(mockObserver).onError(exceptionCaptor.capture());

        final StatusException exception = exceptionCaptor.getValue();
        assertThat(exception, GrpcExceptionMatcher.hasCode(Code.INVALID_ARGUMENT)
                .descriptionContains("No group ID specified"));
    }

    /**
     * Tests the case update group service is called but the new group does not have
     * the updated display.
     * @throws Exception is thrown when something goes wrong.
     */
    @Test
    public void testUpdateGroupMissingDisplayName() throws Exception {
        GroupDefinition group = GroupDefinition
                        .newBuilder(testGrouping)
                        .clearDisplayName()
                        .build();
        final StreamObserver<GroupDTO.UpdateGroupResponse> mockObserver =
                        mock(StreamObserver.class);

        UpdateGroupRequest groupRequest = UpdateGroupRequest
                        .newBuilder()
                        .setId(1L)
                        .setNewDefinition(group)
                        .build();

        groupRpcService.updateGroup(groupRequest, mockObserver);

        //Verify the group was not created
        verify(groupStoreDAO, never())
            .updateGroup(Mockito.anyLong(), Mockito.anyObject(),
                            Mockito.anyObject(), Mockito.anyBoolean());

        //Verify we send the error response
        final ArgumentCaptor<StatusException> exceptionCaptor =
                        ArgumentCaptor.forClass(StatusException.class);
        verify(mockObserver).onError(exceptionCaptor.capture());

        final StatusException exception = exceptionCaptor.getValue();
        assertThat(exception, GrpcExceptionMatcher.hasCode(Code.INVALID_ARGUMENT)
                .descriptionContains("Group display name is blank or not set."));
    }

    /**
     * Tests how {@link StoreOperationException} is treated in gRPC response code.
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testUpdateGroupException() throws Exception {
        GroupDefinition group = testGrouping;

        UpdateGroupRequest groupRequest =
                UpdateGroupRequest.newBuilder().setId(1L).setNewDefinition(group).build();

        final StreamObserver<GroupDTO.UpdateGroupResponse> mockObserver =
                Mockito.mock(StreamObserver.class);

        final String message = "some message";
        Mockito.doThrow(new StoreOperationException(Status.ALREADY_EXISTS, message))
                .when(groupStoreDAO)
                .updateGroup(Mockito.anyLong(), Mockito.anyObject(), Mockito.anyObject(),
                        Mockito.anyBoolean());

        groupRpcService.updateGroup(groupRequest, mockObserver);

        final ArgumentCaptor<StatusException> exceptionCaptor =
                ArgumentCaptor.forClass(StatusException.class);
        verify(mockObserver).onError(exceptionCaptor.capture());

        final StatusException exception = exceptionCaptor.getValue();

        assertThat(exception, GrpcExceptionMatcher.hasCode(Status.ALREADY_EXISTS.getCode())
                .descriptionContains(message));
    }

    /**
     * Tests the findExpectedTypes method in group service when we are dealing with
     * static group of groups.
     */
    @Test
    public void testFindExpectedTypesGroupOfGroups() {
        final MemberType resourceGroupType = MemberType
                        .newBuilder()
                        .setGroup(GroupType.RESOURCE)
                        .build();

        final MemberType entityType1 = MemberType
                        .newBuilder()
                        .setEntity(1)
                        .build();

        final MemberType entityType2 = MemberType
                        .newBuilder()
                        .setEntity(2)
                        .build();

        final MemberType entityType3 = MemberType
                        .newBuilder()
                        .setEntity(3)
                        .build();

        GroupDefinition groupDefinition = GroupDefinition
                        .newBuilder()
                        .setStaticGroupMembers(StaticMembers
                            .newBuilder()
                            .addMembersByType(StaticMembersByType
                                .newBuilder()
                                .setType(resourceGroupType)
                                .addAllMembers(Arrays.asList(101L, 102L))
                                            )
                                              )
                        .build();


        Grouping subGroup1 = Grouping
                        .newBuilder()
                        .setId(101L)
                        .addExpectedTypes(entityType1)
                        .addExpectedTypes(entityType2)
                        .build();

        Grouping subGroup2 = Grouping
                        .newBuilder()
                        .setId(102L)
                        .addExpectedTypes(entityType2)
                        .addExpectedTypes(entityType3)
                        .build();

        given(groupStoreDAO.getGroups(
                            GroupFilter
                                .newBuilder()
                                .addAllId(Arrays.asList(101L, 102L))
                                .build()))
            .willReturn(Arrays.asList(subGroup1, subGroup2));

        Set<MemberType> memberTypes =
                groupRpcService.findGroupExpectedTypes(transactionProvider.getGroupStore(),
                        groupDefinition);
        assertEquals(ImmutableSet.of(resourceGroupType,
                        entityType1,
                        entityType2,
                        entityType3), memberTypes);

    }

    /**
     * Tests the findExpectedTypes method in group service when we are dealing with
     * dynamic group of entities.
     */
    @Test
    public void testFindExpectedTypeDynamicGroup() {
        GroupDefinition groupDefinition = GroupDefinition
                        .newBuilder()
                        .setEntityFilters(EntityFilters
                            .newBuilder()
                            .addEntityFilter(EntityFilter
                                .newBuilder()
                                .setEntityType(2)
                                )
                            .addEntityFilter(EntityFilter
                                            .newBuilder()
                                            .setEntityType(3)
                                            )
                            )
                        .build();

        final Set<MemberType> memberTypes =
                groupRpcService.findGroupExpectedTypes(groupStoreDAO, groupDefinition);

        assertEquals(ImmutableSet.of(
                        MemberType
                            .newBuilder()
                            .setEntity(2)
                            .build(),
                        MemberType
                            .newBuilder()
                            .setEntity(3)
                            .build()
                        ), memberTypes);
    }

    /**
     * Tests the case that getting a generic group succeeds.
     */
    @Test
    public void testGetGroup() {
        GroupID groupId = GroupID
                        .newBuilder()
                        .setId(11L)
                        .build();

        Grouping grouping = Grouping
                        .newBuilder()
                        .setId(11L)
                        .setDefinition(testGrouping)
                        .build();

        given(temporaryGroupCache.getGrouping(11L))
            .willReturn(Optional.of(grouping));

        final StreamObserver<GetGroupResponse> mockObserver =
                        mock(StreamObserver.class);

        groupRpcService.getGroup(groupId, mockObserver);

        verify(temporaryGroupCache).getGrouping(11L);
        verify(mockObserver).onNext(GetGroupResponse.newBuilder()
                .setGroup(grouping)
                .build());
        verify(mockObserver).onCompleted();
    }

    /**
     * Tests the case where a user requested for for a group but they did not
     * provide the id for the group.
     * @throws Exception when something goes wrong.
     */
    @Test
    public void testGetGroupMissingId() throws Exception {
        final StreamObserver<GetGroupResponse> mockObserver =
                        mock(StreamObserver.class);

        GroupID groupId = GroupID
                        .newBuilder()
                        .build();

        groupRpcService.getGroup(groupId, mockObserver);

        verify(temporaryGroupCache, never())
            .getGrouping(Mockito.anyLong());

        final ArgumentCaptor<StatusException> exceptionCaptor =
                        ArgumentCaptor.forClass(StatusException.class);
        verify(mockObserver).onError(exceptionCaptor.capture());

        final StatusException exception = exceptionCaptor.getValue();
        assertThat(exception, GrpcExceptionMatcher.hasCode(Code.INVALID_ARGUMENT)
                .descriptionContains("No group ID specified"));
    }

    /**
     * Tests the case where a user requested for for a group but DAO operation fails.
     * @throws Exception when something goes wrong.
     */
    @Test
    public void testGetGroupDataAccessException() throws Exception {
        final StreamObserver<GetGroupResponse> mockObserver =
                        mock(StreamObserver.class);

        GroupID groupId = GroupID
                        .newBuilder()
                        .setId(11L)
                        .build();

        Mockito.doThrow(new DataAccessException("ERR1"))
            .when(groupStoreDAO).getGroup(Mockito.anyLong());

        groupRpcService.getGroup(groupId, mockObserver);

        final ArgumentCaptor<StatusRuntimeException> exceptionCaptor =
                        ArgumentCaptor.forClass(StatusRuntimeException.class);
        verify(mockObserver).onError(exceptionCaptor.capture());

        final StatusRuntimeException exception = exceptionCaptor.getValue();
        assertThat(exception, GrpcRuntimeExceptionMatcher.hasCode(Code.INTERNAL)
                .descriptionContains("data access error"));
        assertThat(exception, GrpcRuntimeExceptionMatcher.hasCode(Code.INTERNAL)
                        .descriptionContains("ERR1"));
    }

    /**
     * Tests validate group method when no selection criteria is set for group.
     * @throws InvalidGroupDefinitionException when the group definition is invalid.
     */
    @Test(expected = InvalidGroupDefinitionException.class)
    public void testValidateGroupNoSelectionCriteria()
                    throws InvalidGroupDefinitionException {
        GroupDefinition groupDef = GroupDefinition
                        .newBuilder()
                        .setType(GroupType.REGULAR)
                        .setDisplayName("Test")
                        .build();

        groupRpcService.validateGroupDefinition(groupDef);
    }

    /**
     * Tests validate group method when the group is static but no members has been set.
     * @throws InvalidGroupDefinitionException when the group definition is invalid.
     */
    @Test(expected = InvalidGroupDefinitionException.class)
    public void testValidateGroupStaticMembers()
                    throws InvalidGroupDefinitionException {
        GroupDefinition groupDef = GroupDefinition
                        .newBuilder()
                        .setType(GroupType.REGULAR)
                        .setDisplayName("Test")
                        .setStaticGroupMembers(StaticMembers.newBuilder())
                        .build();

        groupRpcService.validateGroupDefinition(groupDef);
    }

    /**
     * Tests validate group method when the group is dynamic but no filter has been set.
     * @throws InvalidGroupDefinitionException when the group definition is invalid.
     */
    @Test(expected = InvalidGroupDefinitionException.class)
    public void testValidateGroupDynamicMembers()
                    throws InvalidGroupDefinitionException {
        GroupDefinition groupDef = GroupDefinition
                        .newBuilder()
                        .setType(GroupType.REGULAR)
                        .setDisplayName("Test")
                        .setEntityFilters(EntityFilters.newBuilder())
                        .build();

        groupRpcService.validateGroupDefinition(groupDef);

    }

    /**
     * Tests validate group method when the group is dynamic but the filter has no search parameter.
     * @throws InvalidGroupDefinitionException when the group definition is invalid.
     */
    @Test(expected = InvalidGroupDefinitionException.class)
    public void testValidateGroupDynamicMembersNoSearchParam()
                    throws InvalidGroupDefinitionException {
        GroupDefinition groupDef = GroupDefinition
                        .newBuilder()
                        .setType(GroupType.REGULAR)
                        .setDisplayName("Test")
                        .setEntityFilters(EntityFilters
                                        .newBuilder()
                                        .addEntityFilter(EntityFilter.newBuilder()))
                        .build();

        groupRpcService.validateGroupDefinition(groupDef);

    }

    /**
     * Tests validate group method when the group is dynamic group of groups but no filters has
     * been set.
     * @throws InvalidGroupDefinitionException when the group definition is invalid.
     */
    @Test(expected = InvalidGroupDefinitionException.class)
    public void testValidateGroupDynamicGroupofGroup()
                    throws InvalidGroupDefinitionException {
        GroupDefinition groupDef = GroupDefinition
                        .newBuilder()
                        .setType(GroupType.REGULAR)
                        .setDisplayName("Test")
                        .setGroupFilters(GroupFilters.newBuilder())
                        .build();

        groupRpcService.validateGroupDefinition(groupDef);

    }

    private static GroupDefinition createGroupDefinition() {
        return GroupDefinition.newBuilder().setDisplayName("test-group")
                .setStaticGroupMembers(StaticMembers
                        .newBuilder()
                        .addMembersByType(
                                StaticMembersByType
                                        .newBuilder()
                                        .addMembers(1L)
                                        .setType(VM_MEMBER_TYPE)
                                        .build())
                        .build())
                .build();
    }

    private static Grouping createGrouping(long groupId, Collection<Long> groupMemberIds) {
        return Grouping.newBuilder()
                .setId(groupId)
                .setDefinition(GroupDefinition.newBuilder()
                        .setStaticGroupMembers(createStaticMembers(groupMemberIds))
                        .build())
                .build();
    }

    private static StaticMembers createStaticMembers(Collection<Long> groupMemberIds) {
        final StaticMembersByType staticMembersByType = StaticMembersByType.newBuilder()
                .addAllMembers(groupMemberIds)
                .setType(
                        MemberType.newBuilder().setEntity(EntityType.VIRTUAL_MACHINE_VALUE).build())
                .build();

        return StaticMembers.newBuilder().addMembersByType(staticMembersByType).build();
    }


    private void givenSearchHanderWillReturn(final List<Long> oids) {
        mockDataReference.set(oids);
    }

    /**
     * Mock search service handler.
     */
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
