package com.vmturbo.group.service;

import static com.vmturbo.group.GroupMockUtil.mockEnvironment;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anySet;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import org.assertj.core.util.Lists;
import org.hamcrest.CoreMatchers;
import org.jooq.exception.DataAccessException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.jdbc.BadSqlGrammarException;

import com.vmturbo.auth.api.authorization.AuthorizationException.UserAccessScopeException;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.auth.api.authorization.scoping.EntityAccessScope;
import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.common.protobuf.common.CloudTypeEnum.CloudType;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.common.protobuf.common.Pagination.PaginationResponse;
import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.CountGroupsResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.CreateGroupRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.CreateGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredGroupsPoliciesSettings;
import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredGroupsPoliciesSettings.UploadedGroup;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupAndImmediateMembersRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupAndImmediateMembersResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsForEntitiesRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsForEntitiesResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetPaginatedGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetPaginatedGroupsResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetTagsResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.EntityFilters;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.EntityFilters.EntityFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.GroupFilters;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.OptimizationMetadata;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupID;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTO.Groupings;
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
import com.vmturbo.common.protobuf.search.Search.GroupFilter.EntityToGroupType;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.StringFilter;
import com.vmturbo.common.protobuf.search.Search.SearchEntityOidsRequest;
import com.vmturbo.common.protobuf.search.Search.SearchEntityOidsResponse;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.SearchMoles.SearchServiceMole;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.common.protobuf.search.SearchableProperties;
import com.vmturbo.common.protobuf.target.TargetDTOMoles.TargetsServiceMole;
import com.vmturbo.common.protobuf.target.TargetsServiceGrpc;
import com.vmturbo.common.protobuf.target.TargetsServiceGrpc.TargetsServiceBlockingStub;
import com.vmturbo.components.api.test.GrpcExceptionMatcher;
import com.vmturbo.components.api.test.GrpcRuntimeExceptionMatcher;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.identity.ArrayOidSet;
import com.vmturbo.components.common.identity.OidSet;
import com.vmturbo.group.DiscoveredObjectVersionIdentity;
import com.vmturbo.group.group.DiscoveredGroupHash;
import com.vmturbo.group.group.GroupDAO.DiscoveredGroupIdImpl;
import com.vmturbo.group.group.GroupEnvironment;
import com.vmturbo.group.group.GroupEnvironmentTypeResolver;
import com.vmturbo.group.group.GroupMembersPlain;
import com.vmturbo.group.group.GroupSeverityCalculator;
import com.vmturbo.group.group.IGroupStore.DiscoveredGroup;
import com.vmturbo.group.group.ProtobufMessageMatcher;
import com.vmturbo.group.group.TemporaryGroupCache;
import com.vmturbo.group.group.pagination.GroupPaginationParams;
import com.vmturbo.group.identity.IdentityProvider;
import com.vmturbo.group.policy.DiscoveredPlacementPolicyUpdater;
import com.vmturbo.group.service.GroupRpcService.InvalidGroupDefinitionException;
import com.vmturbo.group.setting.DiscoveredSettingPoliciesUpdater;
import com.vmturbo.group.stitching.GroupStitchingManager;
import com.vmturbo.group.stitching.GroupTestUtils;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;

/**
 * This class tests {@link GroupRpcService}.
 */
public class GroupRpcServiceTest {

    private static final long TARGET1 = 1001L;
    private static final long TARGET2 = 1002L;

    private AtomicReference<List<Long>> mockDataReference = new AtomicReference<>(Collections.emptyList());
    private SearchServiceMole searchServiceMole;
    private TargetsServiceMole targetsServiceMole;
    private TemporaryGroupCache temporaryGroupCache = mock(TemporaryGroupCache.class);
    private final GroupEnvironmentTypeResolver groupEnvironmentTypeResolver =
            mock(GroupEnvironmentTypeResolver.class);
    private final GroupSeverityCalculator groupSeverityCalculator =
            mock(GroupSeverityCalculator.class);

    private GroupRpcService groupRpcService;

    private UserSessionContext userSessionContext = mock(UserSessionContext.class);

    private GroupStitchingManager groupStitchingManager;

    private MockTransactionProvider transactionProvider;
    private MockGroupStore groupStoreDAO;
    private GrpcTestServer testServer;
    private IdentityProvider identityProvider;
    private DiscoveredSettingPoliciesUpdater settingPolicyUpdater;
    private DiscoveredPlacementPolicyUpdater placementPolicyUpdater;
    private GroupMemberCalculator groupMemberCalculatorSpy;
    private final GroupPaginationParams groupPaginationParams = new GroupPaginationParams(100, 500);

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

    /**
     * Capture expected exceptions in tests.
     */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Captor
    private ArgumentCaptor<Collection<DiscoveredGroup>> groupsCaptor;

    @Before
    public void setUp() throws Exception {
        searchServiceMole = Mockito.spy(new SearchServiceMole());
        targetsServiceMole = Mockito.spy(new TargetsServiceMole());
        testServer = GrpcTestServer.newServer(searchServiceMole);
        testServer.start();
        identityProvider = Mockito.spy(new IdentityProvider(0));
        groupStitchingManager = new GroupStitchingManager(identityProvider);
        final SearchServiceBlockingStub searchServiceRpc =
                SearchServiceGrpc.newBlockingStub(testServer.getChannel());
        final TargetsServiceBlockingStub targetSearchServiceRpc =
                TargetsServiceGrpc.newBlockingStub(testServer.getChannel());
        transactionProvider = new MockTransactionProvider();
        groupStoreDAO = transactionProvider.getGroupStore();
        settingPolicyUpdater = Mockito.mock(DiscoveredSettingPoliciesUpdater.class);
        placementPolicyUpdater = Mockito.mock(DiscoveredPlacementPolicyUpdater.class);
        groupMemberCalculatorSpy = spy(new GroupMemberCalculatorImpl(targetSearchServiceRpc, searchServiceRpc));
        groupRpcService = new GroupRpcService(temporaryGroupCache,
                searchServiceRpc,
                userSessionContext,
                groupStitchingManager,
                transactionProvider,
                identityProvider,
                targetSearchServiceRpc,
                settingPolicyUpdater,
                placementPolicyUpdater,
                groupMemberCalculatorSpy,
                2, 120,
                groupEnvironmentTypeResolver,
                groupSeverityCalculator,
                groupPaginationParams);
        when(temporaryGroupCache.getGrouping(anyLong())).thenReturn(Optional.empty());
        when(temporaryGroupCache.deleteGrouping(anyLong())).thenReturn(Optional.empty());
        MockitoAnnotations.initMocks(this);
    }

    /**
     * Cleanup the test environment.
     */
    @After
    public void cleanup() {
        testServer.close();
    }

    /**
     * Test {@link GroupRpcService#getGroups(GetGroupsRequest, StreamObserver)}.
     */
    @Test
    public void testGetGroups() {
        final long groupId = 1234L;
        final Grouping resultGroup = Grouping.newBuilder().setId(groupId).build();
        groupStoreDAO.addGroup(resultGroup);

        final GetGroupsRequest genericGroupsRequest = GetGroupsRequest.newBuilder()
                .setGroupFilter(GroupFilter.newBuilder().addId(groupId).build()).build();
        Mockito.when(groupStoreDAO.getGroupIds(GroupFilters.newBuilder()
                .addGroupFilter(genericGroupsRequest.getGroupFilter())
                .build())).thenReturn(Collections.singleton(groupId));
        @SuppressWarnings("unchecked")
        final StreamObserver<GroupDTO.Grouping> mockGroupingObserver =
                Mockito.mock(StreamObserver.class);
        groupRpcService.getGroups(genericGroupsRequest, mockGroupingObserver);
        Mockito.verify(mockGroupingObserver).onNext(resultGroup);
        Mockito.verify(mockGroupingObserver).onCompleted();
    }

    /**
     * Test {@link GroupRpcService#getGroups(GetGroupsRequest, StreamObserver)} and getting
     * groups using chunks.
     */
    @Test
    public void testGetGroupsByChunks() {
        final long groupId1 = 1234L;
        final Grouping resultGroup1 = Grouping.newBuilder().setId(groupId1).build();
        groupStoreDAO.addGroup(resultGroup1);
        final long groupId2 = 1235L;
        final Grouping resultGroup2 = Grouping.newBuilder().setId(groupId2).build();
        groupStoreDAO.addGroup(resultGroup2);
        final long groupId3 = 1236L;
        final Grouping resultGroup3 = Grouping.newBuilder().setId(groupId3).build();
        groupStoreDAO.addGroup(resultGroup3);

        final GetGroupsRequest genericGroupsRequest = GetGroupsRequest.newBuilder()
                .setGroupFilter(GroupFilter.newBuilder()
                        .addId(groupId1)
                        .addId(groupId2)
                        .addId(groupId3)
                        .build())
                .build();
        Mockito.when(groupStoreDAO.getGroupIds(GroupFilters.newBuilder()
                .addGroupFilter(genericGroupsRequest.getGroupFilter())
                .build())).thenReturn(Sets.newHashSet(groupId1, groupId2, groupId3));
        @SuppressWarnings("unchecked")
        final StreamObserver<GroupDTO.Grouping> mockGroupingObserver =
                Mockito.mock(StreamObserver.class);
        groupRpcService.getGroups(genericGroupsRequest, mockGroupingObserver);
        final ArgumentCaptor<Grouping> captor = ArgumentCaptor.forClass(Grouping.class);
        Mockito.verify(mockGroupingObserver, Mockito.times(3)).onNext(captor.capture());
        Mockito.verify(mockGroupingObserver).onCompleted();
        Assert.assertEquals(Sets.newHashSet(resultGroup1, resultGroup2, resultGroup3),
                new HashSet<>(captor.getAllValues()));
        Mockito.verify(groupStoreDAO, Mockito.times(2)).getGroupsById(Mockito.any());
    }

    /**
     * Tests that {@link GroupRpcService#getPaginatedGroups(GetPaginatedGroupsRequest, StreamObserver)}
     * returns a paginated response
     */
    @Test
    public void testGetPaginatedGroups() {
        // GIVEN
        final long groupId1 = 1234L;
        final Grouping resultGroup1 = Grouping.newBuilder().setId(groupId1).build();
        groupStoreDAO.addGroup(resultGroup1);
        final long groupId2 = 1235L;
        final Grouping resultGroup2 = Grouping.newBuilder().setId(groupId2).build();
        groupStoreDAO.addGroup(resultGroup2);
        final GetPaginatedGroupsRequest request = GetPaginatedGroupsRequest.newBuilder()
                .setGroupFilter(GroupFilter.getDefaultInstance())
                .setPaginationParameters(PaginationParameters.newBuilder()
                        .setAscending(true)
                        .setLimit(1)
                        .build())
                .build();
        final StreamObserver<GroupDTO.GetPaginatedGroupsResponse> mockPaginatedResponseObserver =
                Mockito.mock(StreamObserver.class);
        // WHEN
        groupRpcService.getPaginatedGroups(request, mockPaginatedResponseObserver);
        // THEN
        Mockito.verify(groupStoreDAO, Mockito.times(1)).getPaginatedGroups(any());
        final ArgumentCaptor<GroupDTO.GetPaginatedGroupsResponse> captor =
                ArgumentCaptor.forClass(GroupDTO.GetPaginatedGroupsResponse.class);
        Mockito.verify(mockPaginatedResponseObserver, Mockito.times(1)).onNext(captor.capture());
        Mockito.verify(mockPaginatedResponseObserver).onCompleted();
        Assert.assertEquals(1, captor.getValue().getGroupsCount());
        Assert.assertNotNull(captor.getValue().getPaginationResponse());
        Assert.assertEquals("1", captor.getValue().getPaginationResponse().getNextCursor());
        Assert.assertEquals(2, captor.getValue().getPaginationResponse().getTotalRecordCount());
    }

    /**
     * Tests that paginated queries for groups correctly handle user scoping limitations.
     */
    @Test
    public void testGetPaginatedGroupsForScopedUser() throws StoreOperationException {
        // GIVEN
        final long group1Id = 1L;
        final long group2Id = 2L;
        final List<Long> groupIds = new ArrayList<>();
        groupIds.add(group1Id);
        groupIds.add(group2Id);
        final long group1MemberId = 11L;
        final long group2MemberId = 22L;
        final Set<Long> group1Members = Collections.singleton(group1MemberId);
        final Set<Long> group2Members = Collections.singleton(group2MemberId);
        final Grouping grouping1 = createGrouping(group1Id, group1Members);
        final Grouping grouping2 = createGrouping(group2Id, group2Members);
        groupStoreDAO.addGroup(grouping1);
        groupStoreDAO.createGroupSupplementaryInfo(group1Id, false,
                mockEnvironment(EnvironmentType.ON_PREM, CloudType.UNKNOWN_CLOUD),
                Severity.NORMAL);
        groupStoreDAO.addGroup(grouping2);
        groupStoreDAO.createGroupSupplementaryInfo(group2Id, false,
                mockEnvironment(EnvironmentType.ON_PREM, CloudType.UNKNOWN_CLOUD),
                Severity.MINOR);
        final EntityAccessScope accessScope =
                new EntityAccessScope(null, null,
                        new ArrayOidSet(Collections.singletonList(group1MemberId)),
                        null);
        final GetPaginatedGroupsRequest request = GetPaginatedGroupsRequest.newBuilder()
                .setGroupFilter(GroupFilter.getDefaultInstance())
                .setPaginationParameters(PaginationParameters.getDefaultInstance())
                .build();
        when(userSessionContext.isUserScoped()).thenReturn(true);
        when(userSessionContext.getUserAccessScope()).thenReturn(accessScope);
        Map<Long, Set<Long>> entityGroups = new HashMap<>();
        entityGroups.put(0L, Collections.singleton(group1Id));
        doReturn(entityGroups).when(groupMemberCalculatorSpy).getEntityGroups(groupStoreDAO,
                group1Members, Collections.emptySet());
        doReturn(group1Members).when(groupMemberCalculatorSpy).getGroupMembers(groupStoreDAO,
                Collections.singleton(group1Id), true);
        doReturn(group2Members).when(groupMemberCalculatorSpy).getGroupMembers(groupStoreDAO,
                Collections.singleton(group2Id), true);
        doReturn(Collections.emptySet()).when(groupMemberCalculatorSpy)
                .getEmptyGroupIds(groupStoreDAO);
        when(groupStoreDAO.getOrderedGroupIds(any(), any())).thenReturn(groupIds);
        final StreamObserver<GetPaginatedGroupsResponse> mockObserver =
                Mockito.mock(StreamObserver.class);
        // WHEN
        groupRpcService.getPaginatedGroups(request, mockObserver);
        // THEN
        ArgumentCaptor<GetPaginatedGroupsResponse> captor =
                ArgumentCaptor.forClass(GetPaginatedGroupsResponse.class);
        verify(mockObserver, times(1)).onNext(captor.capture());
        GetPaginatedGroupsResponse response = captor.getValue();
        PaginationResponse paginationResponse = response.getPaginationResponse();
        assertEquals(1, paginationResponse.getTotalRecordCount());
        assertFalse(paginationResponse.hasNextCursor());
        List<Grouping> groupingList = response.getGroupsList();
        assertEquals(1, groupingList.size());
        assertEquals(group1Id, groupingList.get(0).getId());
        verify(mockObserver).onCompleted();
    }

    /**
     * Tests that paginated queries for groups correctly handle query scope limitations.
     */
    @Test
    public void testGetPaginatedGroupsForScopedRequest() throws StoreOperationException {
        // GIVEN
        final long group1Id = 1L;
        final long group2Id = 2L;
        final List<Long> groupIds = new ArrayList<>();
        groupIds.add(group1Id);
        groupIds.add(group2Id);
        final long group1MemberId = 11L;
        final long group2MemberId = 22L;
        final Set<Long> group1Members = Collections.singleton(group1MemberId);
        final Set<Long> group2Members = Collections.singleton(group2MemberId);
        final Grouping grouping1 = createGrouping(group1Id, group1Members);
        final Grouping grouping2 = createGrouping(group2Id, group2Members);
        groupStoreDAO.addGroup(grouping1);
        groupStoreDAO.createGroupSupplementaryInfo(group1Id, false,
                mockEnvironment(EnvironmentType.ON_PREM, CloudType.UNKNOWN_CLOUD),
                Severity.NORMAL);
        groupStoreDAO.addGroup(grouping2);
        groupStoreDAO.createGroupSupplementaryInfo(group2Id, false,
                mockEnvironment(EnvironmentType.ON_PREM, CloudType.UNKNOWN_CLOUD),
                Severity.MINOR);
        final EntityAccessScope accessScope =
                new EntityAccessScope(Collections.singleton(group1Id), null,
                        new ArrayOidSet(Collections.singletonList(group1MemberId)),
                        null);
        final GetPaginatedGroupsRequest request = GetPaginatedGroupsRequest.newBuilder()
                .setGroupFilter(GroupFilter.getDefaultInstance())
                .addScopes(group1Id)
                .setPaginationParameters(PaginationParameters.getDefaultInstance())
                .build();
        when(userSessionContext.isUserScoped()).thenReturn(false);
        when(userSessionContext.getAccessScope(Collections.singletonList(group1Id)))
                .thenReturn(accessScope);
        Map<Long, Set<Long>> entityGroups = new HashMap<>();
        entityGroups.put(0L, Collections.singleton(group1Id));
        doReturn(entityGroups).when(groupMemberCalculatorSpy).getEntityGroups(groupStoreDAO,
                group1Members, Collections.emptySet());
        doReturn(group1Members).when(groupMemberCalculatorSpy).getGroupMembers(groupStoreDAO,
                Collections.singleton(group1Id), true);
        doReturn(group2Members).when(groupMemberCalculatorSpy).getGroupMembers(groupStoreDAO,
                Collections.singleton(group2Id), true);
        doReturn(Collections.emptySet()).when(groupMemberCalculatorSpy)
                .getEmptyGroupIds(groupStoreDAO);
        when(groupStoreDAO.getOrderedGroupIds(any(), any())).thenReturn(groupIds);
        final StreamObserver<GetPaginatedGroupsResponse> mockObserver =
                Mockito.mock(StreamObserver.class);
        // WHEN
        groupRpcService.getPaginatedGroups(request, mockObserver);
        // THEN
        ArgumentCaptor<GetPaginatedGroupsResponse> captor =
                ArgumentCaptor.forClass(GetPaginatedGroupsResponse.class);
        verify(mockObserver, times(1)).onNext(captor.capture());
        GetPaginatedGroupsResponse response = captor.getValue();
        PaginationResponse paginationResponse = response.getPaginationResponse();
        assertEquals(1, paginationResponse.getTotalRecordCount());
        assertFalse(paginationResponse.hasNextCursor());
        List<Grouping> groupingList = response.getGroupsList();
        assertEquals(1, groupingList.size());
        assertEquals(group1Id, groupingList.get(0).getId());
        verify(mockObserver).onCompleted();
    }

    /**
     * Tests that paginated queries for groups correctly handle the combination of user and query
     * scope limitations, as well as empty groups.
     * Test has 5 groups: user scope contains groups 1 & 2, request scope contains groups 2 & 3,
     * and group5 is empty (so implicitly part of any scope)
     */
    @Test
    public void testGetPaginatedGroupsForComplexScopedCases() throws StoreOperationException {
        // GIVEN
        final long group1Id = 1L;
        final long group2Id = 2L;
        final long group3Id = 3L;
        final long group4Id = 4L;
        final long group5Id = 5L;
        final long requestScopeId = 100L;
        final List<Long> groupIds = new ArrayList<>();
        groupIds.add(group1Id);
        groupIds.add(group2Id);
        groupIds.add(group3Id);
        groupIds.add(group4Id);
        groupIds.add(group5Id);
        final long group1MemberId = 11L;
        final long group2MemberId = 22L;
        final long group3MemberId = 33L;
        final long group4MemberId = 44L;
        final Set<Long> group1Members = Collections.singleton(group1MemberId);
        final Set<Long> group2Members = Collections.singleton(group2MemberId);
        final Set<Long> group3Members = Collections.singleton(group3MemberId);
        final Set<Long> group4Members = Collections.singleton(group4MemberId);
        final Grouping grouping1 = createGrouping(group1Id, group1Members);
        final Grouping grouping2 = createGrouping(group2Id, group2Members);
        final Grouping grouping3 = createGrouping(group3Id, group3Members);
        final Grouping grouping4 = createGrouping(group4Id, group4Members);
        // group5 is empty
        final Grouping grouping5 = createGrouping(group5Id, Collections.emptySet());
        groupStoreDAO.addGroup(grouping1);
        groupStoreDAO.createGroupSupplementaryInfo(group1Id, false,
                mockEnvironment(EnvironmentType.ON_PREM, CloudType.UNKNOWN_CLOUD),
                Severity.NORMAL);
        groupStoreDAO.addGroup(grouping2);
        groupStoreDAO.createGroupSupplementaryInfo(group2Id, false,
                mockEnvironment(EnvironmentType.ON_PREM, CloudType.UNKNOWN_CLOUD),
                Severity.MINOR);
        groupStoreDAO.addGroup(grouping3);
        groupStoreDAO.createGroupSupplementaryInfo(group3Id, false,
                mockEnvironment(EnvironmentType.ON_PREM, CloudType.UNKNOWN_CLOUD),
                Severity.MAJOR);
        groupStoreDAO.addGroup(grouping4);
        groupStoreDAO.createGroupSupplementaryInfo(group4Id, false,
                mockEnvironment(EnvironmentType.ON_PREM, CloudType.UNKNOWN_CLOUD),
                Severity.CRITICAL);
        groupStoreDAO.addGroup(grouping5);
        groupStoreDAO.createGroupSupplementaryInfo(group5Id, true,
                mockEnvironment(EnvironmentType.ON_PREM, CloudType.UNKNOWN_CLOUD),
                Severity.NORMAL);
        final EntityAccessScope userAccessScope =
                new EntityAccessScope(null, null,
                        new ArrayOidSet(Arrays.asList(group1MemberId, group2MemberId)),
                        null);
        final EntityAccessScope requestAccessScope =
                new EntityAccessScope(null, null,
                        new ArrayOidSet(Arrays.asList(group2MemberId, group3MemberId)),
                        null);
        PaginationParameters.Builder paginationParameters = PaginationParameters.newBuilder()
                .setLimit(1);
        GetPaginatedGroupsRequest.Builder requestBuilder = GetPaginatedGroupsRequest.newBuilder()
                .setGroupFilter(GroupFilter.getDefaultInstance())
                .addScopes(requestScopeId)
                .setPaginationParameters(paginationParameters);
        when(userSessionContext.isUserScoped()).thenReturn(true);
        when(userSessionContext.getUserAccessScope()).thenReturn(userAccessScope);
        when(userSessionContext.getAccessScope(Collections.singletonList(requestScopeId)))
                .thenReturn(requestAccessScope);
        Set<Long> groupsInScope = new HashSet<>();
        groupsInScope.add(group1Id);
        groupsInScope.add(group2Id);
        groupsInScope.add(group3Id);
        Map<Long, Set<Long>> entityGroups = new HashMap<>();
        entityGroups.put(0L, groupsInScope);
        doReturn(entityGroups).when(groupMemberCalculatorSpy).getEntityGroups(groupStoreDAO,
                group1Members, Collections.emptySet());
        doReturn(group1Members).when(groupMemberCalculatorSpy).getGroupMembers(groupStoreDAO,
                Collections.singleton(group1Id), true);
        doReturn(group2Members).when(groupMemberCalculatorSpy).getGroupMembers(groupStoreDAO,
                Collections.singleton(group2Id), true);
        doReturn(group3Members).when(groupMemberCalculatorSpy).getGroupMembers(groupStoreDAO,
                Collections.singleton(group3Id), true);
        doReturn(Collections.singleton(group5Id)).when(groupMemberCalculatorSpy)
                .getEmptyGroupIds(groupStoreDAO);
        when(groupStoreDAO.getOrderedGroupIds(any(), any())).thenReturn(groupIds);
        final StreamObserver<GetPaginatedGroupsResponse> mockObserver =
                Mockito.mock(StreamObserver.class);
        // WHEN
        groupRpcService.getPaginatedGroups(requestBuilder.build(), mockObserver);
        // THEN
        ArgumentCaptor<GetPaginatedGroupsResponse> captor =
                ArgumentCaptor.forClass(GetPaginatedGroupsResponse.class);
        verify(mockObserver, times(1)).onNext(captor.capture());
        GetPaginatedGroupsResponse response = captor.getValue();
        PaginationResponse paginationResponse = response.getPaginationResponse();
        assertEquals(2, paginationResponse.getTotalRecordCount());
        assertEquals("1", paginationResponse.getNextCursor());
        List<Grouping> groupingList = response.getGroupsList();
        assertEquals(1, groupingList.size());
        assertEquals(group2Id, groupingList.get(0).getId());
        verify(mockObserver).onCompleted();

        // PREPARATION FOR NEXT CALL
        paginationParameters.setCursor("1");
        requestBuilder.setPaginationParameters(paginationParameters);
        when(groupStoreDAO.getOrderedGroupIds(any(), any())).thenReturn(groupIds);
        // WHEN
        groupRpcService.getPaginatedGroups(requestBuilder.build(), mockObserver);
        // THEN
        verify(mockObserver, times(2)).onNext(captor.capture());
        response = captor.getValue();
        paginationResponse = response.getPaginationResponse();
        assertEquals(2, paginationResponse.getTotalRecordCount());
        assertFalse(paginationResponse.hasNextCursor());
        groupingList = response.getGroupsList();
        assertEquals(1, groupingList.size());
        assertEquals(group5Id, groupingList.get(0).getId());
        verify(mockObserver, times(2)).onCompleted();
    }

    /**
     * Tests that paginated queries for groups correctly handle user scoping limitations, including
     * the case where there are nested groups that the user is allowed to see.
     */
    @Test
    public void testGetPaginatedGroupsForScopedUserWithNestedGroups()
            throws StoreOperationException {
        // GIVEN
        final long group1Id = 1L;
        final long group2Id = 2L;
        final long group3Id = 3L;
        final List<Long> groupIds = new ArrayList<>();
        groupIds.add(group1Id);
        groupIds.add(group2Id);
        groupIds.add(group3Id);
        final long group1MemberId = 11L;
        final long group2MemberId = 22L;
        final Set<Long> group1Members = Collections.singleton(group1MemberId);
        final Set<Long> group2Members = Collections.singleton(group2MemberId);
        // group 3 is nested; it contains group1
        final Set<Long> group3Members = Collections.singleton(group1Id);
        final Grouping grouping1 = createGrouping(group1Id, group1Members);
        final Grouping grouping2 = createGrouping(group2Id, group2Members);
        final Grouping grouping3 = createGrouping(group3Id, group3Members);
        groupStoreDAO.addGroup(grouping1);
        groupStoreDAO.createGroupSupplementaryInfo(group1Id, false,
                mockEnvironment(EnvironmentType.ON_PREM, CloudType.UNKNOWN_CLOUD),
                Severity.NORMAL);
        groupStoreDAO.addGroup(grouping2);
        groupStoreDAO.createGroupSupplementaryInfo(group2Id, false,
                mockEnvironment(EnvironmentType.ON_PREM, CloudType.UNKNOWN_CLOUD),
                Severity.MINOR);
        groupStoreDAO.addGroup(grouping3);
        groupStoreDAO.createGroupSupplementaryInfo(group3Id, false,
                mockEnvironment(EnvironmentType.ON_PREM, CloudType.UNKNOWN_CLOUD),
                Severity.NORMAL);
        final EntityAccessScope accessScope =
                new EntityAccessScope(null, null,
                        new ArrayOidSet(Collections.singletonList(group1MemberId)),
                        null);
        final GetPaginatedGroupsRequest request = GetPaginatedGroupsRequest.newBuilder()
                .setGroupFilter(GroupFilter.getDefaultInstance())
                .setPaginationParameters(PaginationParameters.getDefaultInstance())
                .build();
        when(userSessionContext.isUserScoped()).thenReturn(true);
        when(userSessionContext.getUserAccessScope()).thenReturn(accessScope);
        Map<Long, Set<Long>> entityGroups = new HashMap<>();
        entityGroups.put(0L, Collections.singleton(group1Id));
        doReturn(entityGroups).when(groupMemberCalculatorSpy).getEntityGroups(groupStoreDAO,
                group1Members, Collections.emptySet());
        doReturn(group1Members).when(groupMemberCalculatorSpy).getGroupMembers(groupStoreDAO,
                Collections.singleton(group1Id), true);
        doReturn(group2Members).when(groupMemberCalculatorSpy).getGroupMembers(groupStoreDAO,
                Collections.singleton(group2Id), true);
        doReturn(group1Members).when(groupMemberCalculatorSpy).getGroupMembers(groupStoreDAO,
                Collections.singleton(group3Id), true);
        doReturn(Collections.emptySet()).when(groupMemberCalculatorSpy)
                .getEmptyGroupIds(groupStoreDAO);
        // 2nd call, for nested groups
        Map<Long, Set<Long>> parentGroups = new HashMap<>();
        parentGroups.put(0L, Collections.singleton(group3Id));
        doReturn(parentGroups).when(groupMemberCalculatorSpy).getEntityGroups(groupStoreDAO,
                group3Members, Collections.emptySet());
        when(groupStoreDAO.getOrderedGroupIds(any(), any())).thenReturn(groupIds);
        final StreamObserver<GetPaginatedGroupsResponse> mockObserver =
                Mockito.mock(StreamObserver.class);
        // WHEN
        groupRpcService.getPaginatedGroups(request, mockObserver);
        // THEN
        ArgumentCaptor<GetPaginatedGroupsResponse> captor =
                ArgumentCaptor.forClass(GetPaginatedGroupsResponse.class);
        verify(mockObserver, times(1)).onNext(captor.capture());
        GetPaginatedGroupsResponse response = captor.getValue();
        PaginationResponse paginationResponse = response.getPaginationResponse();
        assertEquals(2, paginationResponse.getTotalRecordCount());
        assertFalse(paginationResponse.hasNextCursor());
        List<Grouping> groupingList = response.getGroupsList();
        assertEquals(2, groupingList.size());
        assertEquals(group1Id, groupingList.get(0).getId());
        assertEquals(group3Id, groupingList.get(1).getId());
        verify(mockObserver).onCompleted();
    }

    /**
     * Tests that paginated queries for groups correctly handle user scoping limitations when the
     * user requests for specific group(s) which belong to the scope.
     */
    @Test
    public void testGetPaginatedGroupsForScopedUserWithSpecificGroupIdsRequested()
            throws StoreOperationException {
        // GIVEN
        final long group1Id = 1L;
        final long group2Id = 2L;
        final long group3Id = 3L;
        final long group1MemberId = 11L;
        final long group2MemberId = 22L;
        final long group3MemberId = 33L;
        final Set<Long> group1Members = Collections.singleton(group1MemberId);
        final Set<Long> group2Members = Collections.singleton(group2MemberId);
        final Set<Long> group3Members = Collections.singleton(group3MemberId);
        final Grouping grouping1 = createGrouping(group1Id, group1Members);
        final Grouping grouping2 = createGrouping(group2Id, group2Members);
        final Grouping grouping3 = createGrouping(group3Id, group3Members);
        groupStoreDAO.addGroup(grouping1);
        groupStoreDAO.createGroupSupplementaryInfo(group1Id, false,
                mockEnvironment(EnvironmentType.ON_PREM, CloudType.UNKNOWN_CLOUD),
                Severity.NORMAL);
        groupStoreDAO.addGroup(grouping2);
        groupStoreDAO.createGroupSupplementaryInfo(group2Id, false,
                mockEnvironment(EnvironmentType.ON_PREM, CloudType.UNKNOWN_CLOUD),
                Severity.MINOR);
        groupStoreDAO.addGroup(grouping3);
        groupStoreDAO.createGroupSupplementaryInfo(group3Id, false,
                mockEnvironment(EnvironmentType.ON_PREM, CloudType.UNKNOWN_CLOUD),
                Severity.MAJOR);
        // group1 & group3 in scope
        final EntityAccessScope accessScope =
                new EntityAccessScope(null, null,
                        new ArrayOidSet(Arrays.asList(group1MemberId, group3MemberId)),
                        null);
        final GetPaginatedGroupsRequest request = GetPaginatedGroupsRequest.newBuilder()
                .setGroupFilter(GroupFilter.newBuilder()
                        // user requests for group3
                        .addId(group3Id)
                        .build())
                .setPaginationParameters(PaginationParameters.getDefaultInstance())
                .build();
        when(userSessionContext.isUserScoped()).thenReturn(true);
        when(userSessionContext.getUserAccessScope()).thenReturn(accessScope);
        Map<Long, Set<Long>> entityGroups = new HashMap<>();
        entityGroups.put(0L, Collections.singleton(group1Id));
        doReturn(entityGroups).when(groupMemberCalculatorSpy).getEntityGroups(groupStoreDAO,
                group1Members, Collections.emptySet());
        doReturn(group1Members).when(groupMemberCalculatorSpy).getGroupMembers(groupStoreDAO,
                Collections.singleton(group1Id), true);
        doReturn(group2Members).when(groupMemberCalculatorSpy).getGroupMembers(groupStoreDAO,
                Collections.singleton(group2Id), true);
        doReturn(group3Members).when(groupMemberCalculatorSpy).getGroupMembers(groupStoreDAO,
                Collections.singleton(group3Id), true);
        doReturn(Collections.emptySet()).when(groupMemberCalculatorSpy)
                .getEmptyGroupIds(groupStoreDAO);
        when(groupStoreDAO.getOrderedGroupIds(any(), any()))
                .thenReturn(Collections.singletonList(group3Id));
        final StreamObserver<GetPaginatedGroupsResponse> mockObserver =
                Mockito.mock(StreamObserver.class);
        // WHEN
        groupRpcService.getPaginatedGroups(request, mockObserver);
        // THEN
        ArgumentCaptor<GetPaginatedGroupsResponse> captor =
                ArgumentCaptor.forClass(GetPaginatedGroupsResponse.class);
        verify(mockObserver, times(1)).onNext(captor.capture());
        GetPaginatedGroupsResponse response = captor.getValue();
        PaginationResponse paginationResponse = response.getPaginationResponse();
        assertEquals(1, paginationResponse.getTotalRecordCount());
        assertFalse(paginationResponse.hasNextCursor());
        List<Grouping> groupingList = response.getGroupsList();
        assertEquals(1, groupingList.size());
        assertEquals(group3Id, groupingList.get(0).getId());
        verify(mockObserver).onCompleted();
    }

    /**
     * Tests that paginated queries for groups correctly handle user scoping limitations when the
     * user requests for specific group(s) that doesn't have access to.
     */
    @Test(expected = UserAccessScopeException.class)
    public void testGetPaginatedGroupsForScopedUserWithRequestedGroupIdsOutOfScope()
            throws StoreOperationException {
        // GIVEN
        final long group1Id = 1L;
        final long group2Id = 2L;
        final long group3Id = 3L;
        final long group1MemberId = 11L;
        final long group2MemberId = 22L;
        final long group3MemberId = 33L;
        final Set<Long> group1Members = Collections.singleton(group1MemberId);
        final Set<Long> group2Members = Collections.singleton(group2MemberId);
        final Set<Long> group3Members = Collections.singleton(group3MemberId);
        final Grouping grouping1 = createGrouping(group1Id, group1Members);
        final Grouping grouping2 = createGrouping(group2Id, group2Members);
        final Grouping grouping3 = createGrouping(group3Id, group3Members);
        groupStoreDAO.addGroup(grouping1);
        groupStoreDAO.createGroupSupplementaryInfo(group1Id, false,
                mockEnvironment(EnvironmentType.ON_PREM, CloudType.UNKNOWN_CLOUD),
                Severity.NORMAL);
        groupStoreDAO.addGroup(grouping2);
        groupStoreDAO.createGroupSupplementaryInfo(group2Id, false,
                mockEnvironment(EnvironmentType.ON_PREM, CloudType.UNKNOWN_CLOUD),
                Severity.MINOR);
        groupStoreDAO.addGroup(grouping3);
        groupStoreDAO.createGroupSupplementaryInfo(group3Id, false,
                mockEnvironment(EnvironmentType.ON_PREM, CloudType.UNKNOWN_CLOUD),
                Severity.MAJOR);
        // group1 in scope
        final EntityAccessScope accessScope =
                new EntityAccessScope(null, null,
                        new ArrayOidSet(Collections.singletonList(group1MemberId)), null);
        final GetPaginatedGroupsRequest request = GetPaginatedGroupsRequest.newBuilder()
                .setGroupFilter(GroupFilter.newBuilder()
                        // user requests for group1 and group3, but group3 is out of scope
                        .addId(group1Id)
                        .addId(group3Id)
                        .build())
                .setPaginationParameters(PaginationParameters.getDefaultInstance())
                .build();
        when(userSessionContext.isUserScoped()).thenReturn(true);
        when(userSessionContext.getUserAccessScope()).thenReturn(accessScope);
        Map<Long, Set<Long>> entityGroups = new HashMap<>();
        entityGroups.put(0L, Collections.singleton(group1Id));
        doReturn(entityGroups).when(groupMemberCalculatorSpy).getEntityGroups(groupStoreDAO,
                group1Members, Collections.emptySet());
        doReturn(group1Members).when(groupMemberCalculatorSpy).getGroupMembers(groupStoreDAO,
                Collections.singleton(group1Id), true);
        doReturn(group2Members).when(groupMemberCalculatorSpy).getGroupMembers(groupStoreDAO,
                Collections.singleton(group2Id), true);
        doReturn(group3Members).when(groupMemberCalculatorSpy).getGroupMembers(groupStoreDAO,
                Collections.singleton(group3Id), true);
        doReturn(Collections.emptySet()).when(groupMemberCalculatorSpy)
                .getEmptyGroupIds(groupStoreDAO);
        when(groupStoreDAO.getOrderedGroupIds(any(), any()))
                .thenReturn(Arrays.asList(group1Id, group3Id));
        final StreamObserver<GetPaginatedGroupsResponse> mockObserver =
                Mockito.mock(StreamObserver.class);
        // WHEN
        groupRpcService.getPaginatedGroups(request, mockObserver);
        // THEN
        // exception expected
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
        Mockito.when(groupStoreDAO.getGroupIds(Mockito.any()))
                .thenReturn(Collections.singletonList(groupId));
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
     * Test {@link GroupRpcService#getGroupsForEntities(GetGroupsForEntitiesRequest,
     * StreamObserver)}.
     */
    @Test
    public void testGetGroupForEntity() {
        final long entityId = 1234L;
        final long entityId2 = 1235L;
        final Set<Long> listOfGroups = Sets.newHashSet(2000L, 2001L);
        final GetGroupsForEntitiesRequest groupForEntityRequest = GetGroupsForEntitiesRequest.newBuilder()
                .addEntityId(entityId)
                .addEntityId(entityId2)
                .build();
        final StreamObserver<GroupDTO.GetGroupsForEntitiesResponse> mockGroupForEntityObserver =
                Mockito.mock(StreamObserver.class);
        final GetGroupsForEntitiesResponse entityResponse =
                GetGroupsForEntitiesResponse.newBuilder()
                        .putEntityGroup(entityId,
                                Groupings.newBuilder().addAllGroupId(listOfGroups).build())
                        .build();
        Mockito.when(groupStoreDAO.getStaticGroupsForEntities(Mockito.anyCollectionOf(Long.class),
                Mockito.anyCollectionOf(GroupType.class)))
                .thenReturn(Collections.singletonMap(entityId, listOfGroups));
        groupRpcService.getGroupsForEntities(groupForEntityRequest, mockGroupForEntityObserver);
        Mockito.verify(mockGroupForEntityObserver)
                .onNext(Mockito.argThat(new GetGroupsForEntitiesResponseMatcher(entityResponse)));
        Mockito.verify(mockGroupForEntityObserver).onCompleted();
    }

    /**
     * Test {@link GroupRpcService#getGroupsForEntities(GetGroupsForEntitiesRequest,
     * StreamObserver)}.
     * when GetGroupsForEntitiesRequest doesn't contain entityId.
     */
    @Test
    public void testGetGroupForEntityExceptionCase() {
        final ArgumentCaptor<StatusException> exceptionCaptor =
                ArgumentCaptor.forClass(StatusException.class);
        // request without entityId
        final GetGroupsForEntitiesRequest groupForEntityRequest =
                GetGroupsForEntitiesRequest.getDefaultInstance();
        final StreamObserver<GroupDTO.GetGroupsForEntitiesResponse> mockGroupForEntityObserver =
                Mockito.mock(StreamObserver.class);
        groupRpcService.getGroupsForEntities(groupForEntityRequest, mockGroupForEntityObserver);
        Mockito.verify(mockGroupForEntityObserver)
                .onNext(GetGroupsForEntitiesResponse.newBuilder().build());
        Mockito.verify(mockGroupForEntityObserver).onCompleted();
    }

    /**
     * Test case when user have access to requested entity.
     *
     * @throws StoreOperationException if something goes wrong.
     */
    @Test
    public void testGetGroupForEntityWhenUserHaveAccess() throws StoreOperationException {
        final long entityId = 1234L;
        final Set<Long> setOfGroups = Sets.newHashSet(2000L, 20003L);
        final GetGroupsForEntitiesRequest groupForEntityRequest =
                GetGroupsForEntitiesRequest.newBuilder().addEntityId(entityId).build();
        final StreamObserver<GetGroupsForEntitiesResponse> mockGroupForEntityObserver =
                Mockito.mock(StreamObserver.class);
        final GetGroupsForEntitiesResponse entityResponse = GetGroupsForEntitiesResponse.newBuilder()
                .putEntityGroup(entityId, Groupings.newBuilder().addAllGroupId(setOfGroups).build())
                .build();
        Mockito.when(groupMemberCalculatorSpy.getEntityGroups(groupStoreDAO,
            Collections.singleton(entityId), Collections.emptySet()))
                .thenReturn(Collections.singletonMap(entityId, setOfGroups));
        final EntityAccessScope accessScope =
                new EntityAccessScope(null, null, new ArrayOidSet(Collections.singletonList(entityId)),
                        null);
        when(userSessionContext.isUserScoped()).thenReturn(true);
        when(userSessionContext.getUserAccessScope()).thenReturn(accessScope);
        groupRpcService.getGroupsForEntities(groupForEntityRequest, mockGroupForEntityObserver);
        Mockito.verify(mockGroupForEntityObserver)
                .onNext(Mockito.argThat(new GetGroupsForEntitiesResponseMatcher(entityResponse)));
        Mockito.verify(mockGroupForEntityObserver).onCompleted();
    }

    /**
     * Test case when user have restrict access to entities.
     */
    @Test(expected = UserAccessScopeException.class)
    public void testGetGroupForEntityWhenUserRestrict() {
        final long requestedEntityId = 1L;
        final long allowedEntityId = 2L;
        final GetGroupsForEntitiesRequest groupForEntityRequest =
                GetGroupsForEntitiesRequest.newBuilder().addEntityId(requestedEntityId).build();
        final StreamObserver<GroupDTO.GetGroupsForEntitiesResponse> mockGroupForEntityObserver =
                Mockito.mock(StreamObserver.class);
        final Set<Long> setOfGroups = Sets.newHashSet(2000L, 20003L);
        final GetGroupsForEntitiesResponse entityResponse = GetGroupsForEntitiesResponse.newBuilder()
                .putEntityGroup(requestedEntityId,
                        Groupings.newBuilder().addAllGroupId(setOfGroups).build())
                .build();
        Mockito.when(groupStoreDAO.getStaticGroupsForEntities(Mockito.any(), Mockito.any()))
                .thenReturn(Collections.singletonMap(requestedEntityId, setOfGroups));
        final EntityAccessScope accessScope =
                new EntityAccessScope(null, null, new ArrayOidSet(Collections.singletonList(allowedEntityId)),
                        null);
        when(userSessionContext.isUserScoped()).thenReturn(true);
        when(userSessionContext.getUserAccessScope()).thenReturn(accessScope);
        groupRpcService.getGroupsForEntities(groupForEntityRequest, mockGroupForEntityObserver);
        Mockito.verify(mockGroupForEntityObserver)
                .onNext(Mockito.argThat(new GetGroupsForEntitiesResponseMatcher(entityResponse)));
        Mockito.verify(mockGroupForEntityObserver).onCompleted();
    }

    /**
     * Test {@link GroupRpcService#getGroupsForEntities(GetGroupsForEntitiesRequest, StreamObserver)}
     * when user doesn't have access to all group members and accordingly does not have access to
     * the group. User does not have access to the requested group - the group is not returned.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testGetGroupForEntityWhenGroupAccessDenied() throws Exception {
        final Set<Long> groups = Collections.singleton(5L);
        final long requestedEntityId = 1L;
        final Collection<Long> allowedEntityId = Arrays.asList(1L, 2L, 3L);
        final Set<Long> groupMembers = Sets.newHashSet(1L, 2L, 3L, 4L);
        final GetGroupsForEntitiesRequest groupForEntityRequest =
                GetGroupsForEntitiesRequest.newBuilder().addEntityId(requestedEntityId).build();
        final StreamObserver<GroupDTO.GetGroupsForEntitiesResponse> mockGroupForEntityObserver =
                Mockito.mock(StreamObserver.class);
        final GetGroupsForEntitiesResponse entityResponse = GetGroupsForEntitiesResponse.newBuilder()
                .putEntityGroup(requestedEntityId, Groupings.newBuilder().build())
                .build();
        Mockito.when(groupStoreDAO.getStaticGroupsForEntities(Mockito.any(), Mockito.any()))
                .thenReturn(Collections.singletonMap(requestedEntityId, groups));
        Mockito.when(groupStoreDAO.getMembers(Mockito.any(), anyBoolean()))
                .thenReturn(new GroupMembersPlain(groupMembers, Collections.emptySet(),
                        Collections.emptySet()));
        final EntityAccessScope accessScope =
                new EntityAccessScope(null, null, new ArrayOidSet(allowedEntityId), null);
        when(userSessionContext.isUserScoped()).thenReturn(true);
        when(userSessionContext.getUserAccessScope()).thenReturn(accessScope);
        groupRpcService.getGroupsForEntities(groupForEntityRequest, mockGroupForEntityObserver);
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
                .updateDiscoveredGroups(Mockito.anyCollection(), Mockito.anyList(),
                        Mockito.anySet());
        Mockito.verify(responseStreamObserver).onError(exceptionCaptor.capture());
        final StatusException exception = exceptionCaptor.getValue();
        Assert.assertThat(exception, GrpcExceptionMatcher.hasCode(Code.INVALID_ARGUMENT)
                .descriptionContains("Request must have a target ID"));
    }

    /**
     * Test that temporary groups get returned when requested in the "getGroups" method.
     */
    @Test
    public void testGetGroupsIncludeTempGroup() {
        final Grouping firstGrouping = createGrouping(1L, Collections.singleton(11L));
        when(temporaryGroupCache.getGrouping(1L)).thenReturn(Optional.of(firstGrouping));
        final GetGroupsRequest genericGroupsRequest = GetGroupsRequest
            .newBuilder()
            .setGroupFilter(GroupFilter.newBuilder()
                .addId(firstGrouping.getId())
                .setIncludeTemporary(true))
            .build();
        final StreamObserver<GroupDTO.Grouping> mockGroupingObserver =
            Mockito.mock(StreamObserver.class);
        groupRpcService.getGroups(genericGroupsRequest, mockGroupingObserver);
        Mockito.verify(mockGroupingObserver).onNext(firstGrouping);
        Mockito.verify(mockGroupingObserver).onCompleted();
    }

    /**
     * Test when user request "all" groups (without certain groupIds) but have access only to
     * entities from one group. In this case will be filtered results and returned only
     * accessible ones.
     *
     * @throws Exception on exception occurred.
     */
    @Test
    public void testGetGroupsWhenUserScoped() throws Exception {
        final long firstGroupId = 1L;
        final long secondGroupId = 2L;
        final long firstGroupMember = 11L;
        final long secondGroupMember = 12L;
        final Collection<Long>  firstGroupMembers =  Collections.singletonList(firstGroupMember);
        final Collection<Long> secondGroupMembers = Collections.singletonList(secondGroupMember);

        final Grouping firstGrouping = createGrouping(firstGroupId, firstGroupMembers);
        final Grouping secondGrouping = createGrouping(secondGroupId, secondGroupMembers);
        groupStoreDAO.addGroup(firstGrouping);
        groupStoreDAO.addGroup(secondGrouping);

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
        Mockito.when(groupStoreDAO.getGroupIds(GroupFilters.newBuilder()
                .addGroupFilter(genericGroupsRequest.getGroupFilter())
                .build())).thenReturn(Arrays.asList(firstGroupId, secondGroupId));
        Mockito.when(groupStoreDAO.getMembers(Collections.singleton(firstGroupId), true))
                .thenReturn(new GroupMembersPlain(Collections.singleton(firstGroupMember),
                        Collections.emptySet(), Collections.emptySet()));
        Mockito.when(groupStoreDAO.getMembers(Collections.singleton(secondGroupId), true))
                .thenReturn(new GroupMembersPlain(Collections.singleton(secondGroupMember),
                        Collections.emptySet(), Collections.emptySet()));

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
        groupStoreDAO.addGroup(firstGrouping);
        groupStoreDAO.addGroup(secondGrouping);

        final StreamObserver<GroupDTO.Grouping> mockGroupingObserver =
                Mockito.mock(StreamObserver.class);
        final GetGroupsRequest genericGroupsRequest = GetGroupsRequest
                .newBuilder()
                .setGroupFilter(GroupFilter.newBuilder().addAllId(Arrays.asList(firstGroupId,
                        secondGroupId))).build();
        Mockito.when(groupStoreDAO.getGroupIds(GroupFilters.newBuilder()
                .addGroupFilter(genericGroupsRequest.getGroupFilter())
                .build())).thenReturn(Sets.newHashSet(firstGroupId, secondGroupId));
        when(userSessionContext.isUserScoped()).thenReturn(true);
        when(userSessionContext.getUserAccessScope()).thenReturn(accessScope);
        groupRpcService.getGroups(genericGroupsRequest, mockGroupingObserver);
        Mockito.verify(mockGroupingObserver).onNext(firstGrouping);
        Mockito.verify(mockGroupingObserver, Mockito.never()).onNext(secondGrouping);
        Mockito.verify(mockGroupingObserver).onCompleted();
    }

    /**
     * Test when user request groups but provided scopes limit in the request, only filtered groups
     * which are within the scopes are returned.
     *
     * @throws StoreOperationException if exception occurred operating with a group store
     */
    @Test
    public void testGetGroupsWithScopesLimit() throws StoreOperationException {
        final long dcId = 3L;
        final long clusterId1 = 1L;
        final long clusterId2 = 2L;
        final long clusterMember1 = 11L;
        final long clusterMember2 = 21L;

        final StreamObserver<GroupDTO.Grouping> mockGroupingObserver = Mockito.mock(StreamObserver.class);
        final GetGroupsRequest genericGroupsRequest = GetGroupsRequest
                .newBuilder()
                .setGroupFilter(GroupFilter.getDefaultInstance())
                .addScopes(dcId)
                .build();
        when(userSessionContext.isUserScoped()).thenReturn(false);
        final EntityAccessScope accessScope = new EntityAccessScope(null, null,
                new ArrayOidSet(Collections.singletonList(clusterMember1)), null);
        when(userSessionContext.getAccessScope(Collections.singletonList(dcId))).thenReturn(accessScope);

        final Grouping cluster1 = createGrouping(clusterId1, Collections.singletonList(clusterMember1));
        final Grouping cluster2 = createGrouping(clusterId2, Collections.singletonList(clusterMember2));

        when(groupStoreDAO.getGroups(genericGroupsRequest.getGroupFilter()))
                .thenReturn(Arrays.asList(cluster1, cluster2));
        when(groupStoreDAO.getGroupIds(any())).thenReturn(Arrays.asList(clusterId1, clusterId2));
        when(groupStoreDAO.getMembers(Collections.singleton(clusterId1), true)).thenReturn(
                new GroupMembersPlain(Collections.singleton(clusterMember1),
                        Collections.emptySet(), Collections.emptySet()));
        when(groupStoreDAO.getMembers(Collections.singleton(clusterId2), true)).thenReturn(
                new GroupMembersPlain(Collections.singleton(clusterMember2),
                        Collections.emptySet(), Collections.emptySet()));
        when(groupStoreDAO.getGroupsById(Lists.newArrayList(clusterId1))).thenReturn(
                Lists.newArrayList(cluster1));

        // act
        groupRpcService.getGroups(genericGroupsRequest, mockGroupingObserver);
        // verify only cluster1 is returned
        verify(mockGroupingObserver).onNext(cluster1);
        verify(mockGroupingObserver, Mockito.never()).onNext(cluster2);
        verify(mockGroupingObserver).onCompleted();
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

        doThrow(new StoreOperationException(Status.NOT_FOUND,
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
        doThrow(new DataAccessException(errorMsg)).when(groupStoreDAO).deleteGroup(idToDelete);

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
                .addId(groupId)
                .build();

        final Grouping group = Grouping
                        .newBuilder()
                        .setId(groupId)
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

        groupStoreDAO.addGroup(group);
        Mockito.when(searchServiceMole.searchEntityOids(Mockito.any()))
                .thenReturn(Search.SearchEntityOidsResponse.newBuilder()
                        .addAllEntities(mockSearchResults)
                        .build());

        groupRpcService.getMembers(req, mockObserver);

        final GroupDTO.GetMembersResponse expectedResponse = GetMembersResponse.newBuilder()
                .setGroupId(groupId)
                .addAllMemberId(mockSearchResults)
                .build();

        verify(mockObserver, never()).onError(any(Exception.class));
        verify(mockObserver).onNext(expectedResponse);
        verify(mockObserver).onCompleted();
    }

    /**
     * Tests getting the members of a group when the group is dynamic group of accounts.
     */
    @Test
    public void testGetMembersOfDynamicGroupOfAccounts() {
        // ARRANGE
        final long groupId = 1234L;
        final List<Long> mockSearchResults = Arrays.asList(1L, 2L);

        final GroupDTO.GetMembersRequest req = GroupDTO.GetMembersRequest.newBuilder()
            .addId(groupId)
            .build();

        final Grouping group = Grouping
            .newBuilder()
            .setId(groupId)
            .setDefinition(GroupDefinition
                .newBuilder()
                .setEntityFilters(
                    EntityFilters
                        .newBuilder()
                        .addEntityFilter(
                            EntityFilter
                                .newBuilder()
                                .setEntityType(EntityType.BUSINESS_ACCOUNT.getNumber())
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

        groupStoreDAO.addGroup(group);
        final ArgumentCaptor<SearchEntityOidsRequest> captor =
            ArgumentCaptor.forClass(SearchEntityOidsRequest.class);
        Mockito.when(searchServiceMole.searchEntityOids(captor.capture()))
            .thenReturn(Search.SearchEntityOidsResponse.newBuilder()
                .addAllEntities(mockSearchResults)
                .build());

        // ACT
        groupRpcService.getMembers(req, mockObserver);

        // ASSERT
        final GroupDTO.GetMembersResponse expectedResponse = GetMembersResponse.newBuilder()
            .setGroupId(groupId)
            .addAllMemberId(mockSearchResults)
            .build();

        verify(mockObserver, never()).onError(any(Exception.class));
        verify(mockObserver).onNext(expectedResponse);
        verify(mockObserver).onCompleted();

        final Search.SearchQuery query = captor.getValue().getSearch();
        assertThat(query.getSearchParametersCount(), is(2));
        final SearchParameters targetSearchParam = query.getSearchParameters(1);
        assertThat(targetSearchParam.getSearchFilterCount(), is(1));
        assertThat(targetSearchParam.getSearchFilter(0).getPropertyFilter().getPropertyName(),
            is(SearchableProperties.ASSOCIATED_TARGET_ID));
    }

    /**
     * Test that exceptions in search service during member resolution don't propagate
     * to the caller of getMembers.
     *
     * @throws Exception To satisfy complier.
     */
    @Test
    public void testDynamicGetMembersSearchException() throws Exception {
        final long groupId = 1234L;
        final List<Long> mockSearchResults = Arrays.asList(1L, 2L);

        final GroupDTO.GetMembersRequest req = GroupDTO.GetMembersRequest.newBuilder()
            .addId(groupId)
            .build();

        final Grouping group = Grouping.newBuilder()
            .setId(groupId)
            .setDefinition(GroupDefinition.newBuilder()
                .setEntityFilters(EntityFilters.newBuilder()
                    .addEntityFilter(EntityFilter.newBuilder()
                        .setSearchParametersCollection(SearchParametersCollection.newBuilder()
                                .addSearchParameters(SearchParameters.getDefaultInstance())))))
            .build();

        final StreamObserver<GroupDTO.GetMembersResponse> mockObserver = mock(StreamObserver.class);

        groupStoreDAO.addGroup(group);
        Mockito.when(searchServiceMole.searchEntityOids(Mockito.any()))
            .thenThrow(Status.INTERNAL.asRuntimeException());

        groupRpcService.getMembers(req, mockObserver);

        final GroupDTO.GetMembersResponse expectedResponse = GetMembersResponse.newBuilder()
            .setGroupId(groupId)
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
                .addId(groupId)
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

        groupStoreDAO.addGroup(grouping);
        final StreamObserver<GroupDTO.GetMembersResponse> mockObserver =
                mock(StreamObserver.class);

        groupRpcService.getMembers(req, mockObserver);

        final GroupDTO.GetMembersResponse expectedResponse = GetMembersResponse.newBuilder()
                .setGroupId(grouping.getId())
                .addAllMemberId(staticGroupMembers)
                .build();

        verify(mockObserver, never()).onError(any(Exception.class));
        verify(mockObserver).onNext(expectedResponse);
        verify(mockObserver).onCompleted();
    }

    /**
     * Test that an error in retrieving members of one group doesn't interfere with the return
     * of members for other groups.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testGetMembersOneGroupFail() throws Exception {
        final long groupId1 = 1234L;
        final long groupId2 = 4567L;
        final List<Long> staticGroupMembers = Arrays.asList(1L, 2L);

        final GroupDTO.GetMembersRequest req = GroupDTO.GetMembersRequest.newBuilder()
            .addId(groupId1)
            .addId(groupId2)
            .build();


        final Grouping grouping1 = Grouping
            .newBuilder()
            .setId(groupId1)
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
        final Grouping grouping2 = grouping1.toBuilder()
            .setId(groupId2)
            .build();

        groupStoreDAO.addGroup(grouping1);
        groupStoreDAO.addGroup(grouping2);

        doThrow(new StoreOperationException(Status.INTERNAL, "Bad group.")).when(groupStoreDAO).getMembers(eq(Collections.singleton(groupId1)), anyBoolean());
        final StreamObserver<GroupDTO.GetMembersResponse> mockObserver =
            mock(StreamObserver.class);

        groupRpcService.getMembers(req, mockObserver);

        final GroupDTO.GetMembersResponse expectedResponse = GetMembersResponse.newBuilder()
            .setGroupId(grouping2.getId())
            .addAllMemberId(staticGroupMembers)
            .build();

        verify(mockObserver, never()).onError(any(Exception.class));
        verify(mockObserver).onNext(expectedResponse);
        verify(mockObserver).onCompleted();
    }

    @Test
    public void testGetMembersExpansion() throws StoreOperationException {
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
        groupStoreDAO.addGroup(cluster1);
        groupStoreDAO.addGroup(cluster2);
        groupStoreDAO.addGroup(groupOfClusters);

        final StreamObserver<GroupDTO.GetMembersResponse> mockObserver =
                mock(StreamObserver.class);

        // a request without expansion should get the list of clusters
        final GroupDTO.GetMembersRequest reqNoExpansion = GroupDTO.GetMembersRequest.newBuilder()
                .addId(groupId)
                .build();

        groupRpcService.getMembers(reqNoExpansion, mockObserver);

        final GroupDTO.GetMembersResponse expectedResponse = GetMembersResponse.newBuilder()
                .setGroupId(groupId)
                .addAllMemberId(clusterGroupMembers)
                .build();

        verify(mockObserver).onNext(expectedResponse);

        // verify that a request WITH expansion should get all of the cluster members.
        final GroupDTO.GetMembersRequest requestExpanded = GroupDTO.GetMembersRequest.newBuilder()
                .addId(groupId)
                .setExpandNestedGroups(true)
                .build();

        final GroupDTO.GetMembersResponse expectedExpandedResponse = GetMembersResponse.newBuilder()
                .setGroupId(groupId)
                .addAllMemberId(cluster1Members)
                .addAllMemberId(cluster2Members)
                .build();

        groupRpcService.getMembers(requestExpanded, mockObserver);

        verify(mockObserver).onNext(
                Mockito.argThat(new GetMembersMatcher(expectedExpandedResponse)));
    }

    @Test
    public void testGetMembersExpansionDynamic() throws Exception {
        final long groupId = 1234L;
        final long cluster1Id = 1L;
        final long cluster2Id = 2L;
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
                        .setId(cluster1Id)
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
                        .setId(cluster2Id)
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

        groupStoreDAO.addGroup(dynamicGroupOfClusters);
        groupStoreDAO.addGroup(cluster1);
        groupStoreDAO.addGroup(cluster2);

        GetGroupsRequest request = GetGroupsRequest
                        .newBuilder()
                        .setGroupFilter(groupFilter)
                        .build();

        Mockito.when(groupStoreDAO.getMembers(Collections.singleton(groupId), false))
                .thenReturn(
                        new GroupMembersPlain(Collections.emptySet(), Sets.newHashSet(cluster1Id),
                                Collections.emptySet()));
        final StreamObserver<GroupDTO.GetMembersResponse> mockObserver =
                mock(StreamObserver.class);

        // a request without expansion should get the list of clusters
        final GroupDTO.GetMembersRequest reqNoExpansion = GroupDTO.GetMembersRequest.newBuilder()
                .addId(groupId)
                .build();

        groupRpcService.getMembers(reqNoExpansion, mockObserver);

        final GroupDTO.GetMembersResponse expectedNonExpandedResponse = GetMembersResponse.newBuilder()
                .setGroupId(groupId)
                .addAllMemberId(Arrays.asList(1L))
                .build();

        verify(mockObserver).onNext(expectedNonExpandedResponse);

        // verify that a request WITH expansion should get all of the cluster members.
        final GroupDTO.GetMembersRequest requestExpanded = GroupDTO.GetMembersRequest.newBuilder()
                .addId(groupId)
                .setExpandNestedGroups(true)
                .build();

        final GroupDTO.GetMembersResponse expectedExpandedResponse = GetMembersResponse.newBuilder()
                .setGroupId(groupId)
                .addAllMemberId(cluster1Members)
                .build();
        Mockito.when(groupStoreDAO.getMembers(Collections.singleton(groupId), true))
                .thenReturn(new GroupMembersPlain(new HashSet<>(cluster1Members),
                        Collections.singleton(cluster1Id), Collections.emptySet()));

        groupRpcService.getMembers(requestExpanded, mockObserver);

        verify(mockObserver).onNext(
                Mockito.argThat(new GetMembersMatcher(expectedExpandedResponse)));
    }

    @Test
    public void testMissingGroup() throws Exception {
        final long groupId = 1234L;

        final GroupDTO.GetMembersRequest req = GroupDTO.GetMembersRequest.newBuilder()
                .addId(groupId)
                .build();

        final StreamObserver<GroupDTO.GetMembersResponse> mockObserver =
                mock(StreamObserver.class);

        groupRpcService.getMembers(req, mockObserver);
        Mockito.verify(groupStoreDAO).getExistingGroupIds(Collections.singleton(groupId));

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
        verify(groupStoreDAO).updateDiscoveredGroups(captor.capture(), Mockito.anyList(),
                Mockito.anySet());
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

        Mockito.verify(placementPolicyUpdater)
                .updateDiscoveredPolicies(Mockito.eq(transactionProvider.getPlacementPolicyStore()),
                        Mockito.any(), Mockito.any(), Mockito.any());
        Mockito.verify(settingPolicyUpdater)
                .updateSettingPolicies(Mockito.eq(transactionProvider.getSettingPolicyStore()),
                        Mockito.any(), Mockito.any(), Mockito.any());
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
        verify(groupStoreDAO).updateDiscoveredGroups(captor.capture(), Mockito.anyList(), Mockito.anySet());
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
     * Test that resource groups are uploaded and stitched successfully in rpc call method.
     *
     * @throws StoreOperationException exception thrown if group is invalid
     */
    @Test
    public void testUpdateGroupsUndiscoveredTargets() throws StoreOperationException {
        final StreamObserver<StoreDiscoveredGroupsPoliciesSettingsResponse> responseObserver =
                mock(StreamObserver.class);
        final StreamObserver<DiscoveredGroupsPoliciesSettings> requestObserver =
                spy(groupRpcService.storeDiscoveredGroupsPoliciesSettings(responseObserver));

        // create two resource groups from two different probes
        final UploadedGroup group1 = GroupTestUtils.createUploadedGroup(GroupType.REGULAR, "src1",
                ImmutableMap.of(EntityType.VIRTUAL_MACHINE_VALUE, Sets.newHashSet(11L),
                        EntityType.DATABASE_VALUE, Sets.newHashSet(21L)));
        final long oid1 = 100001L;
        final long oid2 = 100002L;
        Mockito.when(
                transactionProvider.getGroupStore().getGroupsByTargets(Collections.singleton(112L)))
                .thenReturn(Collections.singleton(oid1));
        Mockito.when(transactionProvider.getGroupStore().getDiscoveredGroupsIds())
                .thenReturn(Arrays.asList(
                        new DiscoveredGroupIdImpl(new DiscoveredObjectVersionIdentity(oid1, null), 112L,
                                "src3", GroupType.RESOURCE),
                        new DiscoveredGroupIdImpl(new DiscoveredObjectVersionIdentity(oid2, null), 110L,
                                "src3", GroupType.RESOURCE)));

        requestObserver.onNext(DiscoveredGroupsPoliciesSettings.newBuilder()
                .setTargetId(110L)
                .setProbeType(SDKProbeType.AZURE.toString())
                .addUploadedGroups(group1)
                .build());
        requestObserver.onNext(DiscoveredGroupsPoliciesSettings.newBuilder()
                .setTargetId(112L)
                .setProbeType(SDKProbeType.VCENTER.toString())
                .setDataAvailable(false)
                .build());
        requestObserver.onCompleted();

        Mockito.verify(transactionProvider.getGroupStore())
                .updateDiscoveredGroups(Mockito.anyCollectionOf(DiscoveredGroup.class),
                        Mockito.anyCollectionOf(DiscoveredGroup.class),
                        Mockito.eq(Collections.singleton(oid2)));
    }

    /**
     * Test that resource groups are uploaded and stitched successfully in rpc call method.
     *
     * @throws StoreOperationException exception thrown if group is invalid
     */
    @Test
    public void testGroupStitchingNoUpdates() throws StoreOperationException {
        final StreamObserver<StoreDiscoveredGroupsPoliciesSettingsResponse> responseObserver =
                mock(StreamObserver.class);
        final StreamObserver<DiscoveredGroupsPoliciesSettings> requestObserver =
                spy(groupRpcService.storeDiscoveredGroupsPoliciesSettings(responseObserver));

        // create two resource groups from two different probes
        final UploadedGroup group1 = GroupTestUtils.createUploadedGroup(GroupType.REGULAR, "src1",
                ImmutableMap.of(EntityType.VIRTUAL_MACHINE_VALUE, Sets.newHashSet(11L),
                        EntityType.DATABASE_VALUE, Sets.newHashSet(21L)));
        final UploadedGroup group2 = GroupTestUtils.createUploadedGroup(GroupType.REGULAR, "src2",
                ImmutableMap.of(EntityType.VIRTUAL_MACHINE_VALUE, Sets.newHashSet(15L)));
        final long oid1 = 100001L;
        final long oid2 = 100002L;
        final DiscoveredGroup discoveredGroup2 = new DiscoveredGroup(oid2, group2.getDefinition(),
                group2.getSourceIdentifier(), group2.getStitchAcrossTargets(),
                Collections.singleton(TARGET1), Collections.singleton(
                MemberType.newBuilder().setEntity(EntityType.VIRTUAL_MACHINE_VALUE).build()), true);
        final byte[] hash = DiscoveredGroupHash.hash(discoveredGroup2);
        Mockito.when(
                transactionProvider.getGroupStore().getGroupsByTargets(Collections.singleton(TARGET1)))
                .thenReturn(Collections.singleton(oid1));
        Mockito.when(transactionProvider.getGroupStore().getDiscoveredGroupsIds())
                .thenReturn(Arrays.asList(
                        new DiscoveredGroupIdImpl(new DiscoveredObjectVersionIdentity(oid1, null), TARGET1,
                                "src1", GroupType.REGULAR),
                        new DiscoveredGroupIdImpl(new DiscoveredObjectVersionIdentity(oid2, hash), TARGET1,
                                "src2", GroupType.REGULAR)));

        requestObserver.onNext(DiscoveredGroupsPoliciesSettings.newBuilder()
                .setTargetId(TARGET1)
                .setProbeType(SDKProbeType.VCENTER.toString())
                .addUploadedGroups(group1)
                .addUploadedGroups(group2)
                .build());
        requestObserver.onCompleted();

        @SuppressWarnings("unchecked")
        final ArgumentCaptor<Collection<DiscoveredGroup>> updatedCaptor = (ArgumentCaptor<Collection<DiscoveredGroup>>)
                (ArgumentCaptor)ArgumentCaptor.forClass(
                        Collection.class);
        Mockito.verify(transactionProvider.getGroupStore()).updateDiscoveredGroups(
                Mockito.eq(Collections.emptyList()), updatedCaptor.capture(),
                Mockito.eq(Collections.emptySet()));
        Assert.assertEquals(Collections.singleton(oid1), updatedCaptor.getValue()
                .stream()
                .map(DiscoveredGroup::getOid)
                .collect(Collectors.toSet()));
    }


    /**
     * Test failed retrieval of tags.
     */
    @Test
    public void testGetTagsFailed() {
        final String errorMessage = "boom";
        when(groupStoreDAO.getTags(Collections.emptyList())).thenThrow(new DataAccessException(errorMessage));
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
        Mockito.when(identityProvider.next()).thenReturn(groupingOid).thenReturn(-1L);
        GroupEnvironment env = mockEnvironment(EnvironmentType.ON_PREM, CloudType.UNKNOWN_CLOUD);
        Mockito.when(groupEnvironmentTypeResolver
                .getEnvironmentAndCloudTypeForGroup(any(), anyLong(), any(), any())).thenReturn(env);
        Mockito.when(groupSeverityCalculator.calculateSeverity(anySet())).thenReturn(Severity.MINOR);

        groupRpcService.createGroup(groupRequest, mockObserver);
        verify(groupStoreDAO).createGroup(eq(groupingOid), eq(origin), eq(group), any(),
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
                        .setEnvironmentType(EnvironmentType.ON_PREM)
                        .setCloudType(CloudType.UNKNOWN_CLOUD)
                        .setSeverity(Severity.MINOR)
                        .build())
                .build());
        verify(mockObserver).onCompleted();
    }

    /**
     * Test that it's impossible to create two groups of groups a and b such that a is a member
     * of b AND b is a member of a.
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testRecursiveGroupOfGroups() throws Exception {
        final long idA = 1;
        final long idB = 2;
        final GroupFilters groupFiltersA =
            GroupFilters.newBuilder().addGroupFilter(GroupFilter.newBuilder()
                .setGroupType(GroupType.REGULAR).addPropertyFilters(PropertyFilter.newBuilder()
                        .setPropertyName("displayName").setStringFilter(StringFilter.newBuilder()
                        .setStringPropertyRegex("^.*foo.*$").setPositiveMatch(true)
                        .setCaseSensitive(false).build()).build()).build()).build();
        final GroupFilters groupFiltersB =
            GroupFilters.newBuilder().addGroupFilter(GroupFilter.newBuilder()
                .setGroupType(GroupType.REGULAR).addPropertyFilters(PropertyFilter.newBuilder()
                    .setPropertyName("displayName").setStringFilter(StringFilter.newBuilder()
                        .setStringPropertyRegex("^.*bar.*$").setPositiveMatch(true)
                        .setCaseSensitive(false).build()).build()).build()).build();
        GroupDefinition groupDefA = GroupDefinition
            .newBuilder()
            .setType(GroupType.REGULAR)
            .setDisplayName("bar")
            .setGroupFilters(groupFiltersA)
            .build();
        GroupDefinition groupDefB = GroupDefinition
            .newBuilder()
            .setType(GroupType.REGULAR)
            .setDisplayName("foo")
            .setGroupFilters(groupFiltersB)
            .build();

        CreateGroupRequest groupRequest = CreateGroupRequest
            .newBuilder()
            .setGroupDefinition(groupDefA)
            .setOrigin(origin)
            .build();

        final StreamObserver<GroupDTO.CreateGroupResponse> mockObserver =
            mock(StreamObserver.class);

        Mockito.when(identityProvider.next()).thenReturn(idA).thenReturn(idB);
        GroupEnvironment env = mockEnvironment(EnvironmentType.ON_PREM, CloudType.UNKNOWN_CLOUD);
        Mockito.when(groupEnvironmentTypeResolver
                .getEnvironmentAndCloudTypeForGroup(any(), anyLong(), any(), any())).thenReturn(env);
        Mockito.when(groupSeverityCalculator.calculateSeverity(anySet())).thenReturn(Severity.MINOR);
        groupRpcService.createGroup(groupRequest, mockObserver);
        HashSet<Long> idAList = new HashSet<>(Collections.singletonList(idA));
        HashSet<Long> idBList = new HashSet<>(Collections.singletonList(idB));

        Grouping groupA = Grouping.newBuilder()
            .setId(idA)
            .setDefinition(groupDefA)
            .setSupportsMemberReverseLookup(false)
            .build();
        Grouping groupB = Grouping.newBuilder()
            .setId(idB)
            .setDefinition(groupDefB)
            .setSupportsMemberReverseLookup(false)
            .build();
        when(groupStoreDAO.getGroupIds(groupFiltersA)).thenReturn(idBList);
        when(groupStoreDAO.getGroupsById(idBList)).thenReturn(Collections.singletonList(groupB));

        when(groupStoreDAO.getGroupIds(groupFiltersB)).thenReturn(idAList);
        when(groupStoreDAO.getGroupsById(idAList)).thenReturn(Arrays.asList(groupA));

        when(groupStoreDAO.getMembers(idBList, false)).thenReturn(new GroupMembersPlain(new HashSet<>(),
            idBList,
            new HashSet<>()));

        CreateGroupRequest groupRequest2 = CreateGroupRequest
            .newBuilder()
            .setGroupDefinition(groupDefB)
            .setOrigin(origin)
            .build();
        groupRpcService.createGroup(groupRequest2, mockObserver);

        //Verify we send the error response
        final ArgumentCaptor<StatusException> exceptionCaptor =
            ArgumentCaptor.forClass(StatusException.class);
        verify(mockObserver).onError(exceptionCaptor.capture());

        final StatusException exception = exceptionCaptor.getValue();
        assertThat(exception, GrpcExceptionMatcher.hasCode(Code.INVALID_ARGUMENT)
            .descriptionContains("Recursive group definition"));
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
                        .setEnvironmentType(EnvironmentType.HYBRID)
                        .setCloudType(CloudType.AZURE)
                        .setSeverity(Severity.MINOR)
                        .build();

        when(temporaryGroupCache.create(eq(group), eq(origin), eq(expectedTypes), any()))
                                .thenReturn(grouping);

        final StreamObserver<GroupDTO.CreateGroupResponse> mockObserver =
                mock(StreamObserver.class);

        CreateGroupRequest groupRequest = CreateGroupRequest
                        .newBuilder()
                        .setGroupDefinition(group)
                        .setOrigin(origin)
                        .build();

        groupRpcService.createGroup(groupRequest, mockObserver);

        verify(temporaryGroupCache).create(eq(group), eq(origin), eq(expectedTypes), any());
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
            .createGroup(Mockito.anyLong(), Mockito.anyObject(), Mockito.anyObject(),
                            Mockito.anyObject(), anyBoolean());

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
            .createGroup(Mockito.anyLong(), Mockito.anyObject(), Mockito.anyObject(),
                            Mockito.anyObject(), anyBoolean());
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
                .createGroup(Mockito.anyLong(), Mockito.anyObject(), Mockito.anyObject(),
                                Mockito.anyObject(), anyBoolean());

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
                        .setEnvironmentType(EnvironmentType.CLOUD)
                        .setCloudType(CloudType.AZURE)
                        .setSeverity(Severity.MINOR)
                        .build();

        when(temporaryGroupCache.create(eq(group), eq(origin), eq(expectedTypes), any()))
                                .thenReturn(grouping);

        final StreamObserver<CreateGroupResponse> mockObserver =
                mock(StreamObserver.class);

        groupRpcService.createGroup(CreateGroupRequest.newBuilder()
                .setGroupDefinition(group)
                .setOrigin(origin)
                .build(), mockObserver);

        verify(temporaryGroupCache).create(eq(group), eq(origin), eq(expectedTypes), any());
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
        doThrow(new StoreOperationException(Status.ABORTED, message))
                .when(groupStoreDAO)
                .createGroup(Mockito.anyLong(), Mockito.anyObject(), Mockito.anyObject(),
                        Mockito.anyObject(), anyBoolean());

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
                        .setEnvironmentType(EnvironmentType.ON_PREM)
                        .setCloudType(CloudType.UNKNOWN_CLOUD)
                        .setSeverity(Severity.MINOR)
                        .build();

        UpdateGroupRequest groupRequest = UpdateGroupRequest
                        .newBuilder()
                        .setId(groupingOid)
                        .setNewDefinition(group)
                        .build();

        final StreamObserver<UpdateGroupResponse> mockObserver =
                        mock(StreamObserver.class);

        GroupEnvironment env = mockEnvironment(EnvironmentType.ON_PREM, CloudType.UNKNOWN_CLOUD);
        Mockito.when(groupEnvironmentTypeResolver
                .getEnvironmentAndCloudTypeForGroup(any(), anyLong(), any(), any())).thenReturn(env);
        Mockito.when(groupSeverityCalculator.calculateSeverity(anySet())).thenReturn(Severity.MINOR);

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
                            Mockito.anyObject(), anyBoolean());

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
                            Mockito.anyObject(), anyBoolean());

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
                            Mockito.anyObject(), anyBoolean());

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
        doThrow(new StoreOperationException(Status.ALREADY_EXISTS, message))
                .when(groupStoreDAO)
                .updateGroup(Mockito.anyLong(), Mockito.anyObject(), Mockito.anyObject(),
                        anyBoolean());

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
      * Tests the findExpectedTypes method in group service when we are dealing with
      * dynamic group of groups.
      */
    @Test
    public void testFindExpectedTypeDynamicGroupOfGroups() {
        // GIVEN
        GroupDefinition groupDefinition = GroupDefinition
                .newBuilder()
                .setGroupFilters(GroupFilters.newBuilder()
                        .addGroupFilter(GroupFilter.newBuilder()
                            .setGroupType(GroupType.COMPUTE_HOST_CLUSTER)
                        )).build();

        // WHEN
        final Set<MemberType> memberTypes =
        groupRpcService.findGroupExpectedTypes(groupStoreDAO, groupDefinition);

        // THEN
        assertEquals(ImmutableSet.of(MemberType.newBuilder()
                        .setGroup(GroupType.COMPUTE_HOST_CLUSTER)
                        .build()), memberTypes);
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

        doThrow(new DataAccessException("ERR1"))
            .when(groupStoreDAO).getGroupsById(Mockito.anyCollectionOf(Long.class));

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

        groupRpcService.validateGroupDefinition(groupStoreDAO, groupDef);
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

        groupRpcService.validateGroupDefinition(groupStoreDAO, groupDef);
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

        groupRpcService.validateGroupDefinition(groupStoreDAO, groupDef);

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

        groupRpcService.validateGroupDefinition(groupStoreDAO, groupDef);

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

        groupRpcService.validateGroupDefinition(groupStoreDAO, groupDef);

    }

    /**
     * Group filter causes SQL grammar exception.
     *
     * @throws InvalidGroupDefinitionException To satisfy compiler.
     */
    @Test
    public void testValidateDynamicGroupOfGroupsInvalidFilter() throws InvalidGroupDefinitionException {
        final SQLException underlyingException = new SQLException("Foo");
        final BadSqlGrammarException grammarException =
                new BadSqlGrammarException("foo", "bar", underlyingException);
        when(groupStoreDAO.getGroupIds(any())).thenThrow(grammarException);
        final GroupDefinition groupDef = GroupDefinition
                .newBuilder()
                .setType(GroupType.REGULAR)
                .setDisplayName("Test")
                .setGroupFilters(GroupFilters.newBuilder()
                    .addGroupFilter(GroupFilter.newBuilder().build()))
                .build();

        expectedException.expect(InvalidGroupDefinitionException.class);
        expectedException.expectMessage(underlyingException.getMessage());
        groupRpcService.validateGroupDefinition(groupStoreDAO, groupDef);
    }

    /**
     * Tests search for group members when one of the search filter is group related, i.e. -
     * should be resolved by a group component.
     *
     * @throws Exception on exceptions occur
     */
    @Test
    public void testGroupMembershipFilters() throws Exception {
        final long groupId = 678L;
        final long subGroup1Id = 123L;
        final long subGroup2Id = 234L;
        final Set<Long> members1 = Sets.newHashSet(345L, 456L);
        final Set<Long> members2 = Sets.newHashSet(567L, 789L);
        final EntityFilters filters = EntityFilters.newBuilder()
                .addEntityFilter(EntityFilter.newBuilder()
                        .setSearchParametersCollection(SearchParametersCollection.newBuilder()
                                .addSearchParameters(SearchParameters.newBuilder()
                                        .addSearchFilter(SearchFilter.newBuilder()
                                                .setGroupFilter(
                                                        Search.GroupFilter.newBuilder()
                                                                .setEntityToGroupType(
                                                                        EntityToGroupType.MEMBER_OF)
                                                                .setGroupSpecifier(
                                                                        PropertyFilter.newBuilder()
                                                                                .setPropertyName(
                                                                                        "oid")
                                                                                .setStringFilter(
                                                                                        StringFilter
                                                                                                .newBuilder()
                                                                                                .addOptions(
                                                                                                        Long.toString(
                                                                                                                subGroup1Id))
                                                                                                .addOptions(
                                                                                                        Long.toString(
                                                                                                                subGroup2Id)))))))))
                .build();
        final Grouping mainGroup = Grouping.newBuilder()
                .setId(groupId)
                .setDefinition(GroupDefinition.newBuilder().setEntityFilters(filters))
                .build();
        final Grouping subGroup1 = createGrouping(subGroup1Id, members1);
        final Grouping subGroup2 = createGrouping(subGroup2Id, members2);
        groupStoreDAO.addGroup(subGroup1);
        groupStoreDAO.addGroup(subGroup2);
        groupStoreDAO.addGroup(mainGroup);
        Mockito.when(groupStoreDAO.getGroups(Mockito.any()))
                .thenReturn(Arrays.asList(subGroup1, subGroup2));

        final Set<Long> searchResults = Sets.newHashSet(111L, 112L, 113L, 114L, 115L, 116L);
        final Set<Long> members = new HashSet<>();
        Mockito.when(searchServiceMole.searchEntityOids(Mockito.any()))
                .thenReturn(SearchEntityOidsResponse.newBuilder()
                        .addAllEntities(searchResults)
                        .build());
        final CountDownLatch latch = new CountDownLatch(1);
        groupRpcService.getMembers(GetMembersRequest.newBuilder().addId(groupId).build(),
                new StreamObserver<GetMembersResponse>() {
                    @Override
                    public void onNext(GetMembersResponse value) {
                        members.addAll(value.getMemberIdList());
                    }

                    @Override
                    public void onError(Throwable t) {
                        latch.countDown();
                    }

                    @Override
                    public void onCompleted() {
                        latch.countDown();
                    }
                });
        latch.await(30, TimeUnit.SECONDS);
        Assert.assertEquals(searchResults, members);
        final ArgumentCaptor<SearchEntityOidsRequest> searchRequestCaptor =
                ArgumentCaptor.forClass(SearchEntityOidsRequest.class);
        Mockito.verify(searchServiceMole).searchEntityOids(searchRequestCaptor.capture());
        Assert.assertEquals(Sets.newHashSet(345L, 456L, 567L, 789L), new HashSet<>(
                searchRequestCaptor.getValue()
                    .getSearch()
                    .getSearchParameters(0)
                    .getSearchFilter(0)
                    .getPropertyFilter()
                    .getStringFilter()
                    .getOptionsList()
                    .stream()
                    .map(Long::parseLong)
                    .collect(Collectors.toSet())));

        final ArgumentCaptor<GroupFilter> getGroupsCaptor =
                ArgumentCaptor.forClass(GroupFilter.class);
        Mockito.verify(groupStoreDAO).getGroups(getGroupsCaptor.capture());
        Assert.assertEquals(Sets.newHashSet(subGroup1Id, subGroup2Id),
                getGroupsCaptor.getAllValues()
                        .get(0)
                        .getPropertyFilters(0)
                        .getStringFilter()
                        .getOptionsList()
                        .stream()
                        .map(Long::parseLong)
                        .collect(Collectors.toSet()));
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

    /**
     * Matcher for {@link GetGroupsForEntitiesResponse} message.
     */
    private static class GetGroupsForEntitiesResponseMatcher extends
            ProtobufMessageMatcher<GetGroupsForEntitiesResponse> {
        GetGroupsForEntitiesResponseMatcher(@Nonnull GetGroupsForEntitiesResponse expected) {
            super(expected, Collections.singleton("entity_group.value.group_id"));
        }
    }

    /**
     * A matcher for group definition. It is used instead of equality operator in order to
     * match orderless collections inside.
     */
    private static class GetMembersMatcher extends ProtobufMessageMatcher<GetMembersResponse> {
        GetMembersMatcher(@Nonnull GetMembersResponse expected) {
            super(expected, Collections.singleton("member_id"));
        }
    }

    /**
     * Tests getGroupAndImmediateMembers for static groups returns static members.
     */
    @Test
    public void testGetGroupAndImmediateMembersForStaticGroup() {
        //GIVEN
        long groupId = 123L;
        GetGroupAndImmediateMembersRequest getGroupAndImmediateMembersRequest =
                        GetGroupAndImmediateMembersRequest.newBuilder()
                                        .setGroupId(groupId)
                                        .build();
        final StreamObserver<GetGroupAndImmediateMembersResponse> responseObserverMock =
                        Mockito.mock(StreamObserver.class);

        Grouping grouping = Grouping.newBuilder().setId(123L)
                        .setDefinition(testGrouping)
                        .build();
        doReturn(Optional.of(grouping)).when(temporaryGroupCache).getGrouping(groupId);

        //WHEN
        groupRpcService.getGroupAndImmediateMembers(getGroupAndImmediateMembersRequest, responseObserverMock);

        //THEN
        ArgumentCaptor<GetGroupAndImmediateMembersResponse> captor = ArgumentCaptor.forClass(GetGroupAndImmediateMembersResponse.class);
        verify(responseObserverMock, Mockito.times(1)).onNext(captor.capture());
        GetGroupAndImmediateMembersResponse response = captor.getValue();

        assertEquals(response.getGroup(), grouping);
        Set<Long> memberIds = new HashSet<>(grouping.getDefinition().getStaticGroupMembers().getMembersByType(0).getMembersList());
        assertEquals(memberIds.size(), response.getImmediateMembersList().size());
        assertTrue(memberIds.contains(response.getImmediateMembersList().get(0)));
        assertTrue(memberIds.contains(response.getImmediateMembersList().get(1)));
    }

    /**
     * Tests getGroupAndImmediateMembers for heterogenous static groups returns all static members.
     */
    @Test
    public void testGetGroupAndImmediateMembersForHeterogenousStaticGroup() {
        //GIVEN
        long groupId = 123L;
        GetGroupAndImmediateMembersRequest getGroupAndImmediateMembersRequest =
            GetGroupAndImmediateMembersRequest.newBuilder()
                .setGroupId(groupId)
                .build();
        final StreamObserver<GetGroupAndImmediateMembersResponse> responseObserverMock =
            Mockito.mock(StreamObserver.class);

        List<Long> membersOfType2 = Arrays.asList(101L, 102L);
        List<Long> membersOfType3 = Arrays.asList(103L, 104L);

        Grouping grouping = Grouping.newBuilder().setId(123L)
            .setDefinition(GroupDefinition.newBuilder()
                .setType(GroupType.RESOURCE)
                .setDisplayName("TestGroup")
                .setStaticGroupMembers(StaticMembers
                    .newBuilder()
                    .addMembersByType(StaticMembersByType
                        .newBuilder()
                        .setType(MemberType
                            .newBuilder()
                            .setEntity(2)
                        )
                        .addAllMembers(membersOfType2)
                    )
                    .addMembersByType(StaticMembersByType
                        .newBuilder()
                        .setType(MemberType
                            .newBuilder()
                            .setEntity(3)
                        )
                        .addAllMembers(membersOfType3)
                    )
                )
            )
            .build();

        doReturn(Optional.of(grouping)).when(temporaryGroupCache).getGrouping(groupId);

        //WHEN
        groupRpcService.getGroupAndImmediateMembers(getGroupAndImmediateMembersRequest, responseObserverMock);

        //THEN
        ArgumentCaptor<GetGroupAndImmediateMembersResponse> captor = ArgumentCaptor.forClass(GetGroupAndImmediateMembersResponse.class);
        verify(responseObserverMock, Mockito.times(1)).onNext(captor.capture());
        GetGroupAndImmediateMembersResponse response = captor.getValue();

        assertEquals(response.getGroup(), grouping);
        Set<Long> memberIds = new HashSet<>(membersOfType2);
        memberIds.addAll(membersOfType3);

        assertEquals(memberIds.size(), response.getImmediateMembersList().size());
        for (Long memberId : response.getImmediateMembersList()) {
            assertTrue(memberIds.contains(memberId));
        }
    }

    /**
     * Tests getGroupAndImmediateMembers for empty static groups returns no static members.
     */
    @Test
    public void testGetGroupAndImmediateMembersForEmptyStaticGroup() {
        //GIVEN
        long groupId = 123L;
        GetGroupAndImmediateMembersRequest getGroupAndImmediateMembersRequest =
            GetGroupAndImmediateMembersRequest.newBuilder()
                .setGroupId(groupId)
                .build();
        final StreamObserver<GetGroupAndImmediateMembersResponse> responseObserverMock =
            Mockito.mock(StreamObserver.class);

        // An empty group
        Grouping grouping = Grouping.newBuilder().setId(123L)
            .setDefinition(GroupDefinition.newBuilder()
                .setType(GroupType.REGULAR)
                .setDisplayName("TestGroup")
                .setStaticGroupMembers(StaticMembers.newBuilder())
            ).build();
        doReturn(Optional.of(grouping)).when(temporaryGroupCache).getGrouping(groupId);

        //WHEN
        groupRpcService.getGroupAndImmediateMembers(getGroupAndImmediateMembersRequest, responseObserverMock);

        //THEN
        ArgumentCaptor<GetGroupAndImmediateMembersResponse> captor = ArgumentCaptor.forClass(GetGroupAndImmediateMembersResponse.class);
        verify(responseObserverMock, Mockito.times(1)).onNext(captor.capture());
        GetGroupAndImmediateMembersResponse response = captor.getValue();

        assertEquals(response.getGroup(), grouping);
        assertEquals(0, response.getImmediateMembersList().size());
    }

    /**
     * Tests getGroupAndImmediateMembers for dynamic temporary groups returns members.
     * @throws Exception Something went wrong
     */
    @Test
    public void testGetGroupAndImmediateMembersForDynamicTemporaryGroup()
                    throws Exception {
        //GIVEN
        long groupId = 123L;
        GetGroupAndImmediateMembersRequest getGroupAndImmediateMembersRequest =
                        GetGroupAndImmediateMembersRequest.newBuilder()
                                        .setGroupId(groupId)
                                        .build();
        final StreamObserver<GetGroupAndImmediateMembersResponse> responseObserverMock =
                        Mockito.mock(StreamObserver.class);

        Grouping grouping = Grouping.newBuilder().setId(123L).build();
        doReturn(Optional.of(grouping)).when(temporaryGroupCache).getGrouping(groupId);
        Set<Long> membersList = Collections.singleton(1L);
        doReturn(membersList).when(groupMemberCalculatorSpy)
                        .getGroupMembers(groupStoreDAO, grouping.getDefinition(), false);

        //WHEN
        groupRpcService.getGroupAndImmediateMembers(getGroupAndImmediateMembersRequest, responseObserverMock);

        //THEN
        ArgumentCaptor<GetGroupAndImmediateMembersResponse> captor = ArgumentCaptor.forClass(GetGroupAndImmediateMembersResponse.class);
        verify(responseObserverMock, Mockito.times(1)).onNext(captor.capture());
        GetGroupAndImmediateMembersResponse response = captor.getValue();
        assertEquals(response.getGroup(), grouping);
        assertEquals(response.getImmediateMembersList().size(), membersList.size());
        response.getImmediateMembersList().forEach(id -> assertTrue(membersList.contains(id)));
    }

    /**
     * Get group and member data for real group.
     * @throws Exception Something went wrong
     */
    @Test
    public void testGetGroupAndImmediateMembersForDynamicRealGroup()
                    throws Exception {
        //GIVEN
        final StreamObserver<GetGroupAndImmediateMembersResponse> responseObserverMock =
                        Mockito.mock(StreamObserver.class);

        Grouping grouping = Grouping.newBuilder().setId(123L).build();
        long groupId = 123L;
        doReturn(Optional.empty()).when(temporaryGroupCache).getGrouping(groupId);
        groupStoreDAO.addGroup(grouping);
        Set<Long> membersList = Collections.singleton(1L);
        doReturn(membersList).when(groupMemberCalculatorSpy)
                        .getGroupMembers(groupStoreDAO, Collections.singleton(groupId), false);
        GetGroupAndImmediateMembersRequest getGroupAndImmediateMembersRequest =
                        GetGroupAndImmediateMembersRequest.newBuilder()
                                        .setGroupId(groupId)
                                        .build();
        //WHEN
        groupRpcService.getGroupAndImmediateMembers(getGroupAndImmediateMembersRequest, responseObserverMock);

        //THEN
        ArgumentCaptor<GetGroupAndImmediateMembersResponse> captor = ArgumentCaptor.forClass(GetGroupAndImmediateMembersResponse.class);
        verify(responseObserverMock, Mockito.times(1)).onNext(captor.capture());
        GetGroupAndImmediateMembersResponse response = captor.getValue();

        assertEquals(response.getGroup(), grouping);
        assertEquals(response.getImmediateMembersList().size(), membersList.size());
        response.getImmediateMembersList().forEach(id -> assertTrue(membersList.contains(id)));
    }

    /**
     * Group not found, return emtpy response.
     */

    @Test
    public void testGetGroupAndImmediateMembersGroupNotFound() {
        //GIVEN
        long groupId = 123L;
        GetGroupAndImmediateMembersRequest getGroupAndImmediateMembersRequest =
                        GetGroupAndImmediateMembersRequest.newBuilder()
                                        .setGroupId(groupId)
                                        .build();
        final StreamObserver<GetGroupAndImmediateMembersResponse> responseObserverMock =
                        Mockito.mock(StreamObserver.class);

        Grouping grouping = Grouping.newBuilder().setId(123L).build();

        doReturn(Optional.empty()).when(temporaryGroupCache).getGrouping(groupId);
        doReturn(Collections.emptyList()).when(groupStoreDAO).getGroupsById(Collections.singleton(groupId));
        Set<Long> membersList = Collections.singleton(1L);
        //WHEN
        groupRpcService.getGroupAndImmediateMembers(getGroupAndImmediateMembersRequest,
                                                    responseObserverMock);

        //Then
        ArgumentCaptor<GetGroupAndImmediateMembersResponse> captor = ArgumentCaptor.forClass(GetGroupAndImmediateMembersResponse.class);
        verify(responseObserverMock, Mockito.times(1)).onNext(captor.capture());
        GetGroupAndImmediateMembersResponse response = captor.getValue();
        assertFalse(response.hasGroup());
    }
}
