package com.vmturbo.api.component.external.api.service;

import static com.vmturbo.api.component.external.api.service.GroupsService.USER_GROUPS;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

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
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.http.ResponseEntity;

import com.vmturbo.api.component.ApiTestUtils;
import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.MultiEntityRequest;
import com.vmturbo.api.component.communication.RepositoryApi.RepositoryRequestResult;
import com.vmturbo.api.component.communication.RepositoryApi.SearchRequest;
import com.vmturbo.api.component.external.api.mapper.ActionSpecMapper;
import com.vmturbo.api.component.external.api.mapper.CloudTypeMapper;
import com.vmturbo.api.component.external.api.mapper.EntityFilterMapper;
import com.vmturbo.api.component.external.api.mapper.GroupFilterMapper;
import com.vmturbo.api.component.external.api.mapper.GroupMapper;
import com.vmturbo.api.component.external.api.mapper.PaginationMapper;
import com.vmturbo.api.component.external.api.mapper.SettingsManagerMappingLoader.SettingsManagerMapping;
import com.vmturbo.api.component.external.api.mapper.SettingsMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.CachedGroupInfo;
import com.vmturbo.api.component.external.api.mapper.aspect.EntityAspectMapper;
import com.vmturbo.api.component.external.api.util.BusinessAccountRetriever;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.component.external.api.util.ObjectsPage;
import com.vmturbo.api.component.external.api.util.ServiceProviderExpander;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory.SupplyChainNodeFetcherBuilder;
import com.vmturbo.api.component.external.api.util.action.ActionSearchUtil;
import com.vmturbo.api.component.external.api.util.action.ActionStatsQueryExecutor;
import com.vmturbo.api.component.external.api.util.cost.CostStatsQueryExecutor;
import com.vmturbo.api.component.external.api.util.setting.EntitySettingQueryExecutor;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.action.ActionApiInputDTO;
import com.vmturbo.api.dto.businessunit.BusinessUnitApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.entity.TagApiDTO;
import com.vmturbo.api.dto.group.FilterApiDTO;
import com.vmturbo.api.dto.group.GroupApiDTO;
import com.vmturbo.api.dto.group.ResourceGroupApiDTO;
import com.vmturbo.api.dto.setting.SettingApiDTO;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.enums.ActionCostType;
import com.vmturbo.api.enums.CloudType;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.api.exceptions.ConversionException;
import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.pagination.GroupMembersPaginationRequest;
import com.vmturbo.api.pagination.GroupMembersPaginationRequest.GroupMemberOrderBy;
import com.vmturbo.api.pagination.GroupMembersPaginationRequest.GroupMembersPaginationResponse;
import com.vmturbo.api.pagination.GroupPaginationRequest;
import com.vmturbo.api.pagination.GroupPaginationRequest.GroupPaginationResponse;
import com.vmturbo.api.pagination.PaginationUtil;
import com.vmturbo.api.pagination.SearchOrderBy;
import com.vmturbo.api.pagination.SearchPaginationRequest;
import com.vmturbo.api.pagination.SearchPaginationRequest.SearchPaginationResponse;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.common.api.mappers.EnvironmentTypeMapper;
import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.common.protobuf.action.ActionDTOMoles.ActionsServiceMole;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.common.Pagination;
import com.vmturbo.common.protobuf.common.Pagination.OrderBy;
import com.vmturbo.common.protobuf.common.Pagination.OrderBy.GroupOrderBy;
import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.CreateGroupRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.CreateGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.CreateTagsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.CreateTagsResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.DeleteTagRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.DeleteTagResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.DeleteTagsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.DeleteTagsResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetPaginatedGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetPaginatedGroupsResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetTagsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetTagsResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.EntityFilters;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.EntityFilters.EntityFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.GroupFilters;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupID;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.group.GroupDTO.Origin;
import com.vmturbo.common.protobuf.group.GroupDTO.Origin.Type;
import com.vmturbo.common.protobuf.group.GroupDTO.OriginFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers.StaticMembersByType;
import com.vmturbo.common.protobuf.group.GroupDTO.UpdateGroupRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.UpdateGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.plan.TemplateDTO;
import com.vmturbo.common.protobuf.plan.TemplateDTO.GetTemplatesRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.GetTemplatesResponse;
import com.vmturbo.common.protobuf.plan.TemplateDTO.SingleTemplateResponse;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateInfo;
import com.vmturbo.common.protobuf.plan.TemplateDTOMoles.TemplateServiceMole;
import com.vmturbo.common.protobuf.plan.TemplateServiceGrpc;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode.MemberList;
import com.vmturbo.common.protobuf.search.Search.LogicalOperator;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.StringFilter;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.search.SearchableProperties;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingPoliciesRequest;
import com.vmturbo.common.protobuf.setting.SettingProtoMoles.SettingPolicyServiceMole;
import com.vmturbo.common.protobuf.tag.Tag.TagValuesDTO;
import com.vmturbo.common.protobuf.tag.Tag.Tags;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.group.api.GroupAndMembers;
import com.vmturbo.group.api.ImmutableGroupAndMembers;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;
import com.vmturbo.topology.processor.api.util.ThinTargetCache.ThinTargetInfo;

public class GroupsServiceTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private static final String NE_MATCH_TYPE = "NE";
    private static final String EQ_MATCH_TYPE = "EQ";
    private static final String GROUP_FILTER_TYPE = "groupsByName";
    private static final String CLUSTER_FILTER_TYPE = "clustersByName";
    private GroupsService groupsService;

    private static final String GROUP_TEST_PATTERN = "groupTestString";
    private static final String CLUSTER_TEST_PATTERN = "clusterTestString";
    private static final Integer DEFAULT_N_ENTITIES = 20;
    
    private static final long VM_ID = 1L;
    private static final long GROUP_ID = 2L;
    private static final String TAG_KEY = "TAG_KEY";
    private static final List<String> TAG_VALUES = ImmutableList.of("tagValue1", "tagValue2");

    @Mock
    private ActionSpecMapper actionSpecMapper;

    @Mock
    private GroupMapper groupMapper;

    @Mock
    private GroupFilterMapper groupFilterMapper;

    @Mock
    private EntityAspectMapper entityAspectMapper;

    @Mock
    private RepositoryApi repositoryApiMock;

    @Mock
    private UuidMapper uuidMapper;

    @Mock
    private GroupExpander groupExpanderMock;

    @Mock
    private ActionStatsQueryExecutor actionStatsQueryExecutor;

    @Mock
    private ThinTargetCache targetCache;

    @Mock
    private EntitySettingQueryExecutor entitySettingQueryExecutor;

    @Mock
    private SupplyChainFetcherFactory supplyChainFetcherFactory;

    @Mock
    private SettingsMapper settingsMapper;

    @Mock
    private ServiceProviderExpander serviceProviderExpander;

    @Mock
    private PaginationMapper paginationMapperMock;

    @Mock
    private UserSessionContext userSessionContextMock;

    @Mock
    private CostStatsQueryExecutor costStatsQueryExecutor;

    @Captor
    private ArgumentCaptor<GetGroupsRequest> getGroupsRequestCaptor;

    private final TemplateServiceMole templateServiceSpy = spy(new TemplateServiceMole());
    private final GroupServiceMole groupServiceSpyMole = spy(new GroupServiceMole());

    private final ActionsServiceMole actionServiceSpy = spy(new ActionsServiceMole());

    private final SettingPolicyServiceMole settingPolicyServiceSpy = spy(new SettingPolicyServiceMole());

    private final FilterApiDTO groupFilterApiDTO = new FilterApiDTO();
    private final FilterApiDTO clusterFilterApiDTO = new FilterApiDTO();

    private BusinessAccountRetriever businessAccountRetriever;

    @Rule
    public GrpcTestServer grpcServer =
        GrpcTestServer.newServer(groupServiceSpyMole, templateServiceSpy, actionServiceSpy,
                settingPolicyServiceSpy);

    private static final String UI_REAL_TIME_MARKET = "Market";
    private static final long CONTEXT_ID = 7777777;
    private Map<Long, GroupApiDTO> mappedGroups;

    @Before
    public void init() throws Exception {
        MockitoAnnotations.initMocks(this);

        // create inputs for the service
        final ActionsServiceBlockingStub actionOrchestratorRpcService =
                ActionsServiceGrpc.newBlockingStub(grpcServer.getChannel());
        final SettingPolicyServiceBlockingStub settingPolicyStub =
                SettingPolicyServiceGrpc.newBlockingStub(grpcServer.getChannel());
        final ActionSearchUtil actionSearchUtil =
                new ActionSearchUtil(
                                actionOrchestratorRpcService, actionSpecMapper,
                                paginationMapperMock, supplyChainFetcherFactory, groupExpanderMock,
                                serviceProviderExpander,
                                CONTEXT_ID, true);
        this.businessAccountRetriever = Mockito.mock(BusinessAccountRetriever.class);
        groupsService =
            new GroupsService(
                actionOrchestratorRpcService,
                GroupServiceGrpc.newBlockingStub(grpcServer.getChannel()),
                groupMapper,
                groupExpanderMock,
                uuidMapper,
                repositoryApiMock,
                CONTEXT_ID,
                mock(SettingsManagerMapping.class),
                TemplateServiceGrpc.newBlockingStub(grpcServer.getChannel()),
                entityAspectMapper,
                actionStatsQueryExecutor,
                supplyChainFetcherFactory,
                actionSearchUtil,
                settingPolicyStub,
                settingsMapper,
                targetCache, entitySettingQueryExecutor,
                groupFilterMapper,
                businessAccountRetriever,
                serviceProviderExpander,
                paginationMapperMock,
                userSessionContextMock,
                costStatsQueryExecutor) {
            @Override
            protected String getUsername() {
                return "testUser";
            }
        };

        groupFilterApiDTO.setFilterType(GROUP_FILTER_TYPE);
        groupFilterApiDTO.setExpVal(GROUP_TEST_PATTERN);
        groupFilterApiDTO.setExpType(EQ_MATCH_TYPE);

        clusterFilterApiDTO.setFilterType(CLUSTER_FILTER_TYPE);
        clusterFilterApiDTO.setExpVal(CLUSTER_TEST_PATTERN);
        clusterFilterApiDTO.setExpType(EQ_MATCH_TYPE);
        mappedGroups = new HashMap<>();
        Mockito.when(groupMapper.toGroupApiDto(Mockito.any(), Mockito.anyBoolean(), Mockito.any(),
                Mockito.any())).thenAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            final List<Grouping> groups = (List<Grouping>)invocation.getArguments()[0];
            final List<GroupApiDTO> result = groups.stream()
                    .map(Grouping::getId)
                    .map(mappedGroups::get)
                    .collect(Collectors.toList());
            return new ObjectsPage<>(result, result.size(), result.size());
        });
    }

    /**
     * Test generating group request with name pattern match.
     */
    @Test
    public void testGetGroupsWithFilterEq() throws Exception {
        // Arrange
        groupFilterApiDTO.setExpType(EQ_MATCH_TYPE);
        when(groupFilterMapper.apiFilterToGroupFilter(eq(GroupType.REGULAR),
                        eq(LogicalOperator.AND),
                        eq(Collections.singletonList(groupFilterApiDTO))))
                .thenReturn(
                        GroupFilter.newBuilder().addPropertyFilters(
                                PropertyFilter.newBuilder()
                                .setStringFilter(
                                        StringFilter.newBuilder()
                                                .setStringPropertyRegex(
                                                        StringConstants.DISPLAY_NAME_ATTR))));

        // Act
        List<FilterApiDTO> filterList = Lists.newArrayList(groupFilterApiDTO);
        final GroupDTO.GetGroupsRequest.Builder groupsRequest =
                groupsService.getGroupsRequestForFilters(GroupType.REGULAR,
                                filterList);

        // Assert
        assertThat(
            groupsRequest.getGroupFilter()
                .getPropertyFilters(0).getStringFilter().getStringPropertyRegex(),
            is(StringConstants.DISPLAY_NAME_ATTR));
        assertThat(
            groupsRequest.getGroupFilter().getPropertyFilters(0).getStringFilter().getPositiveMatch(),
            is(true));
    }

    /**
     * Test generating group request with negation on the name pattern match.
     */
    @Test
    public void testGetGroupsWithFilterNe() throws Exception {
        // Arrange
        groupFilterApiDTO.setExpType(NE_MATCH_TYPE);

        when(groupFilterMapper.apiFilterToGroupFilter(eq(GroupType.REGULAR),
                        eq(LogicalOperator.AND),
                        eq(Collections.singletonList(groupFilterApiDTO))))
                .thenReturn(
                        GroupFilter.newBuilder().addPropertyFilters(
                                PropertyFilter.newBuilder()
                                .setStringFilter(
                                        StringFilter.newBuilder()
                                                .setStringPropertyRegex(
                                                        StringConstants.DISPLAY_NAME_ATTR)
                                        .setPositiveMatch(false))

                                )
                        );

        // Act
        List<FilterApiDTO> filterList = Lists.newArrayList(groupFilterApiDTO);
        final GroupDTO.GetGroupsRequest.Builder groupsRequest =
                groupsService.getGroupsRequestForFilters(GroupType.REGULAR,
                                filterList);

        // Assert
        assertThat(
            groupsRequest.getGroupFilter()
                .getPropertyFilters(0).getStringFilter().getStringPropertyRegex(),
            is(StringConstants.DISPLAY_NAME_ATTR));
        assertThat(
            groupsRequest.getGroupFilter().getPropertyFilters(0).getStringFilter().getPositiveMatch(),
            is(false));
    }

    /**
     * Test getting groups with origin filter given a USER_GROUPS scope.
     * @throws Exception if generic error
     */
    @Test
    public void testGetGroupsWithUserGroupScopes() throws Exception {
        // Arrange
        when(groupFilterMapper.apiFilterToGroupFilter(eq(GroupType.REGULAR),
                eq(LogicalOperator.AND),
                eq(Collections.emptyList()))).thenReturn(GroupFilter.newBuilder()
                    .setGroupType(GroupType.REGULAR));

        OriginFilter originFilter =  OriginFilter.newBuilder()
                .addOrigin(GroupDTO.Origin.Type.USER).build();
        List<String> scopes = Arrays.asList(USER_GROUPS);

        // Act
        List<FilterApiDTO> filterList = Collections.emptyList();
        final GroupDTO.GetGroupsRequest.Builder groupsRequest =
                groupsService.getGroupsRequestForFilters(GroupType.REGULAR,
                        filterList, scopes, true, null );

        // Assert
        assertEquals(groupsRequest.getGroupFilter().getOriginFilter().getOriginCount(), 1);
        assertEquals(groupsRequest.getGroupFilter().getOriginFilter(), originFilter);
    }

    /**
     * Test getting groups with origin filter given a User origin.
     * @throws Exception if generic error
     */
    @Test
    public void testGetGroupsWithOrigin() throws Exception {
        // Arrange
        when(groupFilterMapper.apiFilterToGroupFilter(eq(GroupType.REGULAR),
                eq(LogicalOperator.AND),
                eq(Collections.emptyList()))).thenReturn(GroupFilter.newBuilder()
                .setGroupType(GroupType.REGULAR));

        OriginFilter userOriginFilter =  OriginFilter.newBuilder()
                .addOrigin(GroupDTO.Origin.Type.USER).build();
        OriginFilter discoveredOriginFilter = OriginFilter.newBuilder()
                .addOrigin(Type.DISCOVERED).build();
        List<String> scopes = Collections.emptyList();

        // Act
        List<FilterApiDTO> filterList = Collections.emptyList();
        final GroupDTO.GetGroupsRequest.Builder groupsRequestForUser =
                groupsService.getGroupsRequestForFilters(GroupType.REGULAR,
                        filterList, scopes, true, com.vmturbo.api.enums.Origin.USER);
        final GroupDTO.GetGroupsRequest.Builder groupsRequestForDiscovered =
                groupsService.getGroupsRequestForFilters(GroupType.REGULAR,
                        filterList, scopes, true, com.vmturbo.api.enums.Origin.DISCOVERED);

        // Assert
        assertEquals(groupsRequestForUser.getGroupFilter().getOriginFilter().getOriginCount(), 1);
        assertEquals(groupsRequestForUser.getGroupFilter().getOriginFilter(), userOriginFilter);
        assertNotEquals(groupsRequestForUser.getGroupFilter().getOriginFilter(), discoveredOriginFilter);
        assertEquals(groupsRequestForDiscovered.getGroupFilter().getOriginFilter().getOriginCount(), 1);
        assertEquals(groupsRequestForDiscovered.getGroupFilter().getOriginFilter(), discoveredOriginFilter);
        assertNotEquals(groupsRequestForDiscovered.getGroupFilter().getOriginFilter(), userOriginFilter);
    }

    /**
     * Test getting groups with origin filter given a User Group scope and
     * an origin. Origin prevails against user group.
     * @throws Exception if generic error
     */

    @Test
    public void testGetGroupsWithOriginAndUserGroupScopes() throws Exception {
        // Arrange
        when(groupFilterMapper.apiFilterToGroupFilter(eq(GroupType.REGULAR),
                eq(LogicalOperator.AND),
                eq(Collections.emptyList()))).thenReturn(GroupFilter.newBuilder()
                .setGroupType(GroupType.REGULAR));

        OriginFilter userOriginFilter =  OriginFilter.newBuilder()
                .addOrigin(GroupDTO.Origin.Type.USER).build();
        OriginFilter discoveredOriginFilter = OriginFilter.newBuilder()
                .addOrigin(Type.DISCOVERED).build();
        List<String> scopes = Arrays.asList(USER_GROUPS);

        // Act
        List<FilterApiDTO> filterList = Collections.emptyList();
        final GroupDTO.GetGroupsRequest.Builder groupsRequestForUser =
                groupsService.getGroupsRequestForFilters(GroupType.REGULAR,
                        filterList, scopes, true, com.vmturbo.api.enums.Origin.DISCOVERED);


        // Assert
        assertEquals(groupsRequestForUser.getGroupFilter().getOriginFilter().getOriginCount(), 1);
        assertEquals(groupsRequestForUser.getGroupFilter().getOriginFilter(), discoveredOriginFilter);
        assertNotEquals(groupsRequestForUser.getGroupFilter().getOriginFilter(), userOriginFilter);

    }

    @Test
    public void getGroupByUuid() throws Exception {
        final GroupApiDTO apiGroup = new GroupApiDTO();
        apiGroup.setDisplayName("minion");
        final Grouping xlGroup = Grouping.newBuilder().setId(1).build();
        Mockito.when(groupMapper.groupsToGroupApiDto(Collections.singletonList(xlGroup), true))
                .thenReturn(Collections.singletonMap(1L, apiGroup));

        Mockito.when(groupExpanderMock.getGroup("1")).thenReturn(Optional.of(xlGroup));

        final GroupApiDTO retGroup = groupsService.getGroupByUuid("1", false);
        assertEquals(apiGroup.getDisplayName(), retGroup.getDisplayName());
    }

    /**
     * If the uuid is "GROUP-PhysicalMachineByCluster", return a GroupApiDTO object with the
     * same UUID.
     *
     * @throws Exception
     */
    @Test
    public void testGetGroupByClusterHeadroomUuid() throws Exception {
        String clusterHeadroomGroupUuid = "GROUP-PhysicalMachineByCluster";
        final GroupApiDTO retGroup = groupsService.getGroupByUuid(clusterHeadroomGroupUuid,
                false);

        assertNull(retGroup);
    }

    /**
     * Tests getting of all clusters from groups service. Currently UI provides
     * "GROUP-StorageByStorageCluster" group uuid when it expect to receive all storage clusters.
     *
     * @throws Exception it's thrown by groups service at the top level in case of any error.
     */
    @Test
    public void testGetClustersByClusterHeadroomGroupUuid() throws Exception {
        GroupApiDTO clusterApiDto = new GroupApiDTO();
        clusterApiDto.setUuid("mycluster");
        Grouping cluster = group(1L, GroupType.COMPUTE_HOST_CLUSTER);
        when(groupServiceSpyMole.getGroups(any()))
            .thenReturn(Collections.singletonList(cluster));
        when(groupMapper.toGroupApiDto(Collections.singletonList(group(1, GroupType.COMPUTE_HOST_CLUSTER)), true, null,
                null)).thenReturn(new ObjectsPage<>(Collections.singletonList(clusterApiDto), 1, 1));
        when(groupFilterMapper.apiFilterToGroupFilter(eq(GroupType.COMPUTE_HOST_CLUSTER),
            eq(LogicalOperator.AND),
            eq(Collections.emptyList()))).thenReturn(GroupFilter.newBuilder()
                .setGroupType(GroupType.COMPUTE_HOST_CLUSTER));

        final String clusterHeadroomGroupUuid = "GROUP-PhysicalMachineByCluster";
        GroupMembersPaginationRequest memberRequest = new GroupMembersPaginationRequest(null,
            DEFAULT_N_ENTITIES,
            false, GroupMemberOrderBy.DEFAULT);
        List<BaseApiDTO> clustersList =
            groupsService.getMembersByGroupUuid(clusterHeadroomGroupUuid,
                memberRequest).getRestResponse().getBody();


        verify(groupServiceSpyMole).getGroups(getGroupsRequestCaptor.capture());

        assertThat(clustersList, containsInAnyOrder(clusterApiDto));

        final GetGroupsRequest request = getGroupsRequestCaptor.getValue();
        assertEquals(GroupType.COMPUTE_HOST_CLUSTER, request.getGroupFilter().getGroupType());
    }

    /**
     * Tests getting of all storage clusters from groups service. Currently UI provides
     * "GROUP-StorageByStorageCluster" group uuid when it expect to receive all storage clusters.
     *
     * @throws Exception it's thrown by groups service at the top level in case of any error.
     */
    @Test
    public void testGetClustersByStorageClusterHeadroomGroupUuid() throws Exception {
        GroupApiDTO clusterApiDto = new GroupApiDTO();
        clusterApiDto.setUuid("mycluster");
        Grouping cluster = group(1L, GroupType.STORAGE_CLUSTER);
        when(groupServiceSpyMole.getGroups(any()))
            .thenReturn(Collections.singletonList(cluster));
        when(groupMapper.toGroupApiDto(Collections.singletonList(group(1L, GroupType.STORAGE_CLUSTER)), true, null,
                null)).thenReturn(
                new ObjectsPage<>(Collections.singletonList(clusterApiDto), 1, 1));
        when(groupFilterMapper.apiFilterToGroupFilter(eq(GroupType.STORAGE_CLUSTER),
            eq(LogicalOperator.AND),
            eq(Collections.emptyList()))).thenReturn(GroupFilter.newBuilder()
            .setGroupType(GroupType.STORAGE_CLUSTER));

        final String clusterHeadroomGroupUuid = "GROUP-StorageByStorageCluster";
        GroupMembersPaginationRequest memberRequest = new GroupMembersPaginationRequest("0",
            DEFAULT_N_ENTITIES,
            false, GroupMemberOrderBy.DEFAULT);
        List<BaseApiDTO> clustersList =
            groupsService.getMembersByGroupUuid(clusterHeadroomGroupUuid, memberRequest).getRestResponse().getBody();

        verify(groupServiceSpyMole).getGroups(getGroupsRequestCaptor.capture());
        assertThat(clustersList, containsInAnyOrder(clusterApiDto));
        final GetGroupsRequest request = getGroupsRequestCaptor.getValue();
        assertEquals(GroupType.STORAGE_CLUSTER, request.getGroupFilter().getGroupType());
    }

    /**
     * Tests the putting setting for group API.
     * @throws Exception when something goes wrong.
     */
    @Test
    public void testPutSettingByUuidAndName() throws Exception {
        String groupUuid = "1234";
        String templateId = "3333";

        SettingApiDTO<String> setting = new SettingApiDTO<>();
        setting.setValue(templateId);

        Template template = Template.newBuilder()
                .setId(Long.parseLong(templateId))
                .setType(Template.Type.SYSTEM)
                .setTemplateInfo(TemplateInfo.newBuilder()
                        .setName("template name"))
                .build();
        when(templateServiceSpy.getTemplates(any(GetTemplatesRequest.class)))
                .thenReturn(Arrays.asList(GetTemplatesResponse.newBuilder()
                    .addTemplates(SingleTemplateResponse.newBuilder()
                        .setTemplate(template))
                    .build()));
        groupsService.putSettingByUuidAndName(groupUuid, "capacityplandatamanager",
                "templateName", setting);
        verify(templateServiceSpy).updateHeadroomTemplateForCluster(TemplateDTO.UpdateHeadroomTemplateRequest
                .newBuilder()
                .setGroupId(Long.parseLong(groupUuid))
                .setTemplateId(Long.parseLong(setting.getValue()))
                .build());
    }

    @Test(expected = UnknownObjectException.class)
    public void testGroupAndClusterNotFound() throws Exception {
        when(groupExpanderMock.getGroup("1")).thenReturn(Optional.empty());
        groupsService.getGroupByUuid("1", false);
    }

    @Test
    public void testGetGroupMembersByUuid() throws Exception {
        // Arrange
        final long member1Id = 7;
        final long member2Id = 8;

        Mockito.when(groupServiceSpyMole.getMembers(GetMembersRequest.newBuilder().addId(1L).build()))
                .thenReturn(Collections.singletonList(GetMembersResponse.newBuilder()
                        .setGroupId(1L)
                        .addMemberId(member1Id)
                        .addMemberId(member2Id)
                        .build()));

        final ServiceEntityApiDTO member1Dto = new ServiceEntityApiDTO();
        member1Dto.setUuid(Long.toString(member1Id));

        final ServiceEntityApiDTO member2Dto = new ServiceEntityApiDTO();
        member2Dto.setUuid(Long.toString(member2Id));

        Mockito.when(
                repositoryApiMock.getByIds(Collections.singletonList(member2Id), Collections.emptySet(),
                        false))
                .thenReturn(new RepositoryRequestResult(Collections.emptySet(),
                        Collections.singleton(member2Dto)));

        final GroupAndMembers groupAndMembers =
            groupAndMembers(1L, GroupType.REGULAR, new HashSet<>(Arrays.asList(7L, 8L)));
        when(groupExpanderMock.getGroupWithImmediateMembersOnly("1")).thenReturn(Optional.of(groupAndMembers));
        GroupMembersPaginationRequest request = new GroupMembersPaginationRequest("1",
            DEFAULT_N_ENTITIES,
            true, GroupMemberOrderBy.DEFAULT);
        // Act
        final GroupMembersPaginationResponse response = groupsService.getMembersByGroupUuid("1",
            request);

        // Assert
        assertThat(response.getRestResponse().getBody().size(), is(1));
        // Checks for pagination as well
        assertThat(response.getRestResponse().getBody().get(0), is(member2Dto));
    }

    /**
     * Test is {@link GroupsService#isMagicUiStringGroupUuid(String)}.
     */
    @Test
    public void testIsMagicUiStringGroupUuid() {
        assertTrue(groupsService.isMagicUiStringGroupUuid(GroupsService.CLUSTER_HEADROOM_GROUP_UUID));
        assertTrue(groupsService.isMagicUiStringGroupUuid(GroupsService.STORAGE_CLUSTER_HEADROOM_GROUP_UUID));
        assertTrue(groupsService.isMagicUiStringGroupUuid(GroupsService.USER_GROUPS));
        assertFalse(groupsService.isMagicUiStringGroupUuid("fakeUuid"));
    }

    /**
     * Test is {@link GroupsService#getMembersByGroupUuid(String, GroupMembersPaginationRequest)}.
     *
     * <p>Test magic Ui String calls
     * {@link GroupsService#getMembersForUIMagicGroupUuid(String, GroupMembersPaginationRequest)}
     * </p>
     * @throws Exception problem getting groupType
     */
    @Test
    public void testGetMembersByGroupUuidWithMagicUiStrings() throws Exception {
        //Given
        List<String> magicUiStrings = Arrays.asList(GroupsService.CLUSTER_HEADROOM_GROUP_UUID,
                                                    GroupsService.STORAGE_CLUSTER_HEADROOM_GROUP_UUID,
                                                    GroupsService.USER_GROUPS);
        GroupMembersPaginationRequest request = new GroupMembersPaginationRequest(null, null, true, null);

        GroupsService groupsServiceSpy = spy(groupsService);
        doReturn(Collections.emptyList()).when(groupsServiceSpy).getGroupsByType(Mockito.any(),
                                                                     Mockito.any(), Mockito.any());

        for (String magicUiString: magicUiStrings) {
            //When
            groupsServiceSpy.getMembersByGroupUuid(magicUiString, request);

            //Then
            verify(groupsServiceSpy).getMembersForUIMagicGroupUuid(magicUiString, request);
        }
    }

    /**
     * Tests {@link GroupsService#getMembersByGroupUuid(String, GroupMembersPaginationRequest)}.
     *
     * <p>Test nested Group legacy, unpaginated path</p>
     * @throws Exception unexpected
     */
    @Test
    public void testGetGroupMembersByGroupUuidForNestedGroupsLegacyUnpaginatedCode() throws Exception {
        //Given
        String groupUuid = "12345";
        GroupMembersPaginationRequest request = new GroupMembersPaginationRequest(null, null, true, null);

        Grouping grouping = Grouping.newBuilder()
                        .setDefinition(GroupDefinition.newBuilder()
                                        .setGroupFilters(GroupFilters.getDefaultInstance()))
                        .build();
        List<Long> members = Collections.singletonList(1L);
        GroupAndMembers groupAndMembers = ImmutableGroupAndMembers.builder()
                        .group(grouping)
                        .members(members)
                        .entities(members)
                        .build();

        GroupsService groupsServiceSpy = spy(groupsService);
        doReturn(groupAndMembers).when(groupsServiceSpy).getGroupWithImmediateMembersOnly(groupUuid);
        doReturn(Collections.emptyList()).when(groupsServiceSpy)
                        .getGroupApiDTOS(Mockito.any(GetGroupsRequest.class), eq(true));

        //When
        groupsServiceSpy.getMembersByGroupUuid(groupUuid, request);

        //Then
        ArgumentCaptor<GetGroupsRequest> getGroupsRequestArgumentCaptor =
                        ArgumentCaptor.forClass(GetGroupsRequest.class);
        verify(groupsServiceSpy, times(1)).getGroupApiDTOS(getGroupsRequestArgumentCaptor.capture(), eq(true));
        assertTrue(getGroupsRequestArgumentCaptor.getValue().getGroupFilter().getIdList().equals(members));
    }

    /**
     * Tests {@link GroupsService#getMembersByGroupUuid(String, GroupMembersPaginationRequest)} makes paginated call to get nested groups.
     * @throws Exception unexpected
     */
    @Test
    public void testGetGroupMembersByGroupUuidForNestedGroupsPaginatedCall() throws Exception {
        String groupUuid = "12345";
        final int limit = 10;
        final String cursor = "3";
        final boolean ascending = false;
        GroupMembersPaginationRequest request = new GroupMembersPaginationRequest(cursor, limit, ascending, null);
        Grouping grouping = Grouping.newBuilder()
                        .setDefinition(GroupDefinition.newBuilder()
                                                       .setGroupFilters(GroupFilters.getDefaultInstance()))
                        .build();
        List<Long> members = Collections.singletonList(1L);
        GroupAndMembers groupAndMembers = ImmutableGroupAndMembers.builder()
                        .group(grouping)
                        .members(members)
                        .entities(members)
                        .build();

        GroupsService groupsServiceSpy = spy(groupsService);
        doReturn(groupAndMembers).when(groupsServiceSpy).getGroupWithImmediateMembersOnly(groupUuid);
        doReturn(Collections.emptyList()).when(groupsServiceSpy)
                        .getGroupApiDTOS(Mockito.any(GetGroupsRequest.class), eq(true));
        doReturn(PaginationParameters.getDefaultInstance())
                        .when(paginationMapperMock).toProtoParams(request);


        doReturn(mock(GroupMembersPaginationResponse.class)).when(groupsServiceSpy)
                        .getGroupsPaginatedCall(groupAndMembers.members(), request);
        //WHEN
        GroupMembersPaginationResponse response = groupsServiceSpy.getMembersByGroupUuid(groupUuid, request);

        //THEN
        verify(groupsServiceSpy, times(1)).getGroupsPaginatedCall(groupAndMembers.members(), request);
    }

    /**
     * Tests paginated call to get info on nested groups, next page results with cursor presents.
     * @throws Exception unexpected
     */
    @Test
    public void testGetGroupsPaginatedCallNextCursor() throws Exception {
        //GIVEN
        List<Long> groupUuids = Arrays.asList(1L, 2L, 3L);
        int limit = 19;
        GroupMembersPaginationRequest requestSpy = spy(new GroupMembersPaginationRequest(null, limit, true, null));

        doReturn(PaginationParameters.getDefaultInstance()).when(paginationMapperMock).toProtoParams(requestSpy);
        int totalRecordCount = 200;
        String nextCursor = "4";
        Pagination.PaginationResponse paginationResponse = Pagination.PaginationResponse.newBuilder()
                        .setTotalRecordCount(totalRecordCount)
                        .setNextCursor(nextCursor)
                        .build();
        GetPaginatedGroupsResponse getGroupResponse = GetPaginatedGroupsResponse.newBuilder()
                            .addGroups(newGroup(groupUuids.get(0), null))
                            .addGroups(newGroup(groupUuids.get(1), null))
                            .addGroups(newGroup(groupUuids.get(2), null))
                        .setPaginationResponse(paginationResponse)
                        .build();

        doReturn(getGroupResponse).when(groupServiceSpyMole).getPaginatedGroups(Mockito.any(
                        GetPaginatedGroupsRequest.class));

        //WHEN
        groupsService.getGroupsPaginatedCall(groupUuids, requestSpy);

        //THEN
        verify(requestSpy).nextPageResponse(Mockito.any(), eq(nextCursor), eq(totalRecordCount));
        ArgumentCaptor<GetPaginatedGroupsRequest> getPaginatedGroupsRequestArgumentCaptor = ArgumentCaptor.forClass(GetPaginatedGroupsRequest.class);
        verify(groupServiceSpyMole).getPaginatedGroups(getPaginatedGroupsRequestArgumentCaptor.capture());
        GetPaginatedGroupsRequest getPaginatedGroupsRequest = getPaginatedGroupsRequestArgumentCaptor.getValue();

        assertEquals(getPaginatedGroupsRequest.getGroupFilter().getIdList(), groupUuids);
        assertFalse(getPaginatedGroupsRequest.getGroupFilter().getIncludeHidden());
    }

    /**
     * Tests paginated call to get info on nested groups, final page results.
     * @throws Exception unexpected
     */
    @Test
    public void testGetGroupsPaginatedCallFinalPageResponse()
                    throws Exception {
        //GIVEN
        List<Long> groupUuids = Arrays.asList(1L, 2L, 3L);
        int limit = 19;
        GroupMembersPaginationRequest requestSpy = spy(new GroupMembersPaginationRequest(null, limit, true, null));

        doReturn(PaginationParameters.getDefaultInstance()).when(paginationMapperMock).toProtoParams(requestSpy);
        int totalRecordCount = 200;
        Pagination.PaginationResponse paginationResponse = Pagination.PaginationResponse.newBuilder()
                        .setTotalRecordCount(totalRecordCount)
                        .build();
        GetPaginatedGroupsResponse getGroupResponse = GetPaginatedGroupsResponse.newBuilder()
                        .addGroups(newGroup(groupUuids.get(0), null))
                        .addGroups(newGroup(groupUuids.get(1), null))
                        .addGroups(newGroup(groupUuids.get(2), null))
                        .setPaginationResponse(paginationResponse)
                        .build();

        doReturn(getGroupResponse).when(groupServiceSpyMole).getPaginatedGroups(Mockito.any(
                        GetPaginatedGroupsRequest.class));

        //WHEN
        groupsService.getGroupsPaginatedCall(groupUuids, requestSpy);

        //THEN
        verify(requestSpy).finalPageResponse(Mockito.any(), eq(totalRecordCount));

        ArgumentCaptor<GetPaginatedGroupsRequest> getPaginatedGroupsRequestArgumentCaptor = ArgumentCaptor.forClass(GetPaginatedGroupsRequest.class);
        verify(groupServiceSpyMole).getPaginatedGroups(getPaginatedGroupsRequestArgumentCaptor.capture());
        GetPaginatedGroupsRequest getPaginatedGroupsRequest = getPaginatedGroupsRequestArgumentCaptor.getValue();

        assertEquals(getPaginatedGroupsRequest.getGroupFilter().getIdList(), groupUuids);
        assertFalse(getPaginatedGroupsRequest.getGroupFilter().getIncludeHidden());
    }

    /**
     * Tests retrieval of group of business accounts.
     *
     * @throws Exception unexpected
     */
    @Test
    public void testGetGroupMembersOfBaByUuid() throws Exception {
        // Arrange
        final long member1Id = 7L;
        final long member2Id = 8L;
        final long groupId = 1L;
        final Set<Long> membersSet = Sets.newHashSet(member1Id, member2Id);

        when(groupServiceSpyMole.getMembers(
                GetMembersRequest.newBuilder().addId(1L).build())).thenReturn(
                Collections.singletonList(GetMembersResponse.newBuilder()
                        .setGroupId(1L)
                        .addMemberId(member1Id)
                        .addMemberId(member2Id)
                        .build()));

        final BusinessUnitApiDTO member1Dto = new BusinessUnitApiDTO();
        member1Dto.setUuid(Long.toString(member1Id));

        final BusinessUnitApiDTO member2Dto = new BusinessUnitApiDTO();
        member2Dto.setUuid(Long.toString(member2Id));

        final GroupDefinition groupDefinition =
                GroupDefinition.newBuilder().setType(GroupType.REGULAR).build();
        final Grouping group = Grouping.newBuilder()
                .setDefinition(groupDefinition)
                .setId(groupId)
                .addExpectedTypes(
                        MemberType.newBuilder().setEntity(EntityType.BUSINESS_ACCOUNT_VALUE))
                .build();
        final GroupAndMembers groupAndMembers = groupAndMembers(group, membersSet);
        Mockito.when(groupExpanderMock.getGroupWithImmediateMembersOnly("1")).thenReturn(Optional.of(groupAndMembers));
        Mockito.when(repositoryApiMock.getByIds(Arrays.asList(member1Id, member2Id),
                Collections.emptySet(), false))
                .thenReturn(new RepositoryRequestResult(Arrays.asList(member1Dto, member2Dto),
                        Collections.emptySet()));
        final GroupMembersPaginationRequest request =
                new GroupMembersPaginationRequest(null, null, true, GroupMemberOrderBy.DEFAULT);
        // Act
        final GroupMembersPaginationResponse response =
                groupsService.getMembersByGroupUuid(Long.toString(groupId), request);
        Assert.assertEquals(Sets.newHashSet(member2Dto, member1Dto),
                new HashSet<>(response.getRestResponse().getBody()));
    }

    private Grouping group(final long groupId,
                                            GroupType groupType) {
        return Grouping.newBuilder()
            .setId(groupId)
            .setDefinition(GroupDefinition.newBuilder().setType(groupType))
            .build();
    }

    private GroupAndMembers groupAndMembers(final long groupId,
            GroupType groupType,
            Set<Long> members) {
        final Grouping group = Grouping.newBuilder()
                .setId(groupId)
                .setDefinition(GroupDefinition.newBuilder().setType(groupType))
                .build();
        return groupAndMembers(group, members);
    }

    private GroupAndMembers groupAndMembers(@Nonnull Grouping group, Set<Long> members) {
        return ImmutableGroupAndMembers.builder()
            .group(group)
            .members(members)
            .entities(members)
            .build();
    }

    @Test
    public void testGetGroupMembersByUuidNoMembers() throws Exception {
        // Arrange
        when(groupServiceSpyMole.getMembers(GetMembersRequest.newBuilder()
                .addId(1L)
                .build()))
            // No members in group.
            .thenReturn(Collections.singletonList(GetMembersResponse.newBuilder().setGroupId(1L)
                .build()));

        when(groupExpanderMock.getGroupWithImmediateMembersOnly("1"))
            .thenReturn(Optional.of(groupAndMembers(1L, GroupType.REGULAR, Collections.emptySet())));

        // Act
        GroupMembersPaginationRequest request = new GroupMembersPaginationRequest(null,
            DEFAULT_N_ENTITIES,
            false, GroupMemberOrderBy.DEFAULT);
        GroupMembersPaginationResponse response = groupsService.getMembersByGroupUuid("1", request);

        // Assert
        assertTrue(response.getRestResponse().getBody().isEmpty());
        // Should be no call to repository to get entity information.
        verify(groupServiceSpyMole, never()).getGroups( GetGroupsRequest.getDefaultInstance());
        verifyZeroInteractions(repositoryApiMock);
    }

    /**
     * Tests pagination of getMembersByGroupUuid.
     *
     * @throws Exception should not be thrown.
     */
    @Test
    public void testGetGroupMembersByUuidPagination() throws Exception {
        final long groupId = 1L;
        // contains 78 ids from 0 to 77 .range upperbound is exclusive
        // this is too many, and cannot fit in a page size of 20
        final Set<Long> membersSet = LongStream.range(0, 78).boxed().collect(Collectors.toSet());

        Mockito.when(groupServiceSpyMole.getMembers(
                GetMembersRequest.newBuilder().addId(groupId).build())).thenReturn(
                Collections.singletonList(GetMembersResponse.newBuilder()
                        .setGroupId(groupId)
                        .addAllMemberId(membersSet)
                        .build()));

        final GroupDefinition groupDefinition =
            GroupDefinition.newBuilder().setType(GroupType.REGULAR).build();
        final Grouping group = Grouping.newBuilder()
            .setDefinition(groupDefinition)
            .setId(groupId)
            .addExpectedTypes(
                MemberType.newBuilder().setEntity(EntityType.VIRTUAL_MACHINE_VALUE))
            .build();
        final GroupAndMembers groupAndMembers = groupAndMembers(group, membersSet);
        Mockito.when(groupExpanderMock.getGroupWithImmediateMembersOnly("1")).thenReturn(Optional.of(groupAndMembers));
        final ArgumentCaptor<Collection<Long>> collectionCaptor = ArgumentCaptor.forClass((Class)Collection.class);
        Mockito.when(repositoryApiMock.getByIds(collectionCaptor.capture(), eq(Collections.emptySet()), eq(false)))
            .thenReturn(new RepositoryRequestResult(
                Collections.emptyList(),
                // We assume that we were asked for 20, return 20, and check to make sure we asked
                // the repository for 20 using an argument captor and an assert that later on.
                LongStream.range(0, 20).boxed().map(oid -> makeMinimalDTO(oid)).collect(Collectors.toList())));
        final GroupMembersPaginationRequest request =
            new GroupMembersPaginationRequest(null, 20, true, GroupMemberOrderBy.DEFAULT);

        final GroupMembersPaginationResponse response =
            groupsService.getMembersByGroupUuid(Long.toString(groupId), request);

        final Collection<Long> actualAsk = collectionCaptor.getValue();
        // Should only ask for first 20 items from the repository.
        Assert.assertEquals(20, collectionCaptor.getValue().size());
        Assert.assertEquals(
            LongStream.range(0, 20).boxed().collect(Collectors.toList()), actualAsk);
        Assert.assertEquals("20", response.getRestResponse().getHeaders().get("X-Next-Cursor").get(0));
        Assert.assertEquals("78", response.getRestResponse().getHeaders().get("X-Total-Record-Count").get(0));
    }

    private ServiceEntityApiDTO makeMinimalDTO(long oid) {
        final ServiceEntityApiDTO serviceEntityAPiDDTO = new ServiceEntityApiDTO();
        serviceEntityAPiDDTO.setUuid(Long.toString(oid));
        return serviceEntityAPiDDTO;
    }

    @Test
    public void testCreateGroup() throws Exception {
        final GroupApiDTO apiDTO = new GroupApiDTO();
        final Grouping group = Grouping.newBuilder()
                .setId(7L)
                .build();
        CreateGroupRequest request = CreateGroupRequest
                        .newBuilder()
                        .setGroupDefinition(GroupDefinition.getDefaultInstance())
                        .setOrigin(Origin.newBuilder().setUser(Origin.User.newBuilder()
                                        .setUsername("testUser")))
                        .build();
        when(groupMapper.toGroupDefinition(apiDTO)).thenReturn(GroupDefinition.getDefaultInstance());
        when(groupServiceSpyMole.createGroup(request))
            .thenReturn(CreateGroupResponse.newBuilder()
                    .setGroup(group)
                    .build());
        when(groupMapper.groupsToGroupApiDto(Collections.singletonList(group), true)).thenReturn(
                Collections.singletonMap(group.getId(), apiDTO));
        assertThat(groupsService.createGroup(apiDTO), is(apiDTO));
    }

    @Test
    public void testCreateTempGroupONPREM() throws Exception {
        final GroupApiDTO apiDTO = new GroupApiDTO();
        apiDTO.setTemporary(true);
        apiDTO.setEnvironmentType(EnvironmentType.ONPREM);
        final Grouping group = Grouping.newBuilder()
                .setId(7L)
                .build();
        when(groupMapper.toGroupDefinition(apiDTO)).thenReturn(GroupDefinition.getDefaultInstance());
        when(groupServiceSpyMole.createGroup(CreateGroupRequest.newBuilder()
                .setGroupDefinition(GroupDefinition.getDefaultInstance())
                .setOrigin(Origin.newBuilder().setUser(Origin.User.newBuilder()
                                .setUsername("testUser")))
                .build()))
            .thenReturn(CreateGroupResponse.newBuilder()
                .setGroup(group)
                .build());
        Mockito.when(groupMapper.groupsToGroupApiDto(Collections.singletonList(group), true))
                .thenReturn(Collections.singletonMap(group.getId(), apiDTO));
        assertThat(groupsService.createGroup(apiDTO), is(apiDTO));
    }

    @Test
    public void testCreateTempGroupCloud() throws Exception {
        final GroupApiDTO apiDTO = new GroupApiDTO();
        apiDTO.setTemporary(true);
        apiDTO.setEnvironmentType(EnvironmentType.CLOUD);
        final Grouping group = Grouping.newBuilder()
                .setId(7L)
                .build();
        when(groupMapper.toGroupDefinition(apiDTO)).thenReturn(GroupDefinition.getDefaultInstance());
        when(groupServiceSpyMole.createGroup(CreateGroupRequest.newBuilder()
                .setOrigin(Origin.newBuilder().setUser(Origin.User.newBuilder()
                                .setUsername("testUser")))
                .setGroupDefinition(GroupDefinition.getDefaultInstance())
                .build()))
                .thenReturn(CreateGroupResponse.newBuilder()
                        .setGroup(group)
                        .build());
        Mockito.when(groupMapper.groupsToGroupApiDto(Collections.singletonList(group), true))
                .thenReturn(Collections.singletonMap(group.getId(), apiDTO));
        assertThat(groupsService.createGroup(apiDTO), is(apiDTO));
    }

    /**
     * Test deleting a group.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testEditGroup() throws Exception {
        final GroupApiDTO groupApiDTO = new GroupApiDTO();
        final GroupDefinition groupDefinition = GroupDefinition.getDefaultInstance();
        final Grouping group = Grouping.newBuilder().setId(1).build();

        when(groupMapper.toGroupDefinition(eq(groupApiDTO))).thenReturn(groupDefinition);

        when(groupServiceSpyMole.getGroup(eq(GroupID.newBuilder().setId(1).build()))).thenReturn(
                GetGroupResponse.newBuilder().setGroup(group).build());
        UpdateGroupResponse updateGroupResponse =
                UpdateGroupResponse.newBuilder().setUpdatedGroup(group).build();

        when(groupServiceSpyMole.updateGroup(eq(UpdateGroupRequest.newBuilder()
                .setId(1)
                .setNewDefinition(groupDefinition)
                .build()))).thenReturn(updateGroupResponse);

        Mockito.when(groupMapper.groupsToGroupApiDto(
                Collections.singletonList(updateGroupResponse.getUpdatedGroup()), true))
                .thenReturn(Collections.singletonMap(group.getId(), groupApiDTO));

        assertThat(groupsService.editGroup("1", groupApiDTO), is(groupApiDTO));
    }

    /**
     * Test editing a group with invalid UUID.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testEditGroupInvalidUuid() throws Exception {
        final GroupApiDTO groupApiDTO = new GroupApiDTO();
        when(groupMapper.toGroupDefinition(eq(groupApiDTO))).thenReturn(GroupDefinition.getDefaultInstance());

        when(groupServiceSpyMole.getGroup(eq(GroupID.newBuilder().setId(1).build()))).thenReturn(
                GetGroupResponse.newBuilder().clearGroup().build());

        expectedException.expect(UnknownObjectException.class);
        expectedException.expectMessage("Group with UUID 1 does not exist");
        groupsService.editGroup("1", groupApiDTO);
    }

    @Test
    public void testGetComputerCluster() throws Exception {
        // Arrange
        final String name = "name";

        final GroupApiDTO groupApiDtoMock = new GroupApiDTO();
        groupApiDtoMock.setUuid("2");
        groupApiDtoMock.setDisplayName(name);
        groupApiDtoMock.setClassName(StringConstants.CLUSTER);
        groupApiDtoMock.setSeverity(Severity.NORMAL.toString());
        groupApiDtoMock.setEnvironmentType(EnvironmentType.ONPREM);

        final Grouping cluster = Grouping.newBuilder()
            .setId(2L)
            .setDefinition(GroupDefinition.newBuilder()
                            .setDisplayName(name)
                            .setType(GroupType.COMPUTE_HOST_CLUSTER))
            .build();

        mappedGroups.put(2L, groupApiDtoMock);
        when(groupFilterMapper.apiFilterToGroupFilter(eq(GroupType.COMPUTE_HOST_CLUSTER),
                eq(LogicalOperator.AND),
                eq(Collections.emptyList()))).thenReturn(GroupFilter.newBuilder()
                .setGroupType(GroupType.COMPUTE_HOST_CLUSTER));

        final GetGroupsRequest groupsRequest = GetGroupsRequest.newBuilder()
                .setGroupFilter(GroupFilter.newBuilder()
                        .setGroupType(GroupType.COMPUTE_HOST_CLUSTER))
                .addScopes(1L)
                .build();
        when(groupServiceSpyMole.getGroups(eq(groupsRequest))).thenReturn(
                Collections.singletonList(cluster));

        final ApiId entityApiId = mock(ApiId.class);
        when(entityApiId.isGroup()).thenReturn(false);
        Mockito.when(entityApiId.oid()).thenReturn(1L);

        when(uuidMapper.fromUuid("1")).thenReturn(entityApiId);

        // Act
        final List<GroupApiDTO> groupApiDTOs =
                groupsService.getGroupsByType(GroupType.COMPUTE_HOST_CLUSTER,
                        Collections.singletonList("1"), Collections.emptyList(),
                        EnvironmentType.ONPREM);

        // Assert
        assertThat(groupApiDTOs.size(), is(1));
        final GroupApiDTO groupApiDTO = groupApiDTOs.get(0);
        assertEquals(name, groupApiDTO.getDisplayName());
        assertEquals(StringConstants.CLUSTER, groupApiDTO.getClassName());
        assertEquals("2", groupApiDTO.getUuid());
        assertThat(groupApiDTO.getSeverity(), is(Severity.NORMAL.name()));
    }

    /**
     * Tests the case when we are trying to get resource groups inside the scope of group of
     * resource groups.
     *
     * @throws Exception when something goes wrong.
     */
    @Test
    public void getResourceGroupsInAGroupOfResourceGroups() throws Exception {
        //Arrange
        final ApiId entityApiId = mock(ApiId.class);
        when(entityApiId.isGroup()).thenReturn(true);

        when(uuidMapper.fromUuid("1")).thenReturn(entityApiId);

        final GroupDefinition groupDefinitionRg = GroupDefinition
            .newBuilder()
            .setType(GroupType.REGULAR)
            .setStaticGroupMembers(GroupDTO.StaticMembers.newBuilder()
                .addMembersByType(GroupDTO.StaticMembers.StaticMembersByType.newBuilder()
                    .setType(GroupDTO.MemberType.newBuilder().setGroup(GroupType.RESOURCE).build())
                    .addMembers(2L)
                    .build())
                .build())
            .build();

        final Grouping rgGroup = Grouping.newBuilder()
            .setDefinition(groupDefinitionRg)
            .setId(1L)
            .build();


        final GroupAndMembers groupAndMembers = ImmutableGroupAndMembers.builder()
            .group(rgGroup)
            .members(Arrays.asList(2L))
            .entities(Arrays.asList(3L, 4L))
            .build();

        when(groupExpanderMock.getGroupWithMembersAndEntities("1"))
            .thenReturn(Optional.of(groupAndMembers));


        final GroupDefinition groupDefinitionVm = GroupDefinition
            .newBuilder()
            .setType(GroupType.REGULAR)
            .setStaticGroupMembers(GroupDTO.StaticMembers.newBuilder()
                .addMembersByType(GroupDTO.StaticMembers.StaticMembersByType.newBuilder()
                    .setType(GroupDTO.MemberType.newBuilder()
                        .setEntity(ApiEntityType.VIRTUAL_MACHINE.typeNumber()).build())
                    .addMembers(11L)
                    .build())
                .build())
            .build();

        final Grouping vmGroup = Grouping.newBuilder()
            .setDefinition(groupDefinitionVm)
            .setId(10L)
            .build();


        final GroupAndMembers vmGroupAndMembers = ImmutableGroupAndMembers.builder()
            .group(vmGroup)
            .members(Collections.singletonList(11L))
            .entities(Collections.singletonList(11L))
            .build();

        when(groupExpanderMock.getGroupWithMembersAndEntities("10"))
            .thenReturn(Optional.of(vmGroupAndMembers));

        final GroupFilter groupFilter = GroupFilter.newBuilder()
            .setGroupType(GroupType.RESOURCE)
            .addAllId(Arrays.asList(2L))
            .build();

        final GetGroupsRequest groupsRequest = GetGroupsRequest.newBuilder()
            .setGroupFilter(groupFilter)
            .build();

        when(groupFilterMapper.apiFilterToGroupFilter(eq(GroupType.RESOURCE),
            eq(LogicalOperator.AND),
            eq(Collections.emptyList()))).thenReturn(GroupFilter.newBuilder()
                .setGroupType(GroupType.RESOURCE));

        final Grouping childRG = Grouping.newBuilder()
            .setId(2L)
            .build();

        when(groupServiceSpyMole.getGroups(eq(groupsRequest))).thenReturn(
                Collections.singletonList(childRG));

        final String rgName = "testRG";

        final GroupApiDTO groupApiDtoMock = new GroupApiDTO();
        groupApiDtoMock.setUuid("2");
        groupApiDtoMock.setDisplayName(rgName);
        groupApiDtoMock.setClassName(StringConstants.RESOURCE_GROUP);
        groupApiDtoMock.setSeverity(Severity.NORMAL.toString());

        when(groupMapper.toGroupApiDto(Collections.singletonList(childRG), true,
                null, null)).thenReturn(
                new ObjectsPage<>(Collections.singletonList(groupApiDtoMock), 1, 1));

        //Act
        final List<GroupApiDTO> groupApiDTOs =
            groupsService.getGroupsByType(GroupType.RESOURCE,
                Arrays.asList("1", "10"), Collections.emptyList());

        //Assert
        assertThat(groupApiDTOs.size(), is(1));
        final GroupApiDTO groupApiDTO = groupApiDTOs.get(0);
        assertEquals(rgName, groupApiDTO.getDisplayName());
        assertEquals(StringConstants.RESOURCE_GROUP, groupApiDTO.getClassName());
        assertEquals("2", groupApiDTO.getUuid());
        assertThat(groupApiDTO.getSeverity(), is(Severity.NORMAL.name()));
    }

    /**
     * Tests the case when we are trying to get resource groups inside the scope of single
     * resource group.
     *
     * @throws Exception when something goes wrong.
     */
    @Test
    public void testGetResourcesGroupInsideTheScopeOfSingleResourceGroup() throws Exception {
        //Arrange
        final long resourceGroupOid = 1L;
        final String rgName = "testRG";
        final ApiId entityApiId = mock(ApiId.class);
        when(entityApiId.isGroup()).thenReturn(true);
        when(uuidMapper.fromUuid(String.valueOf(resourceGroupOid))).thenReturn(entityApiId);

        final GroupDefinition groupDefinitionRg = GroupDefinition.newBuilder()
                .setType(GroupType.RESOURCE)
                .setStaticGroupMembers(StaticMembers.newBuilder()
                        .addMembersByType(StaticMembersByType.newBuilder()
                                .setType(MemberType.newBuilder()
                                        .setEntity(EntityType.VIRTUAL_MACHINE_VALUE))
                                .addAllMembers(Arrays.asList(2L, 3L))
                                .build())
                        .build())
                .build();

        final Grouping rgGroup = Grouping.newBuilder()
                .setDefinition(groupDefinitionRg)
                .addExpectedTypes(
                        MemberType.newBuilder().setEntity(EntityType.VIRTUAL_MACHINE_VALUE).build())
                .setId(resourceGroupOid)
                .build();

        final GroupAndMembers rgGroupAndMembers = ImmutableGroupAndMembers.builder()
                .group(rgGroup)
                .members(Arrays.asList(2L, 3L))
                .entities(Arrays.asList(2L, 3L))
                .build();

        when(groupExpanderMock.getGroupWithMembersAndEntities("1"))
                .thenReturn(Optional.of(rgGroupAndMembers));

        final GroupFilter groupFilter = GroupFilter.newBuilder()
                .setGroupType(GroupType.RESOURCE)
                .addAllId(Collections.singletonList(resourceGroupOid))
                .build();

        final GetGroupsRequest groupsRequest = GetGroupsRequest.newBuilder()
                .setGroupFilter(groupFilter)
                .build();

        when(groupFilterMapper.apiFilterToGroupFilter(eq(GroupType.RESOURCE),
                eq(LogicalOperator.AND),
                eq(Collections.emptyList()))).thenReturn(GroupFilter.newBuilder()
                .setGroupType(GroupType.RESOURCE));

        when(groupServiceSpyMole.getGroups(eq(groupsRequest))).thenReturn(
            Collections.singletonList(rgGroup));

        final GroupApiDTO groupApiDtoMock = new GroupApiDTO();
        groupApiDtoMock.setUuid(String.valueOf(resourceGroupOid));
        groupApiDtoMock.setDisplayName(rgName);
        groupApiDtoMock.setClassName(StringConstants.RESOURCE_GROUP);
        groupApiDtoMock.setSeverity(Severity.NORMAL.toString());

        when(groupMapper.toGroupApiDto(Collections.singletonList(rgGroup), true, null,
                null)).thenReturn(
                new ObjectsPage<>(Collections.singletonList(groupApiDtoMock), 1, 1));

        //Act
        final List<GroupApiDTO> groupApiDTOs =
                groupsService.getGroupsByType(GroupType.RESOURCE,
                        Collections.singletonList(String.valueOf(resourceGroupOid)), Collections.emptyList());

        //Assert
        assertThat(groupApiDTOs.size(), is(1));
        final GroupApiDTO groupApiDTO = groupApiDTOs.get(0);
        assertEquals(rgName, groupApiDTO.getDisplayName());
        assertEquals(StringConstants.RESOURCE_GROUP, groupApiDTO.getClassName());
        assertEquals(String.valueOf(resourceGroupOid), groupApiDTO.getUuid());
        assertThat(groupApiDTO.getSeverity(), is(Severity.NORMAL.name()));
    }

    @Test
    public void testExpandUuidsMarket() throws Exception {
        Set<Long> expandedIds = groupsService.expandUuids(
            Sets.newHashSet(UuidMapper.UI_REAL_TIME_MARKET_STR, "12"), null, null);
        assertThat(expandedIds, is(empty()));
    }

    @Test
    public void testExpandUuidsTarget() throws Exception {
        String target1 = "1";
        String target2 = "2";
        long entityId11 = 11;
        long entityId12 = 12;
        long entityId21 = 21;

        SearchRequest req = ApiTestUtils.mockSearchMinReq(Lists.newArrayList(
            MinimalEntity.newBuilder().setOid(entityId11).setEntityType(EntityType.VIRTUAL_MACHINE_VALUE).build(),
            MinimalEntity.newBuilder().setOid(entityId12).setEntityType(EntityType.PHYSICAL_MACHINE_VALUE).build(),
            MinimalEntity.newBuilder().setOid(entityId21).setEntityType(EntityType.VIRTUAL_MACHINE_VALUE).build()));

        when(repositoryApiMock.newSearchRequest(SearchParameters.newBuilder()
            .setStartingFilter(SearchProtoUtil.discoveredBy(Arrays.asList(1L, 2L)))
            .build())).thenReturn(req);

        final ThinTargetInfo targetInfo = mock(ThinTargetInfo.class);
        when(targetCache.getTargetInfo(Long.parseLong(target1))).thenReturn(Optional.of(targetInfo));
        when(targetCache.getTargetInfo(Long.parseLong(target2))).thenReturn(Optional.of(targetInfo));

        // without related entity type
        Set<Long> expandedIds = groupsService.expandUuids(Sets.newHashSet(target1, target2), null, null);
        assertThat(expandedIds, containsInAnyOrder(entityId11, entityId12, entityId21));

        // with related entity type
        expandedIds = groupsService.expandUuids(Sets.newHashSet(target1, target2),
            Lists.newArrayList(ApiEntityType.VIRTUAL_MACHINE.apiStr()), null);
        assertThat(expandedIds, containsInAnyOrder(entityId11, entityId21));
    }

    @Test
    public void testExpandUuidsGroup() throws Exception {
        String groupId11 = "11";
        long vm1 = 11;
        long vm2 = 12;
        long pm1 = 21;
        when(targetCache.getTargetInfo(Long.parseLong(groupId11))).thenReturn(Optional.empty());

        // expand a VM group, provide no entity type and expect to get all vms in the group
        SupplyChainNodeFetcherBuilder fetcherBuilder = ApiTestUtils.mockNodeFetcherBuilder(
                ImmutableMap.of(ApiEntityType.VIRTUAL_MACHINE.apiStr(), SupplyChainNode.newBuilder()
                    .putMembersByState(EntityState.POWERED_ON_VALUE, MemberList.newBuilder()
                        .addMemberOids(vm1).addMemberOids(vm2).build())
                    .build()));
        when(supplyChainFetcherFactory.newNodeFetcher()).thenReturn(fetcherBuilder);

        Set<Long> expandedIds = groupsService.expandUuids(Sets.newHashSet(groupId11), null, null);
        assertThat(expandedIds, containsInAnyOrder(vm1, vm2));

        // expand a VM group, provide PM entity type and expect to get all related PMs in the group
        fetcherBuilder = ApiTestUtils.mockNodeFetcherBuilder(
            ImmutableMap.of(ApiEntityType.PHYSICAL_MACHINE.apiStr(), SupplyChainNode.newBuilder()
                .putMembersByState(EntityState.POWERED_ON_VALUE, MemberList.newBuilder()
                    .addMemberOids(pm1).build())
                .build()));
        when(supplyChainFetcherFactory.newNodeFetcher()).thenReturn(fetcherBuilder);

        expandedIds = groupsService.expandUuids(Sets.newHashSet(groupId11),
            Lists.newArrayList(ApiEntityType.PHYSICAL_MACHINE.apiStr()), null);
        assertThat(expandedIds, containsInAnyOrder(pm1));
    }

    /**
     * Tests that when we call getSettingsByGroupUuid() with a group uuid as argument
     * the rpc method getGroup() is called with the same group uuid as argument.
     * @throws Exception when getSettingsByGroupUuid() throws an exception
     */
    @Test
    public void testGetSettingsByGroupUuid() throws Exception {
        String groupUuid = "1234";
        String templateId = "3333";
        ApiId apiId = mock(ApiId.class);

        Template template = Template.newBuilder()
            .setId(Long.parseLong(templateId))
            .setType(Template.Type.SYSTEM)
            .setTemplateInfo(TemplateInfo.newBuilder()
                .setName("template name"))
            .build();
        when(templateServiceSpy.getTemplates(any(GetTemplatesRequest.class)))
            .thenReturn(Arrays.asList(GetTemplatesResponse.newBuilder()
                .addTemplates(SingleTemplateResponse.newBuilder()
                    .setTemplate(template))
                .build()));
        when(apiId.isGroup()).thenReturn(true);
        when(uuidMapper.fromUuid(groupUuid)).thenReturn(apiId);
        groupsService.getSettingsByGroupUuid(groupUuid, false);
        verify(groupServiceSpyMole).getGroup(GroupID.newBuilder()
            .setId(Long.valueOf(groupUuid))
            .build());
        assertEquals(template, templateServiceSpy.getTemplates(any()).get(0).getTemplates(0).getTemplate());
    }

    /**
     * Tests that when passing a uuid that does not represent a group to getSettingsByGroupUuid(),
     * an IllegalArgumentException is being thrown.
     *
     * @throws Exception on error
     */
    @Test
    public void testGetSettingsByGroupUuidWithInvalidUuid() throws Exception {
        String groupUuid = "1234";
        ApiId apiId = mock(ApiId.class);
        when(apiId.isGroup()).thenReturn(false);
        when(uuidMapper.fromUuid(groupUuid)).thenReturn(apiId);
        expectedException.expect(IllegalArgumentException.class);
        groupsService.getSettingsByGroupUuid(groupUuid, false);
    }

    /**
     * Test get setting policies for a group with 2 members.
     * @throws Exception when getSettingPoliciesByGroupUuid() throws an exception
     */
    @Test
    public void testGetSettingPoliciesByGroupUuid() throws Exception {
        TopologyEntityDTO entity1 = TopologyEntityDTO.newBuilder().setOid(1111L).setEntityType(1).build();
        TopologyEntityDTO entity2 = TopologyEntityDTO.newBuilder().setOid(2222L).setEntityType(1).build();
        List<Long> members = Arrays.asList(entity1.getOid(), entity2.getOid());
        List<TopologyEntityDTO> entities = Arrays.asList(entity1, entity2);
        String groupUuid = "123456";
        MultiEntityRequest multiEntityRequest = mock(MultiEntityRequest.class);
        ImmutableGroupAndMembers groupAndMembers = ImmutableGroupAndMembers.builder()
                .group(Grouping.getDefaultInstance()).members(members).entities(members).build();
        when(groupExpanderMock.getGroupWithMembersAndEntities(groupUuid)).thenReturn(Optional.of(groupAndMembers));
        when(repositoryApiMock.entitiesRequest(Sets.newHashSet(groupAndMembers.members())))
              .thenReturn(multiEntityRequest);
        when(multiEntityRequest.getFullEntities()).thenReturn(entities.stream());
        groupsService.getSettingPoliciesByGroupUuid(groupUuid);
        verify(settingPolicyServiceSpy).getEntitySettingPolicies(GetEntitySettingPoliciesRequest
                .newBuilder().addAllEntityOidList(members).setIncludeInactive(true).build());
    }

    /**
     * Test get Resource groups when scope is group of accounts. Get groups owned by Account.
     *
     * @throws Exception if cannot convert the requested filter criteria.
     */
    @Test
    public void testGetGroupByTypeScopeIsGroupOfAccounts() throws Exception {
        final Long groupOfAccountsId = 111L;
        final Long rgId = 112L;
        final Long accountId1 = 1L;
        final Long accountId2 = 2L;
        final Grouping groupOfAccounts = Grouping.newBuilder()
                .setId(groupOfAccountsId)
                .addExpectedTypes(MemberType.newBuilder()
                        .setEntity(EntityType.BUSINESS_ACCOUNT_VALUE)
                        .build())
                .build();
        final GroupAndMembers groupAndMembers = ImmutableGroupAndMembers.builder()
                .group(groupOfAccounts)
                .members(Arrays.asList(accountId1, accountId2))
                .entities(Arrays.asList(accountId1, accountId2))
                .build();
        final ApiId entityApiId = mock(ApiId.class);
        when(entityApiId.isGroup()).thenReturn(true);
        when(uuidMapper.fromUuid(groupOfAccountsId.toString())).thenReturn(entityApiId);
        when(groupExpanderMock.getGroupWithMembersAndEntities(groupOfAccountsId.toString())).thenReturn(
                Optional.of(groupAndMembers));
        final PropertyFilter propertyFilter = PropertyFilter.newBuilder()
                .setPropertyName("accountID")
                .setStringFilter(StringFilter.newBuilder()
                        .addAllOptions(Arrays.asList(String.valueOf(accountId1),
                                String.valueOf(accountId2)))
                        .setPositiveMatch(true)
                        .setCaseSensitive(false)
                        .build())
                .build();
        final GroupDefinition groupDefinitionRg =
                GroupDefinition.newBuilder().setType(GroupType.RESOURCE).setIsHidden(false).build();
        final Grouping rg1 = Grouping.newBuilder()
                .setId(rgId)
                .addExpectedTypes(
                        MemberType.newBuilder().setEntity(EntityType.VIRTUAL_MACHINE_VALUE).build())
                .setDefinition(groupDefinitionRg)
                .build();
        final ResourceGroupApiDTO groupApiDTO = new ResourceGroupApiDTO();
        groupApiDTO.setUuid(String.valueOf(rgId));
        groupApiDTO.setClassName(StringConstants.RESOURCE_GROUP);
        groupApiDTO.setEnvironmentType(EnvironmentType.CLOUD);
        groupApiDTO.setParentUuid(String.valueOf(accountId1));
        mappedGroups.put(rgId, groupApiDTO);
        when(groupServiceSpyMole.getGroups(GetGroupsRequest.newBuilder()
                .setGroupFilter(GroupFilter.newBuilder().addPropertyFilters(propertyFilter).build())
                .build())).thenReturn(Collections.singletonList(rg1));
        when(groupFilterMapper.apiFilterToGroupFilter(any(), any(), any())).thenReturn(
                GroupFilter.newBuilder());
        final List<GroupApiDTO> groupsByType = groupsService.getGroupsByType(GroupType.RESOURCE,
                Collections.singletonList(groupOfAccountsId.toString()), Collections.emptyList(),
                EnvironmentType.CLOUD);
        GroupApiDTO groupApiDTO1 = groupsByType.iterator().next();
        assertEquals(String.valueOf(rgId), groupApiDTO1.getUuid());
        assertEquals(String.valueOf(accountId1),
                ((ResourceGroupApiDTO)groupApiDTO1).getParentUuid());
    }

    /**
     * Request using global temp group that contains regions should trigger the global optimization.
     *
     * @throws Exception should not be thrown.
     */
    @Test
    public void testGetActionCountStatsByUuidGlobalOptimization() throws Exception {
        // This is the request sent by the SAVINGS widget
        ActionApiInputDTO actionApiInputDTO = new ActionApiInputDTO();
        actionApiInputDTO.setGroupBy(Arrays.asList("actionTypes", "targetType", "risk"));
        actionApiInputDTO.setEnvironmentType(EnvironmentType.CLOUD);
        actionApiInputDTO.setCostType(ActionCostType.SAVING);

        CachedGroupInfo cachedGroupInfo = mock(CachedGroupInfo.class);
        when(cachedGroupInfo.getEntityTypes()).thenReturn(ImmutableSet.of(ApiEntityType.REGION));
        ApiId apiId = mock(ApiId.class);
        when(apiId.isGlobalTempGroup()).thenReturn(true);
        when(apiId.getCachedGroupInfo()).thenReturn(Optional.of(cachedGroupInfo));
        when(uuidMapper.fromUuid("0")).thenReturn(apiId);

        StatSnapshotApiDTO nonEmptyStatSnapshot = new StatSnapshotApiDTO();
        nonEmptyStatSnapshot.setStatistics(Arrays.asList(new StatApiDTO()));
        when(actionStatsQueryExecutor.retrieveActionStats(any())).thenReturn(ImmutableMap.of(
            // global temp group scope that was sent as input
            apiId, Arrays.asList(nonEmptyStatSnapshot),
            // related entity that's within the scope of the input group
            mock(ApiId.class), Arrays.asList(nonEmptyStatSnapshot, nonEmptyStatSnapshot)
        ));

        // let oid 0 be the oid of a global temp group
        List<StatSnapshotApiDTO> actual = groupsService.getActionCountStatsByUuid("0", actionApiInputDTO);
        // The size should be 3 because 1 comes from apiId, 2 more come from the entity within the scope
        // As a result, the the lists are combined into a list of size 3.
        Assert.assertEquals(3, actual.size());

        // environment type of input dto should not change
        Assert.assertEquals(EnvironmentType.CLOUD, actionApiInputDTO.getEnvironmentType());
        // related entity types should by updated to all entity types
        Assert.assertEquals(ApiEntityType.values().length, actionApiInputDTO.getRelatedEntityTypes().size());
    }

    /**
     * Test that if the scope is user groups, we should set a group filter with origin to be user.
     *
     * @throws Exception any error happens
     */
    @Test
    public void testGetPaginatedGroupApiDTOsWithScopes() throws Exception {
        when(groupFilterMapper.apiFilterToGroupFilter(eq(GroupType.REGULAR),
                eq(LogicalOperator.AND),
                eq(Collections.emptyList()))).thenReturn(GroupFilter.newBuilder()
                .setGroupType(GroupType.REGULAR));

        GetGroupsRequest expectedRequest = GetGroupsRequest.newBuilder()
                .setGroupFilter(GroupFilter.newBuilder()
                        .setGroupType(GroupType.REGULAR)
                        .setOriginFilter(OriginFilter.newBuilder()
                                .addOrigin(Type.USER)))
                .build();
        Mockito.when(groupServiceSpyMole.getGroups(expectedRequest)).thenReturn(Collections.emptyList());

        groupsService.getPaginatedGroupApiDTOs(Collections.emptyList(),
                // OrderBy set to COST, so that the old implementation is used.
                new SearchPaginationRequest(null, null, true, SearchOrderBy.COST.name()), null,
                null, null, null,
                Lists.newArrayList(USER_GROUPS), false, null);

        // verify that group filter is populated to origin USER
        verify(groupServiceSpyMole).getGroups(expectedRequest);
    }

    /**
     * Test that getPaginatedGroupApiDTOs works with multiple groupTypes.
     *
     * @throws Exception any error happens
     */
    @Test
    public void testGetPaginatedGroupApiDTOsUserGroupsOfEntitiesOrGroups() throws Exception {
        when(groupFilterMapper.apiFilterToGroupFilter(eq(GroupType.REGULAR),
                eq(LogicalOperator.AND),
                eq(Collections.emptyList()))).thenReturn(GroupFilter.newBuilder()
                .setGroupType(GroupType.REGULAR));
        GetGroupsRequest expectedRequest = GetGroupsRequest.newBuilder()
                .setGroupFilter(GroupFilter.newBuilder()
                        .setGroupType(GroupType.REGULAR)
                        .setOriginFilter(OriginFilter.newBuilder()
                                .addOrigin(Type.USER)))
                .build();
        // when getPaginatedGroupApiDTOs is called with includeAllGroupClasses=true, we will send
        // the default group filter
        final FilterApiDTO nameFilter = new FilterApiDTO();

        final GroupFilter.Builder groupFilterWithNameFilter = GroupFilter.newBuilder()
            .addPropertyFilters(SearchProtoUtil.nameFilterRegex(
                "foobar",
                EntityFilterMapper.isPositiveMatchingOperator(EntityFilterMapper.REGEX_MATCH),
                false));
        GetGroupsRequest allGroupsRequestWithNameFilter = GetGroupsRequest.newBuilder()
            .setGroupFilter(groupFilterWithNameFilter.build())
            .build();
        when(groupFilterMapper.apiFilterToGroupFilter(eq(GroupType.REGULAR),
            eq(LogicalOperator.AND),
            eq(Collections.singletonList(nameFilter)))).thenReturn(groupFilterWithNameFilter);

        final Grouping groupOfClusters = newGroup(11L, def ->
                def.setStaticGroupMembers(StaticMembers.newBuilder()
                    .addMembersByType(StaticMembersByType.newBuilder()
                        .setType(MemberType.newBuilder()
                            .setGroup(GroupType.COMPUTE_HOST_CLUSTER)))));
        final Grouping groupOfVMs = newGroup(12L, def ->
                def.setStaticGroupMembers(StaticMembers.newBuilder()
                    .addMembersByType(StaticMembersByType.newBuilder()
                        .setType(MemberType.newBuilder()
                            .setEntity(EntityType.VIRTUAL_MACHINE_VALUE)))));
        final Grouping clusterOfVMs = newGroup(13L, def ->
            def.setType(GroupType.COMPUTE_VIRTUAL_MACHINE_CLUSTER)
                .setStaticGroupMembers(StaticMembers.newBuilder()
                    .addMembersByType(StaticMembersByType.newBuilder()
                        .setType(MemberType.newBuilder()
                            .setEntity(EntityType.VIRTUAL_MACHINE_VALUE)))));
        when(groupServiceSpyMole.getGroups(expectedRequest)).thenReturn(
            Arrays.asList(groupOfClusters, groupOfVMs));
        when(groupServiceSpyMole.getGroups(allGroupsRequestWithNameFilter))
            .thenReturn(Arrays.asList(groupOfClusters, groupOfVMs, clusterOfVMs));
        // search for group of clusters
        SearchPaginationResponse response = groupsService.getPaginatedGroupApiDTOs(
                Collections.emptyList(),
                // OrderBy set to COST, so that the old implementation is used.
                new SearchPaginationRequest(null, null, true, SearchOrderBy.COST.name()), null,
                Collections.singleton("Cluster"), null, null, Lists.newArrayList(USER_GROUPS),
                false, null);

        assertThat(response.getRestResponse().getBody().stream()
                .map(BaseApiDTO::getUuid)
                .map(Long::valueOf)
                .collect(Collectors.toList()), containsInAnyOrder(groupOfClusters.getId()));

        // search for group of VMs
        response = groupsService.getPaginatedGroupApiDTOs(Collections.emptyList(),
                // OrderBy set to COST, so that the old implementation is used.
                new SearchPaginationRequest(null, null, true, SearchOrderBy.COST.name()), null,
                Collections.singleton("VirtualMachine"), null, null,
                Collections.singletonList(USER_GROUPS), false, null);
        assertThat(response.getRestResponse().getBody().stream()
                .map(BaseApiDTO::getUuid)
                .map(Long::valueOf)
                .collect(Collectors.toList()), containsInAnyOrder(groupOfVMs.getId()));

        // search for groups of all group types
        response = groupsService.getPaginatedGroupApiDTOs(Collections.singletonList(nameFilter),
                // OrderBy set to COST, so that the old implementation is used.
            new SearchPaginationRequest(null, null, true, SearchOrderBy.COST.name()), null,
            null, null, null, null, true, null);
        assertThat(response.getRestResponse().getBody().stream()
            .map(BaseApiDTO::getUuid)
            .map(Long::valueOf)
            .collect(Collectors.toList()), containsInAnyOrder(groupOfVMs.getId(),
                groupOfClusters.getId(), clusterOfVMs.getId()));
    }


    /**
     * Test that getPaginatedGroupApiDTOs works as expected for both groups of entities (group of
     * VMs) and groups of groups (group of clusters).
     *
     * @throws Exception any error happens
     */
    @Test
    public void testGetPaginatedGroupApiDTOsWithMultipleGroupTypes() throws Exception {
        //GIVEN
        final Grouping groupOfVMs = newGroup(11L,
                def -> def.setStaticGroupMembers(StaticMembers.newBuilder()
                    .addMembersByType(StaticMembersByType.newBuilder()
                        .setType(MemberType.newBuilder()
                            .setEntity(EntityType.VIRTUAL_MACHINE_VALUE)))));

        final Grouping groupOfPMs = newGroup(12L,
                def -> def.setStaticGroupMembers(StaticMembers.newBuilder()
                    .addMembersByType(StaticMembersByType.newBuilder()
                        .setType(MemberType.newBuilder()
                            .setEntity(EntityType.PHYSICAL_MACHINE_VALUE)))));

        when(groupFilterMapper.apiFilterToGroupFilter(eq(GroupType.REGULAR),
                eq(LogicalOperator.AND),
                eq(Collections.emptyList()))).thenReturn(GroupFilter.newBuilder()
                .setGroupType(GroupType.REGULAR));
        GetGroupsRequest expectedRequest = GetGroupsRequest.newBuilder()
                .setGroupFilter(GroupFilter.newBuilder()
                        .setGroupType(GroupType.REGULAR)
                        .setOriginFilter(OriginFilter.newBuilder()
                                .addOrigin(Type.USER)))
                .build();
        when(groupServiceSpyMole.getGroups(expectedRequest)).thenReturn(
                Arrays.asList(groupOfVMs, groupOfPMs));

        Set<String> groupEntityTypes = Sets.newHashSet("VirtualMachine", "PhysicalMachine");

        //WHEN
        SearchPaginationResponse response = groupsService.getPaginatedGroupApiDTOs(
                Collections.emptyList(),
                // OrderBy set to COST, so that the old implementation is used.
                new SearchPaginationRequest(null, null, true, SearchOrderBy.COST.name()), null,
                groupEntityTypes, null, null, Lists.newArrayList(USER_GROUPS), false, null);
        //THEN
        assertThat(response.getRestResponse().getBody().stream()
                .map(BaseApiDTO::getUuid)
                .map(Long::valueOf)
                .collect(Collectors.toList()), containsInAnyOrder(groupOfVMs.getId(), groupOfPMs.getId()));
    }


    /**
     * Test that getPaginatedGroupApiDTOs works as expected for dynamic groups of entities.
     *
     * @throws Exception any error happens
     */
    @Test
    public void testGetPaginatedGroupApiDTOsWithDynamicGroup() throws Exception {
        //GIVEN
        final Grouping groupOfVMs = newGroup(11L, def ->
                def.setEntityFilters(EntityFilters.newBuilder()
                    .addEntityFilter(EntityFilter.newBuilder()
                        .setEntityType(ApiEntityType.VIRTUAL_MACHINE.typeNumber()))));

        // Non-matching group.
        final Grouping groupOfPMs = newGroup(12L, def ->
                def.setStaticGroupMembers(StaticMembers.newBuilder()
                    .addMembersByType(StaticMembersByType.newBuilder()
                        .setType(MemberType.newBuilder()
                            .setEntity(EntityType.PHYSICAL_MACHINE_VALUE)))));

        when(groupFilterMapper.apiFilterToGroupFilter(eq(GroupType.REGULAR),
                eq(LogicalOperator.AND),
                eq(Collections.emptyList()))).thenReturn(GroupFilter.newBuilder()
                .setGroupType(GroupType.REGULAR));
        GetGroupsRequest expectedRequest = GetGroupsRequest.newBuilder()
                .setGroupFilter(GroupFilter.newBuilder()
                        .setGroupType(GroupType.REGULAR)
                        .setOriginFilter(OriginFilter.newBuilder()
                                .addOrigin(Type.USER)))
                .build();
        when(groupServiceSpyMole.getGroups(expectedRequest)).thenReturn(
                Arrays.asList(groupOfVMs, groupOfPMs));

        Set<String> groupEntityTypes = Collections.singleton(ApiEntityType.VIRTUAL_MACHINE.apiStr());

        //WHEN
        SearchPaginationResponse response = groupsService.getPaginatedGroupApiDTOs(
                Collections.emptyList(),
                // OrderBy set to COST, so that the old implementation is used.
                new SearchPaginationRequest(null, null, true, SearchOrderBy.COST.name()), null,
                groupEntityTypes, null, null, Lists.newArrayList(USER_GROUPS), false, null);
        //THEN
        assertThat(response.getRestResponse().getBody().stream()
                .map(BaseApiDTO::getUuid)
                .map(Long::valueOf)
                .collect(Collectors.toList()), containsInAnyOrder(groupOfVMs.getId()));
    }

    /**
     * Test that getPaginatedGroupApiDTOs works as expected for dynamic groups of groups.
     *
     * @throws Exception any error happens
     */
    @Test
    public void testGetPaginatedGroupApiDTOsWithDynamicNestedGroup() throws Exception {
        //GIVEN
        final Grouping groupOfClusters = newGroup(11L, def ->
                def.setGroupFilters(GroupFilters.newBuilder()
                        .addGroupFilter(GroupFilter.newBuilder()
                                .setGroupType(GroupType.COMPUTE_HOST_CLUSTER))));

        // Non-matching groups.
        final Grouping groupOfPms = newGroup(12L, def ->
                def.setStaticGroupMembers(StaticMembers.newBuilder()
                        .addMembersByType(StaticMembersByType.newBuilder()
                                .setType(MemberType.newBuilder()
                                        .setEntity(EntityType.PHYSICAL_MACHINE_VALUE)))));

        final Grouping groupOfRGs = newGroup(13L, def ->
                def.setGroupFilters(GroupFilters.newBuilder()
                        .addGroupFilter(GroupFilter.newBuilder()
                                .setGroupType(GroupType.RESOURCE))));

        when(groupFilterMapper.apiFilterToGroupFilter(eq(GroupType.REGULAR),
                eq(LogicalOperator.AND),
                eq(Collections.emptyList()))).thenReturn(GroupFilter.newBuilder()
                .setGroupType(GroupType.REGULAR));
        GetGroupsRequest expectedRequest = GetGroupsRequest.newBuilder()
                .setGroupFilter(GroupFilter.newBuilder()
                        .setGroupType(GroupType.REGULAR)
                        .setOriginFilter(OriginFilter.newBuilder()
                                .addOrigin(Type.USER)))
                .build();
        when(groupServiceSpyMole.getGroups(expectedRequest)).thenReturn(
                Arrays.asList(groupOfClusters, groupOfPms, groupOfRGs));

        Set<String> groupEntityTypes = Collections.singleton("Cluster");

        //WHEN
        SearchPaginationResponse response = groupsService.getPaginatedGroupApiDTOs(
                Collections.emptyList(),
                // OrderBy set to COST, so that the old implementation is used.
                new SearchPaginationRequest(null, null, true, SearchOrderBy.COST.name()), null,
                groupEntityTypes, null, null, Lists.newArrayList(USER_GROUPS), false, null);
        //THEN
        assertThat(response.getRestResponse().getBody().stream()
                .map(BaseApiDTO::getUuid)
                .map(Long::valueOf)
                .collect(Collectors.toList()), containsInAnyOrder(groupOfClusters.getId()));
    }

    /**
     * Tests that {@link GroupsService#getPaginatedGroupApiDTOs} forwards the filters and pagination
     * parameters to group component.
     *
     * @throws Exception to satisfy compiler
     */
    @Test
    public void testGetPaginatedGroupApiDTOsForwardingParameters()
            throws Exception {
        // GIVEN
        final FilterApiDTO filterApiDTO = new FilterApiDTO();
        filterApiDTO.setCaseSensitive(false);
        filterApiDTO.setExpType("RXNEQ");
        filterApiDTO.setExpVal("VM.*");
        filterApiDTO.setFilterType("groupsByName");
        final List<FilterApiDTO> filterList = Collections.singletonList(filterApiDTO);
        final String cursor = "1";
        final int limit = 1;
        final boolean ascending = false;
        final String orderBy = "SEVERITY";
        final SearchPaginationRequest paginationRequest =
                new SearchPaginationRequest(cursor, limit, ascending, orderBy);
        final GroupType groupType = GroupType.RESOURCE;
        final Set<String> groupEntityTypes = Collections.singleton("VirtualMachine");
        final EnvironmentType envType = EnvironmentType.HYBRID;
        final CloudType cloudType = CloudType.AWS;
        final com.vmturbo.api.enums.Origin origin = com.vmturbo.api.enums.Origin.USER;
        final long scope = 1234L;
        when(groupFilterMapper.apiFilterToGroupFilter(eq(groupType),
                eq(LogicalOperator.AND),
                eq(Collections.singletonList(filterApiDTO))))
                .thenReturn(GroupFilter.newBuilder()
                        .setGroupType(groupType)
                        .addPropertyFilters(
                                PropertyFilter.newBuilder()
                                        .setPropertyName(SearchableProperties.DISPLAY_NAME)
                                        .setStringFilter(
                                                StringFilter.newBuilder()
                                                        .setStringPropertyRegex("^VM.*$")
                                                        .setCaseSensitive(false)
                                                        .setPositiveMatch(false))));
        when(paginationMapperMock.toProtoParams(any())).thenReturn(
                PaginationParameters.newBuilder()
                        .setCursor(cursor)
                        .setLimit(limit)
                        .setAscending(ascending)
                        .setOrderBy(OrderBy.newBuilder()
                                .setGroupSearch(GroupOrderBy.GROUP_SEVERITY)
                                .build())
                        .build());
        ApiId apiId = mock(ApiId.class);
        when(apiId.oid()).thenReturn(scope);
        when(uuidMapper.fromUuid(Long.toString(scope))).thenReturn(apiId);
        GetPaginatedGroupsResponse groupResponse = GetPaginatedGroupsResponse.newBuilder().build();
        when(groupServiceSpyMole.getPaginatedGroups(any())).thenReturn(groupResponse);
        // WHEN
        SearchPaginationResponse response = groupsService.getPaginatedGroupApiDTOs(filterList,
                paginationRequest, groupType, groupEntityTypes, envType, cloudType,
                Collections.singletonList(Long.toString(scope)), false, origin);
        //THEN
        ArgumentCaptor<GetPaginatedGroupsRequest> captor =
                ArgumentCaptor.forClass(GetPaginatedGroupsRequest.class);
        verify(groupServiceSpyMole, times(1)).getPaginatedGroups(
                captor.capture());
        // verify input parameters"
        GetPaginatedGroupsRequest requestToTest = captor.getValue();
        // - pagination
        assertTrue(requestToTest.hasPaginationParameters());
        PaginationParameters capturedPaginationParams = requestToTest.getPaginationParameters();
        assertFalse(capturedPaginationParams.getAscending());
        assertEquals(cursor, capturedPaginationParams.getCursor());
        assertEquals(1, capturedPaginationParams.getLimit());
        assertTrue(capturedPaginationParams.getOrderBy().hasGroupSearch());
        assertEquals(GroupOrderBy.GROUP_SEVERITY,
                capturedPaginationParams.getOrderBy().getGroupSearch());
        // - scopes
        assertEquals(1, requestToTest.getScopesCount());
        assertEquals(scope, requestToTest.getScopes(0));
        // - filters
        assertTrue(requestToTest.hasGroupFilter());
        GroupFilter capturedGroupFilter = requestToTest.getGroupFilter();
        assertEquals(1, capturedGroupFilter.getPropertyFiltersCount());
        PropertyFilter propertyFilter = capturedGroupFilter.getPropertyFiltersList().get(0);
        assertEquals(SearchableProperties.DISPLAY_NAME, propertyFilter.getPropertyName());
        assertTrue(propertyFilter.hasStringFilter());
        assertFalse(propertyFilter.getStringFilter().getCaseSensitive());
        assertFalse(propertyFilter.getStringFilter().getPositiveMatch());
        assertTrue(propertyFilter.getStringFilter().hasStringPropertyRegex());
        assertEquals(groupType, capturedGroupFilter.getGroupType());
        assertEquals(1, capturedGroupFilter.getDirectMemberTypesCount());
        assertTrue(capturedGroupFilter.getDirectMemberTypesList().get(0).hasEntity());
        assertEquals(ApiEntityType.fromString(groupEntityTypes.iterator().next()).typeNumber(),
                capturedGroupFilter.getDirectMemberTypesList().get(0).getEntity());
        assertEquals(EnvironmentTypeMapper.fromApiToXL(envType),
                capturedGroupFilter.getEnvironmentType());
        assertEquals(CloudTypeMapper.fromApiToXlProtoEnum(cloudType),
                capturedGroupFilter.getCloudType());
        assertEquals(1, capturedGroupFilter.getOriginFilter().getOriginCount());
        assertEquals(GroupsService.API_ORIGIN_TO_GROUPDTO_ORIGIN.get(origin),
                capturedGroupFilter.getOriginFilter().getOriginList().get(0));
    }

    /**
     * Tests the cases for which {@link GroupsService#getPaginatedGroupApiDTOs} uses the old
     * implementation (retrieving all groups from group component and paginating inside the api
     * component instead of forwarding the paginated query to group component).
     * Currently there is 1 case:
     * - if orderBy is set to COST
     * As the relevant functionality is being implemented in group component (see OM-65839 for
     * orderBy COST), the cases tested here should be removed and eventually the entire test should
     * be removed, once everything is being forwarded to group component.
     */
    @Test
    public void testGetPaginatedGroupApiDTOsNotForwardingRequest()
            throws InvalidOperationException, OperationFailedException, ConversionException,
            InterruptedException {
        final String cursor = "1";
        final int limit = 1;
        final boolean ascending = false;
        when(groupFilterMapper.apiFilterToGroupFilter(any(), any(), any()))
                .thenReturn(GroupFilter.newBuilder());

        // orderBy COST
        when(userSessionContextMock.isUserScoped()).thenReturn(false);
        String orderBy = "COST";
        SearchPaginationRequest paginationRequest =
                new SearchPaginationRequest(cursor, limit, ascending, orderBy);
        groupsService.getPaginatedGroupApiDTOs(Collections.emptyList(), paginationRequest, null,
                null, null, null, null, false, null);
        verify(groupServiceSpyMole, times(0)).getPaginatedGroups(any());
    }

    /**
     * Tests that {@link GroupsService#getPaginatedGroups} forwards pagination parameters to group
     * component.
     *
     * @throws Exception to satisfy compiler
     */
    @Test
    public void testGetPaginatedGroupsForwardingParameters()
            throws Exception {
        // GIVEN
        final String cursor = "1";
        final int limit = 1;
        final boolean ascending = false;
        final String orderBy = "SEVERITY";
        final GroupPaginationRequest paginationRequest =
                new GroupPaginationRequest(cursor, limit, ascending, orderBy);
        when(paginationMapperMock.toProtoParams(any())).thenReturn(
                PaginationParameters.newBuilder()
                        .setCursor(cursor)
                        .setLimit(limit)
                        .setAscending(ascending)
                        .setOrderBy(OrderBy.newBuilder()
                                .setGroupSearch(GroupOrderBy.GROUP_SEVERITY)
                                .build())
                        .build());
        GetPaginatedGroupsResponse groupResponse = GetPaginatedGroupsResponse.newBuilder().build();
        when(groupServiceSpyMole.getPaginatedGroups(any())).thenReturn(groupResponse);
        // WHEN
        GroupPaginationResponse response = groupsService.getPaginatedGroups(paginationRequest, null);
        //THEN
        ArgumentCaptor<GetPaginatedGroupsRequest> captor =
                ArgumentCaptor.forClass(GetPaginatedGroupsRequest.class);
        verify(groupServiceSpyMole, times(1)).getPaginatedGroups(
                captor.capture());
        // verify input parameters
        assertTrue(captor.getValue().hasPaginationParameters());
        PaginationParameters capturedPaginationParams = captor.getValue().getPaginationParameters();
        assertFalse(capturedPaginationParams.getAscending());
        assertEquals(cursor, capturedPaginationParams.getCursor());
        assertEquals(limit, capturedPaginationParams.getLimit());
        assertEquals(GroupOrderBy.GROUP_SEVERITY,
                capturedPaginationParams.getOrderBy().getGroupSearch());
    }

    /**
     * Tests the cases for which {@link GroupsService#getPaginatedGroups} uses the old
     * implementation (retrieving all groups from group component and paginating inside the api
     * component instead of forwarding the paginated query to group component).
     * Currently there is 1 case:
     * - if orderBy is set to COST
     * As the relevant functionality is being implemented in group component (see OM-65839 for
     * orderBy COST), the cases tested here should be removed and eventually the entire test should
     * be removed, once everything is being forwarded to group component.
     */
    @Test
    public void testGetPaginatedGroupsNotForwardingRequest()
            throws InvalidOperationException, OperationFailedException, ConversionException,
            InterruptedException {
        final String cursor = "1";
        final int limit = 1;
        final boolean ascending = false;
        when(groupFilterMapper.apiFilterToGroupFilter(any(), any(), any()))
                .thenReturn(GroupFilter.newBuilder());

        // orderBy COST
        when(userSessionContextMock.isUserScoped()).thenReturn(false);
        String orderBy = "COST";
        GroupPaginationRequest paginationRequest =
                new GroupPaginationRequest(cursor, limit, ascending, orderBy);
        groupsService.getPaginatedGroups(paginationRequest, null);
        verify(groupServiceSpyMole, times(0)).getPaginatedGroups(any());
    }

    /**
     * Tests getting leaf entities for a group and members by groupId via the groupExpander.
     *
     * @throws Exception any error happens
     */
    @Test
    public void testGetLeafEntitiesByGroupUuid() throws Exception {
        // GIVEN
        final Long groupId = 111L;
        final Long hostEntityId1 = 1L;
        final Long hostEntityId2 = 2L;

        final Grouping hostsGrouping = Grouping.newBuilder()
                .setId(groupId)
                .addExpectedTypes(MemberType.newBuilder()
                        .setEntity(EntityType.PHYSICAL_MACHINE_VALUE)
                        .build())
                .build();

        final Optional<GroupAndMembers> groupAndMembers = Optional.of(ImmutableGroupAndMembers.builder()
                .group(hostsGrouping)
                .members(Arrays.asList(hostEntityId1, hostEntityId2))
                .entities(Arrays.asList(hostEntityId1, hostEntityId2))
                .build());

        final ApiId entityApiId = mock(ApiId.class);

        doReturn(groupAndMembers).when(groupExpanderMock).getGroupWithMembersAndEntities(groupId.toString());

        // WHEN
        final Set<Long> leafEntities = groupsService.getLeafEntitiesByGroupUuid(groupId.toString(), entityApiId);

        // THEN
        assertEquals(leafEntities.size(), 2);
        assertThat(leafEntities, containsInAnyOrder(hostEntityId1, hostEntityId2));
    }

    /**
     * Tests getting leaf entities when groupId is for a Target.
     *
     * @throws Exception any error happens
     */
    @Test
    public void testGetLeafEntitiesByGroupUuidForTarget() throws Exception {
        // GIVEN
        final Long targetId = 111L;
        final Long targetVM = 2L;
        final Long targetStorage = 3L;
        final Long targetHost = 4L;
        final ApiId entityApiId = mock(ApiId.class);

        when(entityApiId.isTarget()).thenReturn(true);
        when(groupExpanderMock.getGroupWithMembersAndEntities(targetId.toString())).thenReturn(Optional.empty());

        final GroupsService groupsServiceSpy = spy(groupsService);
        doReturn(Sets.newHashSet(targetVM, targetHost, targetStorage)).when(groupsServiceSpy)
                .expandUuids(Collections.singleton(targetId.toString()), Collections.emptyList(), null);

        // WHEN
        final Set<Long> leafEntitiesFromTarget = groupsServiceSpy.getLeafEntitiesByGroupUuid(targetId.toString(), entityApiId);

        // THEN
        assertThat(leafEntitiesFromTarget, containsInAnyOrder(targetHost, targetStorage, targetVM));
    }

    /**
     * Test getting leaf entities when the groupId references a supported grouping-entity type.
     *
     * @throws Exception any error happens
     */
    @Test
    public void testGetLeafEntitiesByGroupUuidForGroupingEntity() throws Exception {
        // GIVEN
        final Long datacenterId = 111L;
        final Long datacenterHost1 = 2L;
        final Long datacenterHost2 = 3L;

        final ApiId entityApiId = mock(ApiId.class);
        when(entityApiId.isTarget()).thenReturn(false);

        when(groupExpanderMock.getGroupWithMembersAndEntities(datacenterId.toString())).thenReturn(Optional.empty());

        final RepositoryApi.SingleEntityRequest singleEntityRequestMock = mock(RepositoryApi.SingleEntityRequest.class);
        doReturn(singleEntityRequestMock).when(repositoryApiMock).entityRequest(datacenterId);

        final MinimalEntity minimalEntityMock = MinimalEntity.newBuilder().setOid(5L).setEntityType(EntityType.DATACENTER_VALUE).build();
        doReturn(Optional.of(minimalEntityMock)).when(singleEntityRequestMock).getMinimalEntity();
        final GroupsService groupsServiceSpy = spy(groupsService);
        doReturn(Sets.newHashSet(datacenterHost1, datacenterHost2)).when(groupsServiceSpy)
                .expandUuids(Collections.singleton(datacenterId.toString()), ImmutableList.of(ApiEntityType.PHYSICAL_MACHINE.apiStr()), null);

        // WHEN
        final Set<Long> leafHostsFromDatacenter = groupsServiceSpy.getLeafEntitiesByGroupUuid(datacenterId.toString(), entityApiId);

        // THEN
        assertThat(leafHostsFromDatacenter, containsInAnyOrder(datacenterHost1, datacenterHost2));
    }

    /**
     * Test that the response result contains pagination-related headers when called with non-null value for
     * {@link SearchPaginationRequest} argument.
     *
     * @throws Exception if generic error
     */
    @Test
    public void testGetGroupEntitiesPaginated() throws Exception {
        // GIVEN
        final String groupUuid = UI_REAL_TIME_MARKET;
        final String cursor = "1";
        final String nextCursor = "11";
        final String previousCursor = "5";
        final Integer totalRecordCount = 100;

        final SearchPaginationRequest searchPaginationRequest =
                new SearchPaginationRequest(cursor, 10, true, SearchOrderBy.DEFAULT);

        final ApiId apiIdMock = mock(ApiId.class);
        doReturn(apiIdMock).when(uuidMapper).fromUuid(groupUuid);
        doReturn(true).when(apiIdMock).isRealtimeMarket();

        final SearchRequest searchRequestMock = mock(SearchRequest.class);
        doReturn(searchRequestMock).when(repositoryApiMock).newSearchRequest(Mockito.any(SearchParameters.class));

        final ResponseEntity<List<ServiceEntityApiDTO>> responseEntitiesMock =
                PaginationUtil.buildResponseEntity(Collections.emptyList(), previousCursor, nextCursor, totalRecordCount);

        doReturn(responseEntitiesMock).when(searchRequestMock).getPaginatedSEList(Mockito.any(Pagination.PaginationParameters.class));

        // WHEN
        final ResponseEntity<List<ServiceEntityApiDTO>> entitiesResponse =
                groupsService.getEntitiesByGroupUuid(groupUuid, searchPaginationRequest);

        // THEN
        assertEquals(entitiesResponse.getHeaders().get("X-Previous-Cursor").get(0), previousCursor);
        assertEquals(entitiesResponse.getHeaders().get("X-Next-Cursor").get(0), nextCursor);
        assertEquals(entitiesResponse.getHeaders().get("X-Total-Record-Count").get(0), totalRecordCount.toString());
    }

    /**
     * Test that the response result does not contain pagination-related headers when called with null value for
     * {@link SearchPaginationRequest} argument.
     *
     * @throws Exception if generic error
     */
    @Test
    public void testGetGroupEntitiesNotPaginated() throws Exception {
        // GIVEN
        final String groupUuid = UI_REAL_TIME_MARKET;

        final ApiId apiIdMock = mock(ApiId.class);
        doReturn(apiIdMock).when(uuidMapper).fromUuid(groupUuid);
        doReturn(true).when(apiIdMock).isRealtimeMarket();

        final SearchRequest searchRequestMock = mock(SearchRequest.class);
        doReturn(searchRequestMock).when(repositoryApiMock).newSearchRequest(Mockito.any(SearchParameters.class));

        doReturn(Collections.emptyList()).when(searchRequestMock).getSEList();

        // WHEN
        final ResponseEntity<List<ServiceEntityApiDTO>> entitiesResponse =
                groupsService.getEntitiesByGroupUuid(groupUuid, null);

        // THEN
        assertNull(entitiesResponse.getHeaders().get("X-Previous-Cursor"));
        assertNull(entitiesResponse.getHeaders().get("X-Next-Cursor").get(0));
        assertNull(entitiesResponse.getHeaders().get("X-Total-Record-Count"));
    }

    /**
     * Test getting entities by groupID where the group is the canonical global "Market" string.
     *
     * @throws Exception if generic error
     */
    @Test
    public void testGetGroupEntitiesForGlobalMarket() throws Exception {
        // GIVEN
        final ServiceEntityApiDTO vmEntity1 = new ServiceEntityApiDTO();
        vmEntity1.setUuid("1");
        final ServiceEntityApiDTO hostEntity = new ServiceEntityApiDTO();
        hostEntity.setUuid("3");

        final ApiId apiIdMock = mock(ApiId.class);
        doReturn(apiIdMock).when(uuidMapper).fromUuid(UI_REAL_TIME_MARKET);
        doReturn(true).when(apiIdMock).isRealtimeMarket();

        final SearchRequest searchRequestMock = mock(SearchRequest.class);
        final ArgumentCaptor<SearchParameters> searchRequestArgs = ArgumentCaptor.forClass(SearchParameters.class);
        doReturn(searchRequestMock).when(repositoryApiMock).newSearchRequest(searchRequestArgs.capture());

        doReturn(Lists.newArrayList(vmEntity1, hostEntity)).when(searchRequestMock).getSEList();

        // WHEN
        final ResponseEntity<List<ServiceEntityApiDTO>> nonPaginatedResponse =
                groupsService.getEntitiesByGroupUuid(UI_REAL_TIME_MARKET, null);

        // THEN
        final SearchParameters searchParameters = searchRequestArgs.getValue();
        Assert.assertEquals(searchParameters.getStartingFilter(), SearchProtoUtil.entityTypeFilter(SearchProtoUtil.SEARCH_ALL_TYPES));

        assertThat(nonPaginatedResponse.getBody(), containsInAnyOrder(vmEntity1, hostEntity));
    }

    /**
     * Test getting entities by groupID.
     *
     * @throws Exception if generic error
     */
    @Test
    public void testGetGroupEntitiesByUuid() throws Exception {
        // GIVEN
        final String groupUuid = "123456789";

        final ServiceEntityApiDTO vmEntity1 = new ServiceEntityApiDTO();
        vmEntity1.setUuid("1");
        final ServiceEntityApiDTO vmEntity2 = new ServiceEntityApiDTO();
        vmEntity2.setUuid("2");

        final ApiId apiIdMock = mock(ApiId.class);
        doReturn(apiIdMock).when(uuidMapper).fromUuid(groupUuid);
        doReturn(false).when(apiIdMock).isRealtimeMarket();

        final Set<Long> leafEntities = Collections.singleton(13L);
        final GroupsService groupServiceSpy = spy(groupsService);
        doReturn(leafEntities).when(groupServiceSpy).getLeafEntitiesByGroupUuid(groupUuid, apiIdMock);
        final SearchRequest searchRequestMock = mock(SearchRequest.class);
        final ArgumentCaptor<SearchParameters> searchRequestArgs = ArgumentCaptor.forClass(SearchParameters.class);
        doReturn(searchRequestMock).when(repositoryApiMock).newSearchRequest(searchRequestArgs.capture());
        doReturn(Lists.newArrayList(vmEntity1, vmEntity2)).when(searchRequestMock).getSEList();

        // WHEN
        final ResponseEntity<List<ServiceEntityApiDTO>> entitiesResponse =
                groupServiceSpy.getEntitiesByGroupUuid(groupUuid, null);

        // THEN
        final SearchParameters searchParameters = searchRequestArgs.getValue();
        Assert.assertEquals(searchParameters.getStartingFilter(), SearchProtoUtil.idFilter(leafEntities));
        assertThat(entitiesResponse.getBody(), containsInAnyOrder(vmEntity1, vmEntity2));
    }

    /**
     * Test getting paginated entities by groupID - unsupported SearchOrderBy should throw Exception.
     *
     * @throws Exception if generic error
     */
    @Test(expected = InvalidOperationException.class)
    public void testGetGroupEntitiesByUuidPaginatedUnsupportedSearchOrderBy() throws Exception {
        // GIVEN
        final String groupUuid = "Market";
        int limit = 10;
        final boolean ascending = true;
        final String searchOrderBy = "NONSENSE";

        final ApiId apiIdMock = mock(ApiId.class);
        doReturn(apiIdMock).when(uuidMapper).fromUuid(groupUuid);
        doReturn(true).when(apiIdMock).isRealtimeMarket();

        final SearchRequest searchRequestMock = mock(SearchRequest.class);
        doReturn(searchRequestMock).when(repositoryApiMock).newSearchRequest(Mockito.any(SearchParameters.class));

        doReturn(null).when(searchRequestMock).getPaginatedSEList(Mockito.any());

        final SearchPaginationRequest searchPaginationRequest =
                new SearchPaginationRequest(null, limit, ascending, searchOrderBy);

        // WHEN
        groupsService.getEntitiesByGroupUuid(groupUuid, searchPaginationRequest);
    }

    /**
     * Expect error thrown when not group is found.
     */
    @Test(expected = IllegalArgumentException.class)
    public void getGroupWithImmediateMembersOnlyThrowErrorOnEmptyResults() {
        //GIVEN
        String groupUuid = "123";
        doReturn(Optional.empty()).when(groupExpanderMock).getGroupWithImmediateMembersOnly(groupUuid);

        //WHEN
        groupsService.getGroupWithImmediateMembersOnly(groupUuid);
    }

    private Grouping newGroup(final long id,
                                      @Nullable Consumer<GroupDefinition.Builder> definitionCustomizer) {
        GroupApiDTO groupApiDTO1 = new GroupApiDTO();
        groupApiDTO1.setUuid(String.valueOf(id));
        mappedGroups.put(id, groupApiDTO1);
        final Grouping.Builder groupBldr = Grouping.newBuilder()
            .setId(id);
        if (definitionCustomizer != null) {
            definitionCustomizer.accept(groupBldr.getDefinitionBuilder());
        }
        return groupBldr.build();
    }

    /**
     * Test get tags for groups.
     *
     * @throws Exception should not happen.
     */
    @Test
    public void testGetGroupTags() throws Exception {
        final GetTagsResponse tagsForGroupsResponse = GetTagsResponse.newBuilder()
                .putTags(GROUP_ID, Tags.newBuilder()
                        .putTags(TAG_KEY,
                                TagValuesDTO.newBuilder().addAllValues(TAG_VALUES).build())
                        .build())
                .build();
        when(groupServiceSpyMole.getTags(
                GetTagsRequest.newBuilder().addGroupId(GROUP_ID).build())).thenReturn(
                tagsForGroupsResponse);

        ApiId apiId = mock(ApiId.class);
        when(apiId.oid()).thenReturn(GROUP_ID);
        when(apiId.isEntity()).thenReturn(false);
        when(apiId.isGroup()).thenReturn(true);
        when(uuidMapper.fromUuid(Long.toString(GROUP_ID))).thenReturn(apiId);

        final List<TagApiDTO> tags = groupsService.getTagsByGroupUuid(String.valueOf(GROUP_ID));
        Assert.assertEquals(1, tags.size());
        final TagApiDTO tagInfo = tags.iterator().next();
        Assert.assertEquals(TAG_KEY, tagInfo.getKey());
        Assert.assertEquals(TAG_VALUES, tagInfo.getValues());
    }

    /**
     * Get tags by entity id should work as expected, if tags don't exist.
     *
     * @throws Exception should not happen.
     */
    @Test
    public void testGetGroupEmptyTags() throws Exception {
        final GetTagsResponse tagsForGroupsResponse = GetTagsResponse.newBuilder()
                .putTags(GROUP_ID, Tags.newBuilder()
                        .build())
                .build();
        when(groupServiceSpyMole.getTags(
                GetTagsRequest.newBuilder().addGroupId(GROUP_ID).build())).thenReturn(
                tagsForGroupsResponse);

        ApiId apiId = mock(ApiId.class);
        when(apiId.oid()).thenReturn(GROUP_ID);
        when(apiId.isEntity()).thenReturn(false);
        when(apiId.isGroup()).thenReturn(true);
        when(uuidMapper.fromUuid(Long.toString(GROUP_ID))).thenReturn(apiId);

        final List<TagApiDTO> tags = groupsService.getTagsByGroupUuid(String.valueOf(GROUP_ID));

        // check tags
        Assert.assertEquals(0, tags.size());
    }

    /**
     * Tests the illegal group uuid exception, which occurs 
     * when the argument is a non-group uuid (i.e. an entity uuid).
     *
     * @throws Exception in case of illegal group uuid.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testGetTagsForGroup() throws Exception {
        ApiId apiId = mock(ApiId.class);
        when(apiId.oid()).thenReturn(VM_ID);
        when(apiId.isEntity()).thenReturn(true);
        when(apiId.isGroup()).thenReturn(false);
        when(uuidMapper.fromUuid(Long.toString(VM_ID))).thenReturn(apiId);

        final List<TagApiDTO> tags = groupsService.getTagsByGroupUuid(String.valueOf(VM_ID));
    }

    /**
     * Create tags using group id should work as expected.
     *
     * @throws OperationFailedException should not happen.
     */
    @Test
    public void testCreateTags() throws OperationFailedException {

        TagApiDTO tag = new TagApiDTO();
        tag.setKey(TAG_KEY);
        tag.setValues(TAG_VALUES);

        List<TagApiDTO> tags = new ArrayList<>();
        tags.add(tag);

        final Tags tagsDTO = Tags.newBuilder().putTags(TAG_KEY, TagValuesDTO.newBuilder()
                .addAllValues(TAG_VALUES).build()).build();

        final CreateTagsRequest request = CreateTagsRequest.newBuilder()
                .setGroupId(VM_ID)
                .setTags(tagsDTO).build();

        final CreateTagsResponse response = CreateTagsResponse.newBuilder().build();

        when(groupServiceSpyMole.createTags(request)).thenReturn(response);

        ApiId apiId = mock(ApiId.class);
        when(apiId.oid()).thenReturn(VM_ID);
        when(apiId.isGroup()).thenReturn(true);
        when(uuidMapper.fromUuid(Long.toString(VM_ID))).thenReturn(apiId);

        // call service
        final List<TagApiDTO> result = groupsService.createTagsByGroupUuid(Long.toString(VM_ID), tags);

        // check tags
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(TAG_KEY, result.get(0).getKey());
        Assert.assertArrayEquals(TAG_VALUES.toArray(), result.get(0).getValues().toArray());
    }

    /**
     * Create empty tags using group id should work as expected.
     *
     * @throws OperationFailedException should not happen.
     */
    @Test
    public void testCreateEmptyTags() throws OperationFailedException {

        List<TagApiDTO> tags = new ArrayList<>();
        final Tags tagsDTO = Tags.newBuilder().build();

        final CreateTagsRequest request = CreateTagsRequest.newBuilder()
                .setGroupId(VM_ID)
                .setTags(tagsDTO).build();

        final CreateTagsResponse response = CreateTagsResponse.newBuilder().build();

        when(groupServiceSpyMole.createTags(request)).thenReturn(response);

        ApiId apiId = mock(ApiId.class);
        when(apiId.oid()).thenReturn(VM_ID);
        when(apiId.isGroup()).thenReturn(true);
        when(uuidMapper.fromUuid(Long.toString(VM_ID))).thenReturn(apiId);

        // call service
        final List<TagApiDTO> result = groupsService.createTagsByGroupUuid(Long.toString(VM_ID), tags);

        // check tags
        Assert.assertEquals(0, result.size());
    }

    /**
     * Create tags using invalid group id should fail with exception.
     *
     * @throws IllegalArgumentException in case of illegal group uuid.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testCreateTagsInvalidId() throws IllegalArgumentException, OperationFailedException {

        TagApiDTO tag = new TagApiDTO();

        List<TagApiDTO> tags = new ArrayList<>();
        tags.add(tag);

        final Tags tagsDTO = Tags.newBuilder().build();

        final CreateTagsRequest request = CreateTagsRequest.newBuilder()
                .setGroupId(VM_ID)
                .setTags(tagsDTO).build();

        final CreateTagsResponse response = CreateTagsResponse.newBuilder().build();

        when(groupServiceSpyMole.createTags(request)).thenReturn(response);

        ApiId apiId = mock(ApiId.class);
        when(apiId.oid()).thenReturn(VM_ID);
        when(apiId.isGroup()).thenReturn(false);
        when(uuidMapper.fromUuid(Long.toString(VM_ID))).thenReturn(apiId);

        // call service
        groupsService.createTagsByGroupUuid(Long.toString(VM_ID), tags);
    }

    /**
     * Create tags with a tag key of empty string should fail with exception.
     *
     * @throws OperationFailedException
     */
    @Test(expected = OperationFailedException.class)
    public void testCreateTagsEmptyKey() throws OperationFailedException {

        TagApiDTO tag = new TagApiDTO();
        tag.setKey("");
        tag.setValues(TAG_VALUES);

        List<TagApiDTO> tags = new ArrayList<>();
        tags.add(tag);

        final Tags tagsDTO = Tags.newBuilder().build();

        final CreateTagsRequest request = CreateTagsRequest.newBuilder()
                .setGroupId(VM_ID)
                .setTags(tagsDTO).build();

        final CreateTagsResponse response = CreateTagsResponse.newBuilder().build();

        when(groupServiceSpyMole.createTags(request)).thenReturn(response);

        ApiId apiId = mock(ApiId.class);
        when(apiId.oid()).thenReturn(VM_ID);
        when(apiId.isGroup()).thenReturn(true);
        when(uuidMapper.fromUuid(Long.toString(VM_ID))).thenReturn(apiId);

        // call service
        groupsService.createTagsByGroupUuid(Long.toString(VM_ID), tags);
    }

    /**
     * Create tags with a tag with empty list of values should fail with exception.
     *
     * @throws OperationFailedException
     */
    @Test(expected = OperationFailedException.class)
    public void testCreateTagsEmptyValues() throws OperationFailedException {

        TagApiDTO tag = new TagApiDTO();
        tag.setKey(TAG_KEY);
        tag.setValues(new ArrayList<>());

        List<TagApiDTO> tags = new ArrayList<>();
        tags.add(tag);

        final Tags tagsDTO = Tags.newBuilder().build();

        final CreateTagsRequest request = CreateTagsRequest.newBuilder()
                .setGroupId(VM_ID)
                .setTags(tagsDTO).build();

        final CreateTagsResponse response = CreateTagsResponse.newBuilder().build();

        when(groupServiceSpyMole.createTags(request)).thenReturn(response);

        ApiId apiId = mock(ApiId.class);
        when(apiId.oid()).thenReturn(VM_ID);
        when(apiId.isGroup()).thenReturn(true);
        when(uuidMapper.fromUuid(Long.toString(VM_ID))).thenReturn(apiId);

        // call service
        groupsService.createTagsByGroupUuid(Long.toString(VM_ID), tags);
    }

    /**
     * Delete tags using group oid.
     *
     * @throws Exception should not happen.
     */
    @Test
    public void testDeleteTagsGroupOid() throws Exception {
        final DeleteTagsRequest request = DeleteTagsRequest.newBuilder()
                .setGroupOid(VM_ID)
                .build();

        final DeleteTagsResponse response = DeleteTagsResponse.newBuilder().build();

        when(groupServiceSpyMole.deleteTags(request)).thenReturn(response);

        ApiId apiId = mock(ApiId.class);
        when(apiId.isGroup()).thenReturn(true);
        when(uuidMapper.fromUuid(Long.toString(VM_ID))).thenReturn(apiId);
        groupsService.deleteTagsByGroupUuid(Long.toString(VM_ID));
    }

    /**
     * Delete tags with empty key.
     *
     * @throws IllegalArgumentException should be thrown.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testDeleteTagsEmptyKey() throws Exception {
        groupsService.deleteTagsByGroupUuid("");
    }

    /**
     * Test GetGroupCloudCostStats when we have an empty group with no members.
     *
     * @throws Exception
     */
    @Test
    public void testGetEmptyGroupCloudCostStats() throws Exception {
        final Grouping emptyBillingFamily = Grouping.newBuilder().setId(10L).build();
        GroupAndMembers groupAndMembers = ImmutableGroupAndMembers.builder().group(emptyBillingFamily).members(Collections.emptyList()).entities(Collections.emptyList()).build();
        Mockito.when(groupExpanderMock.getGroup("10")).thenReturn(Optional.of(emptyBillingFamily));
        Mockito.when(groupExpanderMock.getMembersForGroup(emptyBillingFamily)).thenReturn(groupAndMembers);
        List<StatSnapshotApiDTO> groupCloudCostStats = groupsService.getGroupCloudCostStats("10", null);
        assertTrue(groupCloudCostStats.isEmpty());
    }
}
