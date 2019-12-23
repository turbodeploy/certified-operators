package com.vmturbo.api.component.external.api.service;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
import org.mockito.MockitoAnnotations;

import com.vmturbo.api.component.ApiTestUtils;
import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.MultiEntityRequest;
import com.vmturbo.api.component.communication.RepositoryApi.SearchRequest;
import com.vmturbo.api.component.external.api.mapper.ActionSpecMapper;
import com.vmturbo.api.component.external.api.mapper.EntityEnvironment;
import com.vmturbo.api.component.external.api.mapper.GroupFilterMapper;
import com.vmturbo.api.component.external.api.mapper.GroupMapper;
import com.vmturbo.api.component.external.api.mapper.PaginationMapper;
import com.vmturbo.api.component.external.api.mapper.PriceIndexPopulator;
import com.vmturbo.api.component.external.api.mapper.SettingsManagerMappingLoader.SettingsManagerMapping;
import com.vmturbo.api.component.external.api.mapper.SettingsMapper;
import com.vmturbo.api.component.external.api.mapper.SeverityPopulator;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.CachedGroupInfo;
import com.vmturbo.api.component.external.api.mapper.aspect.EntityAspectMapper;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.component.external.api.util.GroupExpander.GroupAndMembers;
import com.vmturbo.api.component.external.api.util.ImmutableGroupAndMembers;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory.SupplyChainNodeFetcherBuilder;
import com.vmturbo.api.component.external.api.util.action.ActionSearchUtil;
import com.vmturbo.api.component.external.api.util.action.ActionStatsQueryExecutor;
import com.vmturbo.api.component.external.api.util.setting.EntitySettingQueryExecutor;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.action.ActionApiInputDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.group.FilterApiDTO;
import com.vmturbo.api.dto.group.GroupApiDTO;
import com.vmturbo.api.dto.group.ResourceGroupApiDTO;
import com.vmturbo.api.dto.setting.SettingApiDTO;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.enums.ActionCostType;
import com.vmturbo.api.enums.CloudType;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.pagination.GroupMembersPaginationRequest;
import com.vmturbo.api.pagination.GroupMembersPaginationRequest.GroupMemberOrderBy;
import com.vmturbo.api.pagination.GroupMembersPaginationRequest.GroupMembersPaginationResponse;
import com.vmturbo.api.pagination.SearchPaginationRequest;
import com.vmturbo.api.pagination.SearchPaginationRequest.SearchPaginationResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.common.protobuf.action.ActionDTOMoles.ActionsServiceMole;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.CreateGroupRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.CreateGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupForEntityRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupForEntityResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse.Members;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
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
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.StringFilter;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.utils.StringConstants;
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

    @Mock
    private ActionSpecMapper actionSpecMapper;

    @Mock
    private GroupMapper groupMapper;


    @Mock
    private GroupFilterMapper groupFilterMapper;

    @Mock
    private PaginationMapper paginationMapper;

    @Mock
    private EntityAspectMapper entityAspectMapper;

    @Mock
    private RepositoryApi repositoryApi;

    @Mock
    private UuidMapper uuidMapper;

    @Mock
    private GroupExpander groupExpander;

    @Mock
    private SeverityPopulator severityPopulator;

    @Mock
    private PriceIndexPopulator priceIndexPopulator;

    @Mock
    private ActionStatsQueryExecutor actionStatsQueryExecutor;

    @Mock
    private ThinTargetCache targetCache;

    @Mock
    private EntitySettingQueryExecutor entitySettingQueryExecutor;

    @Mock
    private SupplyChainFetcherFactory supplyChainFetcherFactory;

    @Captor
    private ArgumentCaptor<GetGroupsRequest> getGroupsRequestCaptor;

    private GroupServiceMole groupServiceSpy = spy(new GroupServiceMole());

    private TemplateServiceMole templateServiceSpy = spy(new TemplateServiceMole());

    private ActionsServiceMole actionServiceSpy = spy(new ActionsServiceMole());

    private FilterApiDTO groupFilterApiDTO = new FilterApiDTO();
    private FilterApiDTO clusterFilterApiDTO = new FilterApiDTO();

    @Rule
    public GrpcTestServer grpcServer =
        GrpcTestServer.newServer(groupServiceSpy, templateServiceSpy, actionServiceSpy);

    private static final long CONTEXT_ID = 7777777;

    @Before
    public void init() throws Exception {
        MockitoAnnotations.initMocks(this);

        // create inputs for the service
        final ActionsServiceBlockingStub actionOrchestratorRpcService =
                ActionsServiceGrpc.newBlockingStub(grpcServer.getChannel());
        final SettingPolicyServiceBlockingStub settingPolicyServiceBlockingStub =
                SettingPolicyServiceGrpc.newBlockingStub(grpcServer.getChannel());
        final ActionSearchUtil actionSearchUtil =
                new ActionSearchUtil(
                        actionOrchestratorRpcService, actionSpecMapper,
                        paginationMapper, supplyChainFetcherFactory, groupExpander,
                        CONTEXT_ID);
        final SettingsMapper settingsMapper = mock(SettingsMapper.class);

        groupsService =
            new GroupsService(
                actionOrchestratorRpcService,
                GroupServiceGrpc.newBlockingStub(grpcServer.getChannel()),
                groupMapper,
                groupExpander,
                uuidMapper,
                repositoryApi,
                CONTEXT_ID,
                mock(SettingsManagerMapping.class),
                TemplateServiceGrpc.newBlockingStub(grpcServer.getChannel()),
                entityAspectMapper,
                actionStatsQueryExecutor,
                severityPopulator,
                priceIndexPopulator,
                supplyChainFetcherFactory,
                actionSearchUtil,
                settingPolicyServiceBlockingStub,
                settingsMapper,
                targetCache, entitySettingQueryExecutor,
                groupFilterMapper) {
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
    }

    /**
     * Test generating group request with name pattern match.
     */
    @Test
    public void testGetGroupsWithFilterEq() throws Exception {
        // Arrange
        groupFilterApiDTO.setExpType(EQ_MATCH_TYPE);
        when(groupFilterMapper.apiFilterToGroupFilter(eq(GroupType.REGULAR),
                        eq(Collections.singletonList(groupFilterApiDTO))))
                .thenReturn(
                        GroupFilter.newBuilder().addPropertyFilters(
                                PropertyFilter.newBuilder()
                                .setStringFilter(
                                        StringFilter.newBuilder()
                                                .setStringPropertyRegex(
                                                        StringConstants.DISPLAY_NAME_ATTR))

                                )
                                .build()
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
                                .build()
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

    @Test
    public void getGroupByUuid() throws Exception {
        final GroupApiDTO apiGroup = new GroupApiDTO();
        apiGroup.setDisplayName("minion");
        final Grouping xlGroup = Grouping.newBuilder().setId(1).build();
        final GroupAndMembers groupAndMembers = ImmutableGroupAndMembers.builder()
            .group(xlGroup)
            .members(Collections.singleton(1L))
            .entities(Collections.singleton(1L))
            .build();
        when(groupMapper.getEnvironmentAndCloudTypeForGroup(groupAndMembers)).thenReturn(new EntityEnvironment(EnvironmentType.ONPREM, CloudType.UNKNOWN));
        when(groupMapper.toGroupApiDto(groupAndMembers, EnvironmentType.ONPREM, CloudType.UNKNOWN, true)).thenReturn(apiGroup);

        when(groupExpander.getGroupWithMembers("1"))
            .thenReturn(Optional.of(groupAndMembers));

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
        GroupAndMembers clusterAndMembers = groupAndMembers(1L, GroupType.COMPUTE_HOST_CLUSTER, Collections.singleton(7L));
        when(groupExpander.getGroupsWithMembers(any()))
            .thenReturn(Stream.of(clusterAndMembers));
        when(groupMapper.getEnvironmentAndCloudTypeForGroup(clusterAndMembers)).thenReturn(new EntityEnvironment(EnvironmentType.ONPREM, CloudType.UNKNOWN));
        when(groupMapper.toGroupApiDto(clusterAndMembers, EnvironmentType.ONPREM, CloudType.UNKNOWN, true)).thenReturn(clusterApiDto);
        when(severityPopulator.calculateSeverity(CONTEXT_ID, Collections.singleton(7L)))
            .thenReturn(Optional.of(Severity.NORMAL));
        when(groupFilterMapper.apiFilterToGroupFilter(eq(GroupType.COMPUTE_HOST_CLUSTER),
            eq(Collections.emptyList()))).thenReturn(GroupFilter.newBuilder()
                .setGroupType(GroupType.COMPUTE_HOST_CLUSTER)
                .build());

        final String clusterHeadroomGroupUuid = "GROUP-PhysicalMachineByCluster";
        GroupMembersPaginationRequest memberRequest = new GroupMembersPaginationRequest(null,
            DEFAULT_N_ENTITIES,
            false, GroupMemberOrderBy.DEFAULT);
        List<BaseApiDTO> clustersList =
            groupsService.getMembersByGroupUuid(clusterHeadroomGroupUuid,
                memberRequest).getRestResponse().getBody();


        verify(groupExpander).getGroupsWithMembers(getGroupsRequestCaptor.capture());

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
        GroupAndMembers clusterAndMembers = groupAndMembers(1L, GroupType.STORAGE_CLUSTER, Collections.singleton(7L));
        when(groupExpander.getGroupsWithMembers(any()))
            .thenReturn(Stream.of(clusterAndMembers));
        when(groupMapper.getEnvironmentAndCloudTypeForGroup(clusterAndMembers)).thenReturn(new EntityEnvironment(EnvironmentType.ONPREM, CloudType.UNKNOWN));
        when(groupMapper.toGroupApiDto(clusterAndMembers, EnvironmentType.ONPREM, CloudType.UNKNOWN, true)).thenReturn(clusterApiDto);
        when(severityPopulator.calculateSeverity(CONTEXT_ID, Collections.singleton(7L)))
            .thenReturn(Optional.of(Severity.NORMAL));
        when(groupFilterMapper.apiFilterToGroupFilter(eq(GroupType.STORAGE_CLUSTER),
            eq(Collections.emptyList()))).thenReturn(GroupFilter.newBuilder()
            .setGroupType(GroupType.STORAGE_CLUSTER)
            .build());

        final String clusterHeadroomGroupUuid = "GROUP-StorageByStorageCluster";
        GroupMembersPaginationRequest memberRequest = new GroupMembersPaginationRequest("0",
            DEFAULT_N_ENTITIES,
            false, GroupMemberOrderBy.DEFAULT);
        List<BaseApiDTO> clustersList =
            groupsService.getMembersByGroupUuid(clusterHeadroomGroupUuid, memberRequest).getRestResponse().getBody();

        verify(groupExpander).getGroupsWithMembers(getGroupsRequestCaptor.capture());
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
        when(groupExpander.getGroupWithMembers("1")).thenReturn(Optional.empty());
        groupsService.getGroupByUuid("1", false);
    }

    @Test
    public void testGetGroupMembersByUuid() throws Exception {
        // Arrange
        final long member1Id = 7;
        final long member2Id = 8;

        when(groupServiceSpy.getMembers(GetMembersRequest.newBuilder()
            .setId(1L)
            .build()))
            .thenReturn(GetMembersResponse.newBuilder()
                .setMembers(Members.newBuilder().addAllIds(Arrays.asList(member1Id, member2Id)))
                .build());

        final ServiceEntityApiDTO member1Dto = new ServiceEntityApiDTO();
        member1Dto.setUuid("7");

        final ServiceEntityApiDTO member2Dto = new ServiceEntityApiDTO();
        member2Dto.setUuid("8");

        MultiEntityRequest req = ApiTestUtils.mockMultiSEReq(Lists.newArrayList(
            member2Dto));
        when(repositoryApi.entitiesRequest(Collections.singleton(member2Id)))
            .thenReturn(req);

        final GroupAndMembers groupAndMembers =
            groupAndMembers(1L, GroupType.REGULAR, new HashSet<>(Arrays.asList(7L, 8L)));
        when(groupExpander.getGroupWithMembers("1")).thenReturn(Optional.of(groupAndMembers));
        GroupMembersPaginationRequest request = new GroupMembersPaginationRequest("1",
            DEFAULT_N_ENTITIES,
            false, GroupMemberOrderBy.DEFAULT);
        // Act
        final GroupMembersPaginationResponse response = groupsService.getMembersByGroupUuid("1",
            request);

        // Assert
        assertThat(response.getRestResponse().getBody().size(), is(1));
        // Checks for pagination as well
        assertThat(response.getRestResponse().getBody().get(0), is(member2Dto));
    }

    private GroupAndMembers groupAndMembers(final long groupId,
                                            GroupType groupType,
                                            Set<Long> members) {
        return ImmutableGroupAndMembers.builder()
            .group(Grouping.newBuilder()
                .setId(groupId)
                .setDefinition(GroupDefinition.newBuilder()
                                .setType(groupType).build())
                .build())
            .members(members)
            .entities(members)
            .build();
    }

    @Test
    public void testGetGroupMembersByUuidNoMembers() throws Exception {
        // Arrange
        when(groupServiceSpy.getMembers(GetMembersRequest.newBuilder()
                .setId(1L)
                .build()))
            // No members in group.
            .thenReturn(GetMembersResponse.newBuilder()
                .build());

        when(groupExpander.getGroupWithMembers("1"))
            .thenReturn(Optional.of(groupAndMembers(1L, GroupType.REGULAR, Collections.emptySet())));

        // Act
        GroupMembersPaginationRequest request = new GroupMembersPaginationRequest(null,
            DEFAULT_N_ENTITIES,
            false, GroupMemberOrderBy.DEFAULT);
        GroupMembersPaginationResponse response = groupsService.getMembersByGroupUuid("1", request);

        // Assert
        assertTrue(response.getRestResponse().getBody().isEmpty());
        // Should be no call to repository to get entity information.
        verifyZeroInteractions(repositoryApi);
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
        when(groupServiceSpy.createGroup(request))
            .thenReturn(CreateGroupResponse.newBuilder()
                    .setGroup(group)
                    .build());
        when(groupMapper.toGroupApiDto(group, true)).thenReturn(apiDTO);
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
        when(groupServiceSpy.createGroup(CreateGroupRequest.newBuilder()
                .setGroupDefinition(GroupDefinition.getDefaultInstance())
                .setOrigin(Origin.newBuilder().setUser(Origin.User.newBuilder()
                                .setUsername("testUser")))
                .build()))
            .thenReturn(CreateGroupResponse.newBuilder()
                .setGroup(group)
                .build());
        when(groupMapper.toGroupApiDto(group, true)).thenReturn(apiDTO);
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
        when(groupServiceSpy.createGroup(CreateGroupRequest.newBuilder()
                .setOrigin(Origin.newBuilder().setUser(Origin.User.newBuilder()
                                .setUsername("testUser")))
                .setGroupDefinition(GroupDefinition.getDefaultInstance())
                .build()))
                .thenReturn(CreateGroupResponse.newBuilder()
                        .setGroup(group)
                        .build());
        when(groupMapper.toGroupApiDto(group, true)).thenReturn(apiDTO);
        assertThat(groupsService.createGroup(apiDTO), is(apiDTO));
    }

    /**
     * Test deleting a group.
     *
     * @throws UnknownObjectException in case the group does not exist
     * @throws OperationFailedException in case an error occurred while editing the group
     */
    @Test
    public void testEditGroup() throws UnknownObjectException, OperationFailedException {
        final GroupApiDTO groupApiDTO = new GroupApiDTO();
        final GroupDefinition groupDefinition = GroupDefinition.getDefaultInstance();
        final Grouping group = Grouping.newBuilder().setId(1).build();

        when(groupMapper.toGroupDefinition(eq(groupApiDTO))).thenReturn(groupDefinition);

        when(groupServiceSpy.getGroup(eq(GroupID.newBuilder().setId(1).build()))).thenReturn(
                GetGroupResponse.newBuilder().setGroup(group).build());
        UpdateGroupResponse updateGroupResponse =
                UpdateGroupResponse.newBuilder().setUpdatedGroup(group).build();

        when(groupServiceSpy.updateGroup(eq(UpdateGroupRequest.newBuilder()
                .setId(1)
                .setNewDefinition(groupDefinition)
                .build()))).thenReturn(updateGroupResponse);

        when(groupMapper.toGroupApiDto(eq(updateGroupResponse.getUpdatedGroup()), eq(true)))
                .thenReturn(groupApiDTO);

        assertThat(groupsService.editGroup("1", groupApiDTO), is(groupApiDTO));
    }

    /**
     * Test editing a group with invalid UUID.
     *
     * @throws UnknownObjectException in case the group does not exist
     * @throws OperationFailedException in case an error occurred while editing the group
     */
    @Test
    public void testEditGroupInvalidUuid() throws UnknownObjectException, OperationFailedException {
        final GroupApiDTO groupApiDTO = new GroupApiDTO();
        when(groupMapper.toGroupDefinition(eq(groupApiDTO))).thenReturn(GroupDefinition.getDefaultInstance());

        when(groupServiceSpy.getGroup(eq(GroupID.newBuilder().setId(1).build()))).thenReturn(
                GetGroupResponse.newBuilder().clearGroup().build());

        expectedException.expect(UnknownObjectException.class);
        expectedException.expectMessage("Group with UUID 1 does not exist");
        groupsService.editGroup("1", groupApiDTO);
    }

    @Test
    public void testGetComputerCluster() throws UnknownObjectException, OperationFailedException {
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

        final GroupAndMembers clusterAndMembers = ImmutableGroupAndMembers.builder()
            .group(cluster)
            .members(Collections.emptyList())
            .entities(Collections.emptyList())
            .build();

        when(groupMapper.getEnvironmentAndCloudTypeForGroup(clusterAndMembers)).thenReturn(new EntityEnvironment(EnvironmentType.ONPREM, CloudType.UNKNOWN));
        when(groupMapper.toGroupApiDto(clusterAndMembers, EnvironmentType.ONPREM, CloudType.UNKNOWN, true)).thenReturn(groupApiDtoMock);
        when(groupFilterMapper.apiFilterToGroupFilter(eq(GroupType.COMPUTE_HOST_CLUSTER),
                eq(Collections.emptyList()))).thenReturn(GroupFilter.newBuilder()
                .setGroupType(GroupType.COMPUTE_HOST_CLUSTER).build());

        final GetGroupsRequest groupsRequest = GetGroupsRequest.newBuilder()
                .setGroupFilter(GroupFilter.newBuilder()
                        .setGroupType(GroupType.COMPUTE_HOST_CLUSTER))
                .addScopes(1L)
                .build();
        when(groupExpander.getGroupsWithMembers(eq(groupsRequest))).thenReturn(
                Stream.of(clusterAndMembers));

        final ApiId entityApiId = mock(ApiId.class);
        when(entityApiId.isGroup()).thenReturn(false);

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

        when(groupExpander.getGroupWithMembers("1"))
            .thenReturn(Optional.of(groupAndMembers));


        final GroupDefinition groupDefinitionVm = GroupDefinition
            .newBuilder()
            .setType(GroupType.REGULAR)
            .setStaticGroupMembers(GroupDTO.StaticMembers.newBuilder()
                .addMembersByType(GroupDTO.StaticMembers.StaticMembersByType.newBuilder()
                    .setType(GroupDTO.MemberType.newBuilder()
                        .setEntity(UIEntityType.VIRTUAL_MACHINE.typeNumber()).build())
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

        when(groupExpander.getGroupWithMembers("10"))
            .thenReturn(Optional.of(vmGroupAndMembers));

        final GroupFilter groupFilter = GroupFilter.newBuilder()
            .setGroupType(GroupType.RESOURCE)
            .addAllId(Arrays.asList(2L))
            .build();

        final GetGroupsRequest groupsRequest = GetGroupsRequest.newBuilder()
            .setGroupFilter(groupFilter)
            .build();

        when(groupFilterMapper.apiFilterToGroupFilter(eq(GroupType.RESOURCE),
            eq(Collections.emptyList()))).thenReturn(GroupFilter.newBuilder()
                .setGroupType(GroupType.RESOURCE).build());

        final Grouping childRG = Grouping.newBuilder()
            .setId(2L)
            .build();

        final GroupAndMembers childRgGroupAndMembers = ImmutableGroupAndMembers.builder()
            .group(childRG)
            .members(Collections.emptyList())
            .entities(Collections.emptyList())
            .build();

        when(groupExpander.getGroupsWithMembers(eq(groupsRequest))).thenReturn(
            Collections.singleton(childRgGroupAndMembers).stream());

        final String rgName = "testRG";

        final GroupApiDTO groupApiDtoMock = new GroupApiDTO();
        groupApiDtoMock.setUuid("2");
        groupApiDtoMock.setDisplayName(rgName);
        groupApiDtoMock.setClassName(StringConstants.RESOURCE_GROUP);
        groupApiDtoMock.setSeverity(Severity.NORMAL.toString());

        when(groupMapper.getEnvironmentAndCloudTypeForGroup(eq(childRgGroupAndMembers)))
            .thenReturn(new EntityEnvironment(EnvironmentType.CLOUD, CloudType.AZURE));

        when(groupMapper.toGroupApiDto(childRgGroupAndMembers, EnvironmentType.CLOUD, CloudType.AZURE, true))
            .thenReturn(groupApiDtoMock);

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

        when(groupExpander.getGroupWithMembers("1"))
                .thenReturn(Optional.of(rgGroupAndMembers));

        final GroupFilter groupFilter = GroupFilter.newBuilder()
                .setGroupType(GroupType.RESOURCE)
                .addAllId(Collections.singletonList(resourceGroupOid))
                .build();

        final GetGroupsRequest groupsRequest = GetGroupsRequest.newBuilder()
                .setGroupFilter(groupFilter)
                .build();

        when(groupFilterMapper.apiFilterToGroupFilter(eq(GroupType.RESOURCE),
                eq(Collections.emptyList()))).thenReturn(GroupFilter.newBuilder()
                .setGroupType(GroupType.RESOURCE).build());

        when(groupExpander.getGroupsWithMembers(eq(groupsRequest))).thenReturn(
                Collections.singleton(rgGroupAndMembers).stream());

        final GroupApiDTO groupApiDtoMock = new GroupApiDTO();
        groupApiDtoMock.setUuid(String.valueOf(resourceGroupOid));
        groupApiDtoMock.setDisplayName(rgName);
        groupApiDtoMock.setClassName(StringConstants.RESOURCE_GROUP);
        groupApiDtoMock.setSeverity(Severity.NORMAL.toString());

        when(groupMapper.getEnvironmentAndCloudTypeForGroup(eq(rgGroupAndMembers)))
                .thenReturn(new EntityEnvironment(EnvironmentType.CLOUD, CloudType.AZURE));

        when(groupMapper.toGroupApiDto(rgGroupAndMembers, EnvironmentType.CLOUD, CloudType.AZURE, true))
                .thenReturn(groupApiDtoMock);

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

        when(repositoryApi.newSearchRequest(SearchParameters.newBuilder()
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
            Lists.newArrayList(UIEntityType.VIRTUAL_MACHINE.apiStr()), null);
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
                ImmutableMap.of(UIEntityType.VIRTUAL_MACHINE.apiStr(), SupplyChainNode.newBuilder()
                    .putMembersByState(EntityState.POWERED_ON_VALUE, MemberList.newBuilder()
                        .addMemberOids(vm1).addMemberOids(vm2).build())
                    .build()));
        when(supplyChainFetcherFactory.newNodeFetcher()).thenReturn(fetcherBuilder);

        Set<Long> expandedIds = groupsService.expandUuids(Sets.newHashSet(groupId11), null, null);
        assertThat(expandedIds, containsInAnyOrder(vm1, vm2));

        // expand a VM group, provide PM entity type and expect to get all related PMs in the group
        fetcherBuilder = ApiTestUtils.mockNodeFetcherBuilder(
            ImmutableMap.of(UIEntityType.PHYSICAL_MACHINE.apiStr(), SupplyChainNode.newBuilder()
                .putMembersByState(EntityState.POWERED_ON_VALUE, MemberList.newBuilder()
                    .addMemberOids(pm1).build())
                .build()));
        when(supplyChainFetcherFactory.newNodeFetcher()).thenReturn(fetcherBuilder);

        expandedIds = groupsService.expandUuids(Sets.newHashSet(groupId11),
            Lists.newArrayList(UIEntityType.PHYSICAL_MACHINE.apiStr()), null);
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
        groupsService.getSettingsByGroupUuid(groupUuid, false);
        verify(groupServiceSpy).getGroup(GroupID.newBuilder()
            .setId(Long.valueOf(groupUuid))
            .build());
        assertEquals(template, templateServiceSpy.getTemplates(any()).get(0).getTemplates(0).getTemplate());
    }

    /**
     * Test getting groups and filter by environment type. Only groups matching requested
     * environment type should be returned.
     *
     * @throws Exception any error happens
     */
    @Test
    public void testGetGroupsFilterByEnvironmentType() throws Exception {
        final String name = "group_name";
        final GroupApiDTO groupApiDto1 = new GroupApiDTO();
        groupApiDto1.setUuid("1");
        groupApiDto1.setEnvironmentType(EnvironmentType.ONPREM);
        final GroupApiDTO groupApiDto2 = new GroupApiDTO();
        groupApiDto2.setUuid("2");
        groupApiDto2.setEnvironmentType(EnvironmentType.CLOUD);

        when(groupFilterMapper.apiFilterToGroupFilter(eq(GroupType.REGULAR),
                eq(Collections.emptyList()))).thenReturn(GroupFilter.newBuilder()
                .setGroupType(GroupType.REGULAR).build());

        // onprem group
        final Grouping group1 = Grouping.newBuilder()
                .setId(1L)
                .setDefinition(GroupDefinition.newBuilder()
                        .setDisplayName(name)
                        .setType(GroupType.REGULAR))
                .build();
        final GroupAndMembers groupAndMembers1 = ImmutableGroupAndMembers.builder()
                .group(group1)
                .members(Collections.emptyList())
                .entities(Collections.emptyList())
                .build();
        when(groupExpander.getMembersForGroup(group1)).thenReturn(groupAndMembers1);
        when(groupMapper.getEnvironmentAndCloudTypeForGroup(groupAndMembers1)).thenReturn(new EntityEnvironment(EnvironmentType.ONPREM, CloudType.UNKNOWN));
        when(groupMapper.toGroupApiDto(groupAndMembers1, EnvironmentType.ONPREM, CloudType.UNKNOWN, true)).thenReturn(groupApiDto1);

        // cloud group
        final Grouping group2 = Grouping.newBuilder()
                .setId(2L)
                .setDefinition(GroupDefinition.newBuilder()
                        .setDisplayName(name)
                        .setType(GroupType.REGULAR))
                .build();
        final GroupAndMembers groupAndMembers2 = ImmutableGroupAndMembers.builder()
                .group(group2)
                .members(Collections.emptyList())
                .entities(Collections.emptyList())
                .build();
        when(groupExpander.getMembersForGroup(group2)).thenReturn(groupAndMembers2);
        when(groupMapper.getEnvironmentAndCloudTypeForGroup(groupAndMembers2)).thenReturn(new EntityEnvironment(EnvironmentType.CLOUD, CloudType.AWS));
        when(groupMapper.toGroupApiDto(groupAndMembers2, EnvironmentType.CLOUD, CloudType.AWS, true)).thenReturn(groupApiDto2);
        when(groupServiceSpy.getGroupForEntity(GetGroupForEntityRequest.newBuilder()
                .setEntityId(111L)
                .build()))
                .thenReturn(GetGroupForEntityResponse.newBuilder()
                        .addGroup(group1)
                        .addGroup(group2)
                        .build());

        final ApiId entityApiId = mock(ApiId.class);
        when(entityApiId.isGroup()).thenReturn(false);
        when(uuidMapper.fromUuid("111")).thenReturn(entityApiId);
        final GetGroupsRequest groupsRequest = GetGroupsRequest.newBuilder()
                .setGroupFilter(GroupFilter.newBuilder()
                        .setGroupType(GroupType.REGULAR)
                        .build())
                .addScopes(111L)
                .build();

        when(groupExpander.getGroupsWithMembers(eq(groupsRequest)))
                .thenAnswer(invocation -> Stream.of(groupAndMembers1, groupAndMembers2));
        // check filter by onprem
        List<GroupApiDTO> groupApiDTOs = groupsService.getGroupsByType(GroupType.REGULAR,
                Collections.singletonList("111"), Collections.emptyList(),
                EnvironmentType.ONPREM);
        assertThat(groupApiDTOs.stream().map(GroupApiDTO::getUuid).collect(Collectors.toList()),
                containsInAnyOrder("1"));
        // check filter by cloud
        groupApiDTOs = groupsService.getGroupsByType(GroupType.REGULAR,
                Collections.singletonList("111"), Collections.emptyList(),
                EnvironmentType.CLOUD);
        assertThat(groupApiDTOs.stream().map(GroupApiDTO::getUuid).collect(Collectors.toList()),
                containsInAnyOrder("2"));
        // check filter by hybrid
        groupApiDTOs = groupsService.getGroupsByType(GroupType.REGULAR,
                Collections.singletonList("111"), Collections.emptyList(),
                EnvironmentType.HYBRID);
        assertThat(groupApiDTOs.stream().map(GroupApiDTO::getUuid).collect(Collectors.toList()),
                containsInAnyOrder("1", "2"));
        // check filter by unknown
        groupApiDTOs = groupsService.getGroupsByType(GroupType.REGULAR,
                Collections.singletonList("111"), Collections.emptyList(),
                EnvironmentType.UNKNOWN);
        assertThat(groupApiDTOs, empty());
    }

    /**
     * Test get Resource groups when scope is group of accounts. Get groups owned by Account.
     *
     * @throws OperationFailedException if cannot convert the requested filter criteria.
     */
    @Test
    public void testGetGroupByTypeScopeIsGroupOfAccounts() throws OperationFailedException {
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
        when(groupExpander.getGroupWithMembers(groupOfAccountsId.toString())).thenReturn(
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
        final GroupAndMembers rgGroupAndMembers = ImmutableGroupAndMembers.builder()
                .group(rg1)
                .members(Arrays.asList(21L, 22L))
                .entities(Arrays.asList(21L, 22L))
                .build();
        when(groupMapper.getEnvironmentAndCloudTypeForGroup(rgGroupAndMembers)).thenReturn(
                new EntityEnvironment(EnvironmentType.CLOUD, CloudType.UNKNOWN));
        final ResourceGroupApiDTO groupApiDTO = new ResourceGroupApiDTO();
        groupApiDTO.setUuid(String.valueOf(rgId));
        groupApiDTO.setClassName(StringConstants.RESOURCE_GROUP);
        groupApiDTO.setEnvironmentType(EnvironmentType.CLOUD);
        groupApiDTO.setParentUuid(String.valueOf(accountId1));
        when(groupMapper.toGroupApiDto(rgGroupAndMembers, EnvironmentType.CLOUD, CloudType.UNKNOWN,
                true)).thenReturn(groupApiDTO);
        when(groupExpander.getGroupsWithMembers(GetGroupsRequest.newBuilder()
                .setGroupFilter(GroupFilter.newBuilder().addPropertyFilters(propertyFilter).build())
                .build())).thenReturn(Stream.of(rgGroupAndMembers));
        when(groupFilterMapper.apiFilterToGroupFilter(any(), any())).thenReturn(
                GroupFilter.newBuilder().build());
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
        when(cachedGroupInfo.getEntityTypes()).thenReturn(ImmutableSet.of(UIEntityType.REGION));
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
        Assert.assertEquals(UIEntityType.values().length, actionApiInputDTO.getRelatedEntityTypes().size());
    }

    /**
     * Test that if the scope is user groups, we should set a group filter with origin to be user.
     *
     * @throws Exception any error happens
     */
    @Test
    public void testGetPaginatedGroupApiDTOsWithScopes() throws Exception {
        when(groupFilterMapper.apiFilterToGroupFilter(eq(GroupType.REGULAR),
                eq(Collections.emptyList()))).thenReturn(GroupFilter.newBuilder()
                .setGroupType(GroupType.REGULAR).build());
        GetGroupsRequest expectedRequest = GetGroupsRequest.newBuilder()
                .setGroupFilter(GroupFilter.newBuilder()
                        .setGroupType(GroupType.REGULAR)
                        .setOriginFilter(OriginFilter.newBuilder()
                                .addOrigin(Type.USER)))
                .build();
        when(groupExpander.getGroupsWithMembers(expectedRequest)).thenReturn(Stream.empty());

        groupsService.getPaginatedGroupApiDTOs(Collections.emptyList(),
                new SearchPaginationRequest(null, null, true, null), null, null,
                Lists.newArrayList(GroupsService.USER_GROUPS));

        // verify that group filter is populated to origin USER
        verify(groupExpander).getGroupsWithMembers(expectedRequest);
    }

    /**
     * Test that getPaginatedGroupApiDTOs works as expected for both groups of entities (group of
     * VMs) and groups of groups (group of clusters).
     *
     * @throws Exception any error happens
     */
    @Test
    public void testGetPaginatedGroupApiDTOsUserGroupsOfEntitiesOrGroups() throws Exception {
        when(groupFilterMapper.apiFilterToGroupFilter(eq(GroupType.REGULAR),
                eq(Collections.emptyList()))).thenReturn(GroupFilter.newBuilder()
                .setGroupType(GroupType.REGULAR).build());
        GetGroupsRequest expectedRequest = GetGroupsRequest.newBuilder()
                .setGroupFilter(GroupFilter.newBuilder()
                        .setGroupType(GroupType.REGULAR)
                        .setOriginFilter(OriginFilter.newBuilder()
                                .addOrigin(Type.USER)))
                .build();

        final Grouping groupOfClusters = Grouping.newBuilder()
                .setId(11L)
                .setDefinition(GroupDefinition.newBuilder()
                        .setType(GroupType.REGULAR)
                        .setStaticGroupMembers(StaticMembers.newBuilder()
                                .addMembersByType(StaticMembersByType.newBuilder()
                                        .setType(MemberType.newBuilder()
                                                .setGroup(GroupType.COMPUTE_HOST_CLUSTER)))))
                .build();
        final Grouping groupOfVMs = Grouping.newBuilder()
                .setId(12L)
                .setDefinition(GroupDefinition.newBuilder()
                        .setType(GroupType.REGULAR)
                        .setStaticGroupMembers(StaticMembers.newBuilder()
                                .addMembersByType(StaticMembersByType.newBuilder()
                                        .setType(MemberType.newBuilder()
                                                .setEntity(EntityType.VIRTUAL_MACHINE_VALUE)))))
                .build();

        final GroupAndMembers group1 = ImmutableGroupAndMembers.builder()
                .group(groupOfClusters)
                .members(Collections.emptyList())
                .entities(Collections.emptyList())
                .build();
        final GroupAndMembers group2 = ImmutableGroupAndMembers.builder()
                .group(groupOfVMs)
                .members(Collections.emptyList())
                .entities(Collections.emptyList())
                .build();
        when(groupExpander.getGroupsWithMembers(expectedRequest)).thenAnswer(c ->
                Stream.of(group1, group2));

        GroupApiDTO groupApiDTO1 = new GroupApiDTO();
        groupApiDTO1.setUuid(String.valueOf(groupOfClusters.getId()));
        GroupApiDTO groupApiDTO2 = new GroupApiDTO();
        groupApiDTO2.setUuid(String.valueOf(groupOfVMs.getId()));

        when(groupMapper.toGroupApiDtoWithoutActiveEntities(group1, EnvironmentType.UNKNOWN,
                CloudType.UNKNOWN)).thenReturn(groupApiDTO1);
        when(groupMapper.toGroupApiDtoWithoutActiveEntities(group2, EnvironmentType.UNKNOWN,
                CloudType.UNKNOWN)).thenReturn(groupApiDTO2);

        // search for group of clusters
        SearchPaginationResponse response = groupsService.getPaginatedGroupApiDTOs(
                Collections.emptyList(), new SearchPaginationRequest(null, null, true, null),
                "Cluster", null, Lists.newArrayList(GroupsService.USER_GROUPS));

        assertThat(response.getRestResponse().getBody().stream()
                .map(BaseApiDTO::getUuid)
                .map(Long::valueOf)
                .collect(Collectors.toList()), containsInAnyOrder(groupOfClusters.getId()));

        // search for group of VMs
        response = groupsService.getPaginatedGroupApiDTOs(Collections.emptyList(),
                new SearchPaginationRequest(null, null, true, null), "VirtualMachine", null,
                Lists.newArrayList(GroupsService.USER_GROUPS));
        assertThat(response.getRestResponse().getBody().stream()
                .map(BaseApiDTO::getUuid)
                .map(Long::valueOf)
                .collect(Collectors.toList()), containsInAnyOrder(groupOfVMs.getId()));
    }
}
