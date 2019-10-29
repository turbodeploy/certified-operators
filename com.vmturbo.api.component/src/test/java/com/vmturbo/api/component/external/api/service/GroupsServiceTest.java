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
import java.util.stream.Stream;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.junit.Before;
import org.junit.Ignore;
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
import com.vmturbo.api.component.external.api.mapper.GroupFilterMapper;
import com.vmturbo.api.component.external.api.mapper.GroupMapper;
import com.vmturbo.api.component.external.api.mapper.PaginationMapper;
import com.vmturbo.api.component.external.api.mapper.SettingsManagerMappingLoader.SettingsManagerMapping;
import com.vmturbo.api.component.external.api.mapper.SettingsMapper;
import com.vmturbo.api.component.external.api.mapper.SeverityPopulator;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
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
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.group.FilterApiDTO;
import com.vmturbo.api.dto.group.GroupApiDTO;
import com.vmturbo.api.dto.setting.SettingApiDTO;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.pagination.GroupMembersPaginationRequest;
import com.vmturbo.api.pagination.GroupMembersPaginationRequest.GroupMemberOrderBy;
import com.vmturbo.api.pagination.GroupMembersPaginationRequest.GroupMembersPaginationResponse;
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
import com.vmturbo.common.protobuf.group.GroupDTO.Origin;
import com.vmturbo.common.protobuf.group.GroupDTO.UpdateClusterHeadroomTemplateRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.UpdateGroupRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.UpdateGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
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
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;
import com.vmturbo.topology.processor.api.util.ThinTargetCache.ThinTargetInfo;

public class GroupsServiceTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    public static final String NE_MATCH_TYPE = "NE";
    public static final String EQ_MATCH_TYPE = "EQ";
    public static final String GROUP_FILTER_TYPE = "groupsByName";
    public static final String CLUSTER_FILTER_TYPE = "clustersByName";
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
                        mock(RepositoryApi.class), CONTEXT_ID);
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
        when(groupMapper.getEnvironmentTypeForGroup(groupAndMembers)).thenReturn(EnvironmentType.ONPREM);
        when(groupMapper.toGroupApiDto(groupAndMembers, EnvironmentType.ONPREM)).thenReturn(apiGroup);

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
    @Ignore
    public void testGetClustersByClusterHeadroomGroupUuid() throws Exception {
        GroupApiDTO clusterApiDto = new GroupApiDTO();
        clusterApiDto.setUuid("mycluster");
        GroupAndMembers clusterAndMembers = groupAndMembers(1L, GroupType.COMPUTE_HOST_CLUSTER, Collections.singleton(7L));
        when(groupExpander.getGroupsWithMembers(any()))
            .thenReturn(Stream.of(clusterAndMembers));
        when(groupMapper.getEnvironmentTypeForGroup(clusterAndMembers)).thenReturn(EnvironmentType.ONPREM);
        when(groupMapper.toGroupApiDto(clusterAndMembers, EnvironmentType.ONPREM)).thenReturn(clusterApiDto);
        when(severityPopulator.calculateSeverity(CONTEXT_ID, Collections.singleton(7L)))
            .thenReturn(Optional.of(Severity.NORMAL));

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
    @Ignore
    public void testGetClustersByStorageClusterHeadroomGroupUuid() throws Exception {
        GroupApiDTO clusterApiDto = new GroupApiDTO();
        clusterApiDto.setUuid("mycluster");
        GroupAndMembers clusterAndMembers = groupAndMembers(1L, GroupType.STORAGE_CLUSTER, Collections.singleton(7L));
        when(groupExpander.getGroupsWithMembers(any()))
            .thenReturn(Stream.of(clusterAndMembers));
        when(groupMapper.getEnvironmentTypeForGroup(clusterAndMembers)).thenReturn(EnvironmentType.ONPREM);
        when(groupMapper.toGroupApiDto(clusterAndMembers, EnvironmentType.ONPREM)).thenReturn(clusterApiDto);
        when(severityPopulator.calculateSeverity(CONTEXT_ID, Collections.singleton(7L)))
            .thenReturn(Optional.of(Severity.NORMAL));

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

    @Test
    @Ignore
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
        when(groupMapper.toGroupApiDto(group)).thenReturn(apiDTO);
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
        when(groupMapper.toGroupApiDto(group)).thenReturn(apiDTO);
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
        when(groupMapper.toGroupApiDto(group)).thenReturn(apiDTO);
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

        when(groupMapper.toGroupApiDto(eq(updateGroupResponse.getUpdatedGroup()))).thenReturn(
                groupApiDTO);

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
        groupApiDtoMock.setClassName(CommonDTO.GroupDTO.ConstraintType.CLUSTER.name());

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

        when(groupExpander.getMembersForGroup(cluster)).thenReturn(clusterAndMembers);
        when(groupMapper.getEnvironmentTypeForGroup(clusterAndMembers)).thenReturn(EnvironmentType.ONPREM);
        when(groupMapper.toGroupApiDto(clusterAndMembers, EnvironmentType.ONPREM)).thenReturn(groupApiDtoMock);
        when(groupServiceSpy.getGroupForEntity(GetGroupForEntityRequest.newBuilder()
                .setEntityId(1L)
                .build()))
                .thenReturn(GetGroupForEntityResponse.newBuilder()
                    .addGroup(cluster)
                    .build());
        when(severityPopulator.calculateSeverity(CONTEXT_ID, Collections.emptyList()))
            .thenReturn(Optional.of(Severity.NORMAL));
        final ApiId entityApiId = mock(ApiId.class);
        when(entityApiId.isGroup()).thenReturn(false);

        when(uuidMapper.fromUuid("1")).thenReturn(entityApiId);

        // Act
        final List<GroupApiDTO> groupApiDTOs =
                        groupsService.getGroupsByType(GroupType.COMPUTE_HOST_CLUSTER,
                                        Collections.singletonList("1"), Collections.emptyList());

        // Assert
        assertThat(groupApiDTOs.size(), is(1));
        final GroupApiDTO groupApiDTO = groupApiDTOs.get(0);
        assertEquals(name, groupApiDTO.getDisplayName());
        assertEquals(CommonDTO.GroupDTO.ConstraintType.CLUSTER.name(), groupApiDTO.getClassName());
        assertEquals("2", groupApiDTO.getUuid());
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

}
