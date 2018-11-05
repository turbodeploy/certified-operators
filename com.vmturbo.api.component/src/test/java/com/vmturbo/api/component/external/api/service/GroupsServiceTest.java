package com.vmturbo.api.component.external.api.service;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.google.common.collect.Lists;

import io.grpc.Channel;
import io.grpc.Status;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.mapper.ActionSpecMapper;
import com.vmturbo.api.component.external.api.mapper.GroupMapper;
import com.vmturbo.api.component.external.api.mapper.PaginationMapper;
import com.vmturbo.api.component.external.api.mapper.SettingsManagerMappingLoader.SettingsManagerMapping;
import com.vmturbo.api.dto.group.FilterApiDTO;
import com.vmturbo.api.dto.group.GroupApiDTO;
import com.vmturbo.api.dto.setting.SettingApiInputDTO;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.CreateGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.CreateTempGroupRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.CreateTempGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse.Members;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.Group.Type;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupID;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.TempGroupInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.UpdateClusterHeadroomTemplateRequest;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.plan.TemplateDTO.GetTemplatesByNameRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateInfo;
import com.vmturbo.common.protobuf.plan.TemplateDTOMoles.TemplateServiceMole;
import com.vmturbo.common.protobuf.plan.TemplateServiceGrpc;
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
    private PaginationMapper paginationMapper;

    @Mock
    private RepositoryApi repositoryApi;

    @Mock
    private Channel channelMock;

    @Captor
    private ArgumentCaptor<GetGroupsRequest> getGroupsRequestCaptor;

    private GroupServiceMole groupServiceSpy = spy(new GroupServiceMole());

    private TemplateServiceMole templateServiceSpy = spy(new TemplateServiceMole());

    private FilterApiDTO groupFilterApiDTO = new FilterApiDTO();
    private FilterApiDTO clusterFilterApiDTO = new FilterApiDTO();

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(groupServiceSpy, templateServiceSpy);

    @Before
    public void init() throws Exception {
        MockitoAnnotations.initMocks(this);

        channelMock = Mockito.mock(Channel.class);
        ActionsServiceGrpc.ActionsServiceBlockingStub actionsRpcService = ActionsServiceGrpc.newBlockingStub(channelMock);

        long realtimeTopologyContextId = 7777777;
        groupsService =  new GroupsService(
                actionsRpcService,
                GroupServiceGrpc.newBlockingStub(grpcServer.getChannel()),
                actionSpecMapper,
                groupMapper,
                paginationMapper,
                repositoryApi,
                realtimeTopologyContextId,
                mock(SettingsManagerMapping.class),
                TemplateServiceGrpc.newBlockingStub(grpcServer.getChannel()));

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
        final GroupDTO.GetGroupsRequest.Builder groupsRequest =
                groupsService.getGroupsRequestForFilters(filterList);

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
        final GroupDTO.GetGroupsRequest.Builder groupsRequest =
                groupsService.getGroupsRequestForFilters(filterList);

        // Assert
        assertThat(groupsRequest.getNameFilter().getNameRegex(), is(GROUP_TEST_PATTERN));
        assertThat(groupsRequest.getNameFilter().getNegateMatch(), is(true));
    }

    @Test
    public void getGroupByUuid() throws Exception {
        final GroupApiDTO apiGroup = new GroupApiDTO();
        apiGroup.setDisplayName("minion");
        final Group xlGroup = Group.newBuilder().setId(1).build();
        when(groupMapper.toGroupApiDto(xlGroup)).thenReturn(apiGroup);
        when(groupServiceSpy.getGroup(eq(GroupID.newBuilder()
                .setId(1)
                .build())))
            .thenReturn(GetGroupResponse.newBuilder()
                .setGroup(xlGroup)
                .build());

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

        assertEquals(clusterHeadroomGroupUuid, retGroup.getUuid());
    }

    /**
     * Tests getting of all clusters from groups service. Currently UI provides
     * "GROUP-StorageByStorageCluster" group uuid when it expect to receive all storage clusters.
     *
     * @throws Exception it's thrown by groups service at the top level in case of any error.
     */
    @Test
    public void testGetClustersByClusterHeadroomGroupUuid() throws Exception {
        final String clusterHeadroomGroupUuid = "GROUP-PhysicalMachineByCluster";
        groupsService.getMembersByGroupUuid(clusterHeadroomGroupUuid);
        verify(groupServiceSpy).getGroups(getGroupsRequestCaptor.capture());
        final GetGroupsRequest request = getGroupsRequestCaptor.getValue();
        assertEquals(Type.CLUSTER, request.getTypeFilter());
        assertEquals(GroupDTO.ClusterInfo.Type.COMPUTE, request.getClusterFilter().getTypeFilter());
    }

    /**
     * Tests getting of all storage clusters from groups service. Currently UI provides
     * "GROUP-StorageByStorageCluster" group uuid when it expect to receive all storage clusters.
     *
     * @throws Exception it's thrown by groups service at the top level in case of any error.
     */
    @Test
    public void testGetClustersByStorageClusterHeadroomGroupUuid() throws Exception {
        final String clusterHeadroomGroupUuid = "GROUP-StorageByStorageCluster";
        groupsService.getMembersByGroupUuid(clusterHeadroomGroupUuid);
        verify(groupServiceSpy).getGroups(getGroupsRequestCaptor.capture());
        final GetGroupsRequest request = getGroupsRequestCaptor.getValue();
        assertEquals(Type.CLUSTER, request.getTypeFilter());
        assertEquals(GroupDTO.ClusterInfo.Type.STORAGE, request.getClusterFilter().getTypeFilter());
    }

    @Test
    public void testPutSettingByUuidAndName() throws Exception {
        String groupUuid = "1234";
        String templateId = "3333";

        SettingApiInputDTO setting = new SettingApiInputDTO();
        setting.setValue(templateId);

        Template template = Template.newBuilder()
                .setId(Long.parseLong(templateId))
                .setType(Template.Type.SYSTEM)
                .setTemplateInfo(TemplateInfo.newBuilder()
                        .setName("template name"))
                .build();
        when(templateServiceSpy.getTemplatesByName(any(GetTemplatesByNameRequest.class)))
                .thenReturn(Arrays.asList(template));
        groupsService.putSettingByUuidAndName(groupUuid, "capacityplandatamanager",
                "templateName", setting);
        verify(groupServiceSpy).updateClusterHeadroomTemplate(UpdateClusterHeadroomTemplateRequest.newBuilder()
                .setGroupId(Long.parseLong(groupUuid))
                .setClusterHeadroomTemplateId(Long.parseLong(setting.getValue()))
                .build());
    }

    @Test(expected = UnknownObjectException.class)
    public void testGroupAndClusterNotFound() throws Exception {
        groupsService.getGroupByUuid("1", false);
    }

    @Test
    public void getGroupMembersByUuid() throws Exception {
        final long memberId = 7;
        when(groupServiceSpy.getMembers(GetMembersRequest.newBuilder()
                    .setId(1L)
                    .build()))
            .thenReturn(GetMembersResponse.newBuilder()
                .setMembers(Members.newBuilder().addIds(memberId))
                .build());
        Optional<Set<Long>> retIds = groupsService.getMemberIds("1");
        assertTrue(retIds.isPresent());
        assertEquals(1, retIds.get().size());
        assertEquals(memberId, retIds.get().iterator().next().longValue());
    }

    @Test(expected = UnknownObjectException.class)
    public void testGetMemberIdsInvalidUuid() throws Exception {
        final long groupId = 1;
        when(groupServiceSpy.getMembersError(eq(GetMembersRequest.newBuilder()
                .setId(groupId)
                .build())))
            .thenReturn(Optional.of(Status.NOT_FOUND.asException()));
        groupsService.getMemberIds("1");
    }

    @Test
    public void testCreateGroup() throws Exception {
        final GroupApiDTO apiDTO = new GroupApiDTO();
        final Group group = Group.newBuilder()
                .setId(7L)
                .build();
        when(groupMapper.toGroupInfo(apiDTO)).thenReturn(GroupInfo.getDefaultInstance());
        when(groupServiceSpy.createGroup(GroupInfo.getDefaultInstance()))
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
        final Group group = Group.newBuilder()
                .setId(7L)
                .build();
        when(groupMapper.toTempGroupProto(apiDTO)).thenReturn(TempGroupInfo.getDefaultInstance());
        when(groupServiceSpy.createTempGroup(CreateTempGroupRequest.newBuilder()
                .setGroupInfo(TempGroupInfo.getDefaultInstance())
                .build()))
            .thenReturn(CreateTempGroupResponse.newBuilder()
                .setGroup(group)
                .build());
        when(groupMapper.toGroupApiDto(group, EnvironmentType.ONPREM)).thenReturn(apiDTO);
        assertThat(groupsService.createGroup(apiDTO), is(apiDTO));
    }

    @Test
    public void testCreateTempGroupCloud() throws Exception {
        final GroupApiDTO apiDTO = new GroupApiDTO();
        apiDTO.setTemporary(true);
        apiDTO.setEnvironmentType(EnvironmentType.CLOUD);
        final Group group = Group.newBuilder()
                .setId(7L)
                .build();
        when(groupMapper.toTempGroupProto(apiDTO)).thenReturn(TempGroupInfo.getDefaultInstance());
        when(groupServiceSpy.createTempGroup(CreateTempGroupRequest.newBuilder()
                .setGroupInfo(TempGroupInfo.getDefaultInstance())
                .build()))
                .thenReturn(CreateTempGroupResponse.newBuilder()
                        .setGroup(group)
                        .build());
        when(groupMapper.toGroupApiDto(group, EnvironmentType.CLOUD)).thenReturn(apiDTO);
        assertThat(groupsService.createGroup(apiDTO), is(apiDTO));
    }
}
