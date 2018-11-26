package com.vmturbo.api.component.external.api.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.hamcrest.Matchers.is;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper.UIEntityType;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.service.GroupsService;
import com.vmturbo.api.dto.group.GroupApiDTO;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupID;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.components.api.test.GrpcTestServer;

public class MagicScopeGatewayTest {

    private GroupsService groupsService = mock(GroupsService.class);

    private GroupServiceMole groupBackendMole = spy(new GroupServiceMole());

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(groupBackendMole);

    private static final long REALTIME_CONTEXT = 124521;

    private static final String TEMP_GROUP_UUID = "1";

    private MagicScopeGateway gateway;

    private GroupApiDTO tempGroup;

    @Before
    public void setup() {
        gateway = new MagicScopeGateway(groupsService,
                GroupServiceGrpc.newBlockingStub(grpcServer.getChannel()),
                REALTIME_CONTEXT);

        tempGroup = new GroupApiDTO();
        tempGroup.setUuid(TEMP_GROUP_UUID);
    }

    @Test
    public void testMapOnPremHost() throws Exception {
        final ArgumentCaptor<GroupApiDTO> groupCreateRequestCaptor =
                ArgumentCaptor.forClass(GroupApiDTO.class);

        when(groupsService.createGroup(any())).thenReturn(tempGroup);
        final String mappedUuid = gateway.enter(MagicScopeGateway.ALL_ON_PREM_HOSTS);

        assertThat(mappedUuid, is(TEMP_GROUP_UUID));

        verify(groupsService).createGroup(groupCreateRequestCaptor.capture());
        final GroupApiDTO groupRequest = groupCreateRequestCaptor.getValue();
        assertThat(groupRequest.getTemporary(), is(true));
        assertThat(groupRequest.getGroupType(), is(UIEntityType.PHYSICAL_MACHINE.getValue()));
        assertThat(groupRequest.getScope(), contains(UuidMapper.UI_REAL_TIME_MARKET_STR));
        assertThat(groupRequest.getEnvironmentType(), is(EnvironmentType.ONPREM));
    }

    @Test
    public void testMapOnPremHostGroupExpiredTriggersRefresh() throws Exception {
        // Create the group (this assumes the "normal" test works")
        when(groupsService.createGroup(any())).thenReturn(tempGroup);
        final String mappedUuid = gateway.enter(MagicScopeGateway.ALL_ON_PREM_HOSTS);
        assertThat(mappedUuid, is(TEMP_GROUP_UUID));

        // Next time the "createGroup" method gets called we return a different group.
        final String newTempGroupId = "1029";
        final GroupApiDTO newTempGroup = new GroupApiDTO();
        newTempGroup.setUuid(newTempGroupId);
        when(groupsService.createGroup(any())).thenReturn(newTempGroup);

        // This request should trigger a RPC to the group service to make sure the temp group
        // still exists.
        final GroupID tempGroupId = GroupID.newBuilder()
                .setId(Long.parseLong(TEMP_GROUP_UUID))
                .build();
        // Return a group
        when(groupBackendMole.getGroup(tempGroupId)).thenReturn(GetGroupResponse.newBuilder()
            .setGroup(Group.getDefaultInstance())
            .build());

        final String mappedUuid2 = gateway.enter(MagicScopeGateway.ALL_ON_PREM_HOSTS);
        verify(groupBackendMole).getGroup(tempGroupId);
        // Should still map to the original UUID
        assertThat(mappedUuid2, is(TEMP_GROUP_UUID));

        // Now pretend the temporary group expired - RPC will return nothing.
        when(groupBackendMole.getGroup(tempGroupId)).thenReturn(GetGroupResponse.getDefaultInstance());

        final String mappedUuid3 = gateway.enter(MagicScopeGateway.ALL_ON_PREM_HOSTS);
        verify(groupBackendMole, times(2)).getGroup(tempGroupId);
        assertThat(mappedUuid3, is(newTempGroupId));
    }

    @Test
    public void testMapOnPremHostNewTopologyTriggersRefresh() throws Exception {
        // Create the group (this assumes the "normal" test works")
        when(groupsService.createGroup(any())).thenReturn(tempGroup);
        final String mappedUuid = gateway.enter(MagicScopeGateway.ALL_ON_PREM_HOSTS);
        assertThat(mappedUuid, is(TEMP_GROUP_UUID));

        // Next call to create a group will return a different group.
        final String newTempGroupId = "1029";
        final GroupApiDTO newTempGroup = new GroupApiDTO();
        newTempGroup.setUuid(newTempGroupId);
        when(groupsService.createGroup(any())).thenReturn(newTempGroup);

        // This should "clear" the cached temp group.
        gateway.onSourceTopologyAvailable(1L, REALTIME_CONTEXT);

        final String mappedUuid2 = gateway.enter(MagicScopeGateway.ALL_ON_PREM_HOSTS);
        assertThat(mappedUuid2, is(newTempGroupId));
    }

    @Test
    public void testMapOnPremHostNonRealtimeTopologyIgnored() throws Exception {
        // Create the group (this assumes the "normal" test works")
        when(groupsService.createGroup(any())).thenReturn(tempGroup);
        final String mappedUuid = gateway.enter(MagicScopeGateway.ALL_ON_PREM_HOSTS);
        assertThat(mappedUuid, is(TEMP_GROUP_UUID));

        // Next call to create a group will return a different group.
        final String newTempGroupId = "1029";
        final GroupApiDTO newTempGroup = new GroupApiDTO();
        newTempGroup.setUuid(newTempGroupId);
        when(groupsService.createGroup(any())).thenReturn(newTempGroup);

        // Return a group for the cache check.
        when(groupBackendMole.getGroup(GroupID.newBuilder()
            .setId(Long.parseLong(TEMP_GROUP_UUID))
            .build())).thenReturn(GetGroupResponse.newBuilder()
                .setGroup(Group.getDefaultInstance())
                .build());

        // This should NOT "clear" the cached temp group, because the context is not realtime.
        gateway.onSourceTopologyAvailable(1L, REALTIME_CONTEXT + 1);

        final String mappedUuid2 = gateway.enter(MagicScopeGateway.ALL_ON_PREM_HOSTS);
        assertThat(mappedUuid2, is(TEMP_GROUP_UUID));
    }


}
