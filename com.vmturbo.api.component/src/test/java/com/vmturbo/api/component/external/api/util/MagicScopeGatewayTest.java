package com.vmturbo.api.component.external.api.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.vmturbo.api.component.external.api.mapper.GroupMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.dto.group.GroupApiDTO;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.common.protobuf.group.GroupDTO.CreateGroupRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.CreateGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupID;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTO.Origin;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.api.test.GrpcTestServer;

public class MagicScopeGatewayTest {

    private GroupMapper groupsMapper = mock(GroupMapper.class);

    private GroupServiceMole groupBackendMole = spy(new GroupServiceMole());

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(groupBackendMole);

    private static final long REALTIME_CONTEXT = 124521;

    private static final long TEMP_GROUP_ID = 1;
    private static final String TEMP_GROUP_UUID = Long.toString(TEMP_GROUP_ID);

    private MagicScopeGateway gateway;

    @Before
    public void setup() throws InvalidOperationException, OperationFailedException {
        gateway = new MagicScopeGateway(groupsMapper,
                GroupServiceGrpc.newBlockingStub(grpcServer.getChannel()),
                REALTIME_CONTEXT);

    }

    private void setupMapperAndBackend(long oid) throws Exception {
        GroupDefinition groupDefinition  = GroupDefinition
                        .newBuilder()
                        .setDisplayName(Long.toString(oid))
                        .setIsTemporary(true)
                        .build();

        when(groupsMapper.toGroupDefinition(any())).thenReturn(groupDefinition);

        doReturn(CreateGroupResponse.newBuilder()
            .setGroup(Grouping.newBuilder()
                .setId(oid))
            .build()).when(groupBackendMole).createGroup(CreateGroupRequest.newBuilder()
                .setGroupDefinition(groupDefinition)
                .setOrigin(Origin.newBuilder()
                                .setSystem(Origin
                                        .System
                                        .newBuilder()
                                        .setDescription("Magic Group: " + MagicScopeGateway.ALL_ON_PREM_HOSTS)
                                        )
                                )
                .build());
    }

    @Test
    public void testMapOnPremHost() throws Exception {
        setupMapperAndBackend(TEMP_GROUP_ID);

        final ArgumentCaptor<GroupApiDTO> groupCreateRequestCaptor =
                ArgumentCaptor.forClass(GroupApiDTO.class);

        final String mappedUuid = gateway.enter(MagicScopeGateway.ALL_ON_PREM_HOSTS);

        verify(groupsMapper).toGroupDefinition(groupCreateRequestCaptor.capture());

        assertThat(mappedUuid, is(TEMP_GROUP_UUID));

        final GroupApiDTO groupRequest = groupCreateRequestCaptor.getValue();
        assertThat(groupRequest.getTemporary(), is(true));
        assertThat(groupRequest.getGroupType(), is(UIEntityType.PHYSICAL_MACHINE.apiStr()));
        assertThat(groupRequest.getScope(), contains(UuidMapper.UI_REAL_TIME_MARKET_STR));
        assertThat(groupRequest.getEnvironmentType(), is(EnvironmentType.ONPREM));
    }

    @Test
    public void testMapOnPremHostGroupExpiredTriggersRefresh() throws Exception {
        // Create the group (this assumes the "normal" test works")
        setupMapperAndBackend(TEMP_GROUP_ID);
        final String mappedUuid = gateway.enter(MagicScopeGateway.ALL_ON_PREM_HOSTS);
        assertThat(mappedUuid, is(TEMP_GROUP_UUID));

        // Next time the "createGroup" method gets called we return a different group.
        final long newTempGroupId = 1029;
        setupMapperAndBackend(newTempGroupId);

        // This request should trigger a RPC to the group service to make sure the temp group
        // still exists.
        final GroupID tempGroupId = GroupID.newBuilder()
                .setId(TEMP_GROUP_ID)
                .build();
        // Return a group
        doReturn(GetGroupResponse.newBuilder()
            .setGroup(Grouping.getDefaultInstance())
            .build()).when(groupBackendMole).getGroup(tempGroupId);

        final String mappedUuid2 = gateway.enter(MagicScopeGateway.ALL_ON_PREM_HOSTS);
        verify(groupBackendMole).getGroup(tempGroupId);
        // Should still map to the original UUID
        assertThat(mappedUuid2, is(TEMP_GROUP_UUID));

        // Now pretend the temporary group expired - RPC will return nothing.
        doReturn(GetGroupResponse.getDefaultInstance())
            .when(groupBackendMole).getGroup(tempGroupId);

        final String mappedUuid3 = gateway.enter(MagicScopeGateway.ALL_ON_PREM_HOSTS);
        verify(groupBackendMole, times(2)).getGroup(tempGroupId);
        assertThat(mappedUuid3, is(Long.toString(newTempGroupId)));
    }

    @Test
    public void testMapOnPremHostNewTopologyTriggersRefresh() throws Exception {
        // Create the group (this assumes the "normal" test works")
        setupMapperAndBackend(TEMP_GROUP_ID);
        final String mappedUuid = gateway.enter(MagicScopeGateway.ALL_ON_PREM_HOSTS);
        assertThat(mappedUuid, is(TEMP_GROUP_UUID));

        // Next call to create a group will return a different group.
        final long newTempGroupId = 1029;
        setupMapperAndBackend(newTempGroupId);

        // This should "clear" the cached temp group.
        gateway.onSourceTopologyAvailable(1L, REALTIME_CONTEXT);

        final String mappedUuid2 = gateway.enter(MagicScopeGateway.ALL_ON_PREM_HOSTS);
        assertThat(mappedUuid2, is(Long.toString(newTempGroupId)));
    }

    @Test
    public void testMapOnPremHostNonRealtimeTopologyIgnored() throws Exception {
        // Create the group (this assumes the "normal" test works")
        setupMapperAndBackend(TEMP_GROUP_ID);
        final String mappedUuid = gateway.enter(MagicScopeGateway.ALL_ON_PREM_HOSTS);
        assertThat(mappedUuid, is(TEMP_GROUP_UUID));

        // Next call to create a group will return a different group.
        final long newTempGroupId = 1029;
        setupMapperAndBackend(newTempGroupId);

        // Return a group for the cache check.
        doReturn(GetGroupResponse.newBuilder()
                .setGroup(Grouping.getDefaultInstance())
                .build())
            .when(groupBackendMole).getGroup(GroupID.newBuilder()
                .setId(TEMP_GROUP_ID)
                .build());

        // This should NOT "clear" the cached temp group, because the context is not realtime.
        gateway.onSourceTopologyAvailable(1L, REALTIME_CONTEXT + 1);

        final String mappedUuid2 = gateway.enter(MagicScopeGateway.ALL_ON_PREM_HOSTS);
        assertThat(mappedUuid2, is(TEMP_GROUP_UUID));
    }


}
