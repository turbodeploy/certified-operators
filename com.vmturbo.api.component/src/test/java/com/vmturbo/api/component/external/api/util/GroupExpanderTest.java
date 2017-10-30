package com.vmturbo.api.component.external.api.util;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.*;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import com.vmturbo.api.component.external.api.service.GroupsService;
import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupID;
import com.vmturbo.common.protobuf.group.GroupDTOMoles;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.components.api.test.GrpcTestServer;

public class GroupExpanderTest {
    GroupExpander groupExpander;

    private GroupDTOMoles.GroupServiceMole groupServiceSpy = spy(new GroupDTOMoles.GroupServiceMole());
    @Rule
    public GrpcTestServer testServer = GrpcTestServer.newServer(groupServiceSpy);

    @Before
    public void setup() {
        GroupServiceBlockingStub groupServiceRpc = GroupServiceGrpc.newBlockingStub(testServer.getChannel());
        groupExpander = new GroupExpander(groupServiceRpc);
    }

    /**
     * Test expanding a single ID that is neither "Market" nor a group UUid.
     * The expected result simply contains the input UUID as a long.
     *
     * @throws Exception not expected
     */
    @Test
    public void testExpandNonGroupNonMarketUuid() throws Exception {
        when(groupServiceSpy.getMembers(GroupDTO.GetMembersRequest.newBuilder()
                .setId(1234L)
                .build()))
                .thenThrow(new StatusRuntimeException(Status.ABORTED));
        Set<Long> expandedOids = groupExpander.expandUuid("1234");
        assertThat(expandedOids.size(), equalTo(1));
        assertThat(expandedOids.iterator().next(), equalTo(1234L));
    }

    /**
     * Test expanding a single ID that is "Market", i.e. the live market synonym.
     * The expected result is an empty list.
     *
     * @throws Exception not expected
     */
    @Test
    public void testExpandMarketUuid() throws Exception {
        Set<Long> expandedOids = groupExpander.expandUuid("Market");
        assertThat(expandedOids.size(), equalTo(0));
    }


    /**
     * Test expanding a single ID that is a group, i.e. has members.
     * The expected result is a list of the member OIDs.
     *
     * @throws Exception not expected
     */
    @Test
    public void testExpandGroupUuid() throws Exception {
        when(groupServiceSpy.getMembers(GroupDTO.GetMembersRequest.newBuilder()
                .setId(1234L)
                .build()))
                .thenReturn(GroupDTO.GetMembersResponse.newBuilder()
                        .addMemberId(10)
                        .addMemberId(11)
                        .addMemberId(12)
                        .build());

        Set<Long> expandedOids = groupExpander.expandUuid("1234");
        assertThat(expandedOids.size(), equalTo(3));
        assertThat(expandedOids, containsInAnyOrder(10L, 11L, 12L));
    }



}