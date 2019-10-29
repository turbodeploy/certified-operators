package com.vmturbo.api.component.external.api.util;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.Set;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse.Members;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.EntityFilters;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.EntityFilters.EntityFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupID;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTO.SearchParametersCollection;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;

public class GroupExpanderTest {

    // the class under test
    private GroupExpander groupExpander;

    private GroupServiceMole groupServiceSpy = spy(new GroupServiceMole());

    @Rule
    public GrpcTestServer testServer = GrpcTestServer.newServer(groupServiceSpy);

    @Before
    public void setup() {
        GroupServiceBlockingStub groupServiceRpc = GroupServiceGrpc.newBlockingStub(testServer.getChannel());
        groupExpander = new GroupExpander(groupServiceRpc);
    }

    @Test
    public void testGetGroup() {
        when(groupServiceSpy.getGroup(GroupID.newBuilder().setId(123).build()))
            .thenReturn(GetGroupResponse.newBuilder()
                .setGroup(Grouping.getDefaultInstance())
                .build());
        Optional<Grouping> ret = groupExpander.getGroup("123");
        assertThat(ret.get(), is(Grouping.getDefaultInstance()));
    }

    @Test
    public void testGetGroupNotFound() {
        when(groupServiceSpy.getGroup(GroupID.newBuilder().setId(123).build()))
                .thenReturn(GetGroupResponse.getDefaultInstance());
        Optional<Grouping> ret = groupExpander.getGroup("123");
        assertFalse(ret.isPresent());
    }

    @Test
    public void testGetGroupNotNumeric() {
        Optional<Grouping> ret = groupExpander.getGroup("foo");
        assertFalse(ret.isPresent());
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
            .setExpectPresent(false)
            .build()))
            .thenReturn(GetMembersResponse.getDefaultInstance());
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
        doReturn(GetGroupResponse.newBuilder()
            .setGroup(Grouping.newBuilder()
                .setId(1234)
                .setDefinition(GroupDefinition.newBuilder()
                        .setType(GroupType.REGULAR)
                        .setDisplayName("foo")
                        .setEntityFilters(EntityFilters.newBuilder()
                                        .addEntityFilter(EntityFilter.newBuilder()
                                            .setSearchParametersCollection(
                                                            SearchParametersCollection.getDefaultInstance()
                                                            )
                                            )
                                )
            .build())).build()).when(groupServiceSpy).getGroup(GroupID.newBuilder().setId(1234).build());

        doReturn(GetMembersResponse.newBuilder().setMembers(Members.newBuilder()
            .addIds(10)
            .addIds(11)
            .addIds(12))
            .build()).when(groupServiceSpy).getMembers(GroupDTO.GetMembersRequest.newBuilder()
                .setId(1234L)
                .setExpectPresent(true)
                .build());

        Set<Long> expandedOids = groupExpander.expandUuid("1234");
        assertThat(expandedOids.size(), equalTo(3));
        assertThat(expandedOids, containsInAnyOrder(10L, 11L, 12L));
    }

    /**
     * Test a gRPC error requesting the group expansion - other than the
     * NOT_FOUND which is expected and handled.
     *
     * @throws Exception due to simulated grpc error
     */
    @Test(expected = StatusRuntimeException.class)
    public void testErrorInGroupGrpcCall() throws Exception {
        doReturn(Optional.of(Status.ABORTED.asException())).when(groupServiceSpy).getGroupError(any());
        Set<Long> expandedOids = groupExpander.expandUuid("1234");
        assertThat(expandedOids.size(), equalTo(1));
        assertThat(expandedOids.iterator().next(), equalTo(1234L));
    }
}