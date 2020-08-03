package com.vmturbo.api.component.external.api.util;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetOwnersRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetOwnersResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.EntityFilters;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.EntityFilters.EntityFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupID;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTO.SearchParametersCollection;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.group.api.GroupMemberRetriever;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;

public class GroupExpanderTest {

    // the class under test
    private GroupExpander groupExpander;

    private GroupServiceMole groupServiceSpy = spy(new GroupServiceMole());

    private static final Long RG_OID = 1111L;
    private static final Grouping RG_GROUP = Grouping.newBuilder()
        .setId(RG_OID)
        .setDefinition(GroupDefinition.newBuilder()
            .setType(GroupType.RESOURCE)
            .setDisplayName("rg1")
            .setStaticGroupMembers(GroupDTO.StaticMembers.newBuilder()
                .addMembersByType(GroupDTO.StaticMembers.StaticMembersByType.newBuilder()
                    .setType(GroupDTO.MemberType.newBuilder()
                        .setEntity(ApiEntityType.VIRTUAL_MACHINE.typeNumber()).build())
                    .addMembers(10L)
                    .addMembers(11L)
                    .build())
                .addMembersByType(GroupDTO.StaticMembers.StaticMembersByType.newBuilder()
                    .setType(GroupDTO.MemberType.newBuilder()
                        .setEntity(ApiEntityType.DATABASE.typeNumber()).build())
                    .addMembers(20L)
                    .build())
                .build())
            .build())
        .build();

    private static final Long DYNAMIC_GROUP_OID = 1234L;
    private static final Grouping DYNAMIC_GROUP = Grouping.newBuilder()
        .setId(DYNAMIC_GROUP_OID)
        .setDefinition(GroupDefinition.newBuilder()
            .setType(GroupType.REGULAR)
            .setDisplayName("foo")
            .setEntityFilters(EntityFilters.newBuilder()
                .addEntityFilter(EntityFilter.newBuilder()
                    .setEntityType(ApiEntityType.VIRTUAL_MACHINE.typeNumber())
                    .setSearchParametersCollection(
                        SearchParametersCollection.getDefaultInstance()
                    )
                )
            )
            .build())
        .build();

    private static final Long NESTED_GROUP_OID = 2342L;
    private static final Grouping NESTED_GROUP = Grouping.newBuilder()
        .setId(NESTED_GROUP_OID)
        .addExpectedTypes(GroupDTO.MemberType.newBuilder()
            .setGroup(GroupType.RESOURCE).build())
        .setDefinition(GroupDefinition.newBuilder()
            .setType(GroupType.REGULAR)
            .setDisplayName("nested group")
            .setStaticGroupMembers(GroupDTO.StaticMembers.newBuilder()
                .addMembersByType(GroupDTO.StaticMembers.StaticMembersByType.newBuilder()
                    .setType(GroupDTO.MemberType.newBuilder()
                        .setGroup(GroupType.RESOURCE).build())
                    .addMembers(1111L)
                    .build())
                .addMembersByType(GroupDTO.StaticMembers.StaticMembersByType.newBuilder()
                    .setType(GroupDTO.MemberType.newBuilder()
                        .setGroup(GroupType.REGULAR).build())
                    .addMembers(1234L)
                    .build())
                .build())
            .build())
        .build();

    @Rule
    public GrpcTestServer testServer = GrpcTestServer.newServer(groupServiceSpy);

    @Before
    public void setup() {
        GroupServiceBlockingStub groupServiceRpc = GroupServiceGrpc.newBlockingStub(testServer.getChannel());
        groupExpander = new GroupExpander(groupServiceRpc,
                new GroupMemberRetriever(groupServiceRpc));
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
            .addId(1234L)
            .setExpectPresent(false)
            .build()))
            .thenReturn(Collections.singletonList(GetMembersResponse.getDefaultInstance()));
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
        doReturn(GetGroupResponse.newBuilder().setGroup(DYNAMIC_GROUP).build())
            .when(groupServiceSpy).getGroup(GroupID.newBuilder().setId(DYNAMIC_GROUP_OID).build());
        Mockito.when(groupServiceSpy.getMembers(GroupDTO.GetMembersRequest.newBuilder()
                .addId(DYNAMIC_GROUP_OID)
                .setExpectPresent(true)
                .build()))
                .thenReturn(Collections.singletonList(GetMembersResponse.newBuilder()
                        .setGroupId(DYNAMIC_GROUP_OID)
                        .addMemberId(10)
                        .addMemberId(11)
                        .addMemberId(12)
                        .build()));

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

    /**
     * Tests the case where we call expand uuid to types to entities map on an
     * empty group.
     */
    @Test
    public void testExpandUuidToTypeToEntitiesMapForAnEmptyGroup() {
        // ARRANGE
        final Grouping emptyGroup = Grouping.newBuilder()
            .setId(1234L)
            .setDefinition(GroupDefinition.newBuilder()
                .setType(GroupType.REGULAR)
                .setDisplayName("group1")
                .setStaticGroupMembers(GroupDTO.StaticMembers.newBuilder()
                    .addMembersByType(GroupDTO.StaticMembers.StaticMembersByType.newBuilder()
                        .setType(GroupDTO.MemberType.newBuilder()
                            .setEntity(ApiEntityType.VIRTUAL_MACHINE.typeNumber()).build())
                        .build())
                    .build())
                .build())
            .build();

        doReturn(GetGroupResponse.newBuilder()
            .setGroup(emptyGroup).build())
            .when(groupServiceSpy).getGroup(GroupID.newBuilder().setId(1234L).build());

        // ACT
        Map<ApiEntityType, Set<Long>> result = groupExpander.expandUuidToTypeToEntitiesMap(1234L);

        //ASSERT
        assertThat(result.size(), equalTo(0));
    }

    /**
     * Tests the case where we call expand uuid to types to entities map on a
     * resource group with virtual machine and databases.
     */
    @Test
    public void testExpandUuidToTypeToEntitiesMapForAResourceGroup() {
        // ARRANGE
        doReturn(GetGroupResponse.newBuilder()
                .setGroup(RG_GROUP).build())
            .when(groupServiceSpy).getGroup(GroupID.newBuilder().setId(RG_OID).build());

        // ACT
        Map<ApiEntityType, Set<Long>> result = groupExpander.expandUuidToTypeToEntitiesMap(RG_OID);

        //ASSERT
        assertThat(result.size(), equalTo(2));
        assertThat(result.get(ApiEntityType.VIRTUAL_MACHINE), equalTo(ImmutableSet.of(10L, 11L)));
        assertThat(result.get(ApiEntityType.DATABASE), equalTo(Collections.singleton(20L)));
    }

    /**
     * Tests the case where we call expand uuid to types to entities map on a
     * dynamic group of virtual machines.
     */
    @Test
    public void testExpandUuidToTypeToEntitiesMapForADynamicGroup() {
        // ARRANGE
        doReturn(GetGroupResponse.newBuilder().setGroup(DYNAMIC_GROUP).build())
            .when(groupServiceSpy).getGroup(GroupID.newBuilder().setId(DYNAMIC_GROUP_OID).build());

        Mockito.when(groupServiceSpy.getMembers(GroupDTO.GetMembersRequest.newBuilder()
                .addId(DYNAMIC_GROUP_OID)
                .setExpectPresent(true)
                .build()))
                .thenReturn(Collections.singletonList(GetMembersResponse.newBuilder()
                        .setGroupId(DYNAMIC_GROUP_OID)
                        .addMemberId(10)
                        .addMemberId(11)
                        .addMemberId(12)
                        .build()));

        // ACT
        Map<ApiEntityType, Set<Long>> result = groupExpander.expandUuidToTypeToEntitiesMap(DYNAMIC_GROUP_OID);

        //ASSERT
        assertThat(result.size(), equalTo(1));
        assertThat(result.get(ApiEntityType.VIRTUAL_MACHINE), equalTo(ImmutableSet.of(10L, 11L,
            12L)));
    }

    /**
     * Tests the case where we call expand uuid to types to entities map on a
     * nested group.
     */
    @Test
    public void testExpandUuidToTypeToEntitiesMapForANestedGroup() {
        // ARRANGE
        doReturn(GetGroupResponse.newBuilder()
            .setGroup(NESTED_GROUP).build())
            .when(groupServiceSpy).getGroup(GroupID.newBuilder().setId(NESTED_GROUP_OID).build());

        Mockito.when(groupServiceSpy.getMembers(GroupDTO.GetMembersRequest.newBuilder()
                .addId(NESTED_GROUP_OID)
                .setExpandNestedGroups(true)
                .setExpectPresent(true)
                .build()))
                .thenReturn(Collections.singletonList(GetMembersResponse.newBuilder()
                        .setGroupId(NESTED_GROUP_OID)
                        .addMemberId(10)
                        .addMemberId(11)
                        .addMemberId(12)
                        .addMemberId(20)
                        .build()));

        doReturn(GetGroupResponse.newBuilder()
            .setGroup(RG_GROUP).build())
            .when(groupServiceSpy).getGroup(GroupID.newBuilder().setId(RG_OID).build());

        doReturn(GetGroupResponse.newBuilder().setGroup(DYNAMIC_GROUP).build())
            .when(groupServiceSpy).getGroup(GroupID.newBuilder().setId(DYNAMIC_GROUP_OID).build());

        Mockito.when(groupServiceSpy.getMembers(GroupDTO.GetMembersRequest.newBuilder()
                .addId(DYNAMIC_GROUP_OID)
                .setExpectPresent(true)
                .build()))
                .thenReturn(Collections.singletonList(GetMembersResponse.newBuilder()
                        .setGroupId(DYNAMIC_GROUP_OID)
                        .addMemberId(10)
                        .addMemberId(11)
                        .addMemberId(12)
                        .build()));
        // ACT
        Map<ApiEntityType, Set<Long>> result = groupExpander.expandUuidToTypeToEntitiesMap(NESTED_GROUP_OID);

        //ASSERT
        assertThat(result.size(), equalTo(2));
        assertThat(result.get(ApiEntityType.VIRTUAL_MACHINE), equalTo(ImmutableSet.of(10L, 11L,
            12L)));
        assertThat(result.get(ApiEntityType.DATABASE), equalTo(Collections.singleton(20L)));
    }

    /**
     * Tests getting owners of requested groups.
     */
    @Test
    public void testGetGroupOwners() {
        final Set<Long> ownersIds = Sets.newHashSet(11L);
        final long groupId = 1L;
        final GroupType groupType = GroupType.RESOURCE;
        doReturn(GetOwnersResponse.newBuilder().addAllOwnerId(ownersIds).build()).when(groupServiceSpy)
                .getOwnersOfGroups(GetOwnersRequest.newBuilder()
                        .addGroupId(groupId)
                        .setGroupType(groupType)
                        .build());
        assertEquals(ownersIds,
                groupExpander.getGroupOwners(Collections.singletonList(groupId), groupType));
    }
}
