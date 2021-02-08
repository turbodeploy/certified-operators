package com.vmturbo.api.component.external.api.util;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.api.component.ApiTestUtils;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupAndImmediateMembersResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetOwnersRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetOwnersResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.EntityFilters;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.EntityFilters.EntityFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupID;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTO.SearchParametersCollection;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.group.api.GroupAndMembers;
import com.vmturbo.group.api.GroupMemberRetriever;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;

public class GroupExpanderTest {

    // the class under test
    private GroupExpander groupExpander;

    private UuidMapper uuidMapper = mock(UuidMapper.class);

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
        groupExpander.setUuidMapper(uuidMapper);
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
     * Test that the getGroups method ignores non-numeric group ids and retrieves groups properly.
     */
    @Test
    public void testGetGroups() {
        when(groupServiceSpy.getGroups(
            GetGroupsRequest.newBuilder()
                .setGroupFilter(GroupFilter.getDefaultInstance())
                .addScopes(1234)
                .build()))
            .thenReturn(Collections.singletonList(Grouping.newBuilder().setId(1234).build()));
        Set<Grouping> result = groupExpander.getGroups(Arrays.asList("1234", "abcd"));
        assertEquals(1, result.size());
        assertEquals(1234, result.iterator().next().getId());
    }

    /**
     * Test expanding a single ID that is neither "Market" nor a group UUid.
     * The expected result simply contains the input UUID as a long.
     *
     * @throws Exception not expected
     */
    @Test
    public void testExpandNonGroupNonMarketUuid() throws Exception {
        final ApiId id = ApiTestUtils.mockEntityId("1234", uuidMapper);
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
        final ApiId id = ApiTestUtils.mockRealtimeId("Market", 77777, uuidMapper);
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
        final ApiId id = ApiTestUtils.mockGroupId("1234", uuidMapper);
        when(id.getScopeOids()).thenReturn(Sets.newHashSet(10L, 11L, 12L));

        Set<Long> expandedOids = groupExpander.expandUuid("1234");
        assertThat(expandedOids.size(), equalTo(3));
        assertThat(expandedOids, containsInAnyOrder(10L, 11L, 12L));
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

    /**
     * Tests uuids that affect early exit of code.
     */
    @Test
    public void testGetGroupWithImmediateMembersOnlyEarlyExits() {
        assertFalse(groupExpander.getGroupWithImmediateMembersOnly(DefaultCloudGroupProducer.ALL_CLOULD_WORKLOAD_AWS_AND_AZURE_UUID).isPresent());
        assertFalse(groupExpander.getGroupWithImmediateMembersOnly(DefaultCloudGroupProducer.ALL_CLOUD_VM_UUID).isPresent());
        GroupExpander.GLOBAL_SCOPE_SUPPLY_CHAIN.stream().forEach(uuid ->
                        assertFalse(groupExpander.getGroupWithImmediateMembersOnly(uuid).isPresent()));
        assertFalse(groupExpander.getGroupWithImmediateMembersOnly("nonNumeric").isPresent());
    }

    /**
     * Expect empty optional result if group not found.
     */
    @Test
    public void testGetGroupWithImmediateMembersOnlyGroupNotFound() {
        //GIVEN
        String groupID = "123";

        //WHEN
        Optional<GroupAndMembers> response = groupExpander.getGroupWithImmediateMembersOnly(groupID);

        //THEN
        assertFalse(response.isPresent());
    }

    /**
     * Test calling to get group, members and the response.
     */
    @Test
    public void testGetGroupWithImmediateMembersOnly() {
        //GIVEN
        String groupID = "123";
        List<Long> members = Arrays.asList(1L, 2L, 3L, 4L);
        GetGroupAndImmediateMembersResponse rpcResponse = GetGroupAndImmediateMembersResponse.newBuilder()
                        .setGroup(Grouping.newBuilder().setId(Long.parseLong(groupID)))
                        .addAllImmediateMembers(members)
                        .build();
        doReturn(rpcResponse).when(groupServiceSpy).getGroupAndImmediateMembers(Mockito.any());

        //WHEN
        Optional<GroupAndMembers> response = groupExpander.getGroupWithImmediateMembersOnly(groupID);

        //THEN
        GroupAndMembers groupAndMembers = response.get();

        assertEquals(groupAndMembers.group(), rpcResponse.getGroup());
        assertEquals(groupAndMembers.members(), rpcResponse.getImmediateMembersList());
        assertEquals(groupAndMembers.entities(), rpcResponse.getImmediateMembersList());
    }



}
