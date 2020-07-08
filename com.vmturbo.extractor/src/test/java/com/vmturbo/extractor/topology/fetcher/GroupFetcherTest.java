package com.vmturbo.extractor.topology.fetcher;

import static com.vmturbo.extractor.util.GroupServiceTestUtil.groupList;
import static com.vmturbo.extractor.util.GroupServiceTestUtil.makeGroup;
import static com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType.BILLING_FAMILY;
import static com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType.COMPUTE_HOST_CLUSTER;
import static com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType.COMPUTE_VIRTUAL_MACHINE_CLUSTER;
import static com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType.REGULAR;
import static com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType.RESOURCE;
import static com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType.STORAGE_CLUSTER;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.utils.MultiStageTimer;
import com.vmturbo.extractor.topology.fetcher.GroupFetcher.GroupData;
import com.vmturbo.extractor.util.GroupServiceTestUtil;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Tests of the GroupFetcher class.
 */
public class GroupFetcherTest {

    private final GroupServiceMole groupMole = spy(GroupServiceMole.class);

    /** Rule to manage Grpc server. */
    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(groupMole);

    private GroupServiceBlockingStub groupService;

    /** set up mocked group service. */
    @Before
    public void before() {
        groupService = GroupServiceTestUtil.getGroupService(grpcServer);
    }

    /**
     * Check that group fetching works properly for groups with static members.
     *
     * <p>For each gropu we'll ensure that the members all end up linked to the group and vice-versa
     * in the group-to-leaves and leaf-to-groups maps created by the feature. In addition, we'll
     * verify that each member appears in the group-to-direct-members entry for its group.</p>
     */
    @Test
    public void testStaticGroupsAreProperlyFetched() {
        List<Grouping> groupsResponse = groupList(
                makeGroup(COMPUTE_HOST_CLUSTER, 100L)
                        .withStaticEntityMembers(EntityType.PHYSICAL_MACHINE, 1L, 2L, 3L),
                makeGroup(REGULAR, 200)
                        .withStaticEntityMembers(EntityType.PHYSICAL_MACHINE, 1L, 2L)
                        .withStaticEntityMembers(EntityType.VIRTUAL_MACHINE, 11L, 12L),
                makeGroup(BILLING_FAMILY, 300L)
                        .withStaticEntityMembers(EntityType.BUSINESS_ACCOUNT, 21L, 22L),
                makeGroup(RESOURCE, 400L)
                        .withStaticEntityMembers(EntityType.APPLICATION, 31L, 32L),
                makeGroup(STORAGE_CLUSTER, 500L)
                        .withStaticEntityMembers(EntityType.STORAGE, 41L, 42L),
                makeGroup(COMPUTE_VIRTUAL_MACHINE_CLUSTER, 600L)
                        .withStaticEntityMembers(EntityType.VIRTUAL_MACHINE, 51L, 52L)
        );
        doReturn(groupsResponse).when(groupMole).getGroups(any(GetGroupsRequest.class));
        final AtomicReference<GroupData> gd = new AtomicReference<>();
        new GroupFetcher(groupService, new MultiStageTimer(null), gd::set).fetchAndConsume();
        final GroupData groupData = gd.get();
        // perform leaf map checks for all groups
        // e.g. this first one says that 1, 2, and 3 are oids of members of group 100
        checkLeafEntries(groupData, 100L, 1L, 2L, 3L);
        checkLeafEntries(groupData, 200L, 1L, 2L, 11L, 12L);
        checkLeafEntries(groupData, 300L, 21L, 22L);
        checkLeafEntries(groupData, 400L, 31L, 32L);
        checkLeafEntries(groupData, 500L, 41L, 42L);
        checkLeafEntries(groupData, 600L, 51L, 52L);

        // perform direct-member checks for all groups
        // similar to above - 1, 2, and 3 are direct members of group 100
        checkDirectEntries(groupData, 100L, 1L, 2L, 3L);
        checkDirectEntries(groupData, 200L, 1L, 2L, 11L, 12L);
        checkDirectEntries(groupData, 300L, 21L, 22L);
        checkDirectEntries(groupData, 400L, 31L, 32L);
        checkDirectEntries(groupData, 500L, 41L, 42L);
        checkDirectEntries(groupData, 600L, 51L, 52L);

        // make sure we didn't have any unnoticed entries in any of the maps
        assertThat(groupData.getGroupToLeafEntityIds().size(), is(6));
        assertThat(groupData.getLeafEntityToGroups().size(), is(13));
        assertThat(groupData.getGroupToDirectMemberIds().size(), is(6));
    }

    private void checkLeafEntries(GroupData groupData, long group, Long... members) {
        // check that the group-to-leaves map has an entry for this group, and that the value
        // for that entry consists of the given member ids
        final Long2ObjectMap<List<Long>> g2l = groupData.getGroupToLeafEntityIds();
        assertThat(g2l.keySet(), hasItem(group));
        assertThat(g2l.get(group), containsInAnyOrder(members));
        // likewise, check that each member id appears in the leaf-to-groups map, and that this
        // group appears in the corresponding map entry
        final Long2ObjectMap<List<Grouping>> l2g = groupData.getLeafEntityToGroups();
        for (final Long leaf : members) {
            assertThat(l2g.keySet(), hasItem(leaf));
            final List<Long> groupsForLeaf =
                    l2g.get((long)leaf).stream().map(Grouping::getId).collect(Collectors.toList());
            assertThat(groupsForLeaf, hasItem(group));
        }
    }

    private void checkDirectEntries(GroupData groupData, long group, Long... members) {
        // check that the group-to-direct-members map has an ehtry for this group, and that the
        // value for that entry consists of the given member ids.
        final Long2ObjectMap<List<Long>> g2d = groupData.getGroupToDirectMemberIds();
        assertThat(g2d.keySet(), hasItem(group));
        assertThat(g2d.get(group), containsInAnyOrder(members));
    }
}
