package com.vmturbo.topology.processor.stitching;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticGroupMembers;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.MembersList;
import com.vmturbo.stitching.StitchingMergeInformation;
import com.vmturbo.topology.processor.group.discovery.DiscoveredGroupMemberCache;
import com.vmturbo.topology.processor.group.discovery.InterpretedGroup;

public class StitchingGroupFixerTest {

    static final CommonDTO.GroupDTO groupDto = CommonDTO.GroupDTO.newBuilder()
        .setEntityType(EntityType.VIRTUAL_MACHINE)
        .setDisplayName("foo")
        .setGroupName("group")
        .setMemberList(MembersList.newBuilder()
            .addMember("1"))
        .build();

    final GroupInfo.Builder groupInfo = GroupInfo.newBuilder()
        .setStaticGroupMembers(StaticGroupMembers.newBuilder()
            .addAllStaticMemberOids(
                Arrays.asList(1L, 2L, 3L)));
    final ClusterInfo.Builder clusterInfo = ClusterInfo.newBuilder()
        .setMembers(StaticGroupMembers.newBuilder()
            .addAllStaticMemberOids(
                Arrays.asList(4L, 5L)));

    private static final long GROUP_TARGET_ID = 111L;
    private static final long CLUSTER_TARGET_ID = 222L;
    private static final long NO_DISCOVERED_GROUP_TARGET_ID = 333L;
    private static final long UPDATED_ENTITY_OID = 7293729L;

    final InterpretedGroup group = new InterpretedGroup(groupDto, Optional.of(groupInfo), Optional.empty());
    final InterpretedGroup cluster = new InterpretedGroup(groupDto, Optional.empty(), Optional.of(clusterInfo));
    final TopologyStitchingGraph graph = mock(TopologyStitchingGraph.class);
    final TopologyStitchingEntity entity = mock(TopologyStitchingEntity.class);

    final DiscoveredGroupMemberCache groupMemberCache = new DiscoveredGroupMemberCache(
        ImmutableMap.of(
            GROUP_TARGET_ID, Collections.singletonList(group),
            CLUSTER_TARGET_ID, Collections.singletonList(cluster),
            NO_DISCOVERED_GROUP_TARGET_ID, Collections.emptyList()
        )
    );

    final StitchingGroupFixer groupFixer = new StitchingGroupFixer();

    @Before
    public void setup() {
        when(graph.entities()).thenReturn(Stream.of(entity));

        when(entity.getOid()).thenReturn(UPDATED_ENTITY_OID);
        when(entity.hasMergeInformation()).thenReturn(true);
    }

    @Test
    public void testFixupNotInGroups() {
        when(entity.getMergeInformation()).thenReturn(
            Arrays.asList(
                new StitchingMergeInformation(1L, NO_DISCOVERED_GROUP_TARGET_ID),
                new StitchingMergeInformation(1L, 983921L)));

        groupFixer.fixupGroups(graph, groupMemberCache);
        assertThat(group.getDtoAsGroup().get().getStaticGroupMembers().getStaticMemberOidsList(),
            contains(1L, 2L, 3L));
        assertThat(cluster.getDtoAsCluster().get().getMembers().getStaticMemberOidsList(),
            contains(4L, 5L));
    }

    @Test
    public void testFixupGroup() {
        when(entity.getMergeInformation()).thenReturn(
            Arrays.asList(
                new StitchingMergeInformation(1L, GROUP_TARGET_ID),
                new StitchingMergeInformation(3L, GROUP_TARGET_ID)));

        groupFixer.fixupGroups(graph, groupMemberCache);
        assertThat(group.getDtoAsGroup().get().getStaticGroupMembers().getStaticMemberOidsList(),
            containsInAnyOrder(UPDATED_ENTITY_OID, 2L));
    }

    @Test
    public void testFixupCluster() {
        when(entity.getMergeInformation()).thenReturn(
            Collections.singletonList(new StitchingMergeInformation(4L, CLUSTER_TARGET_ID)));

        groupFixer.fixupGroups(graph, groupMemberCache);
        assertThat(cluster.getDtoAsCluster().get().getMembers().getStaticMemberOidsList(),
            containsInAnyOrder(UPDATED_ENTITY_OID, 5L));
    }

    @Test
    public void testFixupMultiple() {
        when(entity.getMergeInformation()).thenReturn(
            Arrays.asList(
                new StitchingMergeInformation(3L, GROUP_TARGET_ID),
                new StitchingMergeInformation(5L, CLUSTER_TARGET_ID)));

        groupFixer.fixupGroups(graph, groupMemberCache);
        assertThat(group.getDtoAsGroup().get().getStaticGroupMembers().getStaticMemberOidsList(),
            containsInAnyOrder(1L, 2L, UPDATED_ENTITY_OID));
        assertThat(cluster.getDtoAsCluster().get().getMembers().getStaticMemberOidsList(),
            containsInAnyOrder(UPDATED_ENTITY_OID, 4L));
    }
}