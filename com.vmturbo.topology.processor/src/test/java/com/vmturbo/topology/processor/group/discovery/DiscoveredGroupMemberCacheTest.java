package com.vmturbo.topology.processor.group.discovery;

import static junit.framework.TestCase.assertFalse;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;

import org.junit.Test;

import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.topology.StitchingErrors;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.StitchingMergeInformation;
import com.vmturbo.topology.processor.group.discovery.DiscoveredGroupMemberCache.DiscoveredGroupMembers;
import com.vmturbo.topology.processor.util.GroupTestUtils;

public class DiscoveredGroupMemberCacheTest {

    private static final long TARGET_ID = 1L;

    @Test
    public void testHasMemberGroup() {
        final DiscoveredGroupMembers groupMembers = new DiscoveredGroupMembers(
            DiscoveredGroupConstants.PLACEHOLDER_INTERPRETED_GROUP);

        assertTrue(groupMembers.hasMember(DiscoveredGroupConstants.PLACEHOLDER_GROUP_MEMBER));
        assertFalse(groupMembers.hasMember(2L));
    }

    @Test
    public void testHasMemberCluster() {
        final DiscoveredGroupMembers groupMembers = new DiscoveredGroupMembers(
            DiscoveredGroupConstants.PLACEHOLDER_INTERPRETED_CLUSTER);

        assertTrue(groupMembers.hasMember(DiscoveredGroupConstants.PLACEHOLDER_CLUSTER_MEMBER));
        assertFalse(groupMembers.hasMember(2L));
    }

    @Test
    public void testSwap() {
        final DiscoveredGroupMembers groupMembers = new DiscoveredGroupMembers(
            DiscoveredGroupConstants.PLACEHOLDER_INTERPRETED_GROUP);

        assertTrue(groupMembers.hasMember(DiscoveredGroupConstants.PLACEHOLDER_GROUP_MEMBER));
        assertFalse(groupMembers.hasMember(2L));

        groupMembers.swapMember(DiscoveredGroupConstants.PLACEHOLDER_GROUP_MEMBER, 2L);

        assertFalse(groupMembers.hasMember(DiscoveredGroupConstants.PLACEHOLDER_GROUP_MEMBER));
        assertTrue(groupMembers.hasMember(2L));
    }

    @Test
    public void testSwapAlreadyContained() {
        final GroupDefinition.Builder groupDef = GroupTestUtils.createStaticGroupDef("name",
                EntityType.VIRTUAL_MACHINE_VALUE, Arrays.asList(1L, 2L, 3L));

        final InterpretedGroup interpretedGroup = new InterpretedGroup(
                DiscoveredGroupConstants.STATIC_MEMBER_DTO, Optional.of(groupDef));

        final DiscoveredGroupMembers groupMembers = new DiscoveredGroupMembers(interpretedGroup);

        assertTrue(groupMembers.hasMember(1L));
        assertTrue(groupMembers.hasMember(2L));
        assertTrue(groupMembers.hasMember(3L));

        groupMembers.swapMember(3L, 1L);
        assertTrue(groupMembers.hasMember(1L));
        assertTrue(groupMembers.hasMember(2L));
        assertFalse(groupMembers.hasMember(3L));

        groupMembers.swapMember(2L, 1L);
        assertTrue(groupMembers.hasMember(1L));
        assertFalse(groupMembers.hasMember(2L));
        assertFalse(groupMembers.hasMember(3L));

        groupMembers.swapMember(1L, 1L);
        assertTrue(groupMembers.hasMember(1L));
        assertFalse(groupMembers.hasMember(2L));
        assertFalse(groupMembers.hasMember(3L));
    }

    @Test
    public void testGroupsContaining() {
        final Map<Long, List<InterpretedGroup>> groups = ImmutableMap.of(
            123L, Collections.singletonList(DiscoveredGroupConstants.PLACEHOLDER_INTERPRETED_GROUP));

        final DiscoveredGroupMemberCache cache = new DiscoveredGroupMemberCache(groups);
        final List<InterpretedGroup> members = cache
            .groupsContaining(new StitchingMergeInformation(DiscoveredGroupConstants.PLACEHOLDER_GROUP_MEMBER, 123L, StitchingErrors.none()))
            .map(DiscoveredGroupMembers::getAssociatedGroup)
            .collect(Collectors.toList());

        assertThat(members, contains(DiscoveredGroupConstants.PLACEHOLDER_INTERPRETED_GROUP));
    }

    @Test
    public void testGroupsContainingMultiple() {
        final GroupDefinition.Builder groupDef = GroupTestUtils.createStaticGroupDef("name",
                EntityType.VIRTUAL_MACHINE_VALUE, Arrays.asList(1L, 2L, 3L,
                        DiscoveredGroupConstants.PLACEHOLDER_CLUSTER_MEMBER));
        final InterpretedGroup multiMemberGroup = new InterpretedGroup(
                DiscoveredGroupConstants.STATIC_MEMBER_DTO, Optional.of(groupDef));

        final Map<Long, List<InterpretedGroup>> groups = ImmutableMap.of(
            456L, Arrays.asList(multiMemberGroup, DiscoveredGroupConstants.PLACEHOLDER_INTERPRETED_CLUSTER));

        final DiscoveredGroupMemberCache cache = new DiscoveredGroupMemberCache(groups);
        final List<InterpretedGroup> members = cache
            .groupsContaining(new StitchingMergeInformation(DiscoveredGroupConstants.PLACEHOLDER_CLUSTER_MEMBER, 456L, StitchingErrors.none()))
            .map(DiscoveredGroupMembers::getAssociatedGroup)
            .collect(Collectors.toList());

        assertThat(members,
            containsInAnyOrder(DiscoveredGroupConstants.PLACEHOLDER_INTERPRETED_CLUSTER,
            multiMemberGroup));
    }
}