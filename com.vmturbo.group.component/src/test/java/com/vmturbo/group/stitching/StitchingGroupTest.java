package com.vmturbo.group.stitching;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;

import java.util.Arrays;
import java.util.stream.Collectors;

import org.junit.Test;

import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers.StaticMembersByType;
import com.vmturbo.common.protobuf.topology.ApiEntityType;

/**
 * Unit tests for {@link StitchingGroup}.
 */
public class StitchingGroupTest {

    private static final long ID = 7L;

    private static final String SRC_ID = "7";

    /**
     * Test merging two identical copies of the same group discovered by the same target.
     */
    @Test
    public void testMergeMultipleIdenticalGroupsSameTarget() {
        final long targetId = 13;
        StitchingGroup stitchingGroup = new StitchingGroup(ID, newGroup(1L, 2L), SRC_ID, targetId, true, null);
        stitchingGroup.mergedGroup(newGroup(1L, 2L), targetId);
        assertThat(stitchingGroup.buildGroupDefinition().getStaticGroupMembers().getMembersByTypeList().stream()
            .flatMap(m -> m.getMembersList().stream())
            .collect(Collectors.toSet()), containsInAnyOrder(1L, 2L));
    }

    /**
     * Test merging two DIFFERENT VERSIONS of the same group discovered by the same target.
     * We should just keep the first.
     */
    @Test
    public void testMergeDifferentGroupsSameTarget() {
        final long targetId = 13;
        StitchingGroup stitchingGroup = new StitchingGroup(ID, newGroup(1L, 2L), SRC_ID, targetId, true, null);
        stitchingGroup.mergedGroup(newGroup(3L, 4L), targetId);
        assertThat(stitchingGroup.buildGroupDefinition().getStaticGroupMembers().getMembersByTypeList().stream()
            .flatMap(m -> m.getMembersList().stream())
            // The new group members shouldn't be added - we just keep the first.
            .collect(Collectors.toSet()), containsInAnyOrder(1L, 2L));
    }

    /**
     * Test merging groups discovered by different targets.
     * We should combine the members.
     */
    @Test
    public void testMergeGroupsAcrossTargets() {
        final long targetId = 13;
        StitchingGroup stitchingGroup = new StitchingGroup(ID, newGroup(1L, 2L), SRC_ID, targetId, true, null);
        stitchingGroup.mergedGroup(newGroup(3L, 4L), targetId + 1);
        assertThat(stitchingGroup.buildGroupDefinition().getStaticGroupMembers().getMembersByTypeList().stream()
            .flatMap(m -> m.getMembersList().stream())
            // The stitched group should contain all members.
            .collect(Collectors.toSet()), containsInAnyOrder(1L, 2L, 3L, 4L));
    }

    private GroupDefinition newGroup(final Long... members) {
        return GroupDefinition.newBuilder()
            .setStaticGroupMembers(StaticMembers.newBuilder()
                .addMembersByType(StaticMembersByType.newBuilder()
                    .setType(MemberType.newBuilder()
                        .setEntity(ApiEntityType.VIRTUAL_MACHINE.typeNumber()))
                    .addAllMembers(Arrays.asList(members))))
            .build();
    }
}
