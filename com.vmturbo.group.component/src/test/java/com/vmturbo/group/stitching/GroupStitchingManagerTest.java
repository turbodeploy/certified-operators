package com.vmturbo.group.stitching;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredGroupsPoliciesSettings.UploadedGroup;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;

/**
 * Test that {@link GroupStitchingManager} works as expected.
 */
public class GroupStitchingManagerTest {

    private GroupStitchingManager groupStitchingManager = spy(new GroupStitchingManager());

    private GroupStitchingContext groupStitchingContext;

    private static final long TARGET_1 = 111L;
    private static final long TARGET_2 = 112L;
    private static final long TARGET_3 = 113L;
    private static final String PROBE_TYPE = SDKProbeType.VCENTER.toString();
    private static final String GROUP_ID = "group-1";
    private static final long VM_ID_1 = 11;
    private static final long VM_ID_2 = 12;
    private static final long VM_ID_3 = 13;

    private static final UploadedGroup group1 = GroupTestUtils.createUploadedGroup(
            GroupType.REGULAR, GROUP_ID, ImmutableMap.of(
                    EntityType.VIRTUAL_MACHINE_VALUE, Sets.newHashSet(VM_ID_1)
            ));

    private static final UploadedGroup group2 = GroupTestUtils.createUploadedGroup(
            GroupType.REGULAR, GROUP_ID, ImmutableMap.of(
                    EntityType.VIRTUAL_MACHINE_VALUE, Sets.newHashSet(VM_ID_2)
            ));

    private static final UploadedGroup group3 = GroupTestUtils.createUploadedGroup(
            GroupType.COMPUTE_HOST_CLUSTER, GROUP_ID, ImmutableMap.of(
                    EntityType.VIRTUAL_MACHINE_VALUE, Sets.newHashSet(VM_ID_3)
            ));

    /**
     * Set up for each unit test.
     */
    @Before
    public void setup() {
        groupStitchingContext = spy(new GroupStitchingContext());
        groupStitchingContext.setTargetGroups(TARGET_1, PROBE_TYPE, ImmutableList.of(group1));
        groupStitchingContext.setTargetGroups(TARGET_2, PROBE_TYPE, ImmutableList.of(group2));
        groupStitchingContext.setTargetGroups(TARGET_3, PROBE_TYPE, ImmutableList.of(group3));
    }

    /**
     * Test that stitching operations are invoked.
     */
    @Test
    public void testStitchingInvoked() {
        groupStitchingManager.stitch(groupStitchingContext);
        verify(groupStitchingContext).getGroupsOfProbeTypesAndGroupType(any(), any());
        verify(groupStitchingManager).removeDuplicateGroups(groupStitchingContext);
    }

    /**
     * Test that duplicate groups are removed.
     */
    @Test
    public void testRemoveDuplicateGroups() {
        // before stitching, 3 groups with same id from 3 different targets
        assertEquals(3,  groupStitchingContext.getAllStitchingGroups().size());

        groupStitchingManager.stitch(groupStitchingContext);

        // after stitching, verify only two groups are preserved
        assertEquals(2, groupStitchingContext.getAllStitchingGroups().size());

        final Map<Long, StitchingGroup> groupByTarget =
                groupStitchingContext.getAllStitchingGroups().stream()
                        .collect(Collectors.toMap(StitchingGroup::getSourceTargetId, Function.identity()));
        // group in target3 is still preserved
        assertEquals(group3.getDefinition(), groupByTarget.get(TARGET_3).buildGroupDefinition());
        // only one group from target1 or target2 is preserved, their members are not merged
        if (groupByTarget.containsKey(TARGET_1)) {
            assertEquals(group1.getDefinition(), groupByTarget.get(TARGET_1).buildGroupDefinition());
        } else {
            assertEquals(group2.getDefinition(), groupByTarget.get(TARGET_2).buildGroupDefinition());
        }
    }
}
