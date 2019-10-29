package com.vmturbo.group.stitching;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredGroupsPoliciesSettings.UploadedGroup;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers.StaticMembersByType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;

/**
 * Test that {@link ResourceGroupStitchingOperation} works as expected.
 */
public class ResourceGroupStitchingOperationTest {

    private final ResourceGroupStitchingOperation operation = new ResourceGroupStitchingOperation();

    private GroupStitchingContext groupStitchingContext;

    private static final long AZURE_TARGET_ID = 111L;
    private static final long AZURE_STORAGE_BROWSING_TARGET_ID = 112L;
    private static final long APP_INSIGHTS_TARGET_ID = 113L;
    private static final long VC_TARGET_ID = 114L;

    private static final long VM_ID_1 = 11;
    private static final long VM_ID_2 = 12;
    private static final long DB_ID_1 = 21;
    private static final long DB_ID_2 = 22;
    private static final long VOLUME_ID_1 = 41;

    private static final String RESOURCE_GROUP_ID = "resource-group";
    private static final String REGULAR_GROUP_ID = "regular-group";

    private static UploadedGroup rg1 = GroupTestUtils.createUploadedGroup(GroupType.RESOURCE,
            RESOURCE_GROUP_ID, ImmutableMap.of(
                    EntityType.VIRTUAL_MACHINE_VALUE, Sets.newHashSet(VM_ID_1),
                    EntityType.DATABASE_VALUE, Sets.newHashSet(DB_ID_1)
            ));

    private static UploadedGroup rg2 = GroupTestUtils.createUploadedGroup(GroupType.RESOURCE,
            RESOURCE_GROUP_ID, ImmutableMap.of(
                    EntityType.VIRTUAL_VOLUME_VALUE, Sets.newHashSet(VOLUME_ID_1),
                    EntityType.DATABASE_VALUE, Sets.newHashSet(DB_ID_2)
            ));

    private static UploadedGroup rg3 = GroupTestUtils.createUploadedGroup(GroupType.RESOURCE,
            RESOURCE_GROUP_ID, ImmutableMap.of(
                    EntityType.DATABASE_VALUE, Sets.newHashSet(DB_ID_2)
            ));

    private static UploadedGroup regularGroup = GroupTestUtils.createUploadedGroup(
            GroupType.REGULAR, REGULAR_GROUP_ID, ImmutableMap.of(
                    EntityType.VIRTUAL_MACHINE_VALUE, Sets.newHashSet(VM_ID_2)
            ));

    /**
     * Set up for each unit test.
     */
    @Before
    public void setup() {
        groupStitchingContext = new GroupStitchingContext();
        groupStitchingContext.setTargetGroups(AZURE_TARGET_ID, SDKProbeType.AZURE.toString(),
                ImmutableList.of(rg1));
        groupStitchingContext.setTargetGroups(AZURE_STORAGE_BROWSING_TARGET_ID,
                SDKProbeType.AZURE_STORAGE_BROWSE.toString(), ImmutableList.of(rg2));
        groupStitchingContext.setTargetGroups(APP_INSIGHTS_TARGET_ID,
                SDKProbeType.APPINSIGHTS.toString(), ImmutableList.of(rg3));
        groupStitchingContext.setTargetGroups(VC_TARGET_ID, SDKProbeType.VCENTER.toString(),
                ImmutableList.of(regularGroup));
    }

    /**
     * Test that get scope returns correct scope groups.
     */
    @Test
    public void testGetScope() {
        Collection<StitchingGroup> scopeGroups = operation.getScope(groupStitchingContext);
        // verify that only 3 groups are returned
        assertEquals(3, scopeGroups.size());
        // they are all RG
        scopeGroups.forEach(group ->
                assertEquals(GroupType.RESOURCE, group.getGroupDefinition().getType()));
    }

    /**
     * Test that same resource groups from different targets are merged correctly.
     */
    @Test
    public void testMergeResourceGroups() {
        // stitch
        operation.stitch(operation.getScope(groupStitchingContext), groupStitchingContext);

        Map<GroupType, List<StitchingGroup>> groupsByType =
                groupStitchingContext.getAllStitchingGroups().stream()
                        .collect(Collectors.groupingBy(group -> group.getGroupDefinition().getType()));

        List<StitchingGroup> resourceGroups = groupsByType.get(GroupType.RESOURCE);

        // verify that only ONE RG is preserved
        assertEquals(1, resourceGroups.size());
        // verify that target ids are merged
        assertThat(resourceGroups.get(0).getAllTargetIds(), containsInAnyOrder(AZURE_TARGET_ID,
                AZURE_STORAGE_BROWSING_TARGET_ID, APP_INSIGHTS_TARGET_ID));

        // verify that member ids are merged
        Map<Integer, List<Long>> membersByType = resourceGroups.get(0).getGroupDefinition()
                .getStaticGroupMembers().getMembersByTypeList()
                .stream()
                .collect(Collectors.toMap(k -> k.getType().getEntity(),
                        StaticMembersByType::getMembersList));
        assertEquals(3, membersByType.size());
        assertThat(membersByType.get(EntityType.VIRTUAL_MACHINE_VALUE), containsInAnyOrder(VM_ID_1));
        assertThat(membersByType.get(EntityType.VIRTUAL_VOLUME_VALUE), containsInAnyOrder(VOLUME_ID_1));
        assertThat(membersByType.get(EntityType.DATABASE_VALUE), containsInAnyOrder(DB_ID_1, DB_ID_2));

        // verify that the regular group is still there
        assertEquals(1, groupsByType.get(GroupType.REGULAR).size());
        assertEquals(regularGroup.getDefinition(),
                groupsByType.get(GroupType.REGULAR).get(0).buildGroupDefinition());
    }

    /**
     * Test that resource groups are not affected if no same RGs from other targets.
     */
    @Test
    public void testResourceGroupNoStitching() {
        groupStitchingContext = new GroupStitchingContext();
        groupStitchingContext.setTargetGroups(AZURE_TARGET_ID, SDKProbeType.AZURE.toString(),
                ImmutableList.of(rg1));
        // stitch
        operation.stitch(operation.getScope(groupStitchingContext), groupStitchingContext);
        Collection<StitchingGroup> groups = groupStitchingContext.getAllStitchingGroups();
        // verify that RG is not modified
        assertEquals(1, groups.size());
        assertEquals(rg1.getDefinition(), groups.iterator().next().buildGroupDefinition());
    }
}
