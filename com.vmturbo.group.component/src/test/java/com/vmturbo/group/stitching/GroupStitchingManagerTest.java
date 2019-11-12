package com.vmturbo.group.stitching;

import static org.mockito.Mockito.spy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredGroupsPoliciesSettings.UploadedGroup;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers.StaticMembersByType;
import com.vmturbo.group.group.GroupDAO.DiscoveredGroupIdImpl;
import com.vmturbo.group.group.IGroupStore;
import com.vmturbo.group.group.IGroupStore.DiscoveredGroupId;
import com.vmturbo.group.identity.IdentityProvider;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;

/**
 * Test that {@link GroupStitchingManager} works as expected.
 */
public class GroupStitchingManagerTest {

    private static final long TARGET_1 = 111L;
    private static final long TARGET_2 = 112L;
    private static final long TARGET_3 = 113L;
    private static final String PROBE_TYPE = SDKProbeType.VCENTER.toString();
    private static final String GROUP_ID = "group-1";
    private static final String GROUP_ID_2 = "group-2";
    private static final long VM_ID_1 = 11;
    private static final long VM_ID_2 = 12;
    private static final long VM_ID_3 = 13;

    private static final UploadedGroup group1 =
            GroupTestUtils.createUploadedGroup(GroupType.REGULAR, GROUP_ID,
                    ImmutableMap.of(EntityType.VIRTUAL_MACHINE_VALUE, Sets.newHashSet(VM_ID_1)));
    private static final UploadedGroup group2 =
            GroupTestUtils.createUploadedGroup(GroupType.REGULAR, GROUP_ID_2,
                    ImmutableMap.of(EntityType.VIRTUAL_MACHINE_VALUE, Sets.newHashSet(VM_ID_2)));
    private static final UploadedGroup group3 =
            GroupTestUtils.createUploadedGroup(GroupType.COMPUTE_HOST_CLUSTER, GROUP_ID,
                    ImmutableMap.of(EntityType.VIRTUAL_MACHINE_VALUE, Sets.newHashSet(VM_ID_3)));
    private static final UploadedGroup group4 =
            GroupTestUtils.createUploadedGroup(GroupType.RESOURCE, GROUP_ID,
                    ImmutableMap.of(EntityType.VIRTUAL_MACHINE_VALUE, Sets.newHashSet(VM_ID_2)));
    private static final UploadedGroup group41 =
            GroupTestUtils.createUploadedGroup(GroupType.RESOURCE, GROUP_ID,
                    ImmutableMap.of(EntityType.PHYSICAL_MACHINE_VALUE, Sets.newHashSet(VM_ID_3)));
    private static final UploadedGroup group5 =
            GroupTestUtils.createUploadedGroup(GroupType.RESOURCE, GROUP_ID_2,
                    ImmutableMap.of(EntityType.VIRTUAL_MACHINE_VALUE, Sets.newHashSet(VM_ID_3)));

    private IGroupStore groupStore;
    private GroupStitchingManager stitchingManager;
    private GroupStitchingContext groupStitchingContext;

    /**
     * Set up for each unit test.
     */
    @Before
    public void setup() {
        groupStitchingContext = spy(new GroupStitchingContext());
        stitchingManager = new GroupStitchingManager(new IdentityProvider(0));
        groupStore = Mockito.mock(IGroupStore.class);
    }

    /**
     * Tests stitching between different targets. Resource groups with the same source id are
     * stitched together. Other group types are not stitched across the target.
     */
    @Test
    public void testResourceGroupStitching() {
        groupStitchingContext.setTargetGroups(TARGET_1, PROBE_TYPE, Arrays.asList(group1, group2));
        groupStitchingContext.setTargetGroups(TARGET_2, PROBE_TYPE,
                Arrays.asList(group2, group3, group4));
        groupStitchingContext.setTargetGroups(TARGET_3, PROBE_TYPE,
                Arrays.asList(group3, group41, group5));
        Mockito.when(groupStore.getDiscoveredGroupsIds()).thenReturn(Collections.emptyList());
        final StitchingResult result = stitchingManager.stitch(groupStore, groupStitchingContext);
        Assert.assertEquals(Collections.emptySet(), result.getGroupsToDelete());
        Assert.assertEquals(7, result.getGroupsToAddOrUpdate().size());
        Assert.assertEquals(7, result.getGroupsToAddOrUpdate()
                .stream()
                .map(StitchingGroup::getOid)
                .distinct()
                .count());
        final StitchingGroup stitchedGroup = result.getGroupsToAddOrUpdate()
                .stream()
                .filter(group -> group.getGroupDefinition().getType() == GroupType.RESOURCE &&
                        group.getSourceId().equals(GROUP_ID))
                .findFirst()
                .get();
        Assert.assertEquals(Sets.newHashSet(TARGET_2, TARGET_3), stitchedGroup.getTargetIds());
        Assert.assertTrue(stitchedGroup.isNewGroup());
        Assert.assertEquals(getMembers(group4.getDefinition(), group41.getDefinition()),
                getMembers(stitchedGroup.buildGroupDefinition()));

        final StitchingGroup singleGroup = result.getGroupsToAddOrUpdate()
                .stream()
                .filter(group -> group.getGroupDefinition().getType() == GroupType.RESOURCE &&
                        group.getSourceId().equals(GROUP_ID_2))
                .findFirst()
                .get();
        Assert.assertEquals(Collections.singleton(TARGET_3), singleGroup.getTargetIds());
    }

    /**
     * Tests how OIDs are reused while performing stitching operations.
     */
    @Test
    public void testReuseOids() {
        groupStitchingContext.setTargetGroups(TARGET_1, PROBE_TYPE,
                Arrays.asList(group1, group2, group4));
        groupStitchingContext.setTargetGroups(TARGET_2, PROBE_TYPE,
                Arrays.asList(group2, group4, group5));
        final Collection<DiscoveredGroupId> discovered = new ArrayList<>(5);
        final long oid1 = 123L;
        final long oid4 = 234L;
        final long oid1ToDelete = 345L;
        final long oid2ToDelete = 346L;
        discovered.add(new DiscoveredGroupIdImpl(oid1, TARGET_1, GROUP_ID, GroupType.REGULAR));
        discovered.add(new DiscoveredGroupIdImpl(oid4, null, GROUP_ID, GroupType.RESOURCE));
        discovered.add(new DiscoveredGroupIdImpl(oid1ToDelete, TARGET_1, "non-existing-group",
                GroupType.REGULAR));
        discovered.add(new DiscoveredGroupIdImpl(oid2ToDelete, null, "non-existing-group",
                GroupType.RESOURCE));
        Mockito.when(groupStore.getDiscoveredGroupsIds()).thenReturn(discovered);
        final StitchingResult result = stitchingManager.stitch(groupStore, groupStitchingContext);
        Assert.assertEquals(Sets.newHashSet(oid1ToDelete, oid2ToDelete),
                result.getGroupsToDelete());
        Assert.assertEquals(5, result.getGroupsToAddOrUpdate().size());
        Assert.assertEquals(5, result.getGroupsToAddOrUpdate()
                .stream()
                .map(StitchingGroup::getOid)
                .distinct()
                .count());

        Assert.assertNotEquals(Optional.empty(), result.getGroupsToAddOrUpdate()
                .stream()
                .filter(group -> group.getOid() == oid1)
                .findFirst());
        Assert.assertNotEquals(Optional.empty(), result.getGroupsToAddOrUpdate()
                .stream()
                .filter(group -> group.getOid() == oid4)
                .findFirst()
                .get());
    }

    /**
     * Tests stitching of different member types.
     */
    @Test
    public void stitchEntitiesOfDifferentTypes() {
        final Set<Long> hostIds = Sets.newHashSet(345L, 567L, 567L);
        final UploadedGroup groupApps =
                GroupTestUtils.createUploadedGroup(GroupType.RESOURCE, GROUP_ID,
                        ImmutableMap.of(EntityType.APPLICATION_VALUE, hostIds,
                                EntityType.VIRTUAL_MACHINE_VALUE, Collections.singleton(VM_ID_1)));
        groupStitchingContext.setTargetGroups(TARGET_1, PROBE_TYPE, Collections.singleton(group4));
        groupStitchingContext.setTargetGroups(TARGET_2, PROBE_TYPE,
                Collections.singleton(groupApps));
        final StitchingResult result = stitchingManager.stitch(groupStore, groupStitchingContext);
        Assert.assertEquals(Collections.emptySet(), result.getGroupsToDelete());
        Assert.assertEquals(1, result.getGroupsToAddOrUpdate().size());
        final StitchingGroup group = result.getGroupsToAddOrUpdate().iterator().next();
        Assert.assertEquals(getMembers(group4.getDefinition(), groupApps.getDefinition()),
                getMembers(group.buildGroupDefinition()));
    }

    /**
     * Tests stitching of resource groups reported for a single target. {@link IGroupStore} will
     * return a targetId for this group, because there is only one target associated with the
     * resource group.
     */
    @Test
    public void testResourceGroupStitchingResourceGroupSingleTarget() {
        groupStitchingContext.setTargetGroups(TARGET_1, PROBE_TYPE,
                Collections.singletonList(group4));
        final long oid = 123555L;
        Mockito.when(groupStore.getDiscoveredGroupsIds())
                .thenReturn(Collections.singleton(
                        new DiscoveredGroupIdImpl(oid, TARGET_2, GROUP_ID, GroupType.RESOURCE)));
        final StitchingResult result = stitchingManager.stitch(groupStore, groupStitchingContext);
        Assert.assertEquals(Collections.singleton(oid), result.getGroupsToAddOrUpdate()
                .stream()
                .map(StitchingGroup::getOid)
                .collect(Collectors.toSet()));
        Assert.assertEquals(Collections.emptySet(), result.getGroupsToDelete());
    }

    /**
     * Tests that stitching does not suggest removing of targets from undiscovered targets.
     */
    @Test
    public void testUndiscoveredTargets() {
        groupStitchingContext.setTargetGroups(TARGET_1, PROBE_TYPE,
                Arrays.asList(group1, group2));
        groupStitchingContext.setTargetGroups(TARGET_3, PROBE_TYPE, Collections.emptySet());
        groupStitchingContext.addUndiscoveredTarget(TARGET_2);
        final Collection<DiscoveredGroupId> discovered = new ArrayList<>(5);
        final long oid1 = 123L;
        final long oid2 = 234L;
        final long oid1ToDelete = 345L;
        final long oid2ToDelete = 346L;
        discovered.add(new DiscoveredGroupIdImpl(oid1, TARGET_2, GROUP_ID, GroupType.REGULAR));
        discovered.add(new DiscoveredGroupIdImpl(oid2, null, GROUP_ID, GroupType.RESOURCE));
        discovered.add(
                new DiscoveredGroupIdImpl(oid1ToDelete, TARGET_3, GROUP_ID_2, GroupType.REGULAR));
        discovered.add(
                new DiscoveredGroupIdImpl(oid2ToDelete, null, GROUP_ID_2, GroupType.RESOURCE));
        Mockito.when(groupStore.getDiscoveredGroupsIds()).thenReturn(discovered);
        // groups from undiscovered targets
        Mockito.when(groupStore.getGroupsByTargets(Collections.singleton(TARGET_2)))
                .thenReturn(Sets.newHashSet(oid1, oid2));

        final StitchingResult result = stitchingManager.stitch(groupStore, groupStitchingContext);
        Assert.assertEquals(Sets.newHashSet(oid1ToDelete, oid2ToDelete),
                result.getGroupsToDelete());
        Assert.assertEquals(2, result.getGroupsToAddOrUpdate().size());
    }

    @Nonnull
    private Map<MemberType, Set<Long>> getMembers(@Nonnull GroupDefinition... groups) {
        final Map<MemberType, Set<Long>> result = new HashMap<>();
        for (GroupDefinition group : groups) {
            for (StaticMembersByType staticGroupMembers : group.getStaticGroupMembers()
                    .getMembersByTypeList()) {
                result.computeIfAbsent(staticGroupMembers.getType(), key -> new HashSet<>())
                        .addAll(staticGroupMembers.getMembersList());
            }
        }
        return result;
    }
}
