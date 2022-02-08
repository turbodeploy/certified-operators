package com.vmturbo.group.service;

import static org.junit.Assert.assertEquals;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Table;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers.StaticMembersByType;
import com.vmturbo.group.group.IGroupStore.DiscoveredGroup;
import com.vmturbo.group.stitching.StitchingGroup;
import com.vmturbo.group.stitching.StitchingResult;

/**
 * Unit tests for {@link MemberLocalIdResolver}.
 */
public class MemberLocalIdResolverTest {

    private static final Long TARGET_ID = 1L;
    private static final Set<Long> TARGET_IDS = Collections.singleton(TARGET_ID);
    private static final String GROUP_1_ID = "group1";
    private static final String GROUP_2_ID = "group2";
    private static final String GROUP_3_ID = "group3";
    private static final Long GROUP_1_OID = 1L;
    private static final Long GROUP_2_OID = 2L;
    private static final Long GROUP_3_OID = 3L;

    private MemberLocalIdResolver memberLocalIdResolver;

    /**
     * Set up test environment.
     */
    @Before
    public void setUp() {
        final Table<Long, String, Long> allGroupsMap = HashBasedTable.create();
        allGroupsMap.put(TARGET_ID, GROUP_1_ID, GROUP_1_OID);
        allGroupsMap.put(TARGET_ID, GROUP_2_ID, GROUP_2_OID);
        allGroupsMap.put(TARGET_ID, GROUP_3_ID, GROUP_3_OID);
        final Collection<StitchingGroup> groupsToAddOrUpdate = ImmutableList.of(
                createStitchingGroup(GROUP_1_OID, GROUP_1_ID),
                createStitchingGroup(GROUP_2_OID, GROUP_2_ID),
                createStitchingGroup(GROUP_3_OID, GROUP_3_ID));
        final StitchingResult stitchingResult = new StitchingResult(groupsToAddOrUpdate,
                Collections.emptySet());
        memberLocalIdResolver = new MemberLocalIdResolver(stitchingResult);
    }

    /**
     * Test resolution of nested group local ID to OID.
     */
    @Test
    public void testResolveNestedGroupsLocalIds() {
        // ARRANGE
        final GroupDefinition group1Definition = GroupDefinition.newBuilder()
                .setStaticGroupMembers(StaticMembers.newBuilder()
                        .addMembersByType(StaticMembersByType.newBuilder()
                                .addMembers(GROUP_2_OID)
                                .addMemberLocalId(GROUP_3_ID)
                                // This non-existing ID should be skipped
                                .addMemberLocalId("NON EXISTING")
                                .build())
                        .build())
                .build();
        final DiscoveredGroup group1 = createGroup(GROUP_1_OID, GROUP_1_ID, group1Definition);

        final DiscoveredGroup group2 = createGroup(GROUP_2_OID, GROUP_2_ID,
                GroupDefinition.getDefaultInstance());

        final List<DiscoveredGroup> groups = ImmutableList.of(group1, group2);

        // ACT
        final List<DiscoveredGroup> result = memberLocalIdResolver.resolveNestedGroupsLocalIds(groups);

        // ASSERT
        assertEquals(2, result.size());
        final GroupDefinition resolvedGroup1 = result.get(0).getDefinition();
        final GroupDefinition expectedResolvedGroup1 = GroupDefinition.newBuilder()
                .setStaticGroupMembers(StaticMembers.newBuilder()
                        .addMembersByType(StaticMembersByType.newBuilder()
                                .addMembers(GROUP_2_OID)
                                .addMembers(GROUP_3_OID)
                                .build())
                        .build())
                .build();
        assertEquals(expectedResolvedGroup1, resolvedGroup1);

        final GroupDefinition resolvedGroup2 = result.get(1).getDefinition();
        assertEquals(GroupDefinition.getDefaultInstance(), resolvedGroup2);
    }

    private static StitchingGroup createStitchingGroup(
            @Nonnull final Long oid,
            @Nonnull final String sourceId) {
        return new StitchingGroup(oid, GroupDefinition.getDefaultInstance(), sourceId, false,
                TARGET_ID, false, null);
    }

    private static DiscoveredGroup createGroup(
            @Nonnull final Long oid,
            @Nonnull final String sourceIdentifier,
            @Nonnull final GroupDefinition groupDefinition) {
        return new DiscoveredGroup(oid, groupDefinition, sourceIdentifier, false, TARGET_IDS,
                Collections.emptyList(), false);
    }
}
