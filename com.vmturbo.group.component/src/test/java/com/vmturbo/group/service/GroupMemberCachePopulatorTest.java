package com.vmturbo.group.service;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import io.grpc.Status;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ShortMap;
import it.unimi.dsi.fastutil.longs.LongSet;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.group.group.GroupDAO;
import com.vmturbo.group.service.GroupMemberCachePopulator.CachedGroupMembers;
import com.vmturbo.group.service.GroupMemberCachePopulator.GroupMembershipRelationships;
import com.vmturbo.platform.common.dto.CommonDTO;

/**
 * Tests the {@link GroupMemberCachePopulator} class.
 */
public class GroupMemberCachePopulatorTest {
    private static final Logger logger = LogManager.getLogger();
    private static final int NUMBER_OF_GROUPS = 20;
    private static final int NUMBER_OF_ENTITIES = 50;
    private static final int MAX_GROUP_MEMBERS = 10;

    @Mock
    private GroupDAO groupDAO;

    @Mock
    private GroupMemberCalculator calculator;

    private Map<Long, Short> groupToType;

    private Map<Long, Set<Long>> groupMembers;

    private Set<Pair<Long, Long>> membershipPairs;

    /**
     * Prepares data for the test.
     *
     * @throws StoreOperationException if something goes wrong.
     */
    @Before
    public void setupTest() throws StoreOperationException {
        MockitoAnnotations.initMocks(this);
        groupToType = new HashMap<>(NUMBER_OF_GROUPS);
        groupMembers = new HashMap<>(NUMBER_OF_GROUPS);
        List<GroupDTO.Grouping> groupings = new ArrayList<>();
        membershipPairs = new HashSet<>();
        Random random = new Random();

        for (long groupCntr = 0; groupCntr < NUMBER_OF_GROUPS; groupCntr++) {
            final int groupSize = random.nextInt(MAX_GROUP_MEMBERS + 1);
            final Set<Long> members = new HashSet<>();
            for (long entityCntr = 0; entityCntr < groupSize; entityCntr++) {
                long entityId = Long.valueOf(random.nextInt(NUMBER_OF_ENTITIES));
                members.add(entityId);
                membershipPairs.add(Pair.of(groupCntr, entityId));
            }

            int groupType = random.nextInt(CommonDTO.GroupDTO.GroupType.values().length);
            logger.info("Group: {} Type: {} Members: {}", groupCntr,
                    CommonDTO.GroupDTO.GroupType.values()[groupType], members);
            groupMembers.put(groupCntr, members);
            groupToType.put(groupCntr, (short)CommonDTO.GroupDTO.GroupType.values()[groupType].getNumber());
            groupings.add(GroupDTO.Grouping.newBuilder()
                    .setId(groupCntr)
                    .setDefinition(GroupDTO.GroupDefinition.newBuilder()
                            .setType(CommonDTO.GroupDTO.GroupType.values()[groupType])
                            .setDisplayName(String.valueOf(groupCntr))
                    ).build());
        }

        when(groupDAO.getGroups(any())).thenReturn(groupings);
        when(calculator.getGroupMembers(any(), any(GroupDTO.GroupDefinition.class),
                eq(false))).thenAnswer(x -> {
            final GroupDTO.GroupDefinition def = x.getArgumentAt(1, GroupDTO.GroupDefinition.class);
            return groupMembers.get(Long.valueOf(def.getDisplayName()));
        });
    }

    /**
     * Test successful case of regroup.
     */
    @Test
    public void testRegroup() {
        // ACT
        final GroupMembershipRelationships memberships = GroupMemberCachePopulator
                .calculate(calculator, groupDAO, CachedGroupMembers.Type.SET.getFactory(), true);

        // ASSERT
        assertTrue(memberships.getSuccess());
        validateGroupMemberships(memberships.getGroupIdToGroupMemberIdsMap());
        validateGroupTypes(memberships.getGroupIdToType());
        validateParentRelationship(memberships.getEntityIdToGroupIdsMap());
    }

    /**
     * Test successful case of regroup when the feature flag to populate the relationship from
     * entity to their parent is disabled.
     */
    @Test
    public void testRegroupParentRelationshipPopulationDisabled() {
        // ACT
        final GroupMembershipRelationships memberships = GroupMemberCachePopulator
                .calculate(calculator, groupDAO, CachedGroupMembers.Type.SET.getFactory(), false);

        // ASSERT
        assertTrue(memberships.getSuccess());
        validateGroupMemberships(memberships.getGroupIdToGroupMemberIdsMap());
        validateGroupTypes(memberships.getGroupIdToType());
        assertThat(memberships.getEntityIdToGroupIdsMap().size(), equalTo(0));
    }

    /**
     * Test the case the group DAO fails.
     */
    @Test
    public void testRegroupDAOFailure() {
        // ARRANGE
        when(groupDAO.getGroups(any())).thenAnswer(x -> {
            throw new StoreOperationException(Status.DATA_LOSS, "");
        });

        // ACT
        final GroupMembershipRelationships memberships = GroupMemberCachePopulator
                .calculate(calculator, groupDAO, CachedGroupMembers.Type.SET.getFactory(), false);

        // ASSERT
        assertFalse(memberships.getSuccess());
    }

    private void validateGroupMemberships(final Long2ObjectMap<CachedGroupMembers> groupIdToGroupMemberIdsMap) {
        Set<Pair<Long, Long>> membershipPairs = new HashSet<>();
        for (long groupCntr = 0; groupCntr < NUMBER_OF_GROUPS; groupCntr++) {
            final long g = groupCntr;
            groupIdToGroupMemberIdsMap.get(groupCntr)
                    .get(l -> membershipPairs.add(Pair.of(g, l)));
        }
        assertThat(membershipPairs, equalTo(this.membershipPairs));
    }

    private void validateGroupTypes(final Long2ShortMap groupIdToType) {
        assertThat(groupIdToType.size(), equalTo(NUMBER_OF_GROUPS));
        for (long groupCntr = 0; groupCntr < NUMBER_OF_GROUPS; groupCntr++) {
            if (groupIdToType.get(groupCntr) != groupToType.get(groupCntr)) {
                fail("The calculated value for " + groupCntr
                        + " was " + groupIdToType.get(groupCntr)
                        + " and not " + groupToType.get(groupCntr));
            }
        }
    }

    private void validateParentRelationship(final Long2ObjectMap<LongSet> entityIdToGroupIdsMap) {
        Set<Pair<Long, Long>> membershipPairs = new HashSet<>();
        for (Long2ObjectMap.Entry<LongSet> entry : entityIdToGroupIdsMap.long2ObjectEntrySet()) {
            for (long g : entry.getValue()) {
                membershipPairs.add(Pair.of(g, entry.getLongKey()));
            }
        }
        assertThat(membershipPairs, equalTo(this.membershipPairs));
    }
}