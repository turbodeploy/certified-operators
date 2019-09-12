package com.vmturbo.common.protobuf;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.*;

import org.junit.Test;

import com.google.common.collect.Lists;

import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.PlanChanges;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.PlanChanges.IgnoreConstraint;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.SettingOverride;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.TopologyAddition;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.TopologyRemoval;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.TopologyReplace;

/**
 * Unit tests for {@link PlanDTOUtilTest}
 */
public class PlanDTOUtilTest {

    @Test
    public void testTopologyAdditionEntity() {
        final ScenarioChange change = ScenarioChange.newBuilder()
                .setTopologyAddition(TopologyAddition.newBuilder()
                    .setEntityId(1))
                .build();
        final Set<Long> result = PlanDTOUtil.getInvolvedEntities(change);
        assertEquals(1, result.size());
        assertTrue(result.contains(1L));
    }

    @Test
    public void testTopologyAdditionTemplate() {
        final ScenarioChange change = ScenarioChange.newBuilder()
            .setTopologyAddition(TopologyAddition.newBuilder()
                .setTemplateId(1))
            .build();
        final Set<Long> result = PlanDTOUtil.getInvolvedTemplates(change);
        assertEquals(1, result.size());
        assertTrue(result.contains(1L));
    }

    @Test
    public void testTopologyRemovalEntity() {
        final ScenarioChange change = ScenarioChange.newBuilder()
                .setTopologyRemoval(TopologyRemoval.newBuilder()
                        .setEntityId(1))
                .build();
        final Set<Long> result = PlanDTOUtil.getInvolvedEntities(change);
        assertEquals(1, result.size());
        assertTrue(result.contains(1L));
    }

    @Test
    public void testTopologyReplaceEntity() {
        final ScenarioChange change = ScenarioChange.newBuilder()
                .setTopologyReplace(TopologyReplace.newBuilder()
                        .setRemoveEntityId(1))
                .build();
        final Set<Long> result = PlanDTOUtil.getInvolvedEntities(change);
        assertEquals(1, result.size());
        assertTrue(result.contains(1L));
    }

    @Test
    public void testTopologyReplaceTemplate() {
        final ScenarioChange change = ScenarioChange.newBuilder()
            .setTopologyReplace(TopologyReplace.newBuilder()
                .setAddTemplateId(1)
                .setRemoveEntityId(2))
            .build();
        final Set<Long> result = PlanDTOUtil.getInvolvedTemplates(change);
        assertEquals(1, result.size());
        assertTrue(result.contains(1L));
    }

    @Test
    public void testMultiChange() {
        List<ScenarioChange> changes = Lists.newArrayList(
            ScenarioChange.newBuilder()
                .setTopologyReplace(TopologyReplace.newBuilder()
                        .setRemoveEntityId(1))
                .build(),
            ScenarioChange.newBuilder()
                .setTopologyAddition(TopologyAddition.newBuilder()
                    .setEntityId(2))
                .build());

        final Set<Long> result = PlanDTOUtil.getInvolvedEntities(changes);
        assertEquals(2, result.size());
        assertTrue(result.contains(1L));
        assertTrue(result.contains(2L));
    }

    @Test
    public void testNoChange() {
        assertTrue(PlanDTOUtil.getInvolvedEntities(Collections.emptyList()).isEmpty());
    }

    @Test
    public void testInvolvedGroupsEmpty() {
        assertTrue(PlanDTOUtil.getInvolvedGroups(Collections.emptyList()).isEmpty());
    }

    @Test
    public void testInvolvedGroupsMultichange() {
        final ScenarioChange groupAddition = ScenarioChange.newBuilder()
                .setTopologyAddition(TopologyAddition.newBuilder()
                        .setGroupId(1L))
                .build();
        final ScenarioChange groupRemoval = ScenarioChange.newBuilder()
                .setTopologyRemoval(TopologyRemoval.newBuilder()
                        .setGroupId(2L))
                .build();
        assertThat(PlanDTOUtil.getInvolvedGroups(Arrays.asList(groupAddition, groupRemoval)),
                contains(1L, 2L));
    }

    @Test
    public void testInvolvedGroupsNoMatches() {
        final ScenarioChange settingOverride = ScenarioChange.newBuilder()
                .setSettingOverride(SettingOverride.getDefaultInstance())
                .build();
        assertTrue(PlanDTOUtil.getInvolvedGroups(settingOverride).isEmpty());
    }

    @Test
    public void testInvolvedGroupsAddition() {
        final ScenarioChange groupAddition = ScenarioChange.newBuilder()
            .setTopologyAddition(TopologyAddition.newBuilder()
                    .setGroupId(1L))
            .build();
        assertThat(PlanDTOUtil.getInvolvedGroups(groupAddition), contains(1L));
    }

    @Test
    public void testInvolvedGroupsNonGroupAddition() {
        final ScenarioChange groupAddition = ScenarioChange.newBuilder()
                .setTopologyAddition(TopologyAddition.newBuilder()
                        .setEntityId(1L))
                .build();
        assertTrue(PlanDTOUtil.getInvolvedGroups(groupAddition).isEmpty());
    }

    @Test
    public void testInvolvedGroupsRemoval() {
        final ScenarioChange groupRemoval = ScenarioChange.newBuilder()
                .setTopologyRemoval(TopologyRemoval.newBuilder()
                        .setGroupId(1L))
                .build();
        assertThat(PlanDTOUtil.getInvolvedGroups(groupRemoval), contains(1L));
    }

    @Test
    public void testInvolvedGroupsNonGroupRemoval() {
        final ScenarioChange groupRemoval = ScenarioChange.newBuilder()
                .setTopologyRemoval(TopologyRemoval.newBuilder()
                        .setEntityId(1L))
                .build();
        assertTrue(PlanDTOUtil.getInvolvedGroups(groupRemoval).isEmpty());
    }

    @Test
    public void testInvolvedGroupsReplace() {
        final ScenarioChange groupReplace = ScenarioChange.newBuilder()
                .setTopologyReplace(TopologyReplace.newBuilder()
                        .setRemoveGroupId(1L))
                .build();
        assertThat(PlanDTOUtil.getInvolvedGroups(groupReplace), contains(1L));
    }

    @Test
    public void testInvolvedGroupsNonGroupReplace() {
        final ScenarioChange groupReplace = ScenarioChange.newBuilder()
                .setTopologyReplace(TopologyReplace.newBuilder()
                        .setRemoveEntityId(1L))
                .build();
        assertTrue(PlanDTOUtil.getInvolvedGroups(groupReplace).isEmpty());
    }

    /**
     * Test that unique uuids returned from {@link ScenarioChange.PlanChanges.IgnoreConstraint}
     */
    @Test
    public void testgetInvolvedGroupsUUidsFromIgnoreConstraintsWithNonUniqueTargetsUuids() {

        //GIVEN
        Long groupUUid1 = 1234L;
        Long groupUUid2 = 4567L;

        PlanChanges.ConstraintGroup cGroup1 = PlanChanges.ConstraintGroup.newBuilder().setGroupUuid(groupUUid1).build();
        PlanChanges.ConstraintGroup cGroup2 = PlanChanges.ConstraintGroup.newBuilder().setGroupUuid(groupUUid1).build();
        PlanChanges.ConstraintGroup cGroup3 = PlanChanges.ConstraintGroup.newBuilder().setGroupUuid(groupUUid2).build();

        IgnoreConstraint ignoreConstraint1 = IgnoreConstraint.newBuilder().setIgnoreGroup(cGroup1).build();
        IgnoreConstraint ignoreConstraint2 = IgnoreConstraint.newBuilder().setIgnoreGroup(cGroup2).build();
        IgnoreConstraint ignoreConstraint3 = IgnoreConstraint.newBuilder().setIgnoreGroup(cGroup3).build();

        PlanChanges planChanges = PlanChanges.newBuilder().addIgnoreConstraints(ignoreConstraint1)
                .addIgnoreConstraints(ignoreConstraint2).addIgnoreConstraints(ignoreConstraint3)
                .build();

        //WHEN
        Set<Long> ids = PlanDTOUtil.getInvolvedGroupsUUidsFromIgnoreConstraints(planChanges);

        //THEN
        assertTrue(ids.size() == 2);
        assertTrue(ids.contains(groupUUid1));
        assertTrue(ids.contains(groupUUid2));
    }

    /**
     * Test empty {@link ScenarioChange.PlanChanges.IgnoreConstraint}
     * returns empty Set
     */
    @Test
    public void testgetInvolvedGroupsUUidsFromEmptyIgnoreConstraints() {
        //GIVEN
        PlanChanges planChanges = PlanChanges.newBuilder().build();

        //WHEN

        Set<Long> ids = PlanDTOUtil.getInvolvedGroupsUUidsFromIgnoreConstraints(planChanges);

        //THEN
        assertTrue(ids.isEmpty());
    }

    /**
     * Tests getInvolvedGroups() calls getInvolvedGroupsUuid()
     * when {@link ScenarioChange.PlanChanges} is present.
     */
    @Test
    public void testgetInvolvedGroupsCallsGetInvolvedGroupsUuidWhenPlanChangesPresent() {
        //GIVEN
        Long uuid = 1234L;

        PlanChanges.ConstraintGroup cGroup1 = PlanChanges.ConstraintGroup.newBuilder().setGroupUuid(uuid).build();
        IgnoreConstraint ignoreConstraint1 = IgnoreConstraint.newBuilder().setIgnoreGroup(cGroup1).build();

        PlanChanges planChanges = PlanChanges.newBuilder().addIgnoreConstraints(ignoreConstraint1).build();

        final ScenarioChange scenarioChange = ScenarioChange.newBuilder()
                .setPlanChanges(planChanges)
                .build();

        //WHEN
        Set<Long> uuids = PlanDTOUtil.getInvolvedGroups(scenarioChange);

        //THEN
        assertTrue(uuids.contains(uuid));
    }


}
