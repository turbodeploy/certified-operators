package com.vmturbo.common.protobuf;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import com.google.common.collect.Lists;

import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange;
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
}
