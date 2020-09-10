package com.vmturbo.action.orchestrator.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.action.orchestrator.action.AtomicActionSpecsCache;
import com.vmturbo.action.orchestrator.store.AggregatedAction.DeDupedActions;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ReconfigureExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ResizeExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Reconfigure;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.action.ActionMergeSpecDTO.AtomicActionEntity;
import com.vmturbo.common.protobuf.action.ActionMergeSpecDTO.AtomicActionSpec;
import com.vmturbo.common.protobuf.action.ActionMergeSpecDTO.ResizeMergeSpec;
import com.vmturbo.common.protobuf.action.ActionMergeSpecDTO.ResizeMergeSpec.CommodityMergeData;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Unit tests for atomic resize action merge.
 */
public class AtomicResizeMergerTest {

    private AtomicResizeMerger merger;

    private AtomicActionSpecsCache atomicActionSpecsCache;

    private ActionEntity aggregateEntity1;
    private ActionEntity aggregateEntity3;
    private ActionEntity deDupEntity1;
    private ActionEntity deDupEntity2;
    private ActionEntity deDupEntity4;

    /**
     * Set up before the test.
     */
    @Before
    public void setUp() {
        IdentityGenerator.initPrefix(0);

        atomicActionSpecsCache = new AtomicActionSpecsCache();

        aggregateEntity1 = ActionEntity.newBuilder()
                                        .setType(EntityType.WORKLOAD_CONTROLLER_VALUE).setId(31)
                                                .build();
        aggregateEntity3 = ActionEntity.newBuilder()
                .setType(EntityType.WORKLOAD_CONTROLLER_VALUE).setId(33)
                .build();

        deDupEntity1 = ActionEntity.newBuilder()
                .setType(EntityType.CONTAINER_SPEC_VALUE).setId(41)
                .build();
        deDupEntity2 = ActionEntity.newBuilder()
                .setType(EntityType.CONTAINER_SPEC_VALUE).setId(42)
                .build();
        deDupEntity4 = ActionEntity.newBuilder()
                .setType(EntityType.CONTAINER_SPEC_VALUE).setId(44)
                .build();

        AtomicActionSpec spec1 = AtomicActionSpec.newBuilder()
                .addAllEntityIds(Arrays.asList(11L, 12L, 13L))
                .setAggregateEntity(AtomicActionEntity.newBuilder()
                        .setEntity(aggregateEntity1)
                        .setEntityName("controller1"))
                .setResizeSpec(ResizeMergeSpec.newBuilder()
                        .setDeDuplicationTarget(AtomicActionEntity.newBuilder()
                                .setEntity(deDupEntity1)
                                .setEntityName("spec1"))
                        .addCommodityData(CommodityMergeData.newBuilder()
                                .setCommodityType(CommodityType.VCPU))
                        .addCommodityData(CommodityMergeData.newBuilder()
                                .setCommodityType(CommodityType.VMEM)))
                .build();

        AtomicActionSpec spec2 = AtomicActionSpec.newBuilder()
                .addAllEntityIds(Arrays.asList(14L, 15L, 16L))
                .setAggregateEntity(AtomicActionEntity.newBuilder()
                        .setEntity(aggregateEntity1)
                        .setEntityName("controller1"))
                .setResizeSpec(ResizeMergeSpec.newBuilder()
                        .setDeDuplicationTarget(AtomicActionEntity.newBuilder()
                                .setEntity(deDupEntity2)
                                .setEntityName("spec2"))
                        .addCommodityData(CommodityMergeData.newBuilder()
                                .setCommodityType(CommodityType.VCPU))
                        .addCommodityData(CommodityMergeData.newBuilder()
                                .setCommodityType(CommodityType.VMEM)))
                .build();

        AtomicActionSpec spec3 = AtomicActionSpec.newBuilder()
                .addAllEntityIds(Arrays.asList(17L, 18L, 19L))
                .setAggregateEntity(AtomicActionEntity.newBuilder()
                        .setEntity(aggregateEntity3)
                        .setEntityName("controller3"))
                .setResizeSpec(ResizeMergeSpec.newBuilder()
                        .addCommodityData(CommodityMergeData.newBuilder()
                                .setCommodityType(CommodityType.VCPU))
                        .addCommodityData(CommodityMergeData.newBuilder()
                                .setCommodityType(CommodityType.VMEM)))
                .build();

        AtomicActionSpec spec4 = AtomicActionSpec.newBuilder()
                .addAllEntityIds(Arrays.asList(21L, 22L, 23L))
                .setAggregateEntity(AtomicActionEntity.newBuilder()
                        .setEntity(deDupEntity4)
                        .setEntityName("spec4"))
                .setResizeSpec(ResizeMergeSpec.newBuilder()
                        .setDeDuplicationTarget(AtomicActionEntity.newBuilder()
                                .setEntity(deDupEntity4)
                                .setEntityName("spec4"))
                        .addCommodityData(CommodityMergeData.newBuilder()
                                .setCommodityType(CommodityType.VCPU))
                        .addCommodityData(CommodityMergeData.newBuilder()
                                .setCommodityType(CommodityType.VMEM)))
                .build();

        List<AtomicActionSpec> resizeSpecs = Arrays.asList(spec1, spec2, spec3, spec4);

        Map<ActionType, List<AtomicActionSpec>> mergeSpecsInfoMap = new HashMap<>();
        mergeSpecsInfoMap.put(ActionType.RESIZE, resizeSpecs);
        atomicActionSpecsCache.updateAtomicActionSpecsInfo(mergeSpecsInfoMap);
        merger = new AtomicResizeMerger(atomicActionSpecsCache);
    }

    /**
     * Test if the resize action will be merged.
     */
    @Test
    public void testAppliesTo() {
        final ActionDTO.Action resizeActionDto = Action.newBuilder()
                .setId(1)
                .setDeprecatedImportance(0)
                .setInfo(ActionInfo.newBuilder()
                        .setResize(Resize.newBuilder()
                                .setTarget(ActionEntity.newBuilder().setId(11).setType(40).build())
                                .setCommodityType(TopologyDTO.CommodityType.newBuilder().setType(CommodityType.VCPU_VALUE))
                                .build()))
                .setExplanation(Explanation.newBuilder()
                        .setResize(ResizeExplanation.newBuilder()
                                .setDeprecatedStartUtilization(20)
                                .setDeprecatedEndUtilization(90)
                                .build())
                        .build())
                .build();

        assertTrue(merger.appliesTo(resizeActionDto));
    }

    /**
     * Tests that a non resize action will not be merged.
     */
    @Test
    public void testNotAppliesTo() {
        final ActionDTO.Action reconfigureActionDto = Action.newBuilder()
                .setId(1)
                .setDeprecatedImportance(0)
                .setInfo(ActionInfo.newBuilder()
                        .setReconfigure(Reconfigure.newBuilder()
                                .setTarget(ActionEntity.newBuilder().setId(20).setType(40).build())
                                .build()))
                .setExplanation(Explanation.newBuilder()
                        .setReconfigure(ReconfigureExplanation.newBuilder()
                                .build())
                        .build())
                .build();

        assertFalse(merger.appliesTo(reconfigureActionDto));
    }

    private static ActionDTO.Action createResizeAction(long actionId,
                                                       long entityId, int entityType,
                                                       CommodityType commType) {
        ResizeExplanation resizeExplanation = ResizeExplanation.newBuilder()
                .setDeprecatedStartUtilization(20)
                .setDeprecatedEndUtilization(90)
                .build();

        ActionDTO.Action resize = Action.newBuilder()
                .setId(actionId)
                .setDeprecatedImportance(0)
                .setInfo(ActionInfo.newBuilder()
                        .setResize(Resize.newBuilder()
                                .setTarget(ActionEntity.newBuilder()
                                                .setId(entityId).setType(entityType))
                                .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                                                        .setType(commType.getNumber())))
                )
                .setExplanation(Explanation.newBuilder()
                                    .setResize(resizeExplanation))
                .build();
        return resize;
    }

    /**
     * Test that resize for entities belonging to the same de-duplication entity
     * will be merged to one resize object in the AtomicResize.
     *
     */
    @Test
    public void mergeActionsWithSameDeDupTargets() {
        ActionDTO.Action resize1 = createResizeAction(1, 11,
                                                    40, CommodityType.VCPU);
        ActionDTO.Action resize2 = createResizeAction(2, 12,
                                                    40, CommodityType.VCPU);

        List<ActionDTO.Action> actionList =  Arrays.asList(resize1, resize2);

        Map<Long, AggregatedAction> aggregatedActions = merger.mergeActions(actionList);
        assertEquals(1, aggregatedActions.size());

        AggregatedAction aggregatedAction = aggregatedActions.values().stream().findFirst().get();
        assertEquals(aggregateEntity1, aggregatedAction.targetEntity());

        Map<Long, DeDupedActions> deDupedActions = aggregatedAction.deDupedActionsMap();
        assertEquals(1, deDupedActions.size());

        DeDupedActions deDupedActions1 = deDupedActions.values().stream().findFirst().get();
        assertEquals(deDupEntity1, deDupedActions1.targetEntity());
    }

    /**
     * Test that resize for entities belonging to the multiple de-duplication entities will be
     * merged to one AtomicResize containing resize info for each of the de-duplication entity.
     *
     */
    @Test
    public void mergeActionsWithMultipleDeDupTargets() {
        ActionDTO.Action resize1 = createResizeAction(1, 11,
                                                40, CommodityType.VCPU);
        ActionDTO.Action resize2 = createResizeAction(2, 12,
                                                40, CommodityType.VCPU);
        ActionDTO.Action resize3 = createResizeAction(3, 14,
                                                40, CommodityType.VCPU);
        ActionDTO.Action resize4 = createResizeAction(4, 15,
                                                40, CommodityType.VCPU);

        List<ActionDTO.Action> actionList =  Arrays.asList(resize1, resize2, resize3, resize4);

        Map<Long, AggregatedAction> aggregatedActions = merger.mergeActions(actionList);
        assertEquals(1, aggregatedActions.size());

        AggregatedAction aggregatedAction = aggregatedActions.values().stream().findFirst().get();
        assertEquals(aggregateEntity1, aggregatedAction.targetEntity());

        Map<Long, DeDupedActions> deDupedActions = aggregatedAction.deDupedActionsMap();
        assertEquals(2, deDupedActions.size());

        List<ActionEntity> deDupTargets = deDupedActions.values().stream()
                                                .map(action -> action.targetEntity())
                                                .collect(Collectors.toList());

        assertThat(deDupTargets, CoreMatchers.hasItems(deDupEntity1, deDupEntity2));
    }

    /**
     * Test AtomicResize creation for resizes for entities whose actions will be
     * merged by different action execution entities.
     */
    @Test
    public void mergeActionsMultipleAggregateTargets() {
        ActionDTO.Action resize1 = createResizeAction(1, 11,
                                                    40, CommodityType.VCPU);
        ActionDTO.Action resize2 = createResizeAction(2, 17,
                                                    40, CommodityType.VCPU);
        ActionDTO.Action resize3 = createResizeAction(3, 18,
                                                    40, CommodityType.VCPU);

        List<ActionDTO.Action> actionList =  Arrays.asList(resize1, resize2, resize3);

        Map<Long, AggregatedAction> aggregatedActions = merger.mergeActions(actionList);
        assertEquals(2, aggregatedActions.size());

        List<ActionEntity> resizeTargets = aggregatedActions.values().stream()
                                        .map(action -> action.targetEntity())
                .collect(Collectors.toList());
        assertThat(resizeTargets, CoreMatchers.hasItems(aggregateEntity1, aggregateEntity3));
    }

    /**
     * Test atomic resize actions for entities that are not associated with de-duplication target.
     */
    @Test
    public void mergeUsingSpecsWithoutDeDupTargets() {
        ActionDTO.Action resize1 = createResizeAction(1, 17,
                                                    40, CommodityType.VCPU);
        ActionDTO.Action resize2 = createResizeAction(2, 18,
                                                    40, CommodityType.VCPU);

        List<ActionDTO.Action> actionList =   Arrays.asList(resize1, resize2);

        Map<Long, AggregatedAction> aggregatedActions = merger.mergeActions(actionList);
        assertEquals(1, aggregatedActions.size());

        AggregatedAction aggregatedAction = aggregatedActions.values().stream().findFirst().get();
        assertEquals(aggregateEntity3, aggregatedAction.targetEntity());

        Map<Long, DeDupedActions> deDupedActions = aggregatedAction.deDupedActionsMap();
        assertEquals(0, deDupedActions.size());
        assertEquals(2, aggregatedAction.aggregateOnlyActions().size());

        List<ActionEntity> resizeTargets = aggregatedAction.aggregateOnlyActions().stream()
                .map(action -> action.getInfo().getResize().getTarget())
                .collect(Collectors.toList());

        assertThat(resizeTargets,
                CoreMatchers.hasItems(
                        ActionEntity.newBuilder()
                                .setType(EntityType.CONTAINER_VALUE).setId(17).build(),
                        ActionEntity.newBuilder()
                                .setType(EntityType.CONTAINER_VALUE).setId(18).build()));
    }
}
