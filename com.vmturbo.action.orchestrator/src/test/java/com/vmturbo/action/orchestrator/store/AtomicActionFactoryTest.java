package com.vmturbo.action.orchestrator.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.action.orchestrator.ActionOrchestratorTestUtils;
import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.AtomicActionSpecsCache;
import com.vmturbo.action.orchestrator.store.AtomicActionFactory.AtomicActionResult;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
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

//import com.vmturbo.common.protobuf.action.ActionDTO.Action;

/**
 * Unit tests for {@link AtomicActionFactory}.
 */
public class AtomicActionFactoryTest {

    private AtomicActionSpecsCache atomicActionSpecsCache;
    private  ActionEntity aggregateEntity1;

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

        AtomicActionSpec spec1 = AtomicActionSpec.newBuilder()
                .addAllEntityIds(Arrays.asList(11L, 12L, 13L))
                .setAggregateEntity(AtomicActionEntity.newBuilder()
                        .setEntity(aggregateEntity1)
                        .setEntityName("controller1"))
                .setResizeSpec(ResizeMergeSpec.newBuilder()
                        .addCommodityData(CommodityMergeData.newBuilder()
                                .setCommodityType(CommodityType.VCPU))
                        .addCommodityData(CommodityMergeData.newBuilder()
                                .setCommodityType(CommodityType.VMEM)))
                .build();

        List<AtomicActionSpec> resizeSpecs = Arrays.asList(spec1);

        Map<ActionType, List<AtomicActionSpec>> mergeSpecsInfoMap = new HashMap<>();
        mergeSpecsInfoMap.put(ActionType.RESIZE, resizeSpecs);
        atomicActionSpecsCache.updateAtomicActionSpecsInfo(mergeSpecsInfoMap);
    }

    /**
     * Test that actions of entities without merge spec are not selected for merging
     * by the AtomicActionFactory.
     */
    @Test
    public void testMergeActionForEntitiesWithoutSpec() {
        // Actions for entities with action merge spec
        ActionDTO.Action resize1 = createResizeAction(1, 11,
                40, CommodityType.VCPU);
        ActionDTO.Action resize2 = createResizeAction(2, 12,
                40, CommodityType.VCPU);

        // Actions for entities without action merge spec
        ActionDTO.Action resize3 = createResizeAction(3, 21,
                40, CommodityType.VCPU);

        ActionDTO.Action resize4 = createResizeAction(4, 22,
                40, CommodityType.VCPU);

        Action view1 = Mockito.spy(ActionOrchestratorTestUtils.actionFromRecommendation(resize1, 1));
        Action view2 = Mockito.spy(ActionOrchestratorTestUtils.actionFromRecommendation(resize2, 1));
        Action view3 = Mockito.spy(ActionOrchestratorTestUtils.actionFromRecommendation(resize3, 1));
        Action view4 = Mockito.spy(ActionOrchestratorTestUtils.actionFromRecommendation(resize4, 1));
        when(view1.getMode()).thenReturn(ActionMode.MANUAL);
        when(view2.getMode()).thenReturn(ActionMode.MANUAL);
        when(view3.getMode()).thenReturn(ActionMode.MANUAL);
        when(view4.getMode()).thenReturn(ActionMode.MANUAL);

        List<ActionDTO.Action> actionList =  Arrays.asList(resize1, resize2, resize3, resize4);

        AtomicActionFactory atomicActionFactory = new AtomicActionFactory(atomicActionSpecsCache);

        Map<Long, AggregatedAction> aggregatedActions = atomicActionFactory.aggregate(actionList);
        assertEquals(1, aggregatedActions.size());
        AggregatedAction aggregateAction  = aggregatedActions.get(aggregateEntity1.getId());
        aggregateAction.updateActionView(1, view1);
        aggregateAction.updateActionView(2, view2);
        aggregateAction.updateActionView(3, view3);
        aggregateAction.updateActionView(4, view4);

        List<AtomicActionResult> atomicActions = atomicActionFactory.atomicActions(aggregatedActions);

        assertNotNull(atomicActions);
        assertEquals(1, atomicActions.size());
        AtomicActionResult atomicAction = atomicActions.get(0);
        assertEquals(2, atomicAction.mergedActions().size());

        List<ActionDTO.Action> marketActions = atomicAction.deDuplicatedActions().values()
                                        .stream()
                                        .flatMap(Collection::stream)
                                        .collect(Collectors.toList());
        List<Long> entitiesWithoutSpecs = Arrays.asList(21L, 22L);
        List<Long> entitiesWithSpecs = Arrays.asList(11L, 12L);
        marketActions.stream()
                .noneMatch(action -> !entitiesWithoutSpecs.contains(action.getInfo().getResize().getTarget().getId()));
        marketActions.stream()
                .allMatch(action -> !entitiesWithSpecs.contains(action.getInfo().getResize().getTarget().getId()));
    }

    /**
     * Test that only actions types with merge spec are selected for merging
     * by the AtomicActionFactory.
     */
    @Test
    public void testMerge() {
        // Actions for entities with action merge spec
        ActionDTO.Action resize1 = createResizeAction(1, 11,
                40, CommodityType.VCPU);
        ActionDTO.Action resize2 = createResizeAction(2, 12,
                40, CommodityType.VCPU);

        ActionDTO.Action reconfigureActionDto = ActionDTO.Action.newBuilder()
                .setId(1)
                .setDeprecatedImportance(0)
                .setInfo(ActionInfo.newBuilder()
                        .setReconfigure(Reconfigure.newBuilder()
                                .setTarget(ActionEntity.newBuilder().setId(21).setType(40))
                                .setIsProvider(false)))
                .setExplanation(Explanation.newBuilder()
                        .setReconfigure(ReconfigureExplanation.newBuilder()))
                .build();

        Action view1 = Mockito.spy(ActionOrchestratorTestUtils.actionFromRecommendation(resize1, 1));
        Action view2 = Mockito.spy(ActionOrchestratorTestUtils.actionFromRecommendation(resize2, 1));
        Action view3 = Mockito.spy(ActionOrchestratorTestUtils.actionFromRecommendation(reconfigureActionDto, 1));
        when(view1.getMode()).thenReturn(ActionMode.MANUAL);
        when(view2.getMode()).thenReturn(ActionMode.MANUAL);

        List<ActionDTO.Action> actionList =  Arrays.asList(resize1, resize2, reconfigureActionDto);

        AtomicActionFactory atomicActionFactory = new AtomicActionFactory(atomicActionSpecsCache);

        Map<Long, AggregatedAction> aggregatedActions = atomicActionFactory.aggregate(actionList);
        assertEquals(1, aggregatedActions.size());
        AggregatedAction aggregateAction  = aggregatedActions.get(aggregateEntity1.getId());
        aggregateAction.updateActionView(1, view1);
        aggregateAction.updateActionView(2, view2);

        List<AtomicActionResult> atomicActions = atomicActionFactory.atomicActions(aggregatedActions);

        assertEquals(1, atomicActions.size());
        AtomicActionResult atomicAction = atomicActions.get(0);
        assertEquals(2, atomicAction.mergedActions().size());

        List<ActionDTO.Action> marketActions = atomicAction.deDuplicatedActions().values()
                .stream()
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
        marketActions.stream()
                .allMatch(action -> action.getInfo().hasResize());
    }

    private static ActionDTO.Action createResizeAction(long actionId,
                                                       long entityId, int entityType,
                                                       CommodityType commType) {
        ResizeExplanation resizeExplanation = ResizeExplanation.newBuilder()
                .setDeprecatedStartUtilization(20)
                .setDeprecatedEndUtilization(90)
                .build();

        ActionDTO.Action resize = ActionDTO.Action.newBuilder()
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
}
