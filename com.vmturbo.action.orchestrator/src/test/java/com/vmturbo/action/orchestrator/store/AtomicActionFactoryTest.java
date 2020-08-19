package com.vmturbo.action.orchestrator.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.action.orchestrator.action.AtomicActionSpecsCache;
import com.vmturbo.action.orchestrator.store.AtomicActionFactory.AtomicActionResult;
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
 * Unit tests for {@link AtomicActionFactory}.
 */
public class AtomicActionFactoryTest {

    private AtomicActionSpecsCache atomicActionSpecsCache;

    /**
     * Set up before the test.
     */
    @Before
    public void setUp() {
        IdentityGenerator.initPrefix(0);

        atomicActionSpecsCache = new AtomicActionSpecsCache();

        ActionEntity aggregateEntity1 = ActionEntity.newBuilder()
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

        List<ActionDTO.Action> actionList =  Arrays.asList(resize1, resize2, resize3, resize4);

        AtomicActionFactory atomicActionFactory = new AtomicActionFactory(atomicActionSpecsCache);

        List<AtomicActionResult> atomicActions = atomicActionFactory.merge(actionList);
        assertNotNull(atomicActions);
        assertEquals(1, atomicActions.size());
        AtomicActionResult atomicAction = atomicActions.get(0);
        assertEquals(2, atomicAction.marketActions().size());

        List<Long> entitiesWithoutSpecs = Arrays.asList(21L, 22L);
        List<Long> entitiesWithSpecs = Arrays.asList(11L, 12L);
        atomicAction.marketActions().stream()
                .noneMatch(action -> !entitiesWithoutSpecs.contains(action.getInfo().getResize().getTarget().getId()));
        atomicAction.marketActions().stream()
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

        ActionDTO.Action reconfigureActionDto = Action.newBuilder()
                .setId(1)
                .setDeprecatedImportance(0)
                .setInfo(ActionInfo.newBuilder()
                        .setReconfigure(Reconfigure.newBuilder()
                                .setTarget(ActionEntity.newBuilder().setId(21).setType(40))))
                .setExplanation(Explanation.newBuilder()
                        .setReconfigure(ReconfigureExplanation.newBuilder()))
                .build();

        List<ActionDTO.Action> actionList =  Arrays.asList(resize1, resize2, reconfigureActionDto);

        AtomicActionFactory atomicActionFactory = new AtomicActionFactory(atomicActionSpecsCache);

        List<AtomicActionResult> atomicActions = atomicActionFactory.merge(actionList);
        assertEquals(1, atomicActions.size());
        AtomicActionResult atomicAction = atomicActions.get(0);
        assertEquals(2, atomicAction.marketActions().size());

        atomicAction.marketActions().stream()
                .allMatch(action -> action.getInfo().hasResize());
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
}
