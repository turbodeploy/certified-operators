package com.vmturbo.action.orchestrator.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.action.orchestrator.action.AtomicActionSpecsCache;
import com.vmturbo.action.orchestrator.store.AtomicActionFactory.AtomicActionResult;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo.ActionTypeCase;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.action.ActionDTO.AtomicResize;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.AtomicResizeExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ResizeExplanation;
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
 * Unit tests for AtomicResizeBuilder.
 */
public class AtomicResizeBuilderTest {
    private AtomicResizeMerger translator;

    private AtomicActionSpecsCache atomicActionSpecsCache;

    private AtomicActionEntity aggregateEntity1;
    private AtomicActionEntity aggregateEntity3;
    private AtomicActionEntity deDupEntity1;
    private AtomicActionEntity deDupEntity2;
    private ActionEntity container1;
    private ActionEntity container2;

    /**
     * Set up before the test.
     * AggregateEntity1 - {WORKLOAD_CONTROLLER:31}
     * --> Controls deDupEntity1 {CONTAINER_SPEC:41} and deDupEntity2 {CONTAINER_SPEC:42}
     * -------> deDupEntity1 {CONTAINER_SPEC:41} controls containers with ids 11, 12, 13
     * -------> deDupEntity2 {CONTAINER_SPEC:41} controls containers with ids 14, 15, 16
     * AggregateEntity3 - {WORKLOAD_CONTROLLER:33}
     *  * --> Controls containers with ids 17, 18
     */
    @Before
    public void setUp() {
        IdentityGenerator.initPrefix(0);

        atomicActionSpecsCache = new AtomicActionSpecsCache();

        aggregateEntity1 = AtomicActionEntity.newBuilder()
                            .setEntity(ActionEntity.newBuilder()
                                        .setType(EntityType.WORKLOAD_CONTROLLER_VALUE)
                                        .setId(31))
                            .setEntityName("controller1")
                            .build();

        aggregateEntity3 = AtomicActionEntity.newBuilder()
                            .setEntity(ActionEntity.newBuilder()
                                            .setType(EntityType.WORKLOAD_CONTROLLER_VALUE)
                                            .setId(33))
                            .setEntityName("controller3")
                            .build();

        deDupEntity1 = AtomicActionEntity.newBuilder()
                            .setEntity(ActionEntity.newBuilder()
                                            .setType(EntityType.CONTAINER_SPEC_VALUE)
                                            .setId(41))
                            .setEntityName("spec1")
                            .build();

        deDupEntity2 = AtomicActionEntity.newBuilder()
                            .setEntity(ActionEntity.newBuilder()
                            .setType(EntityType.CONTAINER_SPEC_VALUE)
                                        .setId(42))
                                        .setEntityName("spec2")
                            .build();

        container1 = ActionEntity.newBuilder()
                .setType(EntityType.CONTAINER_VALUE).setId(17).build();
        container2 = ActionEntity.newBuilder()
                .setType(EntityType.CONTAINER_VALUE).setId(18).build();

        AtomicActionSpec spec1 = AtomicActionSpec.newBuilder()
                .addAllEntityIds(Arrays.asList(11L, 12L, 13L))
                .setAggregateEntity(aggregateEntity1)
                .setResizeSpec(ResizeMergeSpec.newBuilder()
                        .setDeDuplicationTarget(deDupEntity1)
                        .addCommodityData(CommodityMergeData.newBuilder()
                                .setCommodityType(CommodityType.VCPU))
                        .addCommodityData(CommodityMergeData.newBuilder()
                                .setCommodityType(CommodityType.VMEM)))
                .build();

        AtomicActionSpec spec2 = AtomicActionSpec.newBuilder()
                .addAllEntityIds(Arrays.asList(14L, 15L, 16L))
                .setAggregateEntity(aggregateEntity1)
                .setResizeSpec(ResizeMergeSpec.newBuilder()
                        .setDeDuplicationTarget(deDupEntity2)
                        .addCommodityData(CommodityMergeData.newBuilder()
                                .setCommodityType(CommodityType.VCPU))
                        .addCommodityData(CommodityMergeData.newBuilder()
                                .setCommodityType(CommodityType.VMEM)))
                .build();

        AtomicActionSpec spec3 = AtomicActionSpec.newBuilder()
                .addAllEntityIds(Arrays.asList(17L, 18L, 19L))
                .setAggregateEntity(aggregateEntity3)
                .setResizeSpec(ResizeMergeSpec.newBuilder()
                        .addCommodityData(CommodityMergeData.newBuilder()
                                .setCommodityType(CommodityType.VCPU))
                        .addCommodityData(CommodityMergeData.newBuilder()
                                .setCommodityType(CommodityType.VMEM)))
                .build();

        List<AtomicActionSpec> resizeSpecs = Arrays.asList(spec1, spec2, spec3);

        Map<ActionType, List<AtomicActionSpec>> mergeSpecsInfoMap = new HashMap<>();
        mergeSpecsInfoMap.put(ActionType.RESIZE, resizeSpecs);
        atomicActionSpecsCache.updateAtomicActionSpecsInfo(mergeSpecsInfoMap);
        translator = new AtomicResizeMerger(atomicActionSpecsCache);
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
        final ActionDTO.Action resize1 = createResizeAction(1, 11,
                40, CommodityType.VCPU);
        final ActionDTO.Action resize2 = createResizeAction(2, 12,
                40, CommodityType.VCPU);

        final TopologyDTO.CommodityType commType = TopologyDTO.CommodityType.newBuilder()
                .setType(CommodityType.VCPU_VALUE).build();

        AggregatedAction aggregatedAction = new AggregatedAction(ActionTypeCase.ATOMICRESIZE,
                                                aggregateEntity1.getEntity(),
                                                aggregateEntity1.getEntityName());
        aggregatedAction.addAction(resize1, Optional.of(deDupEntity1));
        aggregatedAction.addAction(resize2, Optional.of(deDupEntity1));

        AtomicResizeBuilder actionBuilder = new AtomicResizeBuilder(aggregatedAction);
        AtomicActionResult atomicActionResult = actionBuilder.build();

        Action atomicAction = atomicActionResult.atomicAction();
        assertTrue(atomicAction.getInfo().hasAtomicResize());

        AtomicResize resize = atomicAction.getInfo().getAtomicResize();
        assertEquals(aggregateEntity1.getEntity(), resize.getExecutionTarget());
        assertEquals(1, resize.getResizesCount());

        assertEquals(deDupEntity1.getEntity(), resize.getResizesList().get(0).getTarget());
        assertEquals(commType, resize.getResizesList().get(0).getCommodityType());

        List<ActionEntity> originalTargets = resize.getResizesList().get(0).getSourceEntitiesList();
        List<ActionEntity> expectedOriginalTargets = Arrays.asList(
                ActionEntity.newBuilder().setType(EntityType.CONTAINER_VALUE).setId(11).build(),
                ActionEntity.newBuilder().setType(EntityType.CONTAINER_VALUE).setId(12).build());

        assertEquals(expectedOriginalTargets, originalTargets);

        assertTrue(atomicAction.getExplanation().hasAtomicResize());
        AtomicResizeExplanation exp = atomicAction.getExplanation().getAtomicResize();
        assertEquals(1, exp.getPerEntityExplanationCount());

        assertEquals(exp.getPerEntityExplanation(0).getPerCommodityExplanation().getCommodityType(),
                commType);

        assertEquals(exp.getPerEntityExplanation(0).getEntityId(), deDupEntity1.getEntityName());
    }

    /**
     * Test that resize for multiple commodities for entities belonging to the
     * same de-duplication entity will be merged to multiple resize objects in the AtomicResize.
     */
    @Test
    public void mergeActionsWithMultipleCommodityResizes() {
        final ActionDTO.Action resize1 = createResizeAction(1, 11,
                40, CommodityType.VCPU);
        final ActionDTO.Action resize2 = createResizeAction(2, 12,
                40, CommodityType.VCPU);
        final ActionDTO.Action resize3 = createResizeAction(1, 11,
                40, CommodityType.VMEM);
        final ActionDTO.Action resize4 = createResizeAction(2, 12,
                40, CommodityType.VMEM);

        final TopologyDTO.CommodityType vCpuComm = TopologyDTO.CommodityType.newBuilder()
                .setType(CommodityType.VCPU_VALUE).build();

        final TopologyDTO.CommodityType vMemComm = TopologyDTO.CommodityType.newBuilder()
                .setType(CommodityType.VMEM_VALUE).build();

        AggregatedAction aggregatedAction = new AggregatedAction(ActionTypeCase.ATOMICRESIZE,
                aggregateEntity1.getEntity(),
                aggregateEntity1.getEntityName());

        aggregatedAction.addAction(resize1, Optional.of(deDupEntity1));
        aggregatedAction.addAction(resize2, Optional.of(deDupEntity1));
        aggregatedAction.addAction(resize3, Optional.of(deDupEntity1));
        aggregatedAction.addAction(resize4, Optional.of(deDupEntity1));

        AtomicResizeBuilder actionBuilder = new AtomicResizeBuilder(aggregatedAction);
        AtomicActionResult atomicActionResult = actionBuilder.build();

        Action atomicAction = atomicActionResult.atomicAction();
        assertTrue(atomicAction.getInfo().hasAtomicResize());

        AtomicResize resize = atomicAction.getInfo().getAtomicResize();
        assertEquals(aggregateEntity1.getEntity(), resize.getExecutionTarget());
        assertEquals(2, resize.getResizesCount());

        resize.getResizesList().stream().forEach(resizeInfo -> {
            assertEquals(deDupEntity1.getEntity(), resizeInfo.getTarget());
        });

        List<TopologyDTO.CommodityType> resizeComms = resize.getResizesList().stream()
                                        .map(resizeInfo -> resizeInfo.getCommodityType())
                                        .collect(Collectors.toList());

        assertThat(resizeComms, CoreMatchers.hasItems(vCpuComm, vMemComm));
    }


    /**
     * Test that resize for entities belonging to the multiple de-duplication entities will be
     * merged to one AtomicResize containing resize info for each of the de-duplication entity.
     *
     */
    @Test
    public void mergeActionsWithMultipleDeDupTargets() {
        final ActionDTO.Action resize1 = createResizeAction(1, 11,
                40, CommodityType.VCPU);
        final ActionDTO.Action resize2 = createResizeAction(2, 12,
                40, CommodityType.VCPU);
        final ActionDTO.Action resize3 = createResizeAction(3, 14,
                40, CommodityType.VCPU);
        final ActionDTO.Action resize4 = createResizeAction(4, 15,
                40, CommodityType.VCPU);

        final TopologyDTO.CommodityType commType = TopologyDTO.CommodityType.newBuilder()
                .setType(CommodityType.VCPU_VALUE).build();

        AggregatedAction aggregatedAction = new AggregatedAction(ActionTypeCase.ATOMICRESIZE,
                aggregateEntity1.getEntity(), aggregateEntity1.getEntityName());

        aggregatedAction.addAction(resize1, Optional.of(deDupEntity1));
        aggregatedAction.addAction(resize2, Optional.of(deDupEntity1));

        aggregatedAction.addAction(resize3, Optional.of(deDupEntity2));
        aggregatedAction.addAction(resize4, Optional.of(deDupEntity2));

        AtomicResizeBuilder actionBuilder = new AtomicResizeBuilder(aggregatedAction);
        AtomicActionResult atomicActionResult = actionBuilder.build();

        Action atomicAction = atomicActionResult.atomicAction();
        assertNotNull(atomicAction);
        assertTrue(atomicAction.getInfo().hasAtomicResize());

        AtomicResize resize = atomicAction.getInfo().getAtomicResize();
        assertEquals(aggregateEntity1.getEntity(), resize.getExecutionTarget());
        assertEquals(2, resize.getResizesCount());

        List<ActionEntity> resizeTargets = resize.getResizesList().stream()
                .map(r -> r.getTarget()).collect(Collectors.toList());
        assertThat(resizeTargets, CoreMatchers.hasItems(deDupEntity1.getEntity(), deDupEntity2.getEntity()));

        assertTrue(atomicAction.getExplanation().hasAtomicResize());
        AtomicResizeExplanation exp = atomicAction.getExplanation().getAtomicResize();
        assertEquals(2, exp.getPerEntityExplanationCount());

        assertEquals(exp.getPerEntityExplanation(0).getPerCommodityExplanation().getCommodityType(),
                commType);

        assertEquals(exp.getPerEntityExplanation(0).getEntityId(), deDupEntity1.getEntityName());

        assertEquals(exp.getPerEntityExplanation(1).getPerCommodityExplanation().getCommodityType(),
                commType);

        assertEquals(exp.getPerEntityExplanation(1).getEntityId(), deDupEntity2.getEntityName());
    }

    /**
     * Test atomic resize actions for entities that are not associated with de-duplication target.
     * Multiple commodity resizes for an entity are merged to the entity.
     */
    @Test
    public void mergeUsingSpecsWithoutDeDupTargets() {
        final ActionDTO.Action resize1 = createResizeAction(1, 17,
                40, CommodityType.VCPU);
        final ActionDTO.Action resize12 = createResizeAction(2, 17,
                40, CommodityType.VMEM);
        final ActionDTO.Action resize2 = createResizeAction(3, 18,
                40, CommodityType.VCPU);
        final ActionDTO.Action resize22 = createResizeAction(4, 18,
                40, CommodityType.VMEM);

        TopologyDTO.CommodityType commType = TopologyDTO.CommodityType.newBuilder()
                .setType(CommodityType.VCPU_VALUE).build();

        AggregatedAction aggregatedAction = new AggregatedAction(ActionTypeCase.ATOMICRESIZE,
                aggregateEntity3.getEntity(), aggregateEntity3.getEntityName());

        aggregatedAction.addAction(resize1, Optional.empty());
        aggregatedAction.addAction(resize2, Optional.empty());
        aggregatedAction.addAction(resize12, Optional.empty());
        aggregatedAction.addAction(resize22, Optional.empty());

        AtomicResizeBuilder actionBuilder = new AtomicResizeBuilder(aggregatedAction);
        AtomicActionResult atomicActionResult = actionBuilder.build();

        Action atomicAction = atomicActionResult.atomicAction();
        assertNotNull(atomicAction);
        assertTrue(atomicAction.getInfo().hasAtomicResize());

        AtomicResize resize = atomicAction.getInfo().getAtomicResize();
        assertEquals(aggregateEntity3.getEntity(), resize.getExecutionTarget());
        assertEquals(4, resize.getResizesCount());

        List<ActionEntity> resizeTargets = resize.getResizesList().stream()
                .map(r -> r.getTarget()).collect(Collectors.toList());

        assertThat(resizeTargets,
                CoreMatchers.hasItems(
                        ActionEntity.newBuilder()
                                .setType(EntityType.CONTAINER_VALUE).setId(17).build(),
                        ActionEntity.newBuilder()
                                .setType(EntityType.CONTAINER_VALUE).setId(18).build()));
    }
}
