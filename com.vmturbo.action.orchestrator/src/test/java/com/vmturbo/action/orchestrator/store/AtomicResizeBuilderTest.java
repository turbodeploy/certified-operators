package com.vmturbo.action.orchestrator.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.action.orchestrator.ActionOrchestratorTestUtils;
import com.vmturbo.action.orchestrator.action.AtomicActionSpecsCache;
import com.vmturbo.action.orchestrator.store.AtomicActionFactory.AtomicActionResult;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo.ActionTypeCase;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
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
    private ActionEntity container7;
    private ActionEntity container8;

    private TopologyDTO.CommodityType vcpuType;
    private TopologyDTO.CommodityType vmemType;

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

        vcpuType = TopologyDTO.CommodityType.newBuilder().setType(CommodityType.VCPU_VALUE).build();
        vmemType = TopologyDTO.CommodityType.newBuilder().setType(CommodityType.VMEM_VALUE).build();

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
                .setType(EntityType.CONTAINER_VALUE).setId(11).build();
        container2 = ActionEntity.newBuilder()
                .setType(EntityType.CONTAINER_VALUE).setId(12).build();

        container7 = ActionEntity.newBuilder()
                .setType(EntityType.CONTAINER_VALUE).setId(17).build();
        container8 = ActionEntity.newBuilder()
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

        createResizeActions();
    }

    private ActionDTO.Action resize1;
    private ActionDTO.Action resize2;
    private ActionDTO.Action resize12;
    private ActionDTO.Action resize22;
    private ActionDTO.Action resize4;
    private ActionDTO.Action resize5;
    private ActionDTO.Action resize7;
    private ActionDTO.Action resize8;
    private ActionDTO.Action resize72;
    private ActionDTO.Action resize82;

    private com.vmturbo.action.orchestrator.action.Action view1;
    private com.vmturbo.action.orchestrator.action.Action view2;
    private com.vmturbo.action.orchestrator.action.Action view12;
    private com.vmturbo.action.orchestrator.action.Action view22;
    private com.vmturbo.action.orchestrator.action.Action view4;
    private com.vmturbo.action.orchestrator.action.Action view5;
    private com.vmturbo.action.orchestrator.action.Action view7;
    private com.vmturbo.action.orchestrator.action.Action view8;
    private com.vmturbo.action.orchestrator.action.Action view72;
    private com.vmturbo.action.orchestrator.action.Action view82;

    private void createResizeActions() {
        resize1 = createResizeAction(1, 11, 40, CommodityType.VCPU);
        resize2 = createResizeAction(2, 12, 40, CommodityType.VCPU);

        view1 = Mockito.spy(ActionOrchestratorTestUtils.actionFromRecommendation(resize1, 1));
        view2 = Mockito.spy(ActionOrchestratorTestUtils.actionFromRecommendation(resize2, 1));

        resize12 = createResizeAction(3, 11, 40, CommodityType.VMEM);
        resize22 = createResizeAction(4, 12, 40, CommodityType.VMEM);

        view12 = Mockito.spy(ActionOrchestratorTestUtils.actionFromRecommendation(resize12, 1));
        view22 = Mockito.spy(ActionOrchestratorTestUtils.actionFromRecommendation(resize22, 1));

        resize4 = createResizeAction(5, 14, 40, CommodityType.VCPU);
        resize5 = createResizeAction(6, 15, 40, CommodityType.VCPU);

        view4 = Mockito.spy(ActionOrchestratorTestUtils.actionFromRecommendation(resize4, 1));
        view5 = Mockito.spy(ActionOrchestratorTestUtils.actionFromRecommendation(resize5, 1));

        resize7 = createResizeAction(7, 17, 40, CommodityType.VCPU);
        resize8 = createResizeAction(8, 18, 40, CommodityType.VCPU);

        view7 = Mockito.spy(ActionOrchestratorTestUtils.actionFromRecommendation(resize7, 1));
        view8 = Mockito.spy(ActionOrchestratorTestUtils.actionFromRecommendation(resize8, 1));

        resize72 = createResizeAction(9, 17, 40, CommodityType.VMEM);
        resize82 = createResizeAction(10, 18, 40, CommodityType.VMEM);

        view72 = Mockito.spy(ActionOrchestratorTestUtils.actionFromRecommendation(resize72, 1));
        view82 = Mockito.spy(ActionOrchestratorTestUtils.actionFromRecommendation(resize82, 1));

        when(view1.getMode()).thenReturn(ActionMode.MANUAL);
        when(view2.getMode()).thenReturn(ActionMode.MANUAL);
        when(view12.getMode()).thenReturn(ActionMode.MANUAL);
        when(view22.getMode()).thenReturn(ActionMode.MANUAL);
        when(view4.getMode()).thenReturn(ActionMode.MANUAL);
        when(view5.getMode()).thenReturn(ActionMode.MANUAL);
        when(view7.getMode()).thenReturn(ActionMode.MANUAL);
        when(view8.getMode()).thenReturn(ActionMode.MANUAL);
        when(view72.getMode()).thenReturn(ActionMode.MANUAL);
        when(view82.getMode()).thenReturn(ActionMode.MANUAL);
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

    /**
     * Test that resize for entities belonging to the same de-duplication entity
     * will be merged to one resize object in the AtomicResize.
     *
     */
    @Test
    public void mergeActionsWithSameDeDupTargets() {
        AggregatedAction aggregatedAction = new AggregatedAction(ActionTypeCase.ATOMICRESIZE,
                                                                    aggregateEntity1.getEntity(),
                                                                    aggregateEntity1.getEntityName());
        aggregatedAction.addAction(resize1, Optional.of(deDupEntity1));
        aggregatedAction.addAction(resize2, Optional.of(deDupEntity1));

        aggregatedAction.updateActionView(resize1.getId(), view1);
        aggregatedAction.updateActionView(resize2.getId(), view2);

        AtomicResizeBuilder actionBuilder = new AtomicResizeBuilder(aggregatedAction);
        Optional<AtomicActionResult> atomicActionResult = actionBuilder.build();

        Action atomicAction = atomicActionResult.get().atomicAction();
        assertTrue(atomicAction.getInfo().hasAtomicResize());

        AtomicResize resize = atomicAction.getInfo().getAtomicResize();
        assertEquals(aggregateEntity1.getEntity(), resize.getExecutionTarget());
        assertEquals(1, resize.getResizesCount());

        assertEquals(deDupEntity1.getEntity(), resize.getResizesList().get(0).getTarget());
        assertEquals(vcpuType, resize.getResizesList().get(0).getCommodityType());

        List<ActionEntity> originalTargets = resize.getResizesList().get(0).getSourceEntitiesList();
        List<ActionEntity> expectedOriginalTargets = Arrays.asList(container1, container2);

        assertEquals(expectedOriginalTargets, originalTargets);

        assertTrue(atomicAction.getExplanation().hasAtomicResize());
        AtomicResizeExplanation exp = atomicAction.getExplanation().getAtomicResize();
        assertEquals(1, exp.getPerEntityExplanationCount());

        assertEquals(exp.getPerEntityExplanation(0).getPerCommodityExplanation().getCommodityType(),
                                        vcpuType);

        assertEquals(exp.getPerEntityExplanation(0).getEntityId(), deDupEntity1.getEntityName());
    }

    /**
     * Test that a merged action is not created if all the actions are RECOMMEND only.
     */
    @Test
    public void mergeAllActionsInRecommendMode() {
        when(view1.getMode()).thenReturn(ActionMode.RECOMMEND);
        when(view2.getMode()).thenReturn(ActionMode.RECOMMEND);

        AggregatedAction aggregatedAction = new AggregatedAction(ActionTypeCase.ATOMICRESIZE,
                aggregateEntity1.getEntity(),
                aggregateEntity1.getEntityName());
        aggregatedAction.addAction(resize1, Optional.of(deDupEntity1));
        aggregatedAction.addAction(resize2, Optional.of(deDupEntity1));
        aggregatedAction.updateActionView(resize1.getId(), view1);
        aggregatedAction.updateActionView(resize2.getId(), view2);

        AtomicResizeBuilder actionBuilder = new AtomicResizeBuilder(aggregatedAction);
        Optional<AtomicActionResult> atomicActionResult = actionBuilder.build();
        assertEquals(Optional.empty(), atomicActionResult);
    }

    /**
     * Test that RECOMMEND only actions are not merged.
     *
     */
    @Test
    public void mergeActionsInRecommendMode() {
        when(view1.getMode()).thenReturn(ActionMode.RECOMMEND);

        AggregatedAction aggregatedAction = new AggregatedAction(ActionTypeCase.ATOMICRESIZE,
                aggregateEntity1.getEntity(),
                aggregateEntity1.getEntityName());
        aggregatedAction.addAction(resize1, Optional.of(deDupEntity1));
        aggregatedAction.addAction(resize2, Optional.of(deDupEntity1));
        aggregatedAction.updateActionView(resize1.getId(), view1);
        aggregatedAction.updateActionView(resize2.getId(), view2);

        AtomicResizeBuilder actionBuilder = new AtomicResizeBuilder(aggregatedAction);
        Optional<AtomicActionResult> atomicActionResult = actionBuilder.build();

        Action atomicAction = atomicActionResult.get().atomicAction();
        assertTrue(atomicAction.getInfo().hasAtomicResize());

        AtomicResize resize = atomicAction.getInfo().getAtomicResize();
        assertEquals(aggregateEntity1.getEntity(), resize.getExecutionTarget());
        assertEquals(1, resize.getResizesCount());

        assertEquals(deDupEntity1.getEntity(), resize.getResizesList().get(0).getTarget());
        assertEquals(vcpuType, resize.getResizesList().get(0).getCommodityType());

        List<ActionEntity> originalTargets = resize.getResizesList().get(0).getSourceEntitiesList();
        List<ActionEntity> expectedOriginalTargets = Arrays.asList(container2);

        assertEquals(expectedOriginalTargets, originalTargets);
    }

    /**
     * Test that resize for multiple commodities for entities belonging to the
     * same de-duplication entity will be merged to multiple resize objects in the AtomicResize.
     */
    @Test
    public void mergeActionsWithMultipleCommodityResizes() {
        AggregatedAction aggregatedAction = new AggregatedAction(ActionTypeCase.ATOMICRESIZE,
                aggregateEntity1.getEntity(),
                aggregateEntity1.getEntityName());

        aggregatedAction.addAction(resize1, Optional.of(deDupEntity1));
        aggregatedAction.addAction(resize2, Optional.of(deDupEntity1));
        aggregatedAction.addAction(resize12, Optional.of(deDupEntity1));
        aggregatedAction.addAction(resize22, Optional.of(deDupEntity1));

        aggregatedAction.updateActionView(resize1.getId(), view1);
        aggregatedAction.updateActionView(resize2.getId(), view2);
        aggregatedAction.updateActionView(resize12.getId(), view12);
        aggregatedAction.updateActionView(resize22.getId(), view22);

        AtomicResizeBuilder actionBuilder = new AtomicResizeBuilder(aggregatedAction);
        Optional<AtomicActionResult> atomicActionResult = actionBuilder.build();

        Action atomicAction = atomicActionResult.get().atomicAction();
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

        assertThat(resizeComms, CoreMatchers.hasItems(vcpuType, vmemType));
    }


    /**
     * Test that resize for entities belonging to the multiple de-duplication entities will be
     * merged to one AtomicResize containing resize info for each of the de-duplication entity.
     *
     */
    @Test
    public void mergeActionsWithMultipleDeDupTargets() {
        AggregatedAction aggregatedAction = new AggregatedAction(ActionTypeCase.ATOMICRESIZE,
                aggregateEntity1.getEntity(), aggregateEntity1.getEntityName());

        aggregatedAction.addAction(resize1, Optional.of(deDupEntity1));
        aggregatedAction.addAction(resize2, Optional.of(deDupEntity1));

        aggregatedAction.addAction(resize4, Optional.of(deDupEntity2));
        aggregatedAction.addAction(resize5, Optional.of(deDupEntity2));

        aggregatedAction.updateActionView(resize1.getId(), view1);
        aggregatedAction.updateActionView(resize2.getId(), view2);
        aggregatedAction.updateActionView(resize4.getId(), view4);
        aggregatedAction.updateActionView(resize5.getId(), view5);

        AtomicResizeBuilder actionBuilder = new AtomicResizeBuilder(aggregatedAction);
        Optional<AtomicActionResult> atomicActionResult = actionBuilder.build();

        Action atomicAction = atomicActionResult.get().atomicAction();
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
                vcpuType);

        assertEquals(exp.getPerEntityExplanation(0).getEntityId(), deDupEntity1.getEntityName());

        assertEquals(exp.getPerEntityExplanation(1).getPerCommodityExplanation().getCommodityType(),
                vcpuType);

        assertEquals(exp.getPerEntityExplanation(1).getEntityId(), deDupEntity2.getEntityName());
    }

    /**
     * Test that RECOMMEND only actions for one of the de-duplication target are not merged.
     */
    @Test
    public void mergeActionsWithRecomendOnlyActionForOneDeDupTarget() {
        when(view1.getMode()).thenReturn(ActionMode.RECOMMEND);
        when(view2.getMode()).thenReturn(ActionMode.RECOMMEND);

        AggregatedAction aggregatedAction = new AggregatedAction(ActionTypeCase.ATOMICRESIZE,
                aggregateEntity1.getEntity(), aggregateEntity1.getEntityName());

        aggregatedAction.addAction(resize1, Optional.of(deDupEntity1));
        aggregatedAction.addAction(resize2, Optional.of(deDupEntity1));

        aggregatedAction.addAction(resize4, Optional.of(deDupEntity2));
        aggregatedAction.addAction(resize5, Optional.of(deDupEntity2));

        aggregatedAction.updateActionView(resize1.getId(), view1);
        aggregatedAction.updateActionView(resize2.getId(), view2);
        aggregatedAction.updateActionView(resize4.getId(), view4);
        aggregatedAction.updateActionView(resize5.getId(), view5);

        AtomicResizeBuilder actionBuilder = new AtomicResizeBuilder(aggregatedAction);
        Optional<AtomicActionResult> atomicActionResult = actionBuilder.build();

        Action atomicAction = atomicActionResult.get().atomicAction();
        assertNotNull(atomicAction);
        assertTrue(atomicAction.getInfo().hasAtomicResize());

        AtomicResize resize = atomicAction.getInfo().getAtomicResize();
        assertEquals(aggregateEntity1.getEntity(), resize.getExecutionTarget());
        assertEquals(1, resize.getResizesCount());

        List<ActionEntity> resizeTargets = resize.getResizesList().stream()
                .map(r -> r.getTarget()).collect(Collectors.toList());
        assertThat(resizeTargets, CoreMatchers.hasItems(deDupEntity2.getEntity()));

        assertTrue(atomicAction.getExplanation().hasAtomicResize());
        AtomicResizeExplanation exp = atomicAction.getExplanation().getAtomicResize();
        assertEquals(1, exp.getPerEntityExplanationCount());

        assertEquals(exp.getPerEntityExplanation(0).getPerCommodityExplanation().getCommodityType(),
                vcpuType);

        assertEquals(exp.getPerEntityExplanation(0).getEntityId(), deDupEntity2.getEntityName());
    }

    /**
     * Test atomic resize actions for entities that are not associated with de-duplication target.
     * Multiple commodity resizes for an entity are merged to the entity.
     */
    @Test
    public void mergeUsingSpecsWithoutDeDupTargets() {
        AggregatedAction aggregatedAction = new AggregatedAction(ActionTypeCase.ATOMICRESIZE,
                aggregateEntity3.getEntity(), aggregateEntity3.getEntityName());

        aggregatedAction.addAction(resize7, Optional.empty());
        aggregatedAction.addAction(resize8, Optional.empty());
        aggregatedAction.addAction(resize72, Optional.empty());
        aggregatedAction.addAction(resize82, Optional.empty());

        aggregatedAction.updateActionView(resize7.getId(), view7);
        aggregatedAction.updateActionView(resize8.getId(), view8);
        aggregatedAction.updateActionView(resize72.getId(), view72);
        aggregatedAction.updateActionView(resize82.getId(), view82);

        AtomicResizeBuilder actionBuilder = new AtomicResizeBuilder(aggregatedAction);
        Optional<AtomicActionResult> atomicActionResult = actionBuilder.build();

        Action atomicAction = atomicActionResult.get().atomicAction();
        assertNotNull(atomicAction);
        assertTrue(atomicAction.getInfo().hasAtomicResize());

        AtomicResize resize = atomicAction.getInfo().getAtomicResize();
        assertEquals(aggregateEntity3.getEntity(), resize.getExecutionTarget());
        assertEquals(4, resize.getResizesCount());

        List<ActionEntity> resizeTargets = resize.getResizesList().stream()
                .map(r -> r.getTarget()).collect(Collectors.toList());

        assertThat(resizeTargets, CoreMatchers.hasItems(container7, container8));
    }
}
