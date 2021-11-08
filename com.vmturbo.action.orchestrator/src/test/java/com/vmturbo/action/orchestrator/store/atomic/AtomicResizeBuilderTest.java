package com.vmturbo.action.orchestrator.store.atomic;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
import com.vmturbo.action.orchestrator.store.atomic.AtomicActionFactory.AtomicActionResult;
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
    private AtomicActionSpecsCache atomicActionSpecsCache;

    protected AtomicActionEntity aggregateEntity1;
    protected AtomicActionEntity aggregateEntity3;
    protected AtomicActionEntity deDupEntity1;
    protected AtomicActionEntity deDupEntity2;
    protected ActionEntity container1;
    protected ActionEntity container2;
    protected ActionEntity container7;
    protected ActionEntity container8;

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
        Map<Long, AtomicActionSpec> resizeSpecsMap = atomicActionSpecsCache.getAtomicActionsSpec(ActionType.RESIZE);

        createResizeActions();
    }

    protected ActionDTO.Action resize1;
    protected ActionDTO.Action resize2;
    protected ActionDTO.Action resize12;
    protected ActionDTO.Action resize22;
    protected ActionDTO.Action resize4;
    protected ActionDTO.Action resize5;
    protected ActionDTO.Action resize7;
    protected ActionDTO.Action resize8;
    protected ActionDTO.Action resize72;
    protected ActionDTO.Action resize82;

    protected com.vmturbo.action.orchestrator.action.Action view1;
    protected com.vmturbo.action.orchestrator.action.Action view2;
    protected com.vmturbo.action.orchestrator.action.Action view12;
    protected com.vmturbo.action.orchestrator.action.Action view22;
    protected com.vmturbo.action.orchestrator.action.Action view4;
    protected com.vmturbo.action.orchestrator.action.Action view5;
    protected com.vmturbo.action.orchestrator.action.Action view7;
    protected com.vmturbo.action.orchestrator.action.Action view8;
    protected com.vmturbo.action.orchestrator.action.Action view72;
    protected com.vmturbo.action.orchestrator.action.Action view82;

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
        return createResizeAction(actionId, entityId, entityType, commType, 10, 20);
    }

    private static ActionDTO.Action createResizeAction(long actionId, long entityId, int entityType,
                                                       CommodityType commType, float oldCapacity,
                                                       float newCapacity) {
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
                                        .setType(commType.getNumber()))
                                .setOldCapacity(oldCapacity)
                                .setNewCapacity(newCapacity))
                )
                .setExplanation(Explanation.newBuilder()
                        .setResize(resizeExplanation))
                .build();
        return resize;
    }

    /**
     * Test that resize for entities belonging to the same de-duplication entity
     * will be merged to one resize object in the AtomicResize.
     * Executable atomic action will be created since the container resizes are in manual mode.
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

        // Executable atomic action
        assertTrue( atomicActionResult.get().atomicAction().isPresent());
        Action atomicAction = atomicActionResult.get().atomicAction().get();
        assertTrue(atomicAction.getInfo().hasAtomicResize());

        AtomicResize resize = atomicAction.getInfo().getAtomicResize();
        // target for the atomic action
        assertEquals(aggregateEntity1.getEntity(), resize.getExecutionTarget());
        // No of resizes after de-duplication
        assertEquals(1, resize.getResizesCount());
        // target and commodity of the de-duplicated resize
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

        assertEquals(exp.getPerEntityExplanation(0).getTargetId(), deDupEntity1.getEntity().getId());

        // Non-Executable atomic action
        assertFalse( atomicActionResult.get().nonExecutableAtomicAction().isPresent());
    }

    /**
     * Test that the executable merged action is not created if all the actions are in RECOMMEND mode only.
     * Non-Executable atomic action will be created.
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

        // Executable atomic action
        assertEquals(Optional.empty(), atomicActionResult.get().atomicAction());

        // Non-Executable atomic action
        assertTrue(atomicActionResult.get().nonExecutableAtomicAction().isPresent());
        Action nonExecutableAtomicAction = atomicActionResult.get().nonExecutableAtomicAction().get();

        // target for the atomic action
        AtomicResize resize = nonExecutableAtomicAction.getInfo().getAtomicResize();
        assertEquals(aggregateEntity1.getEntity(), resize.getExecutionTarget());
        // No of resizes after de-duplication
        assertEquals(1, resize.getResizesCount());
        // target and commodity of the de-duplicated resize
        assertEquals(deDupEntity1.getEntity(), resize.getResizesList().get(0).getTarget());
        assertEquals(vcpuType, resize.getResizesList().get(0).getCommodityType());
    }

    /**
     * Test that both a executable and non-executable aggregated atomic actions are created
     * when one resize action is in recommended and the other is in manual mode.
     * In this case, the
     * <ul>
     * <li>executable atomicAction will aggregate the 1 resize info for the action in manual mode</li>
     * <li>non-executable atomicAction will aggregate the 1 resize info for the action in recommend mode</li>
     * </ul>
     */
    @Test
    public void mergeActionsWithManualAndRecommend() {
        when(view1.getMode()).thenReturn(ActionMode.RECOMMEND);
        when(view12.getMode()).thenReturn(ActionMode.MANUAL);

        AggregatedAction aggregatedAction = new AggregatedAction(ActionTypeCase.ATOMICRESIZE,
            aggregateEntity1.getEntity(),
            aggregateEntity1.getEntityName());
        aggregatedAction.addAction(resize1, Optional.of(deDupEntity1));
        aggregatedAction.addAction(resize12, Optional.of(deDupEntity1));
        aggregatedAction.updateActionView(resize1.getId(), view1);
        aggregatedAction.updateActionView(resize12.getId(), view12);

        AtomicResizeBuilder actionBuilder = new AtomicResizeBuilder(aggregatedAction);
        Optional<AtomicActionResult> atomicActionResult = actionBuilder.build();

        // Non-Executable atomic action
        // WorkloadController non-executable action has 1 resize for the recommend mode action
        assertTrue(atomicActionResult.get().nonExecutableAtomicAction().isPresent());
        Action nonExecutableAtomicAction = atomicActionResult.get().nonExecutableAtomicAction().get();
        // target for the atomic action
        AtomicResize nonExecutableActionResize = nonExecutableAtomicAction.getInfo().getAtomicResize();
        assertEquals(aggregateEntity1.getEntity(), nonExecutableActionResize.getExecutionTarget());
        // No of resizes after de-duplication
        assertEquals(1, nonExecutableActionResize.getResizesCount());

        // target and commodity of the de-duplicated resize
        ActionDTO.ResizeInfo deDupedResizeInfo = nonExecutableActionResize.getResizesList().get(0);
        assertEquals(deDupEntity1.getEntity(), deDupedResizeInfo.getTarget());
        assertEquals(Arrays.asList(container1), deDupedResizeInfo.getSourceEntitiesList());
        assertEquals(CommodityType.VCPU.getNumber(), deDupedResizeInfo.getCommodityType().getType());

        // Executable atomic action
        // WorkloadController executable action has 1 resize for the non-recommend mode action
        assertTrue(atomicActionResult.get().atomicAction().isPresent());
        Action executableAtomicAction = atomicActionResult.get().atomicAction().get();
        // target for the atomic action
        AtomicResize executableActionResize = executableAtomicAction.getInfo().getAtomicResize();
        assertEquals(aggregateEntity1.getEntity(), executableActionResize.getExecutionTarget());
        // No of resizes after de-duplication
        assertEquals(1, executableActionResize.getResizesCount());

        // target and commodity of the de-duplicated resize
        ActionDTO.ResizeInfo aggregatedResizeInfo = executableActionResize.getResizesList().get(0);
        assertEquals(deDupEntity1.getEntity(), aggregatedResizeInfo.getTarget());
        assertEquals(Arrays.asList(container1), aggregatedResizeInfo.getSourceEntitiesList());
        assertEquals(CommodityType.VMEM.getNumber(), aggregatedResizeInfo.getCommodityType().getType());
    }

    /**
     * Test that RECOMMEND only actions are not merged to the executable atomic action.
     *
     */
    @Test
    public void mergeActionsInRecommendMode() {
        when(view1.getMode()).thenReturn(ActionMode.RECOMMEND);

        AggregatedAction aggregatedAction = new AggregatedAction(ActionTypeCase.ATOMICRESIZE,
                aggregateEntity1.getEntity(), aggregateEntity1.getEntityName());
        aggregatedAction.addAction(resize1, Optional.of(deDupEntity1));
        aggregatedAction.addAction(resize2, Optional.of(deDupEntity2));
        aggregatedAction.updateActionView(resize1.getId(), view1);
        aggregatedAction.updateActionView(resize2.getId(), view2);

        AtomicResizeBuilder actionBuilder = new AtomicResizeBuilder(aggregatedAction);
        Optional<AtomicActionResult> atomicActionResult = actionBuilder.build();

        // Executable atomic action for the deDupEntity2 only
        // WorkloadController executable action has 1 resize for the non-recommend mode action
        assertTrue(atomicActionResult.get().atomicAction().isPresent());
        Action atomicAction = atomicActionResult.get().atomicAction().get();
        assertTrue(atomicAction.getInfo().hasAtomicResize());

        // target for the atomic action
        AtomicResize executableActionResize = atomicAction.getInfo().getAtomicResize();
        assertEquals(aggregateEntity1.getEntity(), executableActionResize.getExecutionTarget());
        assertEquals(1, executableActionResize.getResizesCount());

        // target and commodity of the de-duplicated resize
        ActionDTO.ResizeInfo aggregatedResizeInfo = executableActionResize.getResizesList().get(0);
        assertEquals(deDupEntity2.getEntity(), aggregatedResizeInfo.getTarget());
        assertEquals(vcpuType, aggregatedResizeInfo.getCommodityType());

        assertEquals(Arrays.asList(container2), aggregatedResizeInfo.getSourceEntitiesList());

        // Non-Executable atomic action is present for the 2nd deDupEntity1
        assertTrue( atomicActionResult.get().nonExecutableAtomicAction().isPresent());
    }

    /**
     * Test that resize for different commodities for entities belonging to the
     * same de-duplication entity will be merged to multiple resize objects
     * in one executable AtomicResize.
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

        aggregatedAction.updateActionView(resize1.getId(), view1);      //VCPU
        aggregatedAction.updateActionView(resize2.getId(), view2);      //VCPU
        aggregatedAction.updateActionView(resize12.getId(), view12);    //VMem
        aggregatedAction.updateActionView(resize22.getId(), view22);    //VMem

        AtomicResizeBuilder actionBuilder = new AtomicResizeBuilder(aggregatedAction);
        Optional<AtomicActionResult> atomicActionResult = actionBuilder.build();

        // Executable atomic action
        Action atomicAction = atomicActionResult.get().atomicAction().get();
        assertTrue(atomicAction.getInfo().hasAtomicResize());
        // target for the atomic action
        AtomicResize executableActionResize = atomicAction.getInfo().getAtomicResize();
        assertEquals(aggregateEntity1.getEntity(), executableActionResize.getExecutionTarget());
        // No of resizes is equal to the number of different commodities
        assertEquals(2, executableActionResize.getResizesCount());

        // target and commodity of the de-duplicated resize
        executableActionResize.getResizesList().stream().forEach(resizeInfo -> {
            assertEquals(deDupEntity1.getEntity(), resizeInfo.getTarget());
        });

        List<TopologyDTO.CommodityType> resizeComms = executableActionResize.getResizesList().stream()
                                        .map(resizeInfo -> resizeInfo.getCommodityType())
                                        .collect(Collectors.toList());

        assertThat(resizeComms, CoreMatchers.hasItems(vcpuType, vmemType));

        // Non-Executable atomic action
        assertFalse( atomicActionResult.get().nonExecutableAtomicAction().isPresent());
    }


    /**
     * Test that resize for entities belonging to the multiple de-duplication entities will be
     * merged to one executable AtomicResize containing resize info for each of the de-duplication entity.
     *
     */
    @Test
    public void mergeActionsWithMultipleDeDupTargets() {
        AggregatedAction aggregatedAction = new AggregatedAction(ActionTypeCase.ATOMICRESIZE,
                aggregateEntity1.getEntity(), aggregateEntity1.getEntityName());

        aggregatedAction.addAction(resize1, Optional.of(deDupEntity1)); //VCPU
        aggregatedAction.addAction(resize2, Optional.of(deDupEntity1)); //VCPU

        aggregatedAction.addAction(resize4, Optional.of(deDupEntity2)); //VCPU
        aggregatedAction.addAction(resize5, Optional.of(deDupEntity2)); //VCPU

        aggregatedAction.updateActionView(resize1.getId(), view1);
        aggregatedAction.updateActionView(resize2.getId(), view2);
        aggregatedAction.updateActionView(resize4.getId(), view4);
        aggregatedAction.updateActionView(resize5.getId(), view5);

        AtomicResizeBuilder actionBuilder = new AtomicResizeBuilder(aggregatedAction);
        Optional<AtomicActionResult> atomicActionResult = actionBuilder.build();

        // Executable atomic action
        Action atomicAction = atomicActionResult.get().atomicAction().get();
        assertNotNull(atomicAction);
        assertTrue(atomicAction.getInfo().hasAtomicResize());
        // target for the atomic action
        AtomicResize executableActionResize = atomicAction.getInfo().getAtomicResize();
        assertEquals(aggregateEntity1.getEntity(), executableActionResize.getExecutionTarget());
        // No of resizes is equal to the number of different commodities
        assertEquals(2, executableActionResize.getResizesCount());

        // target and commodity of the de-duplicated resize
        List<ActionEntity> resizeTargets = executableActionResize.getResizesList().stream()
                .map(r -> r.getTarget()).collect(Collectors.toList());
        assertThat(resizeTargets, CoreMatchers.hasItems(deDupEntity1.getEntity(), deDupEntity2.getEntity()));

        assertTrue(atomicAction.getExplanation().hasAtomicResize());
        AtomicResizeExplanation exp = atomicAction.getExplanation().getAtomicResize();
        assertEquals(2, exp.getPerEntityExplanationCount());

        assertEquals(exp.getPerEntityExplanation(0).getPerCommodityExplanation().getCommodityType(),
                vcpuType);
        assertEquals(exp.getPerEntityExplanation(0).getTargetId(), deDupEntity1.getEntity().getId());

        assertEquals(exp.getPerEntityExplanation(1).getPerCommodityExplanation().getCommodityType(),
                vcpuType);
        assertEquals(exp.getPerEntityExplanation(1).getTargetId(), deDupEntity2.getEntity().getId());

        // Non-Executable atomic action
        assertFalse( atomicActionResult.get().nonExecutableAtomicAction().isPresent());
    }

    /**
     * Test that RECOMMEND only actions for one of the de-duplication target are not merged
     * in the executable atomic action.
     * A non-executable atomic action is created for this de-duplication target.
     */
    @Test
    public void mergeActionsWithRecommendOnlyActionForOneDeDupTarget() {
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

        // Non-Executable atomic action for the de-duplication target with non-executable resizes
        // WorkloadController non-executable action has 1 resize for the recommend mode action
        assertTrue(atomicActionResult.get().nonExecutableAtomicAction().isPresent());
        Action nonExecutableAtomicAction = atomicActionResult.get().nonExecutableAtomicAction().get();
        // target for the atomic action
        AtomicResize nonExecutableActionResize = nonExecutableAtomicAction.getInfo().getAtomicResize();
        assertEquals(aggregateEntity1.getEntity(), nonExecutableActionResize.getExecutionTarget());
        // No of resizes after de-duplication
        assertEquals(1, nonExecutableActionResize.getResizesCount());
        // target and commodity of the de-duplicated resize
        ActionDTO.ResizeInfo deDupedResizeInfo = nonExecutableActionResize.getResizesList().get(0);
        assertEquals(deDupEntity1.getEntity(), deDupedResizeInfo.getTarget());
        assertEquals(Arrays.asList(container1, container2), deDupedResizeInfo.getSourceEntitiesList());
        assertEquals(CommodityType.VCPU.getNumber(), deDupedResizeInfo.getCommodityType().getType());

        // Executable atomic action for the de-duplication target with executable resizes
        // WorkloadController executable action has 1 resize for the non-recommend mode action
        assertTrue(atomicActionResult.get().atomicAction().isPresent());
        Action executableAtomicAction = atomicActionResult.get().atomicAction().get();
        // target for the atomic action
        AtomicResize executableActionResize = executableAtomicAction.getInfo().getAtomicResize();
        assertEquals(aggregateEntity1.getEntity(), executableActionResize.getExecutionTarget());
        // No of resizes after de-duplication
        assertEquals(1, executableActionResize.getResizesCount());

        // target and commodity of the de-duplicated resize in the executable action
        List<ActionEntity> resizeTargets = executableActionResize.getResizesList().stream()
                .map(r -> r.getTarget()).collect(Collectors.toList());
        assertThat(resizeTargets, CoreMatchers.hasItems(deDupEntity2.getEntity()));

        assertTrue(executableAtomicAction.getExplanation().hasAtomicResize());
        AtomicResizeExplanation exp = executableAtomicAction.getExplanation().getAtomicResize();
        assertEquals(1, exp.getPerEntityExplanationCount());

        assertEquals(exp.getPerEntityExplanation(0).getPerCommodityExplanation().getCommodityType(),
                vcpuType);

        assertEquals(exp.getPerEntityExplanation(0).getTargetId(), deDupEntity2.getEntity().getId());
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

        // Executable atomic action
        assertTrue(atomicActionResult.get().atomicAction().isPresent());
        Action executableAtomicAction = atomicActionResult.get().atomicAction().get();
        // target for the atomic action
        AtomicResize executableActionResize = executableAtomicAction.getInfo().getAtomicResize();
        assertEquals(aggregateEntity3.getEntity(), executableActionResize.getExecutionTarget());

        // No of resizes after aggregation
        assertEquals(4, executableActionResize.getResizesCount());

        List<ActionEntity> resizeTargets = executableActionResize.getResizesList().stream()
                .map(r -> r.getTarget()).collect(Collectors.toList());

        assertThat(resizeTargets, CoreMatchers.hasItems(container7, container8));
    }

    /**
     * Test AtomicAction new capacity from multiple resize up actions with different new capacity values.
     * Final new capacity of AtomicAction is the closet new capacity (smaller one) to old capacity.
     */
    @Test
    public void testAtomicActionNewCapacityFromDifferentResizeUp() {
        final float oldCapacity = 10;
        final float newCapacitySmall = 20;
        final float newCapacityLarge = 30;

        final ActionDTO.Action resize1 = createResizeAction(11, 1111, EntityType.CONTAINER_VALUE,
            CommodityType.VCPU, oldCapacity, newCapacitySmall);
        final ActionDTO.Action resize2 = createResizeAction(22, 2222, EntityType.CONTAINER_VALUE,
            CommodityType.VCPU, oldCapacity, newCapacityLarge);
        final com.vmturbo.action.orchestrator.action.Action view1 =
            Mockito.spy(ActionOrchestratorTestUtils.actionFromRecommendation(resize1, 1));
        final com.vmturbo.action.orchestrator.action.Action view2 =
            Mockito.spy(ActionOrchestratorTestUtils.actionFromRecommendation(resize1, 1));
        when(view1.getMode()).thenReturn(ActionMode.MANUAL);
        when(view2.getMode()).thenReturn(ActionMode.MANUAL);

        final AggregatedAction aggregatedAction = new AggregatedAction(ActionTypeCase.ATOMICRESIZE,
            aggregateEntity1.getEntity(),
            aggregateEntity1.getEntityName());
        aggregatedAction.addAction(resize1, Optional.of(deDupEntity1));
        aggregatedAction.addAction(resize2, Optional.of(deDupEntity1));
        aggregatedAction.updateActionView(resize1.getId(), view1);
        aggregatedAction.updateActionView(resize2.getId(), view2);

        final AtomicResizeBuilder actionBuilder = new AtomicResizeBuilder(aggregatedAction);
        Optional<AtomicActionResult> atomicActionResult = actionBuilder.build();
        final Action action = atomicActionResult.get().atomicAction().get();
        assertEquals(1, action.getInfo().getAtomicResize().getResizesCount());
        assertEquals(newCapacitySmall, action.getInfo().getAtomicResize().getResizes(0).getNewCapacity(), 0);
    }

    /**
     * Test AtomicAction new capacity from multiple resize down actions with different new capacity values.
     * Final new capacity of AtomicAction is the closet new capacity (larger one) to old capacity.
     */
    @Test
    public void testAtomicActionNewCapacityFromDifferentResizeDown() {
        final float oldCapacity = 30;
        final float newCapacitySmall = 10;
        final float newCapacityLarge = 20;

        final ActionDTO.Action resize1 = createResizeAction(11, 1111, EntityType.CONTAINER_VALUE,
            CommodityType.VCPU, oldCapacity, newCapacitySmall);
        final ActionDTO.Action resize2 = createResizeAction(22, 2222, EntityType.CONTAINER_VALUE,
            CommodityType.VCPU, oldCapacity, newCapacityLarge);
        final com.vmturbo.action.orchestrator.action.Action view1 =
            Mockito.spy(ActionOrchestratorTestUtils.actionFromRecommendation(resize1, 1));
        final com.vmturbo.action.orchestrator.action.Action view2 =
            Mockito.spy(ActionOrchestratorTestUtils.actionFromRecommendation(resize1, 1));
        when(view1.getMode()).thenReturn(ActionMode.MANUAL);
        when(view2.getMode()).thenReturn(ActionMode.MANUAL);

        final AggregatedAction aggregatedAction = new AggregatedAction(ActionTypeCase.ATOMICRESIZE,
            aggregateEntity1.getEntity(),
            aggregateEntity1.getEntityName());
        aggregatedAction.addAction(resize1, Optional.of(deDupEntity1));
        aggregatedAction.addAction(resize2, Optional.of(deDupEntity1));
        aggregatedAction.updateActionView(resize1.getId(), view1);
        aggregatedAction.updateActionView(resize2.getId(), view2);

        final AtomicResizeBuilder actionBuilder = new AtomicResizeBuilder(aggregatedAction);
        Optional<AtomicActionResult> atomicActionResult = actionBuilder.build();
        final Action action = atomicActionResult.get().atomicAction().get();
        assertEquals(1, action.getInfo().getAtomicResize().getResizesCount());
        assertEquals(newCapacityLarge, action.getInfo().getAtomicResize().getResizes(0).getNewCapacity(), 0);
    }
}
