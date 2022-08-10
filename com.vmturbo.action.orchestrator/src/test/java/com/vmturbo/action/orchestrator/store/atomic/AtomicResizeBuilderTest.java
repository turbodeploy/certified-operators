package com.vmturbo.action.orchestrator.store.atomic;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
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
import com.vmturbo.common.protobuf.action.ActionDTO.BlockedByRelation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.AtomicResizeExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ResizeExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.MarketRelatedAction;
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
    private static final long DEFAULT_BLOB_SIZE = 64000L;

    private AtomicActionSpecsCache atomicActionSpecsCache;

    protected AtomicActionEntity aggregateEntity1;
    protected AtomicActionEntity aggregateEntity3;
    protected AtomicActionEntity deDupEntity1;
    protected AtomicActionEntity deDupEntity2;
    protected ActionEntity container1;
    protected ActionEntity container2;
    protected ActionEntity container3;
    protected ActionEntity container4;
    protected ActionEntity container7;
    protected ActionEntity container8;
    protected ActionEntity namespace1;
    protected ActionEntity namespace2;

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
        container3 = ActionEntity.newBuilder()
                .setType(EntityType.CONTAINER_VALUE).setId(14).build();
        container4 = ActionEntity.newBuilder()
                .setType(EntityType.CONTAINER_VALUE).setId(15).build();

        container7 = ActionEntity.newBuilder()
                .setType(EntityType.CONTAINER_VALUE).setId(17).build();
        container8 = ActionEntity.newBuilder()
                .setType(EntityType.CONTAINER_VALUE).setId(18).build();

        namespace1 = ActionEntity.newBuilder()
                .setType(EntityType.NAMESPACE_VALUE).setId(100).build();
        namespace2 = ActionEntity.newBuilder()
                .setType(EntityType.NAMESPACE_VALUE).setId(101).build();

        // aggregateEntity1 aggregates VCPU and VMEM resizes
        // for 11, 12, 13 by first de-duplicating them to deDupEntity1
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

        // aggregateEntity2 aggregates VCPU and VMEM resizes
        // for 14L, 15L, 16L by first de-duplicating them to deDupEntity2
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

        // aggregateEntity3 aggregates VCPU and VMEM resizes
        // for 17L, 18L, 19L without de-duplication
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
    protected ActionDTO.Action resize100;
    protected ActionDTO.Action resize102;

    protected ActionDTO.Action blockedResize1;
    protected ActionDTO.Action blockedResize2;
    protected ActionDTO.Action blockedResize12;
    protected ActionDTO.Action blockedResize22;
    protected ActionDTO.Action blockedResize4;
    protected ActionDTO.Action blockedResize5;
    protected ActionDTO.Action blockedResize42;
    protected ActionDTO.Action blockedResize52;

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

    protected com.vmturbo.action.orchestrator.action.Action blockedView1;
    protected com.vmturbo.action.orchestrator.action.Action blockedView2;
    protected com.vmturbo.action.orchestrator.action.Action blockedView12;
    protected com.vmturbo.action.orchestrator.action.Action blockedView22;
    protected com.vmturbo.action.orchestrator.action.Action blockedView4;
    protected com.vmturbo.action.orchestrator.action.Action blockedView5;
    protected com.vmturbo.action.orchestrator.action.Action blockedView42;
    protected com.vmturbo.action.orchestrator.action.Action blockedView52;

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

        // Namespace actions - 100 and 101 are namespace entities
        resize100 = createResizeAction(100, 100, EntityType.NAMESPACE_VALUE, CommodityType.VCPU);
        resize102 = createResizeAction(102, 100, EntityType.NAMESPACE_VALUE, CommodityType.VMEM);

        blockedResize1 = createResizeActionWithBlockingAction(1, container1.getId(), 40, CommodityType.VCPU,
                namespace1, 100, CommodityType.VCPU);
        blockedResize2 = createResizeActionWithBlockingAction(2, container2.getId(), 40, CommodityType.VCPU,
                namespace1, 100, CommodityType.VCPU);
        blockedResize12 = createResizeActionWithBlockingAction(3, container1.getId(), 40, CommodityType.VMEM,
                namespace1, 102, CommodityType.VMEM);
        blockedResize22 = createResizeActionWithBlockingAction(4, container2.getId(), 40, CommodityType.VMEM,
                namespace1, 102, CommodityType.VMEM);

        blockedView1 = Mockito.spy(ActionOrchestratorTestUtils.actionFromRecommendation(blockedResize1, 1));
        blockedView2 = Mockito.spy(ActionOrchestratorTestUtils.actionFromRecommendation(blockedResize2, 1));
        blockedView12 = Mockito.spy(ActionOrchestratorTestUtils.actionFromRecommendation(blockedResize12, 1));
        blockedView22 = Mockito.spy(ActionOrchestratorTestUtils.actionFromRecommendation(blockedResize22, 1));

        when(blockedView1.getMode()).thenReturn(ActionMode.MANUAL);
        when(blockedView2.getMode()).thenReturn(ActionMode.MANUAL);
        when(blockedView12.getMode()).thenReturn(ActionMode.MANUAL);
        when(blockedView22.getMode()).thenReturn(ActionMode.MANUAL);

        blockedResize4 = createResizeActionWithBlockingAction(4, container3.getId(), 40, CommodityType.VCPU,
                namespace1, 100, CommodityType.VCPU);
        blockedResize5 = createResizeActionWithBlockingAction(5, container4.getId(), 40, CommodityType.VCPU,
                namespace1, 100, CommodityType.VCPU);
        blockedResize42 = createResizeActionWithBlockingAction(42, container3.getId(), 40, CommodityType.VMEM,
                namespace1, 102, CommodityType.VMEM);
        blockedResize52 = createResizeActionWithBlockingAction(52, container4.getId(), 40, CommodityType.VMEM,
                namespace1, 102, CommodityType.VMEM);

        blockedView4 = Mockito.spy(ActionOrchestratorTestUtils.actionFromRecommendation(blockedResize4, 1));
        blockedView5 = Mockito.spy(ActionOrchestratorTestUtils.actionFromRecommendation(blockedResize5, 1));
        blockedView42 = Mockito.spy(ActionOrchestratorTestUtils.actionFromRecommendation(blockedResize42, 1));
        blockedView52 = Mockito.spy(ActionOrchestratorTestUtils.actionFromRecommendation(blockedResize52, 1));

        when(blockedView4.getMode()).thenReturn(ActionMode.MANUAL);
        when(blockedView5.getMode()).thenReturn(ActionMode.MANUAL);
        when(blockedView42.getMode()).thenReturn(ActionMode.MANUAL);
        when(blockedView52.getMode()).thenReturn(ActionMode.MANUAL);
    }

    private static ActionDTO.Action createResizeActionWithBlockingAction(long actionId,
                                                                         long entityId, int entityType, CommodityType commType,
                                                                         ActionEntity blockingEntity, long blockingActionId,
                                                                         CommodityType blockingComm) {
        ActionDTO.Action.Builder resize
                = createResizeAction(actionId, entityId, entityType, commType, 10, 20);
        resize.setExecutable(false);
        return resize.build();
    }

    private static MarketRelatedAction createMarketRelatedActionForBlockedResize(ActionEntity blockingEntity,
                                                                                 long blockingActionId,
                                                                                CommodityType blockingComm)  {

        MarketRelatedAction ra = ActionDTO.MarketRelatedAction.newBuilder()
                .setActionEntity(blockingEntity)
                .setActionId(blockingActionId)
                .setBlockedByRelation(BlockedByRelation.newBuilder()
                        .setResize(BlockedByRelation.BlockedByResize.newBuilder()
                                .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                                        .setType(blockingComm.getNumber()))))
                .build();
        return ra;
    }

    private static ActionDTO.Action createResizeAction(long actionId,
                                                       long entityId, int entityType,
                                                       CommodityType commType) {
        return createResizeAction(actionId, entityId, entityType, commType, 10, 20).build();
    }

    private static ActionDTO.Action.Builder createResizeAction(long actionId, long entityId, int entityType,
                                                       CommodityType commType, float oldCapacity,
                                                       float newCapacity) {
        ResizeExplanation resizeExplanation = ResizeExplanation.newBuilder()
                .setDeprecatedStartUtilization(20)
                .setDeprecatedEndUtilization(90)
                .build();

        ActionDTO.Action.Builder resize = ActionDTO.Action.newBuilder()
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
                        .setResize(resizeExplanation));
        resize.setExecutable(true);
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
            CommodityType.VCPU, oldCapacity, newCapacitySmall).build();
        final ActionDTO.Action resize2 = createResizeAction(22, 2222, EntityType.CONTAINER_VALUE,
            CommodityType.VCPU, oldCapacity, newCapacityLarge).build();
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
            CommodityType.VCPU, oldCapacity, newCapacitySmall).build();
        final ActionDTO.Action resize2 = createResizeAction(22, 2222, EntityType.CONTAINER_VALUE,
            CommodityType.VCPU, oldCapacity, newCapacityLarge).build();
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

    //
    /**
     * Related actions Test 1 - 1 namespace action blocking one of many CS in a WC , expect one RA in WC.
     */
    @Test
    public void testAtomicActionWithBlockedActionsForOneContainerSpec() {
        AggregatedAction aggregatedAction = aggregateActionWithBlockedActionsForOneContainerSpec();
        AtomicResizeBuilder actionBuilder = new AtomicResizeBuilder(aggregatedAction);
        Optional<AtomicActionResult> atomicActionResult = actionBuilder.build();

        // 1 Namespace action blocking one commodity resize actions for one spec
        // Non-Executable atomic action for the de-duplication target with non-executable resizes
        // WorkloadController non-executable action has 1 resize for the blocked container action
        assertTrue(atomicActionResult.get().nonExecutableAtomicAction().isPresent());
        Map<ActionEntity, List<CommodityType>> expectedDeDupEntityToComms = new HashMap<>();
        List<CommodityType> comms = new ArrayList<>();
        comms.add(CommodityType.VCPU);
        expectedDeDupEntityToComms.put(deDupEntity1.getEntity(), comms);
        assertionsForNonExecutableAction(atomicActionResult.get(),
                aggregateEntity1.getEntity(), 1, expectedDeDupEntityToComms);
        assertionsForOneBlockingAction(atomicActionResult.get(), namespace1, resize100.getId());

        // Executable atomic action for the de-duplication target with executable resizes
        // WorkloadController executable action has 1 resize for the non-blocked container action
        assertTrue(atomicActionResult.get().atomicAction().isPresent());
        assertionsForExecutableAction(atomicActionResult.get(), aggregateEntity1.getEntity(), 1);
    }

    /**
     * Create AggregatedAction for testAtomicActionWithBlockedActionsForOneContainerSpec().
     *
     * @return  AggregatedAction
     */
    private AggregatedAction aggregateActionWithBlockedActionsForOneContainerSpec() {
        // Atomic action with one aggregate and 2 de-dup targets
        AggregatedAction aggregatedAction = new AggregatedAction(ActionTypeCase.ATOMICRESIZE,
                aggregateEntity1.getEntity(), aggregateEntity1.getEntityName());

        aggregatedAction.addAction(blockedResize1, Optional.of(deDupEntity1)); //VCPU
        aggregatedAction.addAction(blockedResize2, Optional.of(deDupEntity1)); //VCPU
        aggregatedAction.addAction(resize4, Optional.of(deDupEntity2)); //VCPU
        aggregatedAction.addAction(resize5, Optional.of(deDupEntity2)); //VCPU

        aggregatedAction.updateActionView(blockedResize1.getId(), blockedView1);
        aggregatedAction.updateActionView(blockedResize2.getId(), blockedView2);
        aggregatedAction.updateActionView(resize4.getId(), view4);
        aggregatedAction.updateActionView(resize5.getId(), view5);

        MarketRelatedAction ra1 = createMarketRelatedActionForBlockedResize(namespace1, resize100.getId(), CommodityType.VCPU_REQUEST_QUOTA);
        List<MarketRelatedAction> raList = new ArrayList<>();
        raList.add(ra1);
        aggregatedAction.setRelatedActions(blockedResize1.getId(),
                ActionDTO.ActionPlan.MarketRelatedActionsList.newBuilder().addAllRelatedActions(raList).build());
        aggregatedAction.setRelatedActions(blockedResize2.getId(),
                ActionDTO.ActionPlan.MarketRelatedActionsList.newBuilder().addAllRelatedActions(raList).build());
        return aggregatedAction;
    }

    /**
     * Related actions Test 2 - 1 namespace action blocking all CS in a WC, expect one RA in WC.
     */
    @Test
    public void testAtomicActionWithBlockedActionsForAllContainerSpecs() {
        AggregatedAction aggregatedAction = aggregateActionWithBlockedActionsForAllContainerSpec();
        AtomicResizeBuilder actionBuilder = new AtomicResizeBuilder(aggregatedAction);
        Optional<AtomicActionResult> atomicActionResult = actionBuilder.build();

        // 1 Namespace action blocking one commodity resize actions for all specs
        // Non-Executable atomic action for the de-duplication target with non-executable resizes
        // WorkloadController non-executable action has 2 resize for the blocked container action
        assertTrue(atomicActionResult.get().nonExecutableAtomicAction().isPresent());
        Map<ActionEntity, List<CommodityType>> expectedDeDupEntityToComms = new HashMap<>();
        List<CommodityType> comms = new ArrayList<>();
        comms.add(CommodityType.VCPU);
        expectedDeDupEntityToComms.put(deDupEntity1.getEntity(), comms);
        expectedDeDupEntityToComms.put(deDupEntity2.getEntity(), comms);
        assertionsForNonExecutableAction(atomicActionResult.get(),
                aggregateEntity1.getEntity(), 2, expectedDeDupEntityToComms);
        assertionsForOneBlockingAction(atomicActionResult.get(), namespace1, resize100.getId());

        assertFalse(atomicActionResult.get().atomicAction().isPresent());
    }

    /**
     * Create AggregatedAction for aggregateActionWithBlockedActionsForAllContainerSpec.
     * @return AggregatedAction
     */
    private AggregatedAction aggregateActionWithBlockedActionsForAllContainerSpec() {
        // Atomic action with one aggregate and 2 de-dup targets
        AggregatedAction aggregatedAction = new AggregatedAction(ActionTypeCase.ATOMICRESIZE,
                aggregateEntity1.getEntity(), aggregateEntity1.getEntityName());

        aggregatedAction.addAction(blockedResize1, Optional.of(deDupEntity1)); //VCPU
        aggregatedAction.addAction(blockedResize2, Optional.of(deDupEntity1)); //VCPU

        aggregatedAction.addAction(blockedResize4, Optional.of(deDupEntity2)); //VCPU
        aggregatedAction.addAction(blockedResize5, Optional.of(deDupEntity2)); //VCPU

        aggregatedAction.updateActionView(blockedResize1.getId(), blockedView1);
        aggregatedAction.updateActionView(blockedResize2.getId(), blockedView2);
        aggregatedAction.updateActionView(blockedResize4.getId(), blockedView4);
        aggregatedAction.updateActionView(blockedResize5.getId(), blockedView5);

        MarketRelatedAction ra1 = createMarketRelatedActionForBlockedResize(namespace1, resize100.getId(),
                                                                                CommodityType.VCPU_REQUEST_QUOTA);
        List<MarketRelatedAction> raList = new ArrayList<>();
        raList.add(ra1);
        aggregatedAction.setRelatedActions(blockedResize1.getId(),
                ActionDTO.ActionPlan.MarketRelatedActionsList.newBuilder().addAllRelatedActions(raList).build());
        aggregatedAction.setRelatedActions(blockedResize2.getId(),
                ActionDTO.ActionPlan.MarketRelatedActionsList.newBuilder().addAllRelatedActions(raList).build());
        aggregatedAction.setRelatedActions(blockedResize4.getId(),
                ActionDTO.ActionPlan.MarketRelatedActionsList.newBuilder().addAllRelatedActions(raList).build());
        aggregatedAction.setRelatedActions(blockedResize5.getId(),
                ActionDTO.ActionPlan.MarketRelatedActionsList.newBuilder().addAllRelatedActions(raList).build());

        return aggregatedAction;
    }

    /**
     * Related actions Test 3 - 2 namespace action blocking one CS in a WC, expect 2 RA in WC.
     */
    @Test
    public void testAtomicActionWithMultipleBlockedActionsForOneContainerSpec() {
        AggregatedAction aggregatedAction = aggregateActionWithMultipleBlockedActionsForOneContainerSpec();
        AtomicResizeBuilder actionBuilder = new AtomicResizeBuilder(aggregatedAction);
        Optional<AtomicActionResult> atomicActionResult = actionBuilder.build();

        // 2 Namespace actions blocking VCPU and VMEM commodity resize actions for one specs
        // 1st Namespace VCPU action blocks all the container VCPU resizes,
        // 2nd Namespace VMEM action blocks all container VMEM resizes
        // Non-Executable atomic action for the de-duplication target with non-executable resizes
        // WorkloadController non-executable action has 2 resize for the blocked container action
        assertTrue(atomicActionResult.get().nonExecutableAtomicAction().isPresent());
        Map<ActionEntity, List<CommodityType>> expectedDeDupEntityToComms = new HashMap<>();
        List<CommodityType> comms = new ArrayList<>();
        comms.add(CommodityType.VCPU);
        comms.add(CommodityType.VMEM);
        expectedDeDupEntityToComms.put(deDupEntity1.getEntity(), comms);
        assertionsForNonExecutableAction(atomicActionResult.get(), aggregateEntity1.getEntity(), 2, expectedDeDupEntityToComms);

        // RA assertions - 2 RelatedActions corresponding to the two resizes for the same namespace
        Action nonExecutableAtomicAction = atomicActionResult.get().nonExecutableAtomicAction().get();
        assertEquals(2, atomicActionResult.get().relatedActions().size()); //nonExecutableAtomicAction.getRelatedActionsCount());
        List<MarketRelatedAction> raList = atomicActionResult.get().relatedActions();
        raList.stream().forEach( ra -> {
            assertEquals(namespace1, ra.getActionEntity());
            assertTrue((ra.getActionId() == resize100.getId())
                    || (ra.getActionId() == resize102.getId()));
        });

        // Executable atomic action
        assertFalse(atomicActionResult.get().atomicAction().isPresent());
    }

    /**
     * Create AggregatedAction for testAtomicActionWithMultipleBlockedActionsForOneContainerSpec.
     *
     * @return AggregatedAction
     */
    private AggregatedAction aggregateActionWithMultipleBlockedActionsForOneContainerSpec() {
        AggregatedAction aggregatedAction = new AggregatedAction(ActionTypeCase.ATOMICRESIZE,
                aggregateEntity1.getEntity(), aggregateEntity1.getEntityName());

        aggregatedAction.addAction(blockedResize1, Optional.of(deDupEntity1));
        aggregatedAction.addAction(blockedResize2, Optional.of(deDupEntity1));
        aggregatedAction.addAction(blockedResize12, Optional.of(deDupEntity1));
        aggregatedAction.addAction(blockedResize22, Optional.of(deDupEntity1));

        aggregatedAction.updateActionView(blockedResize1.getId(), blockedView1);      //VCPU
        aggregatedAction.updateActionView(blockedResize2.getId(), blockedView2);      //VCPU
        aggregatedAction.updateActionView(blockedResize12.getId(), blockedView12);    //VMem
        aggregatedAction.updateActionView(blockedResize22.getId(), blockedView22);    //VMem

        MarketRelatedAction ra1 = createMarketRelatedActionForBlockedResize(namespace1, resize100.getId(),
                CommodityType.VCPU_REQUEST_QUOTA);
        List<MarketRelatedAction> raList1 = new ArrayList<>();
        raList1.add(ra1);
        MarketRelatedAction ra2 = createMarketRelatedActionForBlockedResize(namespace1, resize102.getId(),
                CommodityType.VCPU_REQUEST_QUOTA);
        List<MarketRelatedAction> raList2 = new ArrayList<>();
        raList2.add(ra2);

        ActionDTO.ActionPlan.MarketRelatedActionsList list1
                =  ActionDTO.ActionPlan.MarketRelatedActionsList.newBuilder().addAllRelatedActions(raList1).build();
        ActionDTO.ActionPlan.MarketRelatedActionsList list2
                =  ActionDTO.ActionPlan.MarketRelatedActionsList.newBuilder().addAllRelatedActions(raList2).build();

        aggregatedAction.setRelatedActions(blockedResize1.getId(), list1);
        aggregatedAction.setRelatedActions(blockedResize2.getId(), list1);

        aggregatedAction.setRelatedActions(blockedResize12.getId(), list2);
        aggregatedAction.setRelatedActions(blockedResize22.getId(), list2);


        return aggregatedAction;
    }

    /**
     * Related actions Test 4 - 2 namespace action blocking all CS in a WC, expect 2 RA in WC.
     */
    @Test
    public void testAtomicActionWithMultipleBlockedActionsForAllContainerSpecs() {
        AggregatedAction aggregatedAction = aggregateActionWithMultipleBlockedActionsForAllContainerSpecs();
        AtomicResizeBuilder actionBuilder = new AtomicResizeBuilder(aggregatedAction);
        Optional<AtomicActionResult> atomicActionResult = actionBuilder.build();

        // 2 Namespace actions blocking VCPU and VMEM commodity resize actions for both the specs
        // 1st Namespace VCPU action blocks all the container VCPU resizes,
        // 2nd Namespace VMEM action blocks all container VMEM resizes
        // Non-Executable atomic action for the de-duplication target with non-executable resizes
        // WorkloadController non-executable action has 2 resize for the blocked container action
        assertTrue(atomicActionResult.get().nonExecutableAtomicAction().isPresent());
        Map<ActionEntity, List<CommodityType>> expectedDeDupEntityToComms = new HashMap<>();
        List<CommodityType> comms = new ArrayList<>();
        comms.add(CommodityType.VCPU);
        comms.add(CommodityType.VMEM);
        expectedDeDupEntityToComms.put(deDupEntity1.getEntity(), comms);
        expectedDeDupEntityToComms.put(deDupEntity2.getEntity(), comms);
        assertionsForNonExecutableAction(atomicActionResult.get(), aggregateEntity1.getEntity(), 4, expectedDeDupEntityToComms);

        // RA assertions - 2 RelatedActions corresponding to the two resizes for the same namespace
        assertEquals(2, atomicActionResult.get().relatedActions().size());
        List<MarketRelatedAction> raList = atomicActionResult.get().relatedActions();
        raList.stream().forEach( ra -> {
            assertEquals(namespace1, ra.getActionEntity()); //same namespace
            assertTrue((ra.getActionId() == resize100.getId())
                    || (ra.getActionId() == resize102.getId()));
        });

        // Executable atomic action
        assertFalse(atomicActionResult.get().atomicAction().isPresent());
    }

    /**
     * Create AggregatedAction for testAtomicActionWithMultipleBlockedActionsForAllContainerSpecs.
     *
     * @return AggregatedAction
     */
    private AggregatedAction aggregateActionWithMultipleBlockedActionsForAllContainerSpecs() {
        AggregatedAction aggregatedAction = new AggregatedAction(ActionTypeCase.ATOMICRESIZE,
                aggregateEntity1.getEntity(), aggregateEntity1.getEntityName());

        aggregatedAction.addAction(blockedResize1, Optional.of(deDupEntity1));
        aggregatedAction.addAction(blockedResize2, Optional.of(deDupEntity1));
        aggregatedAction.addAction(blockedResize12, Optional.of(deDupEntity1));
        aggregatedAction.addAction(blockedResize22, Optional.of(deDupEntity1));
        aggregatedAction.addAction(blockedResize4, Optional.of(deDupEntity2)); //VCPU
        aggregatedAction.addAction(blockedResize5, Optional.of(deDupEntity2)); //VCPU
        aggregatedAction.addAction(blockedResize42, Optional.of(deDupEntity2)); //VMem
        aggregatedAction.addAction(blockedResize52, Optional.of(deDupEntity2)); //VMem

        aggregatedAction.updateActionView(blockedResize1.getId(), blockedView1);      //VCPU
        aggregatedAction.updateActionView(blockedResize2.getId(), blockedView2);      //VCPU
        aggregatedAction.updateActionView(blockedResize12.getId(), blockedView12);    //VMem
        aggregatedAction.updateActionView(blockedResize22.getId(), blockedView22);    //VMem
        aggregatedAction.updateActionView(blockedResize4.getId(), blockedView4);    //VCPU
        aggregatedAction.updateActionView(blockedResize5.getId(), blockedView5);    //VCPU
        aggregatedAction.updateActionView(blockedResize42.getId(), blockedView42);    //VMem
        aggregatedAction.updateActionView(blockedResize52.getId(), blockedView52);    //VMem

        MarketRelatedAction ra1 = createMarketRelatedActionForBlockedResize(namespace1, resize100.getId(),
                CommodityType.VCPU_REQUEST_QUOTA);
        List<MarketRelatedAction> raList1 = new ArrayList<>();
        raList1.add(ra1);
        MarketRelatedAction ra2 = createMarketRelatedActionForBlockedResize(namespace1, resize102.getId(),
                CommodityType.VCPU_REQUEST_QUOTA);
        List<MarketRelatedAction> raList2 = new ArrayList<>();
        raList2.add(ra2);
        ActionDTO.ActionPlan.MarketRelatedActionsList list1
                =  ActionDTO.ActionPlan.MarketRelatedActionsList.newBuilder().addAllRelatedActions(raList1).build();
        ActionDTO.ActionPlan.MarketRelatedActionsList list2
                =  ActionDTO.ActionPlan.MarketRelatedActionsList.newBuilder().addAllRelatedActions(raList2).build();

        aggregatedAction.setRelatedActions(blockedResize1.getId(), list1);
        aggregatedAction.setRelatedActions(blockedResize2.getId(), list1);
        aggregatedAction.setRelatedActions(blockedResize12.getId(), list2);
        aggregatedAction.setRelatedActions(blockedResize22.getId(), list2);
        aggregatedAction.setRelatedActions(blockedResize4.getId(), list1);
        aggregatedAction.setRelatedActions(blockedResize5.getId(), list1);
        aggregatedAction.setRelatedActions(blockedResize42.getId(), list2);
        aggregatedAction.setRelatedActions(blockedResize52.getId(), list2);

        return aggregatedAction;
    }

    /**
     * Assertions for non-executable action.
     *
     * @param atomicActionResult    AtomicActionResult
     * @param aggregateEntity       Action entity
     * @param noOfResizes           number of expected resize infos that will be merged
     * @param expectedDeDupEntityToComms   map of de-duplication entity to the list resize commodity types
     */
    private void assertionsForNonExecutableAction(AtomicActionResult atomicActionResult,
                                                  ActionEntity aggregateEntity, int noOfResizes,
                                                  Map<ActionEntity, List<CommodityType>> expectedDeDupEntityToComms) {
        assertTrue(atomicActionResult.nonExecutableAtomicAction().isPresent());
        Action nonExecutableAtomicAction = atomicActionResult.nonExecutableAtomicAction().get();
        // target for the atomic action
        AtomicResize nonExecutableActionResize = nonExecutableAtomicAction.getInfo().getAtomicResize();
        assertEquals(aggregateEntity, nonExecutableActionResize.getExecutionTarget());
        // No of resizes after de-duplication
        assertEquals(noOfResizes, nonExecutableActionResize.getResizesCount());

        // target and commodity of the de-duplicated resize
        Map<ActionEntity, List<CommodityType>> deDupEntityToComms = new HashMap<>();
        nonExecutableActionResize.getResizesList().stream().forEach(resize -> {
            if (!deDupEntityToComms.containsKey(resize.getTarget())) {
                deDupEntityToComms.put(resize.getTarget(), new ArrayList<>());
            }
            List<CommodityType> commTypes = deDupEntityToComms.get(resize.getTarget());
            commTypes.add(CommodityType.forNumber(resize.getCommodityType().getType()));
        });
        expectedDeDupEntityToComms.keySet().stream().forEach(deDupEntity -> {
            assertTrue(deDupEntityToComms.containsKey(deDupEntity));
            assertTrue(expectedDeDupEntityToComms.get(deDupEntity).containsAll(deDupEntityToComms.get(deDupEntity)));
        });
    }

    /**
     * Assertions for executable action.
     *
     * @param atomicActionResult    AtomicActionResult
     * @param aggregateEntity       Action entity
     * @param noOfResizes           number of expected resize infos that will be merged
     */
    private void assertionsForExecutableAction(AtomicActionResult atomicActionResult,
                                                  ActionEntity aggregateEntity, int noOfResizes) {
        Action executableAtomicAction = atomicActionResult.atomicAction().get();
        // target for the atomic action
        AtomicResize executableActionResize = executableAtomicAction.getInfo().getAtomicResize();
        assertEquals(aggregateEntity, executableActionResize.getExecutionTarget());
        // No of resizes after de-duplication
        assertEquals(noOfResizes, executableActionResize.getResizesCount());
    }


    /**
     * Related actions Test 5 - 1 namespace action blocking one CS in more than one WC, expect 1 RA in NS.
     */
    @Test
    public void testAtomicActionWithOneActionBlockingMultipleAggregateActions() {
        MarketRelatedAction ra1 = createMarketRelatedActionForBlockedResize(namespace1, resize100.getId(),
                CommodityType.VCPU_REQUEST_QUOTA);
        List<MarketRelatedAction> raList1 = new ArrayList<>();
        raList1.add(ra1);

        ActionDTO.ActionPlan.MarketRelatedActionsList list1
                =  ActionDTO.ActionPlan.MarketRelatedActionsList.newBuilder().addAllRelatedActions(raList1).build();


        // Atomic action with one aggregate and 2 de-dup targets
        AggregatedAction aggregatedAction1 = new AggregatedAction(ActionTypeCase.ATOMICRESIZE,
                aggregateEntity1.getEntity(), aggregateEntity1.getEntityName());
        aggregatedAction1.addAction(blockedResize1, Optional.of(deDupEntity1)); //VCPU
        aggregatedAction1.updateActionView(blockedResize1.getId(), blockedView1);
        aggregatedAction1.setRelatedActions(blockedResize1.getId(), list1);

        AggregatedAction aggregatedAction2 = new AggregatedAction(ActionTypeCase.ATOMICRESIZE,
                aggregateEntity3.getEntity(), aggregateEntity3.getEntityName());
        aggregatedAction2.addAction(blockedResize2, Optional.of(deDupEntity2)); //VCPU
        aggregatedAction2.updateActionView(blockedResize2.getId(), blockedView2);
        aggregatedAction2.setRelatedActions(blockedResize2.getId(), list1);

        AtomicResizeBuilder actionBuilder1 = new AtomicResizeBuilder(aggregatedAction1);
        Optional<AtomicActionResult> atomicActionResult1 = actionBuilder1.build();
        assertTrue(atomicActionResult1.get().nonExecutableAtomicAction().isPresent());
        // Namespace1::VCPU action blocking this controller action
        assertionsForNonExecutableActionWithOneBlockingAction(atomicActionResult1.get(), aggregateEntity1.getEntity(),
                1, namespace1, resize100.getId());

        AtomicResizeBuilder actionBuilder2 = new AtomicResizeBuilder(aggregatedAction2);
        Optional<AtomicActionResult> atomicActionResult2 = actionBuilder2.build();
        assertTrue(atomicActionResult2.get().nonExecutableAtomicAction().isPresent());
        // Namespace1::VCPU action also blocking this controller action
        assertionsForNonExecutableActionWithOneBlockingAction(atomicActionResult2.get(), aggregateEntity3.getEntity(),
                1, namespace1, resize100.getId());
    }

    /**
     * Assertions for non-executable action with one blocking action.
     *
     * @param atomicActionResult    AtomicActionResult
     * @param aggregateEntity       ActionEntity
     * @param noOfResizes           number of expected merged resizes
     * @param blockingEntity        Blocking ActionEntity
     * @param blockingActionId      Blocking Action Id
     */
    private void assertionsForNonExecutableActionWithOneBlockingAction(AtomicActionResult atomicActionResult,
                                                                       ActionEntity aggregateEntity, int noOfResizes,
                                                                       ActionEntity blockingEntity, long blockingActionId) {
        assertTrue(atomicActionResult.nonExecutableAtomicAction().isPresent());
        Action nonExecutableAtomicAction = atomicActionResult.nonExecutableAtomicAction().get();
        // target for the atomic action
        AtomicResize nonExecutableActionResize = nonExecutableAtomicAction.getInfo().getAtomicResize();
        assertEquals(aggregateEntity, nonExecutableActionResize.getExecutionTarget());
        // No of resizes after de-duplication
        assertEquals(noOfResizes, nonExecutableActionResize.getResizesCount());
        assertionsForOneBlockingAction(atomicActionResult, blockingEntity, blockingActionId);
    }

    /**
     * Assertions for one blocking action.
     *
     * @param atomicActionResult    AtomicActionResult
     * @param blockingEntity        Blocking ActionEntity
     * @param blockingActionId      Blocking Action Id
     */
    private void assertionsForOneBlockingAction(AtomicActionResult atomicActionResult,
                                                ActionEntity blockingEntity, long blockingActionId) {
        Action nonExecutableAtomicAction = atomicActionResult.nonExecutableAtomicAction().get();
        assertEquals(1, atomicActionResult.relatedActions().size());
        MarketRelatedAction ra = atomicActionResult.relatedActions().get(0);
        assertEquals(blockingEntity, ra.getActionEntity());
        assertEquals(blockingActionId, ra.getActionId());
    }

    /**
     * Test that the size of an atomic action DTO is below the 64k limit of a database blob.
     */
    @Test
    public void  testAtomicActionSize() {
        AggregatedAction aggregatedAction = new AggregatedAction(ActionTypeCase.ATOMICRESIZE,
                aggregateEntity1.getEntity(),
                aggregateEntity1.getEntityName());
        for (long l = 1;  l <= 4000; l++) {
            ActionDTO.Action resize = createResizeAction(l, l, 40, CommodityType.VCPU);

            com.vmturbo.action.orchestrator.action.Action view
                    = Mockito.spy(ActionOrchestratorTestUtils.actionFromRecommendation(resize, 1));
            when(view.getMode()).thenReturn(ActionMode.MANUAL);
            aggregatedAction.addAction(resize, Optional.of(deDupEntity1));
            aggregatedAction.updateActionView(resize.getId(), view);
        }

        for (long l = 5001;  l <= 9000; l++) {
            ActionDTO.Action resize = createResizeAction(l, l, 40, CommodityType.VMEM);

            com.vmturbo.action.orchestrator.action.Action view
                    = Mockito.spy(ActionOrchestratorTestUtils.actionFromRecommendation(resize, 1));
            when(view.getMode()).thenReturn(ActionMode.MANUAL);
            aggregatedAction.addAction(resize, Optional.of(deDupEntity1));
            aggregatedAction.updateActionView(resize.getId(), view);
        }

        AtomicResizeBuilder actionBuilder = new AtomicResizeBuilder(aggregatedAction);
        Optional<AtomicActionResult> atomicActionResult = actionBuilder.build();

        // Executable atomic action
        assertTrue( atomicActionResult.get().atomicAction().isPresent());
        Action atomicAction = atomicActionResult.get().atomicAction().get();
        assertTrue(atomicAction.getInfo().hasAtomicResize());

        assertTrue(atomicAction.toByteArray().length < DEFAULT_BLOB_SIZE);
    }
}
