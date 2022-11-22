package com.vmturbo.action.orchestrator.action;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.vmturbo.action.orchestrator.ActionOrchestratorTestUtils;
import org.junit.Test;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionCategory;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ActivateExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.AllocateExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.Compliance;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.Congestion;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.Evacuation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.InitialPlacement;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.Performance;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.DeactivateExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.DeleteExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.MoveExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ReasonCommodity;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation.ProvisionByDemandExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation.ProvisionBySupplyExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ReconfigureExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ResizeExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;

public class ActionCategoryExtractorTest {
    private static final TopologyDTO.CommodityType CPU = TopologyDTO.CommodityType.newBuilder()
        .setType(CommodityDTO.CommodityType.CPU_VALUE)
        .build();

    private static final TopologyDTO.CommodityType DRS_SEGMENTATION = TopologyDTO.CommodityType.newBuilder()
        .setType(CommodityDTO.CommodityType.DRS_SEGMENTATION_VALUE)
        .build();

    @Test
    public void testComplianceCategory() {
        Explanation compliance = Explanation.newBuilder()
            .setMove(MoveExplanation.newBuilder()
                .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                .setCompliance(Compliance.getDefaultInstance()).build()).build()).build();

        assertThat(ActionCategoryExtractor.assignActionCategory(ActionDTO.Action.newBuilder()
                .setInfo(ActionInfo.getDefaultInstance()).setId(111l).setDeprecatedImportance(1.0)
                .setExplanation(compliance).build()), is(ActionDTO.ActionCategory.COMPLIANCE));
    }

    /**
     * For a compound move with a primary change provider, we use use that to extract category
     */
    @Test
    public void testCategoryForCompoundMoveWithPrimaryChangeProvider() {
        Explanation compoundMoveExplanation = Explanation.newBuilder()
            .setMove(MoveExplanation.newBuilder()
                .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                    .setPerformance(Performance.getDefaultInstance()).setIsPrimaryChangeProviderExplanation(false))
                .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                    .setCompliance(Compliance.getDefaultInstance()).setIsPrimaryChangeProviderExplanation(true))
                .build()).build();
        assertThat(ActionCategoryExtractor.assignActionCategory(ActionDTO.Action.newBuilder()
                .setInfo(ActionInfo.getDefaultInstance()).setId(111l).setDeprecatedImportance(1.0)
                .setExplanation(compoundMoveExplanation).build()), is(ActionDTO.ActionCategory.COMPLIANCE));
    }

    /**
     * For a compound move without a primary change provider, we use the first change provider to
     * extract category
     */
    @Test
    public void testCategoryForCompoundMoveWithoutPrimaryChangeProvider() {
        Explanation compoundMoveExplanation = Explanation.newBuilder()
            .setMove(MoveExplanation.newBuilder()
                .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                    .setPerformance(Performance.getDefaultInstance()))
                .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                    .setCompliance(Compliance.getDefaultInstance()))
                .build()).build();
        assertThat(ActionCategoryExtractor.assignActionCategory(ActionDTO.Action.newBuilder()
                .setInfo(ActionInfo.getDefaultInstance()).setId(111l).setDeprecatedImportance(1.0)
                .setExplanation(compoundMoveExplanation).build()), is(ActionCategory.PREVENTION));
    }

    @Test
    public void testCongestionCategory() {
        Explanation congestion = Explanation.newBuilder().setMove(MoveExplanation.newBuilder()
                        .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                                        .setCongestion(Congestion.newBuilder()
                                                        .addCongestedCommodities(ReasonCommodity
                                                                        .newBuilder()
                                                                        .setCommodityType(CPU)
                                                                        .build())
                                                        .build())
                                        .build())
                        .build()).build();

        assertThat(ActionCategoryExtractor.assignActionCategory(ActionDTO.Action.newBuilder()
                .setInfo(ActionInfo.getDefaultInstance()).setId(111l).setDeprecatedImportance(1.0)
                .setExplanation(congestion).build()), is(ActionCategory.PERFORMANCE_ASSURANCE));
    }

    @Test
    public void testEvacuationCategory() {
        Explanation evacuation = Explanation.newBuilder().setMove(MoveExplanation.newBuilder()
            .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
            .setEvacuation(Evacuation.newBuilder()
                .setSuspendedEntity(100)
                    .setEvacuationExplanation(ChangeProviderExplanation.EvacuationExplanation
                        .newBuilder()
                            .setSuspension(ChangeProviderExplanation.Suspension.newBuilder().build())
                        .build())
                .build())
            .build()).build()).build();

        assertThat(ActionCategoryExtractor.assignActionCategory(ActionDTO.Action.newBuilder()
                .setInfo(ActionInfo.getDefaultInstance()).setId(111l).setDeprecatedImportance(1.0)
                .setExplanation(evacuation).build()), is(ActionCategory.EFFICIENCY_IMPROVEMENT));
    }

    @Test
    public void testInitialPlacementCategory() {
        Explanation initialPlacement = Explanation.newBuilder().setMove(MoveExplanation.newBuilder()
            .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
            .setInitialPlacement(InitialPlacement.getDefaultInstance()).build()).build()).build();

        assertThat(ActionCategoryExtractor.assignActionCategory(ActionDTO.Action.newBuilder()
                .setInfo(ActionInfo.getDefaultInstance()).setId(111l).setDeprecatedImportance(1.0)
                .setExplanation(initialPlacement).build()), is(ActionCategory.EFFICIENCY_IMPROVEMENT));
    }

    @Test
    public void testPerformanceCategory() {
        Explanation performance = Explanation.newBuilder().setMove(MoveExplanation.newBuilder()
            .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
            .setPerformance(Performance.getDefaultInstance()).build()).build()).build();

        assertThat(ActionCategoryExtractor.assignActionCategory(ActionDTO.Action.newBuilder()
                .setInfo(ActionInfo.getDefaultInstance()).setId(111l).setDeprecatedImportance(1.0)
                .setExplanation(performance).build()), is(ActionCategory.PREVENTION));
    }

    @Test
    public void testReconfigureCategory() {
        Explanation reconfigure = Explanation.newBuilder().setReconfigure(ReconfigureExplanation
            .getDefaultInstance()).build();

        assertThat(ActionCategoryExtractor.assignActionCategory(ActionDTO.Action.newBuilder()
                .setInfo(ActionInfo.getDefaultInstance()).setId(111l).setDeprecatedImportance(1.0)
                .setExplanation(reconfigure).build()), is(ActionCategory.COMPLIANCE));
    }

    @Test
    public void testProvisionByDemandCategory() {
        Explanation provisionByDemand = Explanation.newBuilder().setProvision(ProvisionExplanation.newBuilder()
            .setProvisionByDemandExplanation(ProvisionByDemandExplanation.newBuilder()
                .setBuyerId(101).build())
            .build()).build();

        assertThat(ActionCategoryExtractor.assignActionCategory(ActionDTO.Action.newBuilder()
                .setInfo(ActionInfo.getDefaultInstance()).setId(111l).setDeprecatedImportance(1.0)
                .setExplanation(provisionByDemand).build()), is(ActionCategory.PERFORMANCE_ASSURANCE));
    }

    @Test
    public void testProvisionBySupplyPerformanceAssurance() {
        Explanation provisionBySupply1 = Explanation.newBuilder().setProvision(ProvisionExplanation
            .newBuilder().setProvisionBySupplyExplanation(ProvisionBySupplyExplanation
                .newBuilder().setMostExpensiveCommodityInfo(ReasonCommodity.newBuilder()
                                .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                                .setType(102).build())
                                .build())
                        .build())
            .build()).build();

        assertThat(ActionCategoryExtractor.assignActionCategory(ActionDTO.Action.newBuilder()
                .setInfo(ActionInfo.getDefaultInstance()).setId(111l).setDeprecatedImportance(1.0)
                .setExplanation(provisionBySupply1).build()), is(ActionCategory.PERFORMANCE_ASSURANCE));
    }

    @Test
    public void testProvisionBySupplyCompliance() {
        Explanation provisionBySupply2 = Explanation.newBuilder().setProvision(ProvisionExplanation
            .newBuilder().setProvisionBySupplyExplanation(ProvisionBySupplyExplanation
                .newBuilder()
                .setMostExpensiveCommodityInfo(ActionOrchestratorTestUtils.createReasonCommodity(CommodityType.SEGMENTATION_VALUE, null))
                .build())
            .build()).build();

        assertThat(ActionCategoryExtractor.assignActionCategory(ActionDTO.Action.newBuilder()
                .setInfo(ActionInfo.getDefaultInstance()).setId(111l).setDeprecatedImportance(1.0)
                .setExplanation(provisionBySupply2).build()), is(ActionCategory.COMPLIANCE));
    }

    @Test
    public void testActivatePerformanceAssurance() {
        Explanation activate1 = Explanation.newBuilder().setActivate(ActivateExplanation.newBuilder()
            .setMostExpensiveCommodity(CPU.getType()).build()).build();

        assertThat(ActionCategoryExtractor.assignActionCategory(ActionDTO.Action.newBuilder()
                .setInfo(ActionInfo.getDefaultInstance()).setId(111l).setDeprecatedImportance(1.0)
                .setExplanation(activate1).build()), is(ActionCategory.PERFORMANCE_ASSURANCE));
    }

    @Test
    public void testActivateCompliance() {
        Explanation activate2 = Explanation.newBuilder().setActivate(ActivateExplanation.newBuilder()
            .setMostExpensiveCommodity(DRS_SEGMENTATION.getType()).build()).build();

        assertThat(ActionCategoryExtractor.assignActionCategory(ActionDTO.Action.newBuilder()
                .setInfo(ActionInfo.getDefaultInstance()).setId(111l).setDeprecatedImportance(1.0)
                .setExplanation(activate2).build()), is(ActionCategory.COMPLIANCE));
    }

    @Test
    public void testDeactivateCategory() {
        Explanation deactivate = Explanation.newBuilder().setDeactivate(
            DeactivateExplanation.getDefaultInstance()).build();

        assertThat(ActionCategoryExtractor.assignActionCategory(ActionDTO.Action.newBuilder()
                .setInfo(ActionInfo.getDefaultInstance()).setId(111l).setDeprecatedImportance(1.0)
                .setExplanation(deactivate).build()), is(ActionCategory.EFFICIENCY_IMPROVEMENT));
    }

    @Test
    public void testDeleteCategory() {
        Explanation delete = Explanation.newBuilder().setDelete(
            DeleteExplanation.getDefaultInstance()).build();

        assertThat(ActionCategoryExtractor.assignActionCategory(ActionDTO.Action.newBuilder()
                .setInfo(ActionInfo.getDefaultInstance()).setId(111l).setDeprecatedImportance(1.0)
                .setExplanation(delete).build()), is(ActionCategory.EFFICIENCY_IMPROVEMENT));
    }

    /**
     * Tests if the category of the accounting actions are efficiency.
     */
    @Test
    public void testAllocateCategory() {
        Explanation allocate = Explanation.newBuilder().setAllocate(
                AllocateExplanation.getDefaultInstance()).build();

        assertThat(ActionCategoryExtractor.assignActionCategory(ActionDTO.Action.newBuilder()
                .setInfo(ActionInfo.getDefaultInstance()).setId(111l).setDeprecatedImportance(1.0)
                .setExplanation(allocate).build()), is(ActionCategory.EFFICIENCY_IMPROVEMENT));
    }

    @Test
    public void testResizeDownCategory() {
        ActionDTO.Action resizeDown = ActionDTO.Action.newBuilder()
                .setId(111l).setDeprecatedImportance(1.0)
                .setInfo(ActionInfo.newBuilder().setResize(Resize.newBuilder()
                        .setTarget(ActionEntity.newBuilder().setId(222l).setType(24))
                        .setNewCapacity(10).setOldCapacity(50)))
                .setExplanation(Explanation.newBuilder().setResize(ResizeExplanation.newBuilder()
                        .setDeprecatedStartUtilization(0.1f).setDeprecatedEndUtilization(0.5f)))
                .build();
        assertThat(ActionCategoryExtractor.assignActionCategory(resizeDown),
                is(ActionCategory.EFFICIENCY_IMPROVEMENT));
    }

    @Test
    public void testResizeUpCategory() {
        ActionDTO.Action resizeUp = ActionDTO.Action.newBuilder()
                .setId(111l).setDeprecatedImportance(1.0)
                .setInfo(ActionInfo.newBuilder().setResize(Resize.newBuilder()
                        .setTarget(ActionEntity.newBuilder().setId(222l).setType(24))
                        .setNewCapacity(50).setOldCapacity(10)))
                .setExplanation( Explanation.newBuilder().setResize(ResizeExplanation.newBuilder()
                        .setDeprecatedStartUtilization(0.5f).setDeprecatedEndUtilization(0.1f)))
                .build();
        assertThat(ActionCategoryExtractor.assignActionCategory(resizeUp),
                is(ActionCategory.PERFORMANCE_ASSURANCE));
    }

}
