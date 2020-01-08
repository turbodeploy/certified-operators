package com.vmturbo.action.orchestrator.action;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.vmturbo.action.orchestrator.ActionOrchestratorTestUtils;
import org.junit.Test;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionCategory;
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

        assertThat(ActionCategoryExtractor.assignActionCategory(compliance), is(ActionDTO.ActionCategory.COMPLIANCE));
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
        assertThat(ActionCategoryExtractor.assignActionCategory(compoundMoveExplanation), is(ActionDTO.ActionCategory.COMPLIANCE));
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
        assertThat(ActionCategoryExtractor.assignActionCategory(compoundMoveExplanation), is(ActionCategory.PREVENTION));
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

        assertThat(ActionCategoryExtractor.assignActionCategory(congestion), is(ActionCategory.PERFORMANCE_ASSURANCE));
    }

    @Test
    public void testEvacuationCategory() {
        Explanation evacuation = Explanation.newBuilder().setMove(MoveExplanation.newBuilder()
            .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
            .setEvacuation(Evacuation.newBuilder().setSuspendedEntity(100).build())
            .build()).build()).build();

        assertThat(ActionCategoryExtractor.assignActionCategory(evacuation), is(ActionCategory.EFFICIENCY_IMPROVEMENT));
    }

    @Test
    public void testInitialPlacementCategory() {
        Explanation initialPlacement = Explanation.newBuilder().setMove(MoveExplanation.newBuilder()
            .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
            .setInitialPlacement(InitialPlacement.getDefaultInstance()).build()).build()).build();

        assertThat(ActionCategoryExtractor.assignActionCategory(initialPlacement), is(ActionCategory.EFFICIENCY_IMPROVEMENT));
    }

    @Test
    public void testPerformanceCategory() {
        Explanation performance = Explanation.newBuilder().setMove(MoveExplanation.newBuilder()
            .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
            .setPerformance(Performance.getDefaultInstance()).build()).build()).build();

        assertThat(ActionCategoryExtractor.assignActionCategory(performance), is(ActionCategory.PREVENTION));
    }

    @Test
    public void testReconfigureCategory() {
        Explanation reconfigure = Explanation.newBuilder().setReconfigure(ReconfigureExplanation
            .getDefaultInstance()).build();

        assertThat(ActionCategoryExtractor.assignActionCategory(reconfigure), is(ActionCategory.COMPLIANCE));
    }

    @Test
    public void testProvisionByDemandCategory() {
        Explanation provisionByDemand = Explanation.newBuilder().setProvision(ProvisionExplanation.newBuilder()
            .setProvisionByDemandExplanation(ProvisionByDemandExplanation.newBuilder()
                .setBuyerId(101).build())
            .build()).build();

        assertThat(ActionCategoryExtractor.assignActionCategory(provisionByDemand),
                is(ActionCategory.PERFORMANCE_ASSURANCE));
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

        assertThat(ActionCategoryExtractor.assignActionCategory(provisionBySupply1),
                is(ActionCategory.PERFORMANCE_ASSURANCE));
    }

    @Test
    public void testProvisionBySupplyCompliance() {
        Explanation provisionBySupply2 = Explanation.newBuilder().setProvision(ProvisionExplanation
            .newBuilder().setProvisionBySupplyExplanation(ProvisionBySupplyExplanation
                .newBuilder()
                .setMostExpensiveCommodityInfo(ActionOrchestratorTestUtils.createReasonCommodity(CommodityType.SEGMENTATION_VALUE, null))
                .build())
            .build()).build();

        assertThat(ActionCategoryExtractor.assignActionCategory(provisionBySupply2),
                is(ActionCategory.COMPLIANCE));
    }

    @Test
    public void testActivatePerformanceAssurance() {
        Explanation activate1 = Explanation.newBuilder().setActivate(ActivateExplanation.newBuilder()
            .setMostExpensiveCommodity(CPU.getType()).build()).build();

        assertThat(ActionCategoryExtractor.assignActionCategory(activate1),
                is(ActionCategory.PERFORMANCE_ASSURANCE));
    }

    @Test
    public void testActivateCompliance() {
        Explanation activate2 = Explanation.newBuilder().setActivate(ActivateExplanation.newBuilder()
            .setMostExpensiveCommodity(DRS_SEGMENTATION.getType()).build()).build();

        assertThat(ActionCategoryExtractor.assignActionCategory(activate2),
                is(ActionCategory.COMPLIANCE));
    }

    @Test
    public void testDeactivateCategory() {
        Explanation deactivate = Explanation.newBuilder().setDeactivate(
            DeactivateExplanation.getDefaultInstance()).build();

        assertThat(ActionCategoryExtractor.assignActionCategory(deactivate),
            is(ActionCategory.EFFICIENCY_IMPROVEMENT));
    }

    @Test
    public void testDeleteCategory() {
        Explanation delete = Explanation.newBuilder().setDelete(
            DeleteExplanation.getDefaultInstance()).build();

        assertThat(ActionCategoryExtractor.assignActionCategory(delete),
            is(ActionCategory.EFFICIENCY_IMPROVEMENT));
    }

    /**
     * Tests if the category of the accounting actions are efficiency.
     */
    @Test
    public void testAllocateCategory() {
        Explanation allocate = Explanation.newBuilder().setAllocate(
                AllocateExplanation.getDefaultInstance()).build();

        assertThat(ActionCategoryExtractor.assignActionCategory(allocate),
                is(ActionCategory.EFFICIENCY_IMPROVEMENT));
    }

    @Test
    public void testResizeDownCategory() {
        Explanation resizeDown = Explanation.newBuilder().setResize(ResizeExplanation.newBuilder()
            .setStartUtilization(0.1f).setEndUtilization(0.5f).build()).build();

        assertThat(ActionCategoryExtractor.assignActionCategory(resizeDown),
                is(ActionCategory.EFFICIENCY_IMPROVEMENT));
    }

    @Test
    public void testResizeUpCategory() {
        Explanation resizeUp = Explanation.newBuilder().setResize(ResizeExplanation.newBuilder()
            .setStartUtilization(0.5f).setEndUtilization(0.1f).build()).build();

        assertThat(ActionCategoryExtractor.assignActionCategory(resizeUp),
                is(ActionCategory.PERFORMANCE_ASSURANCE));
    }

}
