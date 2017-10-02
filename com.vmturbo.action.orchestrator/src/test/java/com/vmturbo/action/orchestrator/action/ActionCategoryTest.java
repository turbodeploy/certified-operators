package com.vmturbo.action.orchestrator.action;

import org.junit.Test;

import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ActivateExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.DeactivateExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.MoveExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.MoveExplanation.Compliance;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.MoveExplanation.Congestion;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.MoveExplanation.Evacuation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.MoveExplanation.InitialPlacement;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.MoveExplanation.Performance;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation.ProvisionByDemandExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation.ProvisionBySupplyExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ReconfigureExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ResizeExplanation;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;

import static org.junit.Assert.assertEquals;

public class ActionCategoryTest {
    @Test
    public void testComplianceCategory() {
        Explanation compliance = Explanation.newBuilder()
            .setMove(MoveExplanation.newBuilder()
                .setCompliance(Compliance.newBuilder().build()).build()).build();

        assertEquals(ActionCategory.CATEGORY_COMPLIANCE, ActionCategory.assignActionCategory(compliance));
    }

    @Test
    public void testCongestionCategory() {
        Explanation congestion = Explanation.newBuilder().setMove(MoveExplanation.newBuilder()
            .setCongestion(Congestion.newBuilder()
                .addCongestedCommodities(CommodityType.CPU_VALUE).build())
            .build()).build();

        assertEquals(ActionCategory.CATEGORY_PERFORMANCE_ASSURANCE, ActionCategory.assignActionCategory(congestion));
    }

    @Test
    public void testEvacuationCategory() {
        Explanation evacuation = Explanation.newBuilder().setMove(MoveExplanation.newBuilder()
            .setEvacuation(Evacuation.newBuilder().setSuspendedEntity(100).build())
            .build()).build();

        assertEquals(ActionCategory.CATEGORY_EFFICIENCY_IMPROVEMENT, ActionCategory.assignActionCategory(evacuation));
    }

    @Test
    public void testInitialPlacementCategory() {
        Explanation initialPlacement = Explanation.newBuilder().setMove(MoveExplanation.newBuilder()
            .setInitialPlacement(InitialPlacement.newBuilder().build()).build()).build();

        assertEquals(ActionCategory.CATEGORY_EFFICIENCY_IMPROVEMENT, ActionCategory.assignActionCategory(initialPlacement));
    }

    @Test
    public void testPerformanceCategory() {
        Explanation performance = Explanation.newBuilder().setMove(MoveExplanation.newBuilder()
            .setPerformance(Performance.newBuilder().build()).build()).build();

        assertEquals(ActionCategory.CATEGORY_PREVENTION,
            ActionCategory.assignActionCategory(performance));
    }

    @Test
    public void testReconfigureCategory() {
        Explanation reconfigure = Explanation.newBuilder().setReconfigure(ReconfigureExplanation
            .newBuilder().build()).build();

        assertEquals(ActionCategory.CATEGORY_COMPLIANCE,
            ActionCategory.assignActionCategory(reconfigure));
    }

    @Test
    public void testProvisionByDemandCategory() {
        Explanation provisionByDemand = Explanation.newBuilder().setProvision(ProvisionExplanation.newBuilder()
            .setProvisionByDemandExplanation(ProvisionByDemandExplanation.newBuilder()
                .setBuyerId(101).build())
            .build()).build();

        assertEquals(ActionCategory.CATEGORY_PERFORMANCE_ASSURANCE,
            ActionCategory.assignActionCategory(provisionByDemand));
    }

    @Test
    public void testProvisionBySupplyPerformanceAssurance() {
        Explanation provisionBySupply1 = Explanation.newBuilder().setProvision(ProvisionExplanation
            .newBuilder().setProvisionBySupplyExplanation(ProvisionBySupplyExplanation
                .newBuilder().setMostExpensiveCommodity(102).build())
            .build()).build();

        assertEquals(ActionCategory.CATEGORY_PERFORMANCE_ASSURANCE,
            ActionCategory.assignActionCategory(provisionBySupply1));
    }

    @Test
    public void testProvisionBySupplyCompliance() {
        Explanation provisionBySupply2 = Explanation.newBuilder().setProvision(ProvisionExplanation
            .newBuilder().setProvisionBySupplyExplanation(ProvisionBySupplyExplanation
                .newBuilder()
                .setMostExpensiveCommodity(CommodityType.SEGMENTATION_VALUE)
                .build())
            .build()).build();

        assertEquals(ActionCategory.CATEGORY_COMPLIANCE,
            ActionCategory.assignActionCategory(provisionBySupply2));
    }

    @Test
    public void testActivatePerformanceAssurance() {
        Explanation activate1 = Explanation.newBuilder().setActivate(ActivateExplanation.newBuilder()
            .setMostExpensiveCommodity(103).build()).build();

        assertEquals(ActionCategory.CATEGORY_PERFORMANCE_ASSURANCE,
            ActionCategory.assignActionCategory(activate1));
    }

    @Test
    public void testActivateCompliance() {
        Explanation activate2 = Explanation.newBuilder().setActivate(ActivateExplanation.newBuilder()
            .setMostExpensiveCommodity(CommodityType.DRS_SEGMENTATION_VALUE).build()).build();

        assertEquals(ActionCategory.CATEGORY_COMPLIANCE,
            ActionCategory.assignActionCategory(activate2));
    }

    @Test
    public void testDeactivateCategory() {
        Explanation deactivate = Explanation.newBuilder().setDeactivate(
            DeactivateExplanation.newBuilder().build()).build();

        assertEquals(ActionCategory.CATEGORY_EFFICIENCY_IMPROVEMENT,
            ActionCategory.assignActionCategory(deactivate));
    }

    @Test
    public void testResizeDownCategory() {
        Explanation resizeDown = Explanation.newBuilder().setResize(ResizeExplanation.newBuilder()
            .setStartUtilization(0.1f).setEndUtilization(0.5f).build()).build();

        assertEquals(ActionCategory.CATEGORY_EFFICIENCY_IMPROVEMENT,
            ActionCategory.assignActionCategory(resizeDown));
    }

    @Test
    public void testResizeUpCategory() {
        Explanation resizeUp = Explanation.newBuilder().setResize(ResizeExplanation.newBuilder()
            .setStartUtilization(0.5f).setEndUtilization(0.1f).build()).build();

        assertEquals(ActionCategory.CATEGORY_PERFORMANCE_ASSURANCE,
            ActionCategory.assignActionCategory(resizeUp));
    }

}
