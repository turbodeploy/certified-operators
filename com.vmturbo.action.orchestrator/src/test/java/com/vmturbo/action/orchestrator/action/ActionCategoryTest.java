package com.vmturbo.action.orchestrator.action;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ActivateExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.Compliance;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.Congestion;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.Evacuation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.InitialPlacement;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.Performance;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.DeactivateExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.MoveExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation.ProvisionByDemandExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation.ProvisionBySupplyExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ReconfigureExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ResizeExplanation;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;

public class ActionCategoryTest {
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

        assertEquals(ActionCategory.CATEGORY_COMPLIANCE, ActionCategory.assignActionCategory(compliance));
    }

    @Test
    public void testCongestionCategory() {
        Explanation congestion = Explanation.newBuilder().setMove(MoveExplanation.newBuilder()
            .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
            .setCongestion(Congestion.newBuilder()
                .addCongestedCommodities(CPU).build())
            .build()).build()).build();

        assertEquals(ActionCategory.CATEGORY_PERFORMANCE_ASSURANCE, ActionCategory.assignActionCategory(congestion));
    }

    @Test
    public void testEvacuationCategory() {
        Explanation evacuation = Explanation.newBuilder().setMove(MoveExplanation.newBuilder()
            .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
            .setEvacuation(Evacuation.newBuilder().setSuspendedEntity(100).build())
            .build()).build()).build();

        assertEquals(ActionCategory.CATEGORY_EFFICIENCY_IMPROVEMENT, ActionCategory.assignActionCategory(evacuation));
    }

    @Test
    public void testInitialPlacementCategory() {
        Explanation initialPlacement = Explanation.newBuilder().setMove(MoveExplanation.newBuilder()
            .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
            .setInitialPlacement(InitialPlacement.getDefaultInstance()).build()).build()).build();

        assertEquals(ActionCategory.CATEGORY_EFFICIENCY_IMPROVEMENT, ActionCategory.assignActionCategory(initialPlacement));
    }

    @Test
    public void testPerformanceCategory() {
        Explanation performance = Explanation.newBuilder().setMove(MoveExplanation.newBuilder()
            .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
            .setPerformance(Performance.getDefaultInstance()).build()).build()).build();

        assertEquals(ActionCategory.CATEGORY_PREVENTION,
            ActionCategory.assignActionCategory(performance));
    }

    @Test
    public void testReconfigureCategory() {
        Explanation reconfigure = Explanation.newBuilder().setReconfigure(ReconfigureExplanation
            .getDefaultInstance()).build();

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
            .setMostExpensiveCommodity(CPU.getType()).build()).build();

        assertEquals(ActionCategory.CATEGORY_PERFORMANCE_ASSURANCE,
            ActionCategory.assignActionCategory(activate1));
    }

    @Test
    public void testActivateCompliance() {
        Explanation activate2 = Explanation.newBuilder().setActivate(ActivateExplanation.newBuilder()
            .setMostExpensiveCommodity(DRS_SEGMENTATION.getType()).build()).build();

        assertEquals(ActionCategory.CATEGORY_COMPLIANCE,
            ActionCategory.assignActionCategory(activate2));
    }

    @Test
    public void testDeactivateCategory() {
        Explanation deactivate = Explanation.newBuilder().setDeactivate(
            DeactivateExplanation.getDefaultInstance()).build();

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
