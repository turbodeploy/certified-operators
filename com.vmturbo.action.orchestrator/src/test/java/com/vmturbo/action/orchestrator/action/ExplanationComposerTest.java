package com.vmturbo.action.orchestrator.action;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ActivateExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.Compliance;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.DeactivateExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.MoveExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation.ProvisionBySupplyExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ReconfigureExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ResizeExplanation;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;

/**
 * Tests for Action Explanation generation in the {@link ExplanationComposer} class.
 */
public class ExplanationComposerTest {
    private static final CommodityType MEM = CommodityType.newBuilder()
        .setType(CommodityDTO.CommodityType.MEM_VALUE)
        .build();

    private static final CommodityType CPU = CommodityType.newBuilder()
        .setType(CommodityDTO.CommodityType.CPU_VALUE)
        .build();

    private static final CommodityType SEGMENTATION = CommodityType.newBuilder()
        .setType(CommodityDTO.CommodityType.SEGMENTATION_VALUE)
        .build();

    @Test
    public void testMoveExplanation() throws Exception {
        Explanation compliance = Explanation.newBuilder()
            .setMove(MoveExplanation.newBuilder()
                .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                    .setCompliance(Compliance.newBuilder()
                        .addMissingCommodities(MEM)
                        .addMissingCommodities(CPU).build())
                    .build())
                .build())
            .build();

        assertEquals("Current supplier can not satisfy the request for resource(s) MEM CPU",
            ExplanationComposer.composeExplanation(compliance));
    }

    @Test
    public void testReconfigureExplanation() throws Exception {
        Explanation reconfigure =
            Explanation.newBuilder()
                .setReconfigure(ReconfigureExplanation.newBuilder()
                    .addReconfigureCommodity(SEGMENTATION).build())
                .build();

        assertEquals("Enable supplier to offer requested resource(s) SEGMENTATION",
            ExplanationComposer.composeExplanation(reconfigure));
    }

    @Test
    public void testProvisionExplanation() throws Exception {
        Explanation provision = Explanation.newBuilder().setProvision(ProvisionExplanation
            .newBuilder().setProvisionBySupplyExplanation(ProvisionBySupplyExplanation
                .newBuilder().setMostExpensiveCommodity(21).build())
            .build()).build();

        assertEquals("High utilization on resource(s) MEM", ExplanationComposer.composeExplanation(provision));
    }

    @Test
    public void testResizeExplanation() throws Exception {
        Explanation resize = Explanation.newBuilder()
            .setResize(ResizeExplanation.newBuilder()
                .setStartUtilization(0.2f)
                .setEndUtilization(0.4f).build())
            .build();

        assertEquals("Address the issue of underutilization from 0.2 to 0.4",
            ExplanationComposer.composeExplanation(resize));
    }

    @Test
    public void testActivateExplanation() throws Exception {
        Explanation activate =
            Explanation.newBuilder()
                .setActivate(ActivateExplanation.newBuilder()
                    .setMostExpensiveCommodity(CPU.getType()).build())
                .build();

        assertEquals("Address high utilization of CPU", ExplanationComposer.composeExplanation(activate));
    }

    @Test
    public void testDeactivateExplanation() throws Exception {
        Explanation deactivate = Explanation.newBuilder()
            .setDeactivate(DeactivateExplanation.newBuilder().build()).build();

        assertEquals("Improve infrastructure efficiency", ExplanationComposer.composeExplanation(deactivate));
    }
}
