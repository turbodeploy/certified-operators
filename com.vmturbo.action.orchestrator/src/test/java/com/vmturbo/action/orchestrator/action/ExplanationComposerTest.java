package com.vmturbo.action.orchestrator.action;

import static com.vmturbo.common.protobuf.action.ActionDTOUtil.COMMODITY_KEY_SEPARATOR;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.BuyRI;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ActivateExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.Builder;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.BuyRIExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.Compliance;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.Performance;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.DeactivateExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.DeleteExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.MoveExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation.ProvisionBySupplyExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ReasonCommodity;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ReconfigureExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ResizeExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Tests for Action Explanation generation in the {@link ExplanationComposer} class.
 */
public class ExplanationComposerTest {
    private static final ReasonCommodity MEM =
                    createReasonCommodity(CommodityDTO.CommodityType.MEM_VALUE, null);
    private static final ReasonCommodity CPU =
                    createReasonCommodity(CommodityDTO.CommodityType.CPU_VALUE, null);
    private static final ReasonCommodity SEGMENTATION =
                    createReasonCommodity(CommodityDTO.CommodityType.SEGMENTATION_VALUE, null);
    private static final ReasonCommodity NETWORK =
                    createReasonCommodity(CommodityDTO.CommodityType.NETWORK_VALUE, "testNetwork1");

    // sometimes we are creating the key in a particular way: by having a prefix with the name of the
    // commodity type, a separation, and the name of the network itself
    // in the explanation we want to show the name of the network to the user
    private static final String NETWORK_KEY_PREFIX = CommodityDTO.CommodityType.NETWORK.name()
            + COMMODITY_KEY_SEPARATOR;

    private static final ReasonCommodity NETWORK_WITH_PREFIX_IN_KEY =
                    createReasonCommodity(CommodityDTO.CommodityType.NETWORK_VALUE,
                                          NETWORK_KEY_PREFIX + "testNetwork2");

    @Test
    public void testMoveExplanation() {
        ActionDTO.Action action = ActionDTO.Action.newBuilder()
                .setId(0).setInfo(ActionInfo.newBuilder()
                        .setMove(Move.newBuilder()
                                .setTarget(ActionEntity.newBuilder()
                                        .setId(2).setType(EntityType.PHYSICAL_MACHINE.getNumber()))
                                .addChanges(ChangeProvider.newBuilder()
                                    .setSource(ActionEntity.newBuilder()
                                            .setId(1).setType(EntityType.PHYSICAL_MACHINE.getNumber())
                                    )))).setImportance(0)
                .setExplanation(Explanation.newBuilder()
                    .setMove(MoveExplanation.newBuilder()
                        .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                            .setCompliance(Compliance.newBuilder()
                                .addMissingCommodities(MEM)
                                .addMissingCommodities(CPU)))))
                .build();

        assertEquals("(^_^)~{entity:1:displayName:Physical Machine} can not satisfy the request for resource(s) Mem Cpu",
            ExplanationComposer.composeExplanation(action));
    }

    /**
     * For a compound move, we will only use the primary change explanation while composing explanation.
     */
    @Test
    public void testCompoundMoveExplanation() {
        ActionDTO.Action action = ActionDTO.Action.newBuilder()
            .setId(0).setInfo(ActionInfo.newBuilder()
                .setMove(Move.newBuilder()
                    .setTarget(ActionEntity.newBuilder()
                        .setId(1).setType(EntityType.VIRTUAL_MACHINE_VALUE))
                    .addChanges(ChangeProvider.newBuilder()
                        .setSource(ActionEntity.newBuilder().setId(2).setType(EntityType.PHYSICAL_MACHINE_VALUE))
                        .setDestination(ActionEntity.newBuilder().setId(3).setType(EntityType.PHYSICAL_MACHINE_VALUE))
                        )
                    .addChanges(ChangeProvider.newBuilder()
                        .setSource(ActionEntity.newBuilder().setId(4).setType(EntityType.STORAGE_VALUE))
                        .setDestination(ActionEntity.newBuilder().setId(5).setType(EntityType.STORAGE_VALUE))
                    ))).setImportance(0)
            .setExplanation(Explanation.newBuilder()
                .setMove(MoveExplanation.newBuilder()
                    .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                        .setCompliance(Compliance.newBuilder()
                            .addMissingCommodities(MEM)
                            .addMissingCommodities(CPU)).setIsPrimaryChangeProviderExplanation(true))
                    .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                        .setPerformance(Performance.newBuilder()))))
            .build();

        assertEquals("(^_^)~Current supplier can not satisfy the request for resource(s) Mem Cpu",
            ExplanationComposer.composeExplanation(action));
    }

    @Test
    public void testBuyRIExplanation() {
        final Builder validExplanation = Explanation.newBuilder()
                .setBuyRI(BuyRIExplanation.newBuilder()
                        .setCoveredAverageDemand(5f)
                        .setTotalAverageDemand(10f)
                        .build());

        ActionDTO.Action action = ActionDTO.Action.newBuilder()
                .setId(0).setInfo(ActionInfo.getDefaultInstance()).setImportance(0)
                .setExplanation(validExplanation)
                .build();

        assertEquals("Increase RI Coverage by 50.0%.",
                ExplanationComposer.composeExplanation(action));

        final Builder invalidExplanation = Explanation.newBuilder()
                .setBuyRI(BuyRIExplanation.newBuilder()
                        .setCoveredAverageDemand(5f)
                        .setTotalAverageDemand(0f)
                        .build());

        action = ActionDTO.Action.newBuilder()
                .setId(0).setInfo(ActionInfo.getDefaultInstance()).setImportance(0)
                .setExplanation(invalidExplanation)
                .build();

        assertEquals("Invalid total demand.",
                ExplanationComposer.composeExplanation(action));
    }

    @Test
    public void testReconfigureExplanation() {
        ActionDTO.Action reconfigure = ActionDTO.Action.newBuilder()
                .setId(0).setInfo(ActionInfo.getDefaultInstance()).setImportance(0)
                .setExplanation(Explanation.newBuilder()
                    .setReconfigure(ReconfigureExplanation.newBuilder()
                        .addReconfigureCommodity(SEGMENTATION).addReconfigureCommodity(NETWORK)))
                    .build();

        ActionDTO.Action reconfigureWithPrefix = ActionDTO.Action.newBuilder()
                .setId(0).setInfo(ActionInfo.getDefaultInstance()).setImportance(0)
                .setExplanation(Explanation.newBuilder()
                        .setReconfigure(ReconfigureExplanation.newBuilder()
                                        .addReconfigureCommodity(SEGMENTATION)
                                        .addReconfigureCommodity(NETWORK_WITH_PREFIX_IN_KEY)))
                .build();

        assertEquals("Enable supplier to offer requested resource(s) Segmentation, Network " +
                        "testNetwork1",
            ExplanationComposer.composeExplanation(reconfigure));

        assertEquals("Enable supplier to offer requested resource(s) Segmentation, Network " +
                        "testNetwork2",
                ExplanationComposer.composeExplanation(reconfigureWithPrefix));
    }

    @Test
    public void testProvisionExplanation() {
        ActionDTO.Action provision = ActionDTO.Action.newBuilder()
                .setId(0).setInfo(ActionInfo.getDefaultInstance()).setImportance(0)
                .setExplanation(Explanation.newBuilder()
                    .setProvision(ProvisionExplanation.newBuilder()
                            .setProvisionBySupplyExplanation(ProvisionBySupplyExplanation.newBuilder()
                                    .setMostExpensiveCommodity(21))))
                .build();

        assertEquals("Mem congestion", ExplanationComposer.composeExplanation(provision));
    }

    @Test
    public void testResizeUpExplanation() {
        ActionDTO.Action.Builder action = ActionDTO.Action.newBuilder()
                .setId(0).setImportance(0)
                .setInfo(ActionInfo.newBuilder()
                    .setResize(Resize.newBuilder()
                        .setTarget(ActionEntity.newBuilder()
                                .setId(0)
                                .setType(EntityType.VIRTUAL_MACHINE.getNumber()))
                        .setCommodityType(CommodityType.newBuilder()
                                .setType(CommodityDTO.CommodityType.VMEM_VALUE))))
                .setExplanation(Explanation.newBuilder()
                    .setResize(ResizeExplanation.newBuilder()
                        .setStartUtilization(0.2f)
                        .setEndUtilization(0.4f)));

        // test a resize up
        assertEquals("(^_^)~Underutilized Vmem in Virtual Machine {entity:0:displayName:}",
            ExplanationComposer.composeExplanation(action.build()));

        // test a resize down
        action.getExplanationBuilder().getResizeBuilder().setStartUtilization(1).setEndUtilization(0);
        assertEquals("(^_^)~Vmem congestion in Virtual Machine {entity:0:displayName:}",
                ExplanationComposer.composeExplanation(action.build()));
    }

    @Test
    public void testActivateExplanation() {
        ActionDTO.Action activate = ActionDTO.Action.newBuilder()
                .setId(0).setInfo(ActionInfo.getDefaultInstance()).setImportance(0)
                .setExplanation(Explanation.newBuilder()
                .setActivate(ActivateExplanation.newBuilder()
                    .setMostExpensiveCommodity(CPU.getCommodityType().getType())))
                .build();

        assertEquals("Address high utilization of CPU", ExplanationComposer.composeExplanation(activate));
    }

    @Test
    public void testDeactivateExplanation() {
        ActionDTO.Action deactivate = ActionDTO.Action.newBuilder()
            .setId(0).setInfo(ActionInfo.getDefaultInstance()).setImportance(0)
            .setExplanation(Explanation.newBuilder()
                .setDeactivate(DeactivateExplanation.getDefaultInstance()))
            .build();

        assertEquals("Improve infrastructure efficiency", ExplanationComposer.composeExplanation(deactivate));
    }

    @Test
    public void testDeleteExplanation() {
        ActionDTO.Action delete = ActionDTO.Action.newBuilder()
            .setId(0).setInfo(ActionInfo.getDefaultInstance()).setImportance(0)
            .setExplanation(Explanation.newBuilder()
                .setDelete(DeleteExplanation.getDefaultInstance()))
            .build();

        assertEquals("Idle or non-productive",
            ExplanationComposer.composeExplanation(delete));
    }

    private static ReasonCommodity createReasonCommodity(int baseType, String key) {
        CommodityType.Builder ct = TopologyDTO.CommodityType.newBuilder()
                        .setType(baseType);
        if (key != null) {
            ct.setKey(key);
        }
        return ReasonCommodity.newBuilder().setCommodityType(ct.build()).build();
    }
}
