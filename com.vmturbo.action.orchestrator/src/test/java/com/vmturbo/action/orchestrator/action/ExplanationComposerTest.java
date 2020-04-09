package com.vmturbo.action.orchestrator.action;

import static com.vmturbo.common.protobuf.action.ActionDTOUtil.COMMODITY_KEY_SEPARATOR;
import static org.junit.Assert.assertEquals;

import java.util.Collection;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.junit.Test;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.Allocate;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Delete;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ActivateExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.AllocateExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.Builder;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.BuyRIExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.Compliance;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.Congestion;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.Evacuation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.Performance;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.DeactivateExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.DeleteExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.MoveExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation.ProvisionByDemandExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation.ProvisionByDemandExplanation.CommodityMaxAmountAvailableEntry;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation.ProvisionBySupplyExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ReasonCommodity;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ReasonCommodity.TimeSlotReasonInformation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ReconfigureExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ResizeExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.action.ActionDTO.Provision;
import com.vmturbo.common.protobuf.action.ActionDTO.Reconfigure;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
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
    public void testMoveComplianceReasonCommodityExplanation() {
        ActionDTO.Action action = ActionDTO.Action.newBuilder()
                .setId(0).setInfo(ActionInfo.newBuilder()
                        .setMove(Move.newBuilder()
                                .setTarget(ActionEntity.newBuilder()
                                        .setId(2).setType(EntityType.PHYSICAL_MACHINE.getNumber()))
                                .addChanges(ChangeProvider.newBuilder()
                                    .setSource(ActionEntity.newBuilder()
                                            .setId(1).setType(EntityType.PHYSICAL_MACHINE.getNumber())
                                    )))).setDeprecatedImportance(0)
                .setExplanation(Explanation.newBuilder()
                    .setMove(MoveExplanation.newBuilder()
                        .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                            .setCompliance(Compliance.newBuilder()
                                .addMissingCommodities(MEM)
                                .addMissingCommodities(CPU)))))
                .build();

        assertEquals("(^_^)~{entity:1:displayName:Physical Machine} can not satisfy the request for resource(s) Mem, CPU",
            ExplanationComposer.composeExplanation(action));
            assertEquals("Current supplier can not satisfy the request for resource(s) Mem, CPU",
            ExplanationComposer.shortExplanation(action));
    }

    /**
     * Test congestion explanation.
     */
    @Test
    public void testMoveCongestionReasonCommodity() {
        final ActionInfo actionInfo = ActionInfo.newBuilder()
                .setMove(Move.newBuilder()
                    .setTarget(ActionEntity.newBuilder()
                        .setId(2).setType(EntityType.DESKTOP_POOL_VALUE))
                    .addChanges(ChangeProvider.newBuilder()
                        .setSource(ActionEntity.newBuilder()
                            .setId(1).setType(EntityType.DESKTOP_POOL_VALUE)
                        )))
            .build();

        // commodity with no time slots
        Explanation explanation = createMoveExplanationWithCongestion(ImmutableList.of(MEM, CPU));
        ActionDTO.Action action = createAction(actionInfo, explanation);
        assertEquals("(^_^)~Mem, CPU congestion",
            ExplanationComposer.composeExplanation(action));
        assertEquals("Mem, CPU congestion",
            ExplanationComposer.shortExplanation(action));

        final ReasonCommodity tsCommoditySlot0Total6 = createReasonCommodity(CommodityDTO.CommodityType.POOL_CPU_VALUE,
            null, 0, 6);
        explanation = createMoveExplanationWithCongestion(ImmutableList.of(MEM, tsCommoditySlot0Total6));
        action = createAction(actionInfo, explanation);

        assertEquals("(^_^)~Mem, Pool CPU at 12:00 AM - 04:00 AM congestion",
            ExplanationComposer.composeExplanation(action));
        assertEquals("Mem, Pool CPU at 12:00 AM - 04:00 AM congestion",
            ExplanationComposer.shortExplanation(action));

        final ReasonCommodity tsCommoditySlot1Total3 = createReasonCommodity(CommodityDTO.CommodityType.POOL_CPU_VALUE,
            null, 1, 3);
        explanation = createMoveExplanationWithCongestion(ImmutableList.of(MEM, tsCommoditySlot1Total3));
        action = createAction(actionInfo, explanation);

        assertEquals("(^_^)~Mem, Pool CPU at 08:00 AM - 04:00 PM congestion",
            ExplanationComposer.composeExplanation(action));
        assertEquals("Mem, Pool CPU at 08:00 AM - 04:00 PM congestion",
            ExplanationComposer.shortExplanation(action));

        final ReasonCommodity tsInvalidSlot = createReasonCommodity(CommodityDTO.CommodityType.POOL_CPU_VALUE,
            null, -1, 3);
        explanation = createMoveExplanationWithCongestion(ImmutableList.of(MEM, tsInvalidSlot));
        action = createAction(actionInfo, explanation);
        assertEquals("(^_^)~Mem, Pool CPU congestion",
            ExplanationComposer.composeExplanation(action));
        assertEquals("Mem, Pool CPU congestion",
            ExplanationComposer.shortExplanation(action));

        final ReasonCommodity tsInvalidTotalSlotNumber = createReasonCommodity(CommodityDTO.CommodityType.POOL_CPU_VALUE,
            null, -0, 0);
        explanation = createMoveExplanationWithCongestion(ImmutableList.of(MEM, tsInvalidTotalSlotNumber));
        action = createAction(actionInfo, explanation);
        assertEquals("(^_^)~Mem, Pool CPU congestion",
            ExplanationComposer.composeExplanation(action));
        assertEquals("Mem, Pool CPU congestion",
            ExplanationComposer.shortExplanation(action));
    }

    @Test
    public void testMoveComplianceReasonSettingsExplanation() {
        long reasonSetting1 = 1L;
        long reasonSetting2 = 2L;

        ActionDTO.Action moveAction = ActionDTO.Action.newBuilder()
            .setId(0).setInfo(ActionInfo.newBuilder().setMove(
                Move.newBuilder().setTarget(ActionEntity.newBuilder()
                    .setId(1).setType(EntityType.VIRTUAL_MACHINE_VALUE))))
            .setDeprecatedImportance(0)
            .setExplanation(Explanation.newBuilder()
                .setMove(MoveExplanation.newBuilder()
                    .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                        .setIsPrimaryChangeProviderExplanation(true)
                        .setCompliance(Compliance.newBuilder()
                            .addReasonSettings(reasonSetting1)
                            .addReasonSettings(reasonSetting2)))))
            .build();

        assertEquals("(^_^)~{entity:1:displayName:Virtual Machine} doesn't comply with setting1, setting2",
            ExplanationComposer.composeExplanation(moveAction,
                ImmutableMap.of(reasonSetting1, "setting1", reasonSetting2, "setting2")));
        assertEquals("Current entity doesn't comply with setting",
            ExplanationComposer.shortExplanation(moveAction));
    }

    /**
     * Test the explanation of move action due to evacuation.
     */
    @Test
    public void testMoveEvacuationSuspensionExplanation() {
        ActionDTO.Action action = ActionDTO.Action.newBuilder()
            .setId(0).setDeprecatedImportance(0).setInfo(ActionInfo.newBuilder()
                .setMove(Move.newBuilder()
                    .setTarget(ActionEntity.newBuilder()
                        .setId(1).setType(EntityType.VIRTUAL_MACHINE.getNumber()))))
            .setExplanation(Explanation.newBuilder()
                .setMove(MoveExplanation.newBuilder()
                    .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                        .setEvacuation(Evacuation.newBuilder()
                            .setSuspendedEntity(2)))))
            .build();

        assertEquals("(^_^)~{entity:2:displayName:Current supplier} can be suspended to improve efficiency",
            ExplanationComposer.composeExplanation(action));
        assertEquals("Current supplier can be suspended to improve efficiency",
            ExplanationComposer.shortExplanation(action));
    }

    @Test
    public void testMoveEvacuationAvailabilityExplanation() {
        ActionDTO.Action action = ActionDTO.Action.newBuilder()
            .setId(0).setDeprecatedImportance(0).setInfo(ActionInfo.newBuilder()
                .setMove(Move.newBuilder()
                    .setTarget(ActionEntity.newBuilder()
                        .setId(1).setType(EntityType.VIRTUAL_MACHINE.getNumber()))))
            .setExplanation(Explanation.newBuilder()
                .setMove(MoveExplanation.newBuilder()
                    .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                        .setEvacuation(Evacuation.newBuilder()
                            .setSuspendedEntity(2)
                            .setIsAvailable(false)))))
            .build();

        assertEquals("(^_^)~{entity:2:displayName:Current supplier} is not available",
            ExplanationComposer.composeExplanation(action));
        assertEquals("Current supplier is not available",
            ExplanationComposer.shortExplanation(action));
    }

    @Test
    public void testMoveInitialPlacementExplanation() {
        ActionDTO.Action action = ActionDTO.Action.newBuilder()
            .setId(0).setDeprecatedImportance(0).setInfo(ActionInfo.newBuilder()
                .setMove(Move.newBuilder()
                    .setTarget(ActionEntity.newBuilder()
                        .setId(1).setType(EntityType.VIRTUAL_MACHINE.getNumber()))))
            .setExplanation(Explanation.newBuilder()
                .setMove(MoveExplanation.newBuilder()
                    .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                        .setInitialPlacement(
                            ChangeProviderExplanation.InitialPlacement.getDefaultInstance()))))
            .build();

        assertEquals("Improve overall performance",
            ExplanationComposer.composeExplanation(action));
        assertEquals("Improve overall performance",
            ExplanationComposer.shortExplanation(action));
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
                    ))).setDeprecatedImportance(0)
            .setExplanation(Explanation.newBuilder()
                .setMove(MoveExplanation.newBuilder()
                    .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                        .setCompliance(Compliance.newBuilder()
                            .addMissingCommodities(MEM)
                            .addMissingCommodities(CPU)).setIsPrimaryChangeProviderExplanation(true))
                    .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                        .setPerformance(Performance.newBuilder()))))
            .build();

        assertEquals("(^_^)~Current supplier can not satisfy the request for resource(s) Mem, CPU",
            ExplanationComposer.composeExplanation(action));
        assertEquals("Current supplier can not satisfy the request for resource(s) Mem, CPU",
            ExplanationComposer.shortExplanation(action));
    }

    @Test
    public void testBuyRIExplanation() {
        final Builder validExplanation = Explanation.newBuilder()
                .setBuyRI(BuyRIExplanation.newBuilder()
                        .setCoveredAverageDemand(5f)
                        .setTotalAverageDemand(10f)
                        .build());

        ActionDTO.Action action = ActionDTO.Action.newBuilder()
                .setId(0).setInfo(ActionInfo.getDefaultInstance()).setDeprecatedImportance(0)
                .setExplanation(validExplanation)
                .build();

        assertEquals("Increase RI Coverage by 50%.",
                ExplanationComposer.composeExplanation(action));
        assertEquals("Increase RI Coverage",
            ExplanationComposer.shortExplanation(action));

        final Builder invalidExplanation = Explanation.newBuilder()
                .setBuyRI(BuyRIExplanation.newBuilder()
                        .setCoveredAverageDemand(5f)
                        .setTotalAverageDemand(0f)
                        .build());

        action = ActionDTO.Action.newBuilder()
                .setId(0).setInfo(ActionInfo.getDefaultInstance()).setDeprecatedImportance(0)
                .setExplanation(invalidExplanation)
                .build();

        assertEquals("Invalid total demand.",
                ExplanationComposer.composeExplanation(action));
        assertEquals("Invalid total demand.",
            ExplanationComposer.shortExplanation(action));
    }

    /**
     * Test the explanation of reconfigure action with reason commodities.
     */
    @Test
    public void testReconfigureReasonCommodityExplanation() {
        ActionDTO.Action reconfigure = ActionDTO.Action.newBuilder()
                .setId(0).setInfo(ActionInfo.getDefaultInstance()).setDeprecatedImportance(0)
                .setExplanation(Explanation.newBuilder()
                    .setReconfigure(ReconfigureExplanation.newBuilder()
                        .addReconfigureCommodity(SEGMENTATION).addReconfigureCommodity(NETWORK)))
                    .build();

        ActionDTO.Action.Builder builder = ActionDTO.Action.newBuilder()
                .setId(0).setInfo(ActionInfo.getDefaultInstance()).setDeprecatedImportance(0)
                .setExplanation(Explanation.newBuilder()
                        .setReconfigure(ReconfigureExplanation.newBuilder()
                                        .addReconfigureCommodity(SEGMENTATION)
                                        .addReconfigureCommodity(NETWORK_WITH_PREFIX_IN_KEY)));

        ActionDTO.Action reconfigureWithPrefix = builder.build();

        assertEquals("Enable supplier to offer requested resource(s) Segmentation Commodity, Network Commodity " +
                        "testNetwork1",
            ExplanationComposer.composeExplanation(reconfigure));
        assertEquals("Enable supplier to offer requested resource(s) Segmentation Commodity, Network Commodity",
            ExplanationComposer.shortExplanation(reconfigure));

        assertEquals("Enable supplier to offer requested resource(s) Segmentation Commodity, Network Commodity " +
                        "testNetwork2",
                ExplanationComposer.composeExplanation(reconfigureWithPrefix));
        assertEquals("Enable supplier to offer requested resource(s) Segmentation Commodity, Network Commodity",
            ExplanationComposer.shortExplanation(reconfigureWithPrefix));

        // Make the reconfigure with prefix  for a member of a scaling group
        builder.getExplanationBuilder().getReconfigureBuilder().setScalingGroupId("example group");
        ActionDTO.Action reconfigureWithPrefixCSG = builder.build();
        assertEquals("Enable supplier to offer requested resource(s) Segmentation Commodity, " +
                    "Network Commodity testNetwork2 (Scaling Groups: example group)",
            ExplanationComposer.composeExplanation(reconfigureWithPrefixCSG));
        assertEquals("Enable supplier to offer requested resource(s) Segmentation Commodity, " +
                    "Network Commodity (Scaling Groups: example group)",
            ExplanationComposer.shortExplanation(reconfigureWithPrefixCSG));
    }

    /**
     * Test the explanation of reconfigure action with reason settings.
     */
    @Test
    public void testReconfigureReasonSettingsExplanation() {
        long reasonSetting1 = 1L;
        long reasonSetting2 = 2L;

        ActionDTO.Action reconfigureAction = ActionDTO.Action.newBuilder()
            .setId(0).setInfo(ActionInfo.newBuilder().setReconfigure(
                Reconfigure.newBuilder().setTarget(ActionEntity.newBuilder()
                    .setId(1).setType(EntityType.VIRTUAL_MACHINE_VALUE))))
            .setDeprecatedImportance(0)
            .setExplanation(Explanation.newBuilder()
                .setReconfigure(ReconfigureExplanation.newBuilder()
                    .addReasonSettings(reasonSetting1)
                    .addReasonSettings(reasonSetting2)))
            .build();

        assertEquals("(^_^)~{entity:1:displayName:Virtual Machine} doesn't comply with setting1, setting2",
            ExplanationComposer.composeExplanation(reconfigureAction,
                ImmutableMap.of(reasonSetting1, "setting1", reasonSetting2, "setting2")));
        assertEquals("Current entity doesn't comply with setting",
            ExplanationComposer.shortExplanation(reconfigureAction));
    }

    /**
     * Test the explanation of provision by supply action.
     */
    @Test
    public void testProvisionBySupplyExplanation() {
        ActionDTO.Action provision = ActionDTO.Action.newBuilder()
                .setId(0).setInfo(ActionInfo.getDefaultInstance()).setDeprecatedImportance(0)
                .setExplanation(Explanation.newBuilder()
                    .setProvision(ProvisionExplanation.newBuilder()
                            .setProvisionBySupplyExplanation(ProvisionBySupplyExplanation.newBuilder()
                                    .setMostExpensiveCommodityInfo(ReasonCommodity.newBuilder()
                                            .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                                                    .setType(21).build()).build()).build()).build()))

                .build();

        assertEquals("Mem congestion", ExplanationComposer.composeExplanation(provision));
        assertEquals("Mem congestion", ExplanationComposer.shortExplanation(provision));
    }

    /**
     * Test the explanation of provision by demand action.
     */
    @Test
    public void testProvisionByDemandExplanation() {
        ActionDTO.Action provision = ActionDTO.Action.newBuilder()
            .setId(0).setDeprecatedImportance(0).setInfo(ActionInfo.newBuilder().setProvision(
                Provision.newBuilder().setEntityToClone(
                    ActionEntity.newBuilder().setId(1).setType(EntityType.PHYSICAL_MACHINE_VALUE))))
            .setDeprecatedImportance(0)
            .setExplanation(Explanation.newBuilder()
                .setProvision(ProvisionExplanation.newBuilder()
                    .setProvisionByDemandExplanation(ProvisionByDemandExplanation.newBuilder().setBuyerId(2)
                        .addCommodityMaxAmountAvailable(CommodityMaxAmountAvailableEntry.newBuilder()
                            .setCommodityBaseType(40).setMaxAmountAvailable(0).setRequestedAmount(0))
                        .addCommodityMaxAmountAvailable(CommodityMaxAmountAvailableEntry.newBuilder()
                            .setCommodityBaseType(21).setMaxAmountAvailable(0).setRequestedAmount(0)))))
            .build();

        assertEquals("(^_^)~CPU, Mem congestion in '{entity:1:displayName:Physical Machine}'",
            ExplanationComposer.composeExplanation(provision));
        assertEquals("CPU, Mem congestion",
            ExplanationComposer.shortExplanation(provision));
    }

    @Test
    public void testResizeUpExplanation() {
        ActionDTO.Action.Builder action = ActionDTO.Action.newBuilder()
                .setId(0).setDeprecatedImportance(0)
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
        assertEquals("(^_^)~Underutilized VMem in Virtual Machine {entity:0:displayName:}",
            ExplanationComposer.composeExplanation(action.build()));
        assertEquals("Underutilized VMem",
            ExplanationComposer.shortExplanation(action.build()));

        // test a resize down
        action.getExplanationBuilder().getResizeBuilder().setStartUtilization(1).setEndUtilization(0);
        assertEquals("(^_^)~VMem congestion in Virtual Machine {entity:0:displayName:}",
                ExplanationComposer.composeExplanation(action.build()));
        assertEquals("VMem congestion",
            ExplanationComposer.shortExplanation(action.build()));

        // Test the resize down again with scaling group information
        action.getExplanationBuilder().getResizeBuilder()
            .setScalingGroupId("example scaling group");
        assertEquals("(^_^)~VMem congestion in Virtual Machine {entity:0:displayName:}" +
                " (Scaling Groups: example scaling group)",
            ExplanationComposer.composeExplanation(action.build()));
        assertEquals("VMem congestion (Scaling Groups: example scaling group)",
            ExplanationComposer.shortExplanation(action.build()));

        // Test action without Resize action
        action.getInfoBuilder().clearResize();
        assertEquals("", ExplanationComposer.composeExplanation(action.build()));
    }

    @Test
    public void testActivateExplanation() {
        ActionDTO.Action activate = ActionDTO.Action.newBuilder()
                .setId(0).setInfo(ActionInfo.getDefaultInstance()).setDeprecatedImportance(0)
                .setExplanation(Explanation.newBuilder()
                .setActivate(ActivateExplanation.newBuilder()
                    .setMostExpensiveCommodity(CPU.getCommodityType().getType())))
                .build();

        assertEquals("Address high utilization of CPU", ExplanationComposer.composeExplanation(activate));
        assertEquals("Address high utilization of CPU", ExplanationComposer.shortExplanation(activate));
    }

    @Test
    public void testDeactivateExplanation() {
        ActionDTO.Action deactivate = ActionDTO.Action.newBuilder()
            .setId(0).setInfo(ActionInfo.getDefaultInstance()).setDeprecatedImportance(0)
            .setExplanation(Explanation.newBuilder()
                .setDeactivate(DeactivateExplanation.getDefaultInstance()))
            .build();

        assertEquals("Improve infrastructure efficiency", ExplanationComposer.composeExplanation(deactivate));
        assertEquals("Improve infrastructure efficiency", ExplanationComposer.shortExplanation(deactivate));
    }

    /**
     * Test Delete Storage Explanation
     */
    @Test
    public void testDeleteExplanation() {
        // Test Cloud Delete Storage Action
        ActionDTO.Action deleteVolume = ActionDTO.Action.newBuilder()
            .setId(0)
            .setInfo(ActionInfo.newBuilder()
                .setDelete(Delete.newBuilder()
                    .setTarget(ActionEntity.newBuilder()
                        .setId(88L)
                        .setType(EntityType.VIRTUAL_VOLUME.getNumber())
                        .setEnvironmentType(EnvironmentType.CLOUD)
                        .build())
                    .build())
                .build())
            .setDeprecatedImportance(0)
            .setExplanation(Explanation.newBuilder()
                .setDelete(DeleteExplanation.getDefaultInstance()))
            .build();

        assertEquals("Increase savings",
            ExplanationComposer.composeExplanation(deleteVolume));
        assertEquals("Increase savings",
            ExplanationComposer.shortExplanation(deleteVolume));


        // Test On-Prem Delete Storage Action
        ActionDTO.Action deleteFiles = ActionDTO.Action.newBuilder()
            .setId(0)
            .setInfo(ActionInfo.newBuilder()
                .setDelete(Delete.newBuilder()
                    .setTarget(ActionEntity.newBuilder()
                        .setId(99L)
                        .setType(EntityType.STORAGE.getNumber())
                        .setEnvironmentType(EnvironmentType.ON_PREM)
                        .build())
                    .build())
                .build())
            .setDeprecatedImportance(0)
            .setExplanation(Explanation.newBuilder()
                .setDelete(DeleteExplanation.getDefaultInstance()))
            .build();

        assertEquals("Idle or non-productive",
            ExplanationComposer.composeExplanation(deleteFiles));
        assertEquals("Idle or non-productive",
            ExplanationComposer.shortExplanation(deleteFiles));
    }

    /**
     * Test building explanation for Allocate action.
     */
    @Test
    public void testAllocateExplanation() {
        // Arrange
        final ActionEntity vm = createActionEntity(1, EntityType.VIRTUAL_MACHINE_VALUE);
        final ActionEntity tier = createActionEntity(2, EntityType.COMPUTE_TIER_VALUE);
        final ActionInfo actionInfo = ActionInfo.newBuilder()
                .setAllocate(Allocate.newBuilder()
                        .setTarget(vm)
                        .setWorkloadTier(tier))
                .build();
        final Explanation explanation = Explanation.newBuilder()
                .setAllocate(AllocateExplanation.newBuilder()
                        .setInstanceSizeFamily("m4"))
                .build();
        final ActionDTO.Action action = createAction(actionInfo, explanation);

        // Act
        final String shortExplanation = ExplanationComposer.shortExplanation(action);
        final String fullExplanation = ExplanationComposer.composeExplanation(action);

        // Assert
        final String expectedExplanation = "Virtual Machine can be covered by m4 RI";
        assertEquals(expectedExplanation, shortExplanation);
        assertEquals(expectedExplanation, fullExplanation);
    }

    private static ActionEntity createActionEntity(long id, int type) {
        return ActionEntity.newBuilder()
                .setId(id)
                .setType(type)
                .build();
    }

    private static ActionDTO.Action createAction(
            @Nonnull final ActionInfo actionInfo,
            @Nonnull final Explanation explanation) {
        return ActionDTO.Action.newBuilder()
                .setId(123121)
                .setExplanation(explanation)
                .setInfo(actionInfo)
                .setDeprecatedImportance(1)
                .build();
    }

    private static Explanation createMoveExplanationWithCongestion(
        final Collection<ReasonCommodity> reasonCommodities) {
       return Explanation.newBuilder()
            .setMove(MoveExplanation.newBuilder()
                .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                    .setCongestion(Congestion.newBuilder()
                        .addAllCongestedCommodities(reasonCommodities))
                    .build())
                .build())
            .build();
    }

    private static ReasonCommodity createReasonCommodity(int baseType, String key) {
        return createReasonCommodity(baseType, key, null, null);
    }

    private static ReasonCommodity createReasonCommodity(int baseType, String key,
                 final Integer slot, final Integer totalSlotNumber) {
        CommodityType.Builder ct = TopologyDTO.CommodityType.newBuilder()
            .setType(baseType);
        if (key != null) {
            ct.setKey(key);
        }

        final ReasonCommodity.Builder reasonCommodity =
            ReasonCommodity.newBuilder().setCommodityType(ct.build());
        if (slot != null || totalSlotNumber != null) {
            final TimeSlotReasonInformation.Builder timeSlot = TimeSlotReasonInformation.newBuilder();
            if (slot != null) {
                timeSlot.setSlot(slot);
            }
            if (totalSlotNumber != null) {
                timeSlot.setTotalSlotNumber(totalSlotNumber);
            }
            reasonCommodity.setTimeSlot(timeSlot.build());
        }
        return reasonCommodity.build();
    }
}
