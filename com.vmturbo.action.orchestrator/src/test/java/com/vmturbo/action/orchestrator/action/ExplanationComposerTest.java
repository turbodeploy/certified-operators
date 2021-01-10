package com.vmturbo.action.orchestrator.action;

import static com.vmturbo.common.protobuf.action.ActionDTOUtil.COMMODITY_KEY_SEPARATOR;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import org.junit.Test;

import com.vmturbo.action.orchestrator.topology.ActionGraphEntity;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.Allocate;
import com.vmturbo.common.protobuf.action.ActionDTO.AtomicResize;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Delete;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ActivateExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.AllocateExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.AtomicResizeExplanation;
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
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ScaleExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.action.ActionDTO.Provision;
import com.vmturbo.common.protobuf.action.ActionDTO.Reconfigure;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.action.ActionDTO.ResizeInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.Scale;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityAttribute;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.graph.TopologyGraphCreator;

/**
 * Tests for Action Explanation generation in the {@link ExplanationComposer} class.
 */
public class ExplanationComposerTest {
    private static final ReasonCommodity MEM =
                    createReasonCommodity(CommodityDTO.CommodityType.MEM_VALUE, null);
    private static final ReasonCommodity CPU =
                    createReasonCommodity(CommodityDTO.CommodityType.CPU_VALUE, null);
    private static final ReasonCommodity IOPS =
                    createReasonCommodity(CommodityDTO.CommodityType.STORAGE_ACCESS_VALUE, null);
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

    private final TopologyGraphCreator<ActionGraphEntity.Builder, ActionGraphEntity> graphCreator =
        new TopologyGraphCreator<>();

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
                                .addMissingCommodities(CPU)
                                .addMissingCommodities(SEGMENTATION)))))
                .build();

        assertEquals("(^_^)~{entity:1:displayName:Physical Machine} can not satisfy the " +
                "request for resource(s) Mem, CPU, Segmentation",
            ExplanationComposer.composeExplanation(action));
        assertEquals(ImmutableSet.of("Mem compliance", "CPU compliance", "Placement policy compliance"),
            ExplanationComposer.composeRelatedRisks(action));
    }

    /**
     * Test move for compliance where the involved entities are in the ActionRealtimeTopology.
     */
    @Test
    public void testMoveComplianceReasonCommodityExplanationInTopology() {
        ActionDTO.Action action = ActionDTO.Action.newBuilder()
                .setId(0).setInfo(ActionInfo.newBuilder()
                        .setMove(Move.newBuilder()
                                .setTarget(ActionEntity.newBuilder()
                                        .setId(2).setType(EntityType.PHYSICAL_MACHINE_VALUE))
                                .addChanges(ChangeProvider.newBuilder()
                                    .setSource(ActionEntity.newBuilder()
                                            .setId(1).setType(EntityType.PHYSICAL_MACHINE_VALUE)
                                    )))).setDeprecatedImportance(0)
                .setExplanation(Explanation.newBuilder()
                    .setMove(MoveExplanation.newBuilder()
                        .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                            .setCompliance(Compliance.newBuilder()
                                .addMissingCommodities(MEM)
                                .addMissingCommodities(CPU)
                                .addMissingCommodities(SEGMENTATION)))))
                .build();
        setupTopologyGraph(host(1, "Alice"));

        assertEquals("(^_^)~Alice can not satisfy the "
                + "request for resource(s) Mem, CPU, Segmentation",
            ExplanationComposer.composeExplanation(action, Collections.emptyMap(),
                Optional.of(graphCreator.build()), null));
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
        Explanation explanation = createMoveExplanationWithCongestion(ImmutableList.of(MEM, CPU, IOPS));
        ActionDTO.Action action = createAction(actionInfo, explanation);
        assertEquals("(^_^)~Mem, CPU, IOPS Congestion",
            ExplanationComposer.composeExplanation(action));
        assertEquals(ImmutableSet.of("Mem Congestion", "CPU Congestion", "IOPS Congestion"),
            ExplanationComposer.composeRelatedRisks(action));

        final ReasonCommodity tsCommoditySlot0Total6 = createReasonCommodity(CommodityDTO.CommodityType.POOL_CPU_VALUE,
            null, 0, 6);
        explanation = createMoveExplanationWithCongestion(ImmutableList.of(MEM, tsCommoditySlot0Total6));
        action = createAction(actionInfo, explanation);

        assertEquals("(^_^)~Mem, Pool CPU at 12:00 AM - 04:00 AM Congestion",
            ExplanationComposer.composeExplanation(action));
        assertEquals(ImmutableSet.of("Mem Congestion", "Pool CPU Congestion"),
            ExplanationComposer.composeRelatedRisks(action));

        final ReasonCommodity tsCommoditySlot1Total3 = createReasonCommodity(CommodityDTO.CommodityType.POOL_CPU_VALUE,
            null, 1, 3);
        explanation = createMoveExplanationWithCongestion(ImmutableList.of(MEM, tsCommoditySlot1Total3));
        action = createAction(actionInfo, explanation);

        assertEquals("(^_^)~Mem, Pool CPU at 08:00 AM - 04:00 PM Congestion",
            ExplanationComposer.composeExplanation(action));
        assertEquals(ImmutableSet.of("Mem Congestion", "Pool CPU Congestion"),
            ExplanationComposer.composeRelatedRisks(action));

        final ReasonCommodity tsInvalidSlot = createReasonCommodity(CommodityDTO.CommodityType.POOL_CPU_VALUE,
            null, -1, 3);
        explanation = createMoveExplanationWithCongestion(ImmutableList.of(MEM, tsInvalidSlot));
        action = createAction(actionInfo, explanation);
        assertEquals("(^_^)~Mem, Pool CPU Congestion",
            ExplanationComposer.composeExplanation(action));
        assertEquals(ImmutableSet.of("Mem Congestion", "Pool CPU Congestion"),
            ExplanationComposer.composeRelatedRisks(action));

        final ReasonCommodity tsInvalidTotalSlotNumber = createReasonCommodity(CommodityDTO.CommodityType.POOL_CPU_VALUE,
            null, -0, 0);
        explanation = createMoveExplanationWithCongestion(ImmutableList.of(MEM, tsInvalidTotalSlotNumber));
        action = createAction(actionInfo, explanation);
        assertEquals("(^_^)~Mem, Pool CPU Congestion",
            ExplanationComposer.composeExplanation(action));
        assertEquals(ImmutableSet.of("Mem Congestion", "Pool CPU Congestion"),
            ExplanationComposer.composeRelatedRisks(action));
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
        setupTopologyGraph(entity(1, "Emily", EntityType.VIRTUAL_MACHINE_VALUE));

        assertEquals("(^_^)~{entity:1:displayName:Virtual Machine} doesn't comply with setting1, setting2",
            ExplanationComposer.composeExplanation(moveAction,
                ImmutableMap.of(reasonSetting1, "setting1", reasonSetting2, "setting2"),
                Optional.empty(), null));
        assertEquals(Collections.singleton("Setting policy compliance"),
            ExplanationComposer.composeRelatedRisks(moveAction));
        assertEquals("(^_^)~Emily doesn't comply with setting1, setting2",
            ExplanationComposer.composeExplanation(moveAction,
                ImmutableMap.of(reasonSetting1, "setting1", reasonSetting2, "setting2"),
                Optional.of(graphCreator.build()), null));
    }

    /**
     * Test that an action purely for CSG compliance is explained as "Comply to Auto Scaling
     * Groups: GroupName".
     */
    @Test
    public void testScaleCsgCompliance() {
        ActionDTO.Action scaleAction = ActionDTO.Action.newBuilder()
            .setId(0).setInfo(ActionInfo.newBuilder().setScale(
                Scale.newBuilder().setTarget(ActionEntity.newBuilder()
                    .setId(1).setType(EntityType.VIRTUAL_MACHINE_VALUE))))
            .setDeprecatedImportance(0)
            .setExplanation(Explanation.newBuilder()
                .setScale(ScaleExplanation.newBuilder()
                    .setScalingGroupId("TestScalingGroup123")
                    .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                        .setIsPrimaryChangeProviderExplanation(true)
                        .setCompliance(Compliance.newBuilder()
                            .setIsCsgCompliance(true)))))
            .build();

        assertEquals("(^_^)~Comply to Auto Scaling Groups: TestScalingGroup123",
            ExplanationComposer.composeExplanation(scaleAction, Maps.newHashMap(), Optional.empty(), null));
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
                            .setEvacuationExplanation(ChangeProviderExplanation.EvacuationExplanation
                                    .newBuilder()
                                       .setSuspension(ChangeProviderExplanation.Suspension.newBuilder().build())
                                   .build())
                            .setSuspendedEntity(2)))))
            .build();
        setupTopologyGraph(entity(2, "Fred", EntityType.PHYSICAL_MACHINE_VALUE));

        assertEquals("(^_^)~{entity:2:displayName:Current supplier} can be suspended to improve efficiency",
            ExplanationComposer.composeExplanation(action));
        assertEquals(Collections.singleton("Underutilized resources"),
            ExplanationComposer.composeRelatedRisks(action));
        assertEquals("(^_^)~Fred can be suspended to improve efficiency",
            ExplanationComposer.composeExplanation(action, Collections.emptyMap(),
                Optional.of(graphCreator.build()), null));
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
                            .setEvacuationExplanation(ChangeProviderExplanation.EvacuationExplanation
                                 .newBuilder()
                                    .setSuspension(ChangeProviderExplanation.Suspension.newBuilder().build())
                                .build())
                            .setIsAvailable(false)))))
            .build();
        setupTopologyGraph(host(2, "Gwen"));

        assertEquals("(^_^)~{entity:2:displayName:Current supplier} is not available",
            ExplanationComposer.composeExplanation(action));
        assertEquals(Collections.singleton("Underutilized resources"),
            ExplanationComposer.composeRelatedRisks(action));
        assertEquals("(^_^)~Gwen is not available",
            ExplanationComposer.composeExplanation(action, Collections.emptyMap(),
                Optional.of(graphCreator.build()), null));
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

        assertEquals("(^_^)~Improve overall performance",
            ExplanationComposer.composeExplanation(action));
        assertEquals(Collections.singleton("Improve overall performance"),
            ExplanationComposer.composeRelatedRisks(action));
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
        assertEquals(ImmutableSet.of("Mem compliance", "CPU compliance"),
            ExplanationComposer.composeRelatedRisks(action));
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

        assertEquals("Increase RI Coverage by 50%",
                ExplanationComposer.composeExplanation(action));
        assertEquals(Collections.singleton("Increase RI Coverage"),
            ExplanationComposer.composeRelatedRisks(action));

        final Builder invalidExplanation = Explanation.newBuilder()
                .setBuyRI(BuyRIExplanation.newBuilder()
                        .setCoveredAverageDemand(5f)
                        .setTotalAverageDemand(0f)
                        .build());

        action = ActionDTO.Action.newBuilder()
                .setId(0).setInfo(ActionInfo.getDefaultInstance()).setDeprecatedImportance(0)
                .setExplanation(invalidExplanation)
                .build();

        assertEquals("Invalid total demand",
                ExplanationComposer.composeExplanation(action));
        assertEquals(Collections.singleton("Invalid total demand"),
            ExplanationComposer.composeRelatedRisks(action));
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

        assertEquals("Configure supplier to update resource(s) Segmentation, Network " +
                        "testNetwork1",
            ExplanationComposer.composeExplanation(reconfigure));
        assertEquals(Collections.singleton("Misconfiguration"),
            ExplanationComposer.composeRelatedRisks(reconfigure));

        assertEquals("Configure supplier to update resource(s) Segmentation, Network " +
                        "testNetwork2",
                ExplanationComposer.composeExplanation(reconfigureWithPrefix));
        assertEquals(Collections.singleton("Misconfiguration"),
            ExplanationComposer.composeRelatedRisks(reconfigureWithPrefix));

        // Make the reconfigure with prefix  for a member of a scaling group
        builder.getExplanationBuilder().getReconfigureBuilder().setScalingGroupId("example group");
        ActionDTO.Action reconfigureWithPrefixCSG = builder.build();
        assertEquals("Configure supplier to update resource(s) Segmentation, " +
                    "Network testNetwork2 (Auto Scaling Groups: example group)",
            ExplanationComposer.composeExplanation(reconfigureWithPrefixCSG));
        assertEquals(Collections.singleton("Misconfiguration"),
            ExplanationComposer.composeRelatedRisks(reconfigureWithPrefixCSG));
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
                    .setId(1).setType(EntityType.VIRTUAL_MACHINE_VALUE))
                .setIsProvider(false)))
            .setDeprecatedImportance(0)
            .setExplanation(Explanation.newBuilder()
                .setReconfigure(ReconfigureExplanation.newBuilder()
                    .addReasonSettings(reasonSetting1)
                    .addReasonSettings(reasonSetting2)))
            .build();

        assertEquals("(^_^)~{entity:1:displayName:Virtual Machine} doesn't comply with setting1, setting2",
            ExplanationComposer.composeExplanation(reconfigureAction,
                ImmutableMap.of(reasonSetting1, "setting1", reasonSetting2, "setting2"),
                Optional.empty(), null));
        assertEquals(Collections.singleton("Misconfiguration"),
            ExplanationComposer.composeRelatedRisks(reconfigureAction));
    }

    /**
     * Test the explanation of reconfigure action with reason settings.
     */
    @Test
    public void testReconfigureReasonSettingsExplanationInTopology() {
        long reasonSetting1 = 1L;
        long reasonSetting2 = 2L;

        ActionDTO.Action reconfigureAction = ActionDTO.Action.newBuilder()
            .setId(0).setInfo(ActionInfo.newBuilder().setReconfigure(
                Reconfigure.newBuilder().setTarget(ActionEntity.newBuilder()
                    .setId(1).setType(EntityType.VIRTUAL_MACHINE_VALUE))
                .setIsProvider(false)))
            .setDeprecatedImportance(0)
            .setExplanation(Explanation.newBuilder()
                .setReconfigure(ReconfigureExplanation.newBuilder()
                    .addReasonSettings(reasonSetting1)
                    .addReasonSettings(reasonSetting2)))
            .build();
        setupTopologyGraph(entity(1, "Bob", EntityType.VIRTUAL_MACHINE_VALUE));

        assertEquals("(^_^)~Bob doesn't comply with setting1, setting2",
            ExplanationComposer.composeExplanation(reconfigureAction,
                ImmutableMap.of(reasonSetting1, "setting1", reasonSetting2, "setting2"),
                Optional.of(graphCreator.build()), null));
        assertEquals(Collections.singleton("Misconfiguration"),
            ExplanationComposer.composeRelatedRisks(reconfigureAction));
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

        assertEquals("Mem Congestion", ExplanationComposer.composeExplanation(provision));
        assertEquals(Collections.singleton("Mem Congestion"), ExplanationComposer.composeRelatedRisks(provision));
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

        assertTrue(ImmutableSet.of("(^_^)~CPU, Mem Congestion in '{entity:1:displayName:Physical Machine}'",
            "(^_^)~Mem, CPU Congestion in '{entity:1:displayName:Physical Machine}'").contains(
                ExplanationComposer.composeExplanation(provision)));
        assertEquals(ImmutableSet.of("Mem Congestion", "CPU Congestion"),
            ExplanationComposer.composeRelatedRisks(provision));
    }

    /**
     * Test the explanation of provision by demand action.
     */
    @Test
    public void testProvisionByDemandExplanationInTopology() {
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
        setupTopologyGraph(entity(1, "Christie", EntityType.PHYSICAL_MACHINE_VALUE));

        assertThat(ImmutableSet.of("(^_^)~CPU, Mem Congestion in 'Christie'",
            "(^_^)~Mem, CPU Congestion in 'Christie'"), hasItem(
            ExplanationComposer.composeExplanation(provision, Collections.emptyMap(),
                Optional.of(graphCreator.build()), null)));
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
                    .setDeprecatedStartUtilization(0.2f)
                    .setDeprecatedEndUtilization(0.4f)));
        setupTopologyGraph(entity(0, "Danny", EntityType.VIRTUAL_MACHINE_VALUE));

        // test resize down by capacity
        action.getInfoBuilder().getResizeBuilder().setOldCapacity(4).setNewCapacity(2).build();
        assertEquals("(^_^)~Underutilized VMem in Virtual Machine {entity:0:displayName:}",
            ExplanationComposer.composeExplanation(action.build()));
        assertEquals(Collections.singleton("Underutilized VMem"),
            ExplanationComposer.composeRelatedRisks(action.build()));
        assertEquals("(^_^)~Underutilized VMem in Virtual Machine Danny",
            ExplanationComposer.composeExplanation(action.build(), Collections.emptyMap(),
                Optional.of(graphCreator.build()), null));

        // test resize up by capacity
        action.getInfoBuilder().getResizeBuilder().setOldCapacity(2).setNewCapacity(4).build();
        assertEquals("(^_^)~VMem Congestion in Virtual Machine {entity:0:displayName:}",
            ExplanationComposer.composeExplanation(action.build()));
        assertEquals(Collections.singleton("VMem Congestion"),
            ExplanationComposer.composeRelatedRisks(action.build()));
        assertEquals("(^_^)~VMem Congestion in Virtual Machine Danny",
            ExplanationComposer.composeExplanation(action.build(), Collections.emptyMap(),
                Optional.of(graphCreator.build()), null));

        // Test the resize down again with scaling group information
        action.getExplanationBuilder().getResizeBuilder()
            .setScalingGroupId("example scaling group");
        assertEquals("(^_^)~VMem Congestion in Virtual Machine {entity:0:displayName:}" +
                " (Auto Scaling Groups: example scaling group)",
            ExplanationComposer.composeExplanation(action.build()));
        assertEquals(Collections.singleton("VMem Congestion"),
            ExplanationComposer.composeRelatedRisks(action.build()));
        assertEquals("(^_^)~VMem Congestion in Virtual Machine Danny"
                + " (Auto Scaling Groups: example scaling group)",
            ExplanationComposer.composeExplanation(action.build(), Collections.emptyMap(),
                Optional.of(graphCreator.build()), null));

        // Test action without Resize action
        action.getInfoBuilder().clearResize();
        assertEquals("", ExplanationComposer.composeExplanation(action.build()));
    }

    /**
     * Tests for explanation generation for atomic resize.
     */
    @Test
    public void testAtomicResizeExplanationForContainerSpecs() {
        final ActionDTO.Action.Builder action = ActionDTO.Action.newBuilder()
            .setId(0).setDeprecatedImportance(0)
            .setInfo(ActionInfo.newBuilder()
                .setAtomicResize(AtomicResize.newBuilder()
                    .setExecutionTarget(ActionEntity.newBuilder()
                        .setId(1)
                        .setType(EntityType.CONTAINER_SPEC_VALUE))
                    .addResizes(ResizeInfo.newBuilder()
                        .setTarget(ActionEntity.newBuilder()
                            .setId(1)
                            .setType(EntityType.CONTAINER_SPEC_VALUE))
                        .setCommodityType(CommodityType.newBuilder().setType(CommodityDTO.CommodityType.VCPU_VALUE))
                        .setCommodityAttribute(CommodityAttribute.CAPACITY)
                        .setOldCapacity(123)
                        .setNewCapacity(456))
                    .addResizes(ResizeInfo.newBuilder()
                         .setTarget(ActionEntity.newBuilder()
                               .setId(1)
                                .setType(EntityType.CONTAINER_SPEC_VALUE))
                         .setCommodityType(CommodityType.newBuilder().setType(CommodityDTO.CommodityType.VMEM_REQUEST_VALUE))
                         .setCommodityAttribute(CommodityAttribute.CAPACITY)
                         .setOldCapacity(890)
                          .setNewCapacity(567))))
            .setExplanation(Explanation.newBuilder()
                .setAtomicResize(AtomicResizeExplanation.newBuilder()
                    .setMergeGroupId("bar")));

        setupTopologyGraph(entity(1, "Irene", EntityType.CONTAINER_SPEC_VALUE));


        assertEquals("Container Resize - "
                + "Resize UP VCPU Limit from 123 millicores to 456 millicores, "
                + "Resize DOWN VMem Request from 890 KB to 567 KB"
                + " in Container Spec {entity:1:displayName:}",
            ExplanationComposer.composeExplanation(action.build()));

        assertEquals("Container Resize - "
                        + "Resize UP VCPU Limit from 123 millicores to 456 millicores, "
                        + "Resize DOWN VMem Request from 890 KB to 567 KB"
                        + " in Container Spec Irene",
            ExplanationComposer.composeExplanation(action.build(), Collections.emptyMap(),
                Optional.of(graphCreator.build()), null));
    }

    /**
     * Tests for explanation generation for atomic resize.
     */
    @Test
    public void testAtomicResizeExplanation() {
        final ActionDTO.Action.Builder action = ActionDTO.Action.newBuilder()
                .setId(0).setDeprecatedImportance(0)
                .setInfo(ActionInfo.newBuilder()
                        .setAtomicResize(AtomicResize.newBuilder()
                                .setExecutionTarget(ActionEntity.newBuilder()
                                        .setId(0)
                                        .setType(EntityType.WORKLOAD_CONTROLLER_VALUE))
                                .addResizes(ResizeInfo.newBuilder()
                                        .setTarget(ActionEntity.newBuilder()
                                                .setId(1)
                                                .setType(EntityType.CONTAINER_SPEC_VALUE))
                                        .setCommodityType(CommodityType.newBuilder().setType(CommodityDTO.CommodityType.VCPU_VALUE))
                                        .setCommodityAttribute(CommodityAttribute.CAPACITY)
                                        .setOldCapacity(123)
                                        .setNewCapacity(456))
                                .addResizes(ResizeInfo.newBuilder()
                                        .setTarget(ActionEntity.newBuilder()
                                                .setId(1)
                                                .setType(EntityType.CONTAINER_SPEC_VALUE))
                                        .setCommodityType(CommodityType.newBuilder().setType(CommodityDTO.CommodityType.VMEM_VALUE))
                                        .setCommodityAttribute(CommodityAttribute.CAPACITY)
                                        .setOldCapacity(890)
                                        .setNewCapacity(567))
                                .addResizes(ResizeInfo.newBuilder()
                                        .setTarget(ActionEntity.newBuilder()
                                                .setId(2)
                                                .setType(EntityType.CONTAINER_SPEC_VALUE))
                                        .setCommodityType(CommodityType.newBuilder().setType(CommodityDTO.CommodityType.VMEM_VALUE))
                                        .setCommodityAttribute(CommodityAttribute.CAPACITY)
                                        .setOldCapacity(890)
                                        .setNewCapacity(567))
                                .addResizes(ResizeInfo.newBuilder()
                                        .setTarget(ActionEntity.newBuilder()
                                                .setId(2)
                                                .setType(EntityType.CONTAINER_SPEC_VALUE))
                                        .setCommodityType(CommodityType.newBuilder().setType(CommodityDTO.CommodityType.VCPU_VALUE))
                                        .setCommodityAttribute(CommodityAttribute.CAPACITY)
                                        .setOldCapacity(123)
                                        .setNewCapacity(456))))
                .setExplanation(Explanation.newBuilder()
                        .setAtomicResize(AtomicResizeExplanation.newBuilder()
                                .setMergeGroupId("bar")));
        setupTopologyGraph(entity(0, "Harry", EntityType.WORKLOAD_CONTROLLER_VALUE));
        setupTopologyGraph(entity(1, "Irene", EntityType.CONTAINER_SPEC_VALUE));
        setupTopologyGraph(entity(2, "John", EntityType.CONTAINER_SPEC_VALUE));

        String explanation = ExplanationComposer.composeExplanation(action.build());
        String coreExplanation =  ExplanationComposer.buildAtomicResizeCoreExplanation(action.build());
        assertEquals("Controller Resize", coreExplanation);
        assertTrue(explanation.startsWith(coreExplanation));
        assertTrue(explanation.contains("Resize DOWN VMem Limit from 890 KB to 567 KB, "
                                         + "Resize UP VCPU Limit from 123 millicores to 456 millicores"
                                         + " in Container Spec {entity:2:displayName:}"));
        assertTrue(explanation.contains("Resize UP VCPU Limit from 123 millicores to 456 millicores, "
                + "Resize DOWN VMem Limit from 890 KB to 567 KB"
                + " in Container Spec {entity:1:displayName:}"));

        String translatedExplanation = ExplanationComposer.composeExplanation(action.build(), Collections.emptyMap(),
                Optional.of(graphCreator.build()), null);
        assertTrue(translatedExplanation.startsWith(coreExplanation));
        assertTrue(translatedExplanation.contains("Resize DOWN VMem Limit from 890 KB to 567 KB, "
                + "Resize UP VCPU Limit from 123 millicores to 456 millicores"
                + " in Container Spec John"));
        assertTrue(translatedExplanation.contains("Resize UP VCPU Limit from 123 millicores to 456 millicores, "
                + "Resize DOWN VMem Limit from 890 KB to 567 KB"
                + " in Container Spec Irene"));
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
        assertEquals(Collections.singleton("CPU Congestion"),
            ExplanationComposer.composeRelatedRisks(activate));
    }

    @Test
    public void testDeactivateExplanation() {
        ActionDTO.Action deactivate = ActionDTO.Action.newBuilder()
            .setId(0).setInfo(ActionInfo.getDefaultInstance()).setDeprecatedImportance(0)
            .setExplanation(Explanation.newBuilder()
                .setDeactivate(DeactivateExplanation.getDefaultInstance()))
            .build();

        assertEquals("Improve infrastructure efficiency", ExplanationComposer.composeExplanation(deactivate));
        assertEquals(Collections.singleton("Improve infrastructure efficiency"),
            ExplanationComposer.composeRelatedRisks(deactivate));
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
        assertEquals(Collections.singleton("Increase savings"),
            ExplanationComposer.composeRelatedRisks(deleteVolume));


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
        assertEquals(Collections.singleton("Idle or non-productive"),
            ExplanationComposer.composeRelatedRisks(deleteFiles));
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

        // Assert
        final String expectedExplanation = "Virtual Machine can be covered by m4 RI";
        assertEquals(expectedExplanation, ExplanationComposer.composeExplanation(action));
        assertEquals(Collections.singleton("Virtual Machine RI Coverage"),
            ExplanationComposer.composeRelatedRisks(action));
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

    private TopologyEntityDTO host(final long oid, final String displayName) {
        return TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
            .setDisplayName(displayName)
            .setOid(oid)
            .build();
    }

    private TopologyEntityDTO entity(final long oid, final String displayName, int entityType) {
        return TopologyEntityDTO.newBuilder()
            .setEntityType(entityType)
            .setDisplayName(displayName)
            .setOid(oid)
            .build();
    }

    private void setupTopologyGraph(@Nonnull final TopologyEntityDTO... entities) {
        for (TopologyEntityDTO entity : entities) {
            graphCreator.addEntity(new ActionGraphEntity.Builder(entity));
        }
    }
}
