package com.vmturbo.action.orchestrator.action;

import static com.vmturbo.common.protobuf.action.ActionDTOUtil.COMMODITY_KEY_SEPARATOR;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import org.junit.Rule;
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
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.AtomicResizeExplanation.ResizeExplanationPerEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.AtomicResizeExplanation.ResizeExplanationPerEntity.ResizeExplanationPerCommodity;
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
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ReasonCommodity.Suffix;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ReasonCommodity.TimeSlotReasonInformation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ReconfigureExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ResizeExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ScaleExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.action.ActionDTO.Provision;
import com.vmturbo.common.protobuf.action.ActionDTO.Reconfigure;
import com.vmturbo.common.protobuf.action.ActionDTO.Reconfigure.SettingChange;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.action.ActionDTO.ResizeInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.Scale;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityAttribute;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityAttribute;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.components.common.featureflags.FeatureFlags;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.test.utils.FeatureFlagTestRule;
import com.vmturbo.topology.graph.TopologyGraphCreator;

/**
 * Tests for Action Explanation generation in the {@link ExplanationComposer} class.
 */
public class ExplanationComposerTest {
    private static final ReasonCommodity MEM =
                    createReasonCommodity(CommodityDTO.CommodityType.MEM_VALUE, "");
    private static final ReasonCommodity CPU =
                    createReasonCommodity(CommodityDTO.CommodityType.CPU_VALUE, "");
    private static final ReasonCommodity IOPS =
                    createReasonCommodity(CommodityDTO.CommodityType.STORAGE_ACCESS_VALUE, "");
    private static final ReasonCommodity SEGMENTATION =
                    createReasonCommodity(CommodityDTO.CommodityType.SEGMENTATION_VALUE, "");
    private static final ReasonCommodity NETWORK =
                    createReasonCommodity(CommodityDTO.CommodityType.NETWORK_VALUE, "testNetwork1");
    private static final ReasonCommodity LABEL =
            createReasonCommodity(CommodityDTO.CommodityType.LABEL_VALUE, "foo=bar");
    private static final ReasonCommodity TAINT =
            createReasonCommodity(CommodityDTO.CommodityType.TAINT_VALUE, "foo=bar");
    private static final ReasonCommodity CLUSTER =
            createReasonCommodity(CommodityDTO.CommodityType.CLUSTER_VALUE, "Node-1-NotReady");

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

    /**
     * Rule to initialize FeatureFlags store.
     **/
    @Rule
    public FeatureFlagTestRule featureFlagTestRule = new FeatureFlagTestRule();

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
            ExplanationComposer.composeExplanation(action, Collections.emptyList()));
        assertEquals(ImmutableSet.of("Mem compliance", "CPU compliance", "Placement policy compliance"),
            ExplanationComposer.composeRelatedRisks(action, Collections.emptyList()));
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
            ExplanationComposer.composeExplanation(action, Collections.emptyMap(), Collections.emptyMap(),
                Optional.of(graphCreator.build()), null, Collections.emptyList()));
    }

    /**
     * Test move for compliance where the involved entities are missing the appropriate kubernetes
     * label configured for nodeSelector.
     */
    @Test
    public void testMoveComplianceReasonLabelCommodityExplanation() {
        ActionDTO.Action action = ActionDTO.Action.newBuilder()
            .setId(0)
            .setInfo(ActionInfo.newBuilder()
                .setMove(Move.newBuilder()
                    .setTarget(ActionEntity.newBuilder()
                        .setId(2)
                        .setType(EntityType.VIRTUAL_MACHINE.getNumber()))
                    .addChanges(ChangeProvider.newBuilder()
                        .setSource(ActionEntity.newBuilder()
                            .setId(1)
                            .setType(EntityType.VIRTUAL_MACHINE.getNumber())))))
            .setDeprecatedImportance(0)
            .setExplanation(Explanation.newBuilder()
                .setMove(MoveExplanation.newBuilder()
                    .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                        .setCompliance(Compliance.newBuilder()
                            .addMissingCommodities(LABEL)))))
            .build();

        assertEquals(
            "(^_^)~{entity:1:displayName:Virtual Machine} can not satisfy "
                + "the request for resource(s) Kubernetes Label",
            ExplanationComposer.composeExplanation(action, Collections.emptyList()));
        assertEquals(
            ImmutableSet.of("Kubernetes Label compliance"),
            ExplanationComposer.composeRelatedRisks(action, Collections.emptyList()));
    }

    @Test
    public void testMoveComplianceCPTaintReasonCommodityExplanation() {
        ActionDTO.Action action = ActionDTO.Action.newBuilder()
                .setId(0).setInfo(ActionInfo.newBuilder()
                        .setMove(Move.newBuilder()
                                .setTarget(ActionEntity.newBuilder()
                                        .setId(2).setType(EntityType.CONTAINER_POD_VALUE))
                                .addChanges(ChangeProvider.newBuilder()
                                        .setSource(ActionEntity.newBuilder()
                                                .setId(1).setType(EntityType.VIRTUAL_MACHINE_VALUE)
                                        )))).setDeprecatedImportance(0)
                .setExplanation(Explanation.newBuilder()
                        .setMove(MoveExplanation.newBuilder()
                                .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                                        .setCompliance(Compliance.newBuilder()
                                                .addMissingCommodities(TAINT)))))

                .build();

        assertEquals("(^_^)~Container Pod {entity:2:displayName:} cannot tolerate taints " +
                        "foo=bar on {entity:1:displayName:Virtual Machine}",
                ExplanationComposer.composeExplanation(action, Collections.emptyList()));
        assertEquals(ImmutableSet.of("Kubernetes Taint compliance"),
                ExplanationComposer.composeRelatedRisks(action, Collections.emptyList()));
    }

    @Test
    public void testMoveComplianceCPLabelReasonCommodityExplanation() {
        ActionDTO.Action action = ActionDTO.Action.newBuilder()
                .setId(0).setInfo(ActionInfo.newBuilder()
                        .setMove(Move.newBuilder()
                                .setTarget(ActionEntity.newBuilder()
                                        .setId(2).setType(EntityType.CONTAINER_POD_VALUE))
                                .addChanges(ChangeProvider.newBuilder()
                                        .setSource(ActionEntity.newBuilder()
                                                .setId(1).setType(EntityType.VIRTUAL_MACHINE_VALUE)
                                        )))).setDeprecatedImportance(0)
                .setExplanation(Explanation.newBuilder()
                        .setMove(MoveExplanation.newBuilder()
                                .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                                        .setCompliance(Compliance.newBuilder()
                                                .addMissingCommodities(LABEL)))))

                .build();

        assertEquals("(^_^)~{entity:1:displayName:Virtual Machine} cannot satisfy nodeSelector " +
                        "foo=bar",
                ExplanationComposer.composeExplanation(action, Collections.emptyList()));
        assertEquals(ImmutableSet.of("Kubernetes Label compliance"),
                ExplanationComposer.composeRelatedRisks(action, Collections.emptyList()));
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
            ExplanationComposer.composeExplanation(action, Collections.emptyList()));
        assertEquals(ImmutableSet.of("Mem Congestion", "CPU Congestion", "IOPS Congestion"),
            ExplanationComposer.composeRelatedRisks(action, Collections.emptyList()));

        final ReasonCommodity tsCommoditySlot0Total6 = createReasonCommodity(CommodityDTO.CommodityType.POOL_CPU_VALUE,
            null, 0, 6);
        explanation = createMoveExplanationWithCongestion(ImmutableList.of(MEM, tsCommoditySlot0Total6));
        action = createAction(actionInfo, explanation);

        assertEquals("(^_^)~Mem, Pool CPU at 12:00 AM - 04:00 AM Congestion",
            ExplanationComposer.composeExplanation(action, Collections.emptyList()));
        assertEquals(ImmutableSet.of("Mem Congestion", "Pool CPU Congestion"),
            ExplanationComposer.composeRelatedRisks(action, Collections.emptyList()));

        final ReasonCommodity tsCommoditySlot1Total3 = createReasonCommodity(CommodityDTO.CommodityType.POOL_CPU_VALUE,
            null, 1, 3);
        explanation = createMoveExplanationWithCongestion(ImmutableList.of(MEM, tsCommoditySlot1Total3));
        action = createAction(actionInfo, explanation);

        assertEquals("(^_^)~Mem, Pool CPU at 08:00 AM - 04:00 PM Congestion",
            ExplanationComposer.composeExplanation(action, Collections.emptyList()));
        assertEquals(ImmutableSet.of("Mem Congestion", "Pool CPU Congestion"),
            ExplanationComposer.composeRelatedRisks(action, Collections.emptyList()));

        final ReasonCommodity tsInvalidSlot = createReasonCommodity(CommodityDTO.CommodityType.POOL_CPU_VALUE,
            null, -1, 3);
        explanation = createMoveExplanationWithCongestion(ImmutableList.of(MEM, tsInvalidSlot));
        action = createAction(actionInfo, explanation);
        assertEquals("(^_^)~Mem, Pool CPU Congestion",
            ExplanationComposer.composeExplanation(action, Collections.emptyList()));
        assertEquals(ImmutableSet.of("Mem Congestion", "Pool CPU Congestion"),
            ExplanationComposer.composeRelatedRisks(action, Collections.emptyList()));

        final ReasonCommodity tsInvalidTotalSlotNumber = createReasonCommodity(CommodityDTO.CommodityType.POOL_CPU_VALUE,
            null, -0, 0);
        explanation = createMoveExplanationWithCongestion(ImmutableList.of(MEM, tsInvalidTotalSlotNumber));
        action = createAction(actionInfo, explanation);
        assertEquals("(^_^)~Mem, Pool CPU Congestion",
            ExplanationComposer.composeExplanation(action, Collections.emptyList()));
        assertEquals(ImmutableSet.of("Mem Congestion", "Pool CPU Congestion"),
            ExplanationComposer.composeRelatedRisks(action, Collections.emptyList()));

        final ReasonCommodity reasonWithReservationSuffix = createReasonCommodity(CommodityDTO.CommodityType.CPU_VALUE, Suffix.RESERVATION);
        explanation = createMoveExplanationWithCongestion(ImmutableList.of(MEM, reasonWithReservationSuffix));
        action = createAction(actionInfo, explanation);
        assertEquals("(^_^)~Mem, CPU Reservation Congestion",
                ExplanationComposer.composeExplanation(action, Collections.emptyList()));
        assertEquals(ImmutableSet.of("Mem Congestion", "CPU Reservation Congestion"),
                ExplanationComposer.composeRelatedRisks(action, Collections.emptyList()));
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
                ImmutableMap.of(reasonSetting1, "setting1", reasonSetting2, "setting2"), Collections.emptyMap(),
                Optional.empty(), null, Collections.emptyList()));
        assertEquals(Collections.singleton("Setting policy compliance"),
            ExplanationComposer.composeRelatedRisks(moveAction, Collections.emptyList()));
        assertEquals("(^_^)~Emily doesn't comply with setting1, setting2",
            ExplanationComposer.composeExplanation(moveAction,
                ImmutableMap.of(reasonSetting1, "setting1", reasonSetting2, "setting2"), Collections.emptyMap(),
                Optional.of(graphCreator.build()), null, Collections.emptyList()));
        // Test deleted policy(s).
        assertEquals("(^_^)~Emily doesn't comply with setting1, a compliance policy that used to exist",
                     ExplanationComposer.composeExplanation(moveAction,
                                                ImmutableMap.of(reasonSetting1,
                                                    "setting1"), Collections.emptyMap(),
                                                Optional.of(graphCreator.build()),
                                                null, Collections.emptyList()));
        assertEquals("(^_^)~Emily doesn't comply with compliance policies that used to exist",
                     ExplanationComposer.composeExplanation(moveAction,
                                                ImmutableMap.of(), Collections.emptyMap(),
                                                Optional.of(graphCreator.build()),
                                                null, Collections.emptyList()));
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
                ExplanationComposer.composeExplanation(scaleAction, Maps.newHashMap(),
                        Maps.newHashMap(), Optional.empty(), null, Collections.emptyList()));
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
            ExplanationComposer.composeExplanation(action, Collections.emptyList()));
        assertEquals(Collections.singleton("Underutilized resources"),
            ExplanationComposer.composeRelatedRisks(action, Collections.emptyList()));
        assertEquals("(^_^)~Fred can be suspended to improve efficiency",
            ExplanationComposer.composeExplanation(action, Collections.emptyMap(), Collections.emptyMap(),
                Optional.of(graphCreator.build()), null, Collections.emptyList()));
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
            ExplanationComposer.composeExplanation(action, Collections.emptyList()));
        assertEquals(Collections.singleton("Underutilized resources"),
            ExplanationComposer.composeRelatedRisks(action, Collections.emptyList()));
        assertEquals("(^_^)~Gwen is not available",
            ExplanationComposer.composeExplanation(action, Collections.emptyMap(), Collections.emptyMap(),
                Optional.of(graphCreator.build()), null, Collections.emptyList()));
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
            ExplanationComposer.composeExplanation(action, Collections.emptyList()));
        assertEquals(Collections.singleton("Improve overall performance"),
            ExplanationComposer.composeRelatedRisks(action, Collections.emptyList()));
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
            ExplanationComposer.composeExplanation(action, Collections.emptyList()));
        assertEquals(ImmutableSet.of("Mem compliance", "CPU compliance"),
            ExplanationComposer.composeRelatedRisks(action, Collections.emptyList()));
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
                ExplanationComposer.composeExplanation(action, Collections.emptyList()));
        assertEquals(Collections.singleton("Increase RI Coverage"),
            ExplanationComposer.composeRelatedRisks(action, Collections.emptyList()));

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
                ExplanationComposer.composeExplanation(action, Collections.emptyList()));
        assertEquals(Collections.singleton("Invalid total demand"),
            ExplanationComposer.composeRelatedRisks(action, Collections.emptyList()));
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
            ExplanationComposer.composeExplanation(reconfigure, Collections.emptyList()));
        assertEquals(Collections.singleton("Misconfiguration"),
            ExplanationComposer.composeRelatedRisks(reconfigure, Collections.emptyList()));

        assertEquals("Configure supplier to update resource(s) Segmentation, Network " +
                        "testNetwork2",
                ExplanationComposer.composeExplanation(reconfigureWithPrefix, Collections.emptyList()));
        assertEquals(Collections.singleton("Misconfiguration"),
            ExplanationComposer.composeRelatedRisks(reconfigureWithPrefix, Collections.emptyList()));

        // Make the reconfigure with prefix  for a member of a scaling group
        builder.getExplanationBuilder().getReconfigureBuilder().setScalingGroupId("example group");
        ActionDTO.Action reconfigureWithPrefixCSG = builder.build();
        assertEquals("Configure supplier to update resource(s) Segmentation, " +
                    "Network testNetwork2 (Auto Scaling Groups: example group)",
            ExplanationComposer.composeExplanation(reconfigureWithPrefixCSG, Collections.emptyList()));
        assertEquals(Collections.singleton("Misconfiguration"),
            ExplanationComposer.composeRelatedRisks(reconfigureWithPrefixCSG, Collections.emptyList()));
    }

    /**
     * Test the explanation of reconfigure action with reason commodities.
     */
    @Test
    public void testReconfigurePodSingleCommodityExplanation() {

        ActionDTO.Action reconfigure = ActionDTO.Action.newBuilder()
                .setId(0).setInfo(ActionInfo.newBuilder().setReconfigure(
                        Reconfigure.newBuilder().setTarget(ActionEntity.newBuilder()
                                .setId(1).setType(EntityType.CONTAINER_POD_VALUE))
                                .setIsProvider(false)))
                .setDeprecatedImportance(0)
                .setExplanation(Explanation.newBuilder()
                        .setReconfigure(ReconfigureExplanation.newBuilder()
                                .addReconfigureCommodity(LABEL)))
                .build();

        assertEquals("Zero nodes match Pod's node selector",
                ExplanationComposer.composeExplanation(reconfigure, Collections.emptyList()));
        assertEquals(Collections.singleton("Misconfiguration"),
                ExplanationComposer.composeRelatedRisks(reconfigure, Collections.emptyList()));

    }

    /**
     * Test the explanation of reconfigure action with reason commodities.
     */
    @Test
    public void testReconfigurePodMultipleCommodityExplanation() {

        ActionDTO.Action reconfigure = ActionDTO.Action.newBuilder()
                .setId(0).setInfo(ActionInfo.newBuilder().setReconfigure(
                        Reconfigure.newBuilder().setTarget(ActionEntity.newBuilder()
                                .setId(1).setType(EntityType.CONTAINER_POD_VALUE))
                                .setIsProvider(false)))
                .setDeprecatedImportance(0)
                .setExplanation(Explanation.newBuilder()
                        .setReconfigure(ReconfigureExplanation.newBuilder()
                                .addReconfigureCommodity(LABEL)
                                .addReconfigureCommodity(TAINT)))
                .build();

        assertEquals("Configure node or pod to update resource(s) Taint, node-selector, affinity or anti-affinity",
                ExplanationComposer.composeExplanation(reconfigure, Collections.emptyList()));
        assertEquals(Collections.singleton("Misconfiguration"),
                ExplanationComposer.composeRelatedRisks(reconfigure, Collections.emptyList()));

    }

    /**
     * Test the explanation of reconfigure action with reason commodities other than TAINT,LABEL,VMPMACCESS.
     */
    @Test
    public void testReconfigurePodOtherCommodityExplanation() {

        ActionDTO.Action reconfigure = ActionDTO.Action.newBuilder()
                .setId(0).setInfo(ActionInfo.newBuilder().setReconfigure(
                        Reconfigure.newBuilder().setTarget(ActionEntity.newBuilder()
                                        .setId(1).setType(EntityType.CONTAINER_POD_VALUE))
                                .setIsProvider(false)))
                .setDeprecatedImportance(0)
                .setExplanation(Explanation.newBuilder()
                        .setReconfigure(ReconfigureExplanation.newBuilder()
                                .addReconfigureCommodity(NETWORK)))
                                //.addReconfigureCommodity(TAINT)))
                .build();

        assertEquals("Configure supplier to update resource(s) Network testNetwork1",
                ExplanationComposer.composeExplanation(reconfigure, Collections.emptyList()));
        assertEquals(Collections.singleton("Misconfiguration"),
                ExplanationComposer.composeRelatedRisks(reconfigure, Collections.emptyList()));

    }

    /**
     * Test explanation for Reconfigure for NotReady node action.
     */
    @Test
    public void testReconfigureForNotReadyNodeExplanation() {
        ActionDTO.Action reconfigure = ActionDTO.Action.newBuilder()
                .setId(0).setInfo(ActionInfo.newBuilder().setReconfigure(
                        Reconfigure.newBuilder().setTarget(ActionEntity.newBuilder()
                                        .setId(1).setType(EntityType.VIRTUAL_MACHINE_VALUE))
                                .setIsProvider(false)))
                .setDeprecatedImportance(0)
                .setExplanation(Explanation.newBuilder()
                        .setReconfigure(ReconfigureExplanation.newBuilder()
                                .addReconfigureCommodity(CLUSTER)))
                .build();
        featureFlagTestRule.enable(FeatureFlags.ENABLE_RECONFIGURE_ACTION_FOR_NOTREADY_NODE);
        assertEquals("The node is in a NotReady status",
                     ExplanationComposer.composeExplanation(reconfigure, Collections.emptyList()));
        featureFlagTestRule.reset();
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
                ImmutableMap.of(reasonSetting1, "setting1", reasonSetting2, "setting2"), Collections.emptyMap(),
                Optional.empty(), null, Collections.emptyList()));
        assertEquals(Collections.singleton("Misconfiguration"),
            ExplanationComposer.composeRelatedRisks(reconfigureAction, Collections.emptyList()));
    }

    /**
     * Test the explanation of reconfigure action with reason setting and setting changes.
     */
    @Test
    public void testReconfigureReasonSettingsExplanationWithSettingChange() {
        long reasonSetting1 = 1L;

        ActionDTO.Action reconfigureAction = ActionDTO.Action.newBuilder()
                .setId(0).setInfo(ActionInfo.newBuilder().setReconfigure(
                        Reconfigure.newBuilder()
                                .addSettingChange(SettingChange.newBuilder()
                                        .setCurrentValue(1)
                                        .setNewValue(2)
                                        .setEntityAttribute(EntityAttribute.SOCKET)
                                        .build())
                                .addSettingChange(SettingChange.newBuilder()
                                        .setCurrentValue(1)
                                        .setNewValue(2)
                                        .setEntityAttribute(EntityAttribute.CORES_PER_SOCKET)
                                        .build())
                                .setTarget(ActionEntity.newBuilder()
                                        .setId(1).setType(EntityType.VIRTUAL_MACHINE_VALUE)
                                        .setEnvironmentType(EnvironmentType.ON_PREM))
                                .setIsProvider(false)))
                .setDeprecatedImportance(0)
                .setExplanation(Explanation.newBuilder()
                        .setReconfigure(ReconfigureExplanation.newBuilder()
                                .addReasonSettings(reasonSetting1)))
                .build();

        assertEquals("(^_^)~setting1 out of compliance",
                ExplanationComposer.composeExplanation(reconfigureAction,
                        ImmutableMap.of(reasonSetting1, "setting1"), ImmutableMap.of(reasonSetting1, "setting1"),
                        Optional.empty(), null, Collections.emptyList()));
        assertEquals(Collections.singleton("Required setting change"),
                ExplanationComposer.composeRelatedRisks(reconfigureAction, Collections.emptyList()));
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
                ImmutableMap.of(reasonSetting1, "setting1", reasonSetting2, "setting2"), Collections.emptyMap(),
                Optional.of(graphCreator.build()), null, Collections.emptyList()));
        assertEquals(Collections.singleton("Misconfiguration"),
            ExplanationComposer.composeRelatedRisks(reconfigureAction, Collections.emptyList()));
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

        assertEquals("Mem Congestion", ExplanationComposer.composeExplanation(provision, Collections.emptyList()));
        assertEquals(Collections.singleton("Mem Congestion"), ExplanationComposer.composeRelatedRisks(provision, Collections.emptyList()));
    }

    /**
     * Test the explanation of provision by supply action for a daemon.
     */
    @Test
    public void testDaemonProvisionBySupplyExplanation() {
        ActionDTO.Action provision = ActionDTO.Action.newBuilder()
                .setId(0).setInfo(ActionInfo.getDefaultInstance()).setDeprecatedImportance(0)
                .setExplanation(Explanation.newBuilder()
                    .setProvision(ProvisionExplanation.newBuilder()
                            .setProvisionBySupplyExplanation(ProvisionBySupplyExplanation.newBuilder()
                                    .setMostExpensiveCommodityInfo(ReasonCommodity.newBuilder()
                                            .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                                                    .setType(21).build())
                                            .build())
                                    .build())
                            .setReasonEntity(1)
                        .build()))
                .build();

        assertEquals("Clone entity on cloned provider", ExplanationComposer.composeExplanation(provision, Collections.emptyList()));
        assertEquals(Collections.singleton("Clone entity on cloned provider"), ExplanationComposer.composeRelatedRisks(provision, Collections.emptyList()));
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
                ExplanationComposer.composeExplanation(provision, Collections.emptyList())));
        assertEquals(ImmutableSet.of("Mem Congestion", "CPU Congestion"),
            ExplanationComposer.composeRelatedRisks(provision, Collections.emptyList()));
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
            ExplanationComposer.composeExplanation(provision, Collections.emptyMap(), Collections.emptyMap(),
                Optional.of(graphCreator.build()), null, Collections.emptyList())));
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
                            .setType(CommodityDTO.CommodityType.VCPU_VALUE))))
            .setExplanation(Explanation.newBuilder()
                .setResize(ResizeExplanation.newBuilder()
                    .setDeprecatedStartUtilization(0.2f)
                    .setDeprecatedEndUtilization(0.4f)));
        setupTopologyGraph(entity(0, "Danny", EntityType.VIRTUAL_MACHINE_VALUE));

        // test resize down by capacity
        action.getInfoBuilder().getResizeBuilder().setOldCapacity(4).setNewCapacity(2).build();
        assertEquals("(^_^)~Underutilized VCPU in Virtual Machine {entity:0:displayName:}",
            ExplanationComposer.composeExplanation(action.build(), Collections.emptyList()));
        assertEquals(Collections.singleton("Underutilized VCPU"),
            ExplanationComposer.composeRelatedRisks(action.build(), Collections.emptyList()));
        assertEquals("(^_^)~Underutilized VCPU in Virtual Machine Danny",
            ExplanationComposer.composeExplanation(action.build(), Collections.emptyMap(), Collections.emptyMap(),
                Optional.of(graphCreator.build()), null, Collections.emptyList()));

        // test resize up by capacity
        action.getInfoBuilder().getResizeBuilder().setOldCapacity(2).setNewCapacity(4)
                .build();
        action.getExplanationBuilder().getResizeBuilder()
                .setReason(CommodityType.newBuilder().setType(CommodityDTO.CommodityType.VCPU_THROTTLING_VALUE))
                .build();
        assertEquals("(^_^)~VCPU Throttling Congestion in Virtual Machine {entity:0:displayName:}",
            ExplanationComposer.composeExplanation(action.build(), Collections.emptyList()));
        assertEquals(Collections.singleton("VCPU Throttling Congestion"),
            ExplanationComposer.composeRelatedRisks(action.build(), Collections.emptyList()));
        assertEquals("(^_^)~VCPU Throttling Congestion in Virtual Machine Danny",
            ExplanationComposer.composeExplanation(action.build(), Collections.emptyMap(), Collections.emptyMap(),
                Optional.of(graphCreator.build()), null, Collections.emptyList()));

        // Test the resize up again with scaling group information
        action.getExplanationBuilder().getResizeBuilder()
            .setScalingGroupId("example scaling group");
        assertEquals("(^_^)~VCPU Throttling Congestion in Virtual Machine {entity:0:displayName:}" +
                " (Auto Scaling Groups: example scaling group)",
            ExplanationComposer.composeExplanation(action.build(), Collections.emptyList()));
        assertEquals(Collections.singleton("VCPU Throttling Congestion"),
            ExplanationComposer.composeRelatedRisks(action.build(), Collections.emptyList()));
        assertEquals("(^_^)~VCPU Throttling Congestion in Virtual Machine Danny"
                + " (Auto Scaling Groups: example scaling group)",
            ExplanationComposer.composeExplanation(action.build(), Collections.emptyMap(), Collections.emptyMap(),
                Optional.of(graphCreator.build()), null, Collections.emptyList()));

        // Test action without Resize action
        action.getInfoBuilder().clearResize();
        assertEquals("", ExplanationComposer.composeExplanation(action.build(), Collections.emptyList()));
    }

    @Test
    public void testResizeExplanationUsingRelatedActions() {
        ActionDTO.Action.Builder action = ActionDTO.Action.newBuilder()
                .setId(0).setDeprecatedImportance(0)
                .setInfo(ActionInfo.newBuilder()
                        .setResize(Resize.newBuilder()
                                .setTarget(ActionEntity.newBuilder()
                                        .setId(0)
                                        .setType(EntityType.NAMESPACE.getNumber()))
                                .setCommodityType(CommodityType.newBuilder()
                                        .setType(CommodityDTO.CommodityType.VCPU_LIMIT_QUOTA_VALUE))))
                .setExplanation(Explanation.newBuilder()
                        .setResize(ResizeExplanation.newBuilder()
                                .setDeprecatedStartUtilization(0.2f)
                                .setDeprecatedEndUtilization(0.4f)));
        setupTopologyGraph(entity(0, "Quotas", EntityType.NAMESPACE_VALUE));

        List<ActionDTO.RelatedAction> relatedActionList = new ArrayList<>();
        ActionDTO.RelatedAction ra = ActionDTO.RelatedAction.newBuilder()
                .setActionEntity(ActionEntity.newBuilder().setId(11l).setType(EntityType.WORKLOAD_CONTROLLER_VALUE))
                .setBlockingRelation(ActionDTO.BlockingRelation.newBuilder()
                                    .setResize(ActionDTO.BlockingRelation.BlockingResize.newBuilder()
                                                    .setCommodityType(CommodityType.newBuilder()
                                                            .setType(CommodityDTO.CommodityType.VCPU_VALUE))))
                .build();
        relatedActionList.add(ra);

        ActionDTO.RelatedAction ra2 = ActionDTO.RelatedAction.newBuilder()
                .setActionEntity(ActionEntity.newBuilder().setId(12l).setType(EntityType.WORKLOAD_CONTROLLER_VALUE))
                .setBlockingRelation(ActionDTO.BlockingRelation.newBuilder()
                        .setResize(ActionDTO.BlockingRelation.BlockingResize.newBuilder()
                                .setCommodityType(CommodityType.newBuilder()
                                        .setType(CommodityDTO.CommodityType.VCPU_VALUE))))
                .build();
        relatedActionList.add(ra2);

        assertEquals("VCPU Limit Congestion in Related Workload Controller",
                ExplanationComposer.composeExplanation(action.build(),relatedActionList));

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
                                .setMergeGroupId("bar")
                                .addPerEntityExplanation(ResizeExplanationPerEntity.newBuilder()
                                        .setTargetId(1)
                                        .setPerCommodityExplanation(ResizeExplanationPerCommodity.newBuilder()
                                                .setCommodityType(CommodityType.newBuilder().setType(CommodityDTO.CommodityType.VCPU_VALUE))
                                                .setReason(CommodityType.newBuilder().setType(CommodityDTO.CommodityType.VCPU_VALUE))
                                                .build())
                                        .build())
                                .addPerEntityExplanation(ResizeExplanationPerEntity.newBuilder()
                                        .setTargetId(1)
                                        .setPerCommodityExplanation(ResizeExplanationPerCommodity.newBuilder()
                                                .setCommodityType(CommodityType.newBuilder().setType(CommodityDTO.CommodityType.VMEM_REQUEST_VALUE))
                                                .setReason(CommodityType.newBuilder().setType(CommodityDTO.CommodityType.VMEM_REQUEST_VALUE))
                                                .build())
                                        .build())
                        ));

        setupTopologyGraph(entity(1, "Irene", EntityType.CONTAINER_SPEC_VALUE));


        assertEquals("(^_^)~VCPU Limit Congestion, "
                + "Underutilized VMem Request"
                + " in Container Spec {entity:1:displayName:}",
            ExplanationComposer.composeExplanation(action.build(), Collections.emptyList()));

        assertEquals("(^_^)~VCPU Limit Congestion, "
                        + "Underutilized VMem Request"
                        + " in Container Spec Irene",
            ExplanationComposer.composeExplanation(action.build(), Collections.emptyMap(), Collections.emptyMap(),
                Optional.of(graphCreator.build()), null, Collections.emptyList()));
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
                                        .setNewCapacity(456))
                                .addResizes(ResizeInfo.newBuilder()
                                        .setTarget(ActionEntity.newBuilder()
                                                .setId(3)
                                                .setType(EntityType.CONTAINER_SPEC_VALUE))
                                        .setCommodityType(CommodityType.newBuilder().setType(CommodityDTO.CommodityType.VCPU_VALUE))
                                        .setCommodityAttribute(CommodityAttribute.CAPACITY)
                                        .setOldCapacity(456)
                                        .setNewCapacity(123))))
                .setExplanation(Explanation.newBuilder()
                        .setAtomicResize(AtomicResizeExplanation.newBuilder()
                                .setMergeGroupId("bar")
                                .addPerEntityExplanation(ResizeExplanationPerEntity.newBuilder()
                                        .setTargetId(1)
                                        .setPerCommodityExplanation(ResizeExplanationPerCommodity.newBuilder()
                                                .setCommodityType(CommodityType.newBuilder().setType(CommodityDTO.CommodityType.VCPU_VALUE))
                                                .setReason(CommodityType.newBuilder().setType(CommodityDTO.CommodityType.VCPU_VALUE))
                                                .build())
                                        .build())
                                .addPerEntityExplanation(ResizeExplanationPerEntity.newBuilder()
                                        .setTargetId(1)
                                        .setPerCommodityExplanation(ResizeExplanationPerCommodity.newBuilder()
                                                .setCommodityType(CommodityType.newBuilder().setType(CommodityDTO.CommodityType.VMEM_VALUE))
                                                .setReason(CommodityType.newBuilder().setType(CommodityDTO.CommodityType.VMEM_VALUE))
                                                .build())
                                        .build())
                                .addPerEntityExplanation(ResizeExplanationPerEntity.newBuilder()
                                        .setTargetId(2)
                                        .setPerCommodityExplanation(ResizeExplanationPerCommodity.newBuilder()
                                                .setCommodityType(CommodityType.newBuilder().setType(CommodityDTO.CommodityType.VCPU_VALUE))
                                                .setReason(CommodityType.newBuilder().setType(CommodityDTO.CommodityType.VCPU_THROTTLING_VALUE))
                                                .build())
                                        .build())
                                .addPerEntityExplanation(ResizeExplanationPerEntity.newBuilder()
                                        .setTargetId(2)
                                        .setPerCommodityExplanation(ResizeExplanationPerCommodity.newBuilder()
                                                .setCommodityType(CommodityType.newBuilder().setType(CommodityDTO.CommodityType.VMEM_VALUE))
                                                .setReason(CommodityType.newBuilder().setType(CommodityDTO.CommodityType.VMEM_VALUE))
                                                .build())
                                        .build())
                                .addPerEntityExplanation(ResizeExplanationPerEntity.newBuilder()
                                        .setTargetId(3)
                                        .setPerCommodityExplanation(ResizeExplanationPerCommodity.newBuilder()
                                                .setCommodityType(CommodityType.newBuilder().setType(CommodityDTO.CommodityType.VCPU_VALUE))
                                                .setReason(CommodityType.newBuilder().setType(CommodityDTO.CommodityType.VCPU_THROTTLING_VALUE))
                                                .build())
                                        .build())
                        ));
        setupTopologyGraph(entity(0, "Harry", EntityType.WORKLOAD_CONTROLLER_VALUE));
        setupTopologyGraph(entity(1, "Irene", EntityType.CONTAINER_SPEC_VALUE));
        setupTopologyGraph(entity(2, "John", EntityType.CONTAINER_SPEC_VALUE));
        setupTopologyGraph(entity(3, "Kelly", EntityType.CONTAINER_SPEC_VALUE));

        String explanation = ExplanationComposer.composeExplanation(action.build(), Collections.emptyList());
        assertTrue(explanation.contains("Underutilized VMem Limit, "
                                         + "VCPU Throttling Congestion"
                                         + " in Container Spec {entity:2:displayName:}"));
        assertTrue(explanation.contains("VCPU Limit Congestion, "
                + "Underutilized VMem Limit"
                + " in Container Spec {entity:1:displayName:}"));
        assertTrue(explanation.contains("Underutilized VCPU Limit"
                + " in Container Spec {entity:3:displayName:}"));

        String translatedExplanation = ExplanationComposer.composeExplanation(action.build(), Collections.emptyMap(), Collections.emptyMap(),
                Optional.of(graphCreator.build()), null, Collections.emptyList());
        assertTrue(translatedExplanation.contains("Underutilized VMem Limit, "
                + "VCPU Throttling Congestion"
                + " in Container Spec John"));
        assertTrue(translatedExplanation.contains("VCPU Limit Congestion, "
                + "Underutilized VMem Limit"
                + " in Container Spec Irene"));
        assertTrue(translatedExplanation.contains("Underutilized VCPU Limit"
                + " in Container Spec Kelly"));
    }

    @Test
    public void testActivateExplanation() {
        ActionDTO.Action activate = ActionDTO.Action.newBuilder()
                .setId(0).setInfo(ActionInfo.getDefaultInstance()).setDeprecatedImportance(0)
                .setExplanation(Explanation.newBuilder()
                .setActivate(ActivateExplanation.newBuilder()
                    .setMostExpensiveCommodity(CPU.getCommodityType().getType())))
                .build();

        assertEquals("Address high utilization of CPU", ExplanationComposer.composeExplanation(activate, Collections.emptyList()));
        assertEquals(Collections.singleton("CPU Congestion"),
            ExplanationComposer.composeRelatedRisks(activate, Collections.emptyList()));
    }

    @Test
    public void testDeactivateExplanation() {
        ActionDTO.Action deactivate = ActionDTO.Action.newBuilder()
            .setId(0).setInfo(ActionInfo.getDefaultInstance()).setDeprecatedImportance(0)
            .setExplanation(Explanation.newBuilder()
                .setDeactivate(DeactivateExplanation.getDefaultInstance()))
            .build();

        assertEquals("Improve infrastructure efficiency", ExplanationComposer.composeExplanation(deactivate, Collections.emptyList()));
        assertEquals(Collections.singleton("Improve infrastructure efficiency"),
            ExplanationComposer.composeRelatedRisks(deactivate, Collections.emptyList()));
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
            ExplanationComposer.composeExplanation(deleteVolume, Collections.emptyList()));
        assertEquals(Collections.singleton("Increase savings"),
            ExplanationComposer.composeRelatedRisks(deleteVolume, Collections.emptyList()));


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
            ExplanationComposer.composeExplanation(deleteFiles, Collections.emptyList()));
        assertEquals(Collections.singleton("Idle or non-productive"),
            ExplanationComposer.composeRelatedRisks(deleteFiles, Collections.emptyList()));
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
        assertEquals(expectedExplanation, ExplanationComposer.composeExplanation(action, Collections.emptyList()));
        assertEquals(Collections.singleton("Virtual Machine RI Coverage"),
            ExplanationComposer.composeRelatedRisks(action, Collections.emptyList()));
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

    private static ReasonCommodity createReasonCommodity(int baseType, Suffix suffix) {
        CommodityType.Builder ct = TopologyDTO.CommodityType.newBuilder()
                .setType(baseType);
        final ReasonCommodity.Builder reasonCommodity =
                ReasonCommodity.newBuilder().setCommodityType(ct.build()).setSuffix(suffix);
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
