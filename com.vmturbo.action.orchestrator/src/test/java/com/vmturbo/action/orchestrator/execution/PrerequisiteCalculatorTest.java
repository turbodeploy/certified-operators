package com.vmturbo.action.orchestrator.execution;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;

import org.junit.Test;

import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory.EntitiesAndSettingsSnapshot;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.Prerequisite;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.PrerequisiteType;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.action.ActionDTO.Reconfigure;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ApplicationInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.Architecture;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualizationType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;

/**
 * Tests for calculating pre-requisites of an action.
 */
public class PrerequisiteCalculatorTest {

    private final EntitiesAndSettingsSnapshot snapshot = mock(EntitiesAndSettingsSnapshot.class);

    /**
     * Build an action.
     *
     * @param destinationId destination id of an action
     * @return an action
     */
    private static Action buildAction(final long destinationId) {
        return Action.newBuilder().setId(0).setDeprecatedImportance(0)
            .setInfo(ActionInfo.newBuilder().setMove(Move.newBuilder()
                .setTarget(ActionEntity.newBuilder().setId(1)
                    .setType(EntityType.VIRTUAL_MACHINE.getNumber()))
                .addChanges(ChangeProvider.newBuilder()
                    .setDestination(ActionEntity.newBuilder()
                        .setId(destinationId).setType(EntityType.COMPUTE_TIER.getNumber())
                    ))))
            .setExplanation(Explanation.getDefaultInstance()).build();
    }

    /**
     * Build an {@link ActionPartialEntity} for vm.
     *
     * @param prerequisiteTypes an array of {@link PrerequisiteType}
     * @return an {@link ActionPartialEntity}
     */
    private static ActionPartialEntity buildVMActionPartialEntity(PrerequisiteType... prerequisiteTypes) {
        final VirtualMachineInfo.Builder builder = VirtualMachineInfo.newBuilder();
        for (PrerequisiteType type : prerequisiteTypes) {
            switch (type) {
                case ENA:
                    builder.getDriverInfoBuilder().setHasEnaDriver(false);
                    break;
                case NVME:
                    builder.getDriverInfoBuilder().setHasNvmeDriver(false);
                    break;
                case ARCHITECTURE:
                    builder.setArchitecture(Architecture.BIT_32);
                    break;
                case VIRTUALIZATION_TYPE:
                    builder.setVirtualizationType(VirtualizationType.HVM);
                    break;
            }
        }

        return ActionPartialEntity.newBuilder().setOid(0)
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                .setVirtualMachine(builder)).build();
    }

    /**
     * Build an {@link ActionPartialEntity} for compute tier.
     *
     * @param prerequisiteTypes an array of {@link PrerequisiteType}
     * @return an {@link ActionPartialEntity}
     */
    private static ActionPartialEntity buildComputeTierActionPartialEntity(PrerequisiteType... prerequisiteTypes) {
        final ComputeTierInfo.Builder builder = ComputeTierInfo.newBuilder();
        for (PrerequisiteType type : prerequisiteTypes) {
            switch (type) {
                case ENA:
                    builder.getSupportedCustomerInfoBuilder().setSupportsOnlyEnaVms(true);
                    break;
                case NVME:
                    builder.getSupportedCustomerInfoBuilder().setSupportsOnlyNVMeVms(true);
                    break;
                case ARCHITECTURE:
                    builder.getSupportedCustomerInfoBuilder()
                        .addSupportedArchitectures(Architecture.BIT_64);
                    break;
                case VIRTUALIZATION_TYPE:
                    builder.getSupportedCustomerInfoBuilder()
                        .addSupportedVirtualizationTypes(VirtualizationType.PVM);
                    break;
            }
        }

        return ActionPartialEntity.newBuilder().setOid(0)
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                .setComputeTier(builder)).build();
    }

    /**
     * Test {@link PrerequisiteCalculator#calculatePrerequisites}.
     */
    @Test
    public void testCalculatePrerequisitesEna() {
        final long destinationId = 1;
        final Action action = buildAction(destinationId);
        final ActionPartialEntity target = buildVMActionPartialEntity(PrerequisiteType.ENA);
        final ActionPartialEntity destination =
            buildComputeTierActionPartialEntity(PrerequisiteType.ENA);

        doReturn(Optional.of(destination)).when(snapshot).getEntityFromOid(destinationId);

        assertThat(PrerequisiteCalculator.calculatePrerequisites(
            action, target, snapshot, ProbeCategory.CLOUD_MANAGEMENT),
            is(Collections.singleton(Prerequisite.newBuilder()
                .setPrerequisiteType(PrerequisiteType.ENA).build())));
    }

    /**
     * Test {@link PrerequisiteCalculator#calculatePrerequisites}.
     */
    @Test
    public void testCalculatePrerequisitesNVMe() {
        final long destinationId = 1;
        final Action action = buildAction(destinationId);
        final ActionPartialEntity target = buildVMActionPartialEntity(PrerequisiteType.NVME);
        final ActionPartialEntity destination =
            buildComputeTierActionPartialEntity(PrerequisiteType.NVME);

        doReturn(Optional.of(destination)).when(snapshot).getEntityFromOid(destinationId);

        assertThat(PrerequisiteCalculator.calculatePrerequisites(
            action, target, snapshot, ProbeCategory.CLOUD_MANAGEMENT),
            is(Collections.singleton(Prerequisite.newBuilder()
                .setPrerequisiteType(PrerequisiteType.NVME).build())));
    }

    /**
     * Test {@link PrerequisiteCalculator#calculatePrerequisites}.
     */
    @Test
    public void testCalculatePrerequisitesArchitecture() {
        final long destinationId = 1;
        final Action action = buildAction(destinationId);
        final ActionPartialEntity target = buildVMActionPartialEntity(PrerequisiteType.ARCHITECTURE);
        final ActionPartialEntity destination =
            buildComputeTierActionPartialEntity(PrerequisiteType.ARCHITECTURE);

        doReturn(Optional.of(destination)).when(snapshot).getEntityFromOid(destinationId);

        assertThat(PrerequisiteCalculator.calculatePrerequisites(
            action, target, snapshot, ProbeCategory.CLOUD_MANAGEMENT),
            is(Collections.singleton(Prerequisite.newBuilder()
                .setPrerequisiteType(PrerequisiteType.ARCHITECTURE).build())));
    }

    /**
     * Test {@link PrerequisiteCalculator#calculatePrerequisites}.
     */
    @Test
    public void testCalculatePrerequisitesVirtualizationType() {
        final long destinationId = 1;
        final Action action = buildAction(destinationId);
        final ActionPartialEntity target =
            buildVMActionPartialEntity(PrerequisiteType.VIRTUALIZATION_TYPE);
        final ActionPartialEntity destination =
            buildComputeTierActionPartialEntity(PrerequisiteType.VIRTUALIZATION_TYPE);

        doReturn(Optional.of(destination)).when(snapshot).getEntityFromOid(destinationId);

        assertThat(PrerequisiteCalculator.calculatePrerequisites(
            action, target, snapshot, ProbeCategory.CLOUD_MANAGEMENT),
            is(Collections.singleton(Prerequisite.newBuilder()
                .setPrerequisiteType(PrerequisiteType.VIRTUALIZATION_TYPE).build())));
    }

    /**
     * Test {@link PrerequisiteCalculator#calculatePrerequisites}.
     */
    @Test
    public void testCalculatePrerequisitesMultiplePrerequisites() {
        final long destinationId = 1;
        final Action action = buildAction(destinationId);
        final ActionPartialEntity target = buildVMActionPartialEntity(PrerequisiteType.ENA,
            PrerequisiteType.NVME, PrerequisiteType.ARCHITECTURE, PrerequisiteType.VIRTUALIZATION_TYPE);
        final ActionPartialEntity destination =
            buildComputeTierActionPartialEntity(PrerequisiteType.ENA,
                PrerequisiteType.NVME, PrerequisiteType.ARCHITECTURE, PrerequisiteType.VIRTUALIZATION_TYPE);

        doReturn(Optional.of(destination)).when(snapshot).getEntityFromOid(destinationId);

        assertThat(new HashSet<>(PrerequisiteCalculator.calculatePrerequisites(
            action, target, snapshot, ProbeCategory.CLOUD_MANAGEMENT)),
            is(new HashSet<>(Arrays.asList(
                Prerequisite.newBuilder().setPrerequisiteType(PrerequisiteType.ENA).build(),
                Prerequisite.newBuilder().setPrerequisiteType(PrerequisiteType.NVME).build(),
                Prerequisite.newBuilder().setPrerequisiteType(PrerequisiteType.ARCHITECTURE).build(),
                Prerequisite.newBuilder().setPrerequisiteType(PrerequisiteType.VIRTUALIZATION_TYPE).build()))));
    }

    /**
     * Test {@link PrerequisiteCalculator#calculatePrerequisites}.
     */
    @Test
    public void testCalculatePrerequisitesReturnsEmptyWithCorrectInput() {
        final long destinationId = 1;
        doReturn(Optional.empty()).when(snapshot).getEntityFromOid(destinationId);
        assertTrue(PrerequisiteCalculator.calculatePrerequisites(
            buildAction(destinationId),
            buildVMActionPartialEntity(PrerequisiteType.ENA),
            snapshot,
            ProbeCategory.CLOUD_MANAGEMENT).isEmpty());
        verify(snapshot, times(1)).getEntityFromOid(destinationId);
    }

    /**
     * Test {@link PrerequisiteCalculator#calculatePrerequisites}.
     */
    @Test
    public void testCalculatePrerequisitesReturnsEmptyWithWrongProbeCategory() {
        assertTrue(PrerequisiteCalculator.calculatePrerequisites(
            Action.getDefaultInstance(),
            ActionPartialEntity.getDefaultInstance(),
            snapshot,
            ProbeCategory.HYPERVISOR).isEmpty());
    }

    /**
     * Test {@link PrerequisiteCalculator#calculatePrerequisites}.
     */
    @Test
    public void testCalculatePrerequisitesReturnsEmptyWithWrongActionType() {
        assertTrue(PrerequisiteCalculator.calculatePrerequisites(
            Action.newBuilder().setId(0).setDeprecatedImportance(0)
                .setInfo(ActionInfo.newBuilder().setReconfigure(Reconfigure.newBuilder()
                    .setTarget(ActionEntity.newBuilder().setId(1)
                        .setType(EntityType.VIRTUAL_MACHINE.getNumber()))))
                .setExplanation(Explanation.getDefaultInstance()).build(),
            ActionPartialEntity.getDefaultInstance(),
            snapshot,
            ProbeCategory.CLOUD_MANAGEMENT).isEmpty());
    }

    /**
     * Test {@link PrerequisiteCalculator#calculatePrerequisites}.
     */
    @Test
    public void testCalculatePrerequisitesReturnsEmptyWithoutVirtualMachineInfo() {
        assertTrue(PrerequisiteCalculator.calculatePrerequisites(
            buildAction(0),
            ActionPartialEntity.newBuilder().setOid(2)
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                    .setApplication(ApplicationInfo.getDefaultInstance())).build(),
            snapshot,
            ProbeCategory.CLOUD_MANAGEMENT).isEmpty());
    }
}