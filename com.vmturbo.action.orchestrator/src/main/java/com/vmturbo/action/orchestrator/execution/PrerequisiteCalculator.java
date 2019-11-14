package com.vmturbo.action.orchestrator.execution;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;

import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory.EntitiesAndSettingsSnapshot;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.Prerequisite;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.PrerequisiteType;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo.ActionTypeCase;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;

/**
 * A class responsible for calculating pre-requisites of an action. Here pre-requisites of an action
 * means that some pre-requisites need to be satisfied in order to execute an action.
 *
 * <p>For example, if an action recommends moving a 32-bit VM to a compute tier that only supports
 * 64-bit, then a pre-requisite of enabling 64-bit AMIs for the VM or excluding templates that
 * require 64-bit AMIs will be generated along with the action.
 */
class PrerequisiteCalculator {

    // A list of single calculators for different type of pre-requisite.
    private static List<SinglePrerequisiteCalculator> prerequisiteCalculators = ImmutableList.of(
        PrerequisiteCalculator::calculatePrerequisiteEna,
        PrerequisiteCalculator::calculatePrerequisiteNVMe,
        PrerequisiteCalculator::calculatePrerequisiteArchitecture,
        PrerequisiteCalculator::calculatePrerequisiteVirtualizationType
    );

    /**
     * Private to prevent instantiation.
     */
    private PrerequisiteCalculator() {}

    /**
     * Calculate pre-requisites for a given action.
     *
     * @param action the action pre-requisites will be calculated for
     * @param target the target of the action
     * @param snapshot the snapshot of entities used to fetch entity information
     * @param probeCategory the category of the probe which discovers the target
     * @return a set of pre-requisites
     */
    @Nonnull
    static Set<Prerequisite> calculatePrerequisites(
            @Nonnull final Action action,
            @Nonnull final ActionPartialEntity target,
            @Nonnull final EntitiesAndSettingsSnapshot snapshot,
            @Nonnull final ProbeCategory probeCategory) {
        // Check if the category of the probe which discovers the target is CLOUD_MANAGEMENT and this
        // action is a Move action and the target of the action has virtual machine type specific info.
        // If not, there's no need to calculate pre-requisites for this action because
        // no pre-requisites will be generated for such an action.
        if (probeCategory != ProbeCategory.CLOUD_MANAGEMENT ||
            action.getInfo().getActionTypeCase() != ActionTypeCase.MOVE ||
            !target.getTypeSpecificInfo().hasVirtualMachine()) {
            return Collections.emptySet();
        }

        for (ChangeProvider changeProvider : action.getInfo().getMove().getChangesList()) {
            if (changeProvider.hasDestination()) {
                long destinationId = changeProvider.getDestination().getId();
                Optional<ActionPartialEntity> destinationOptional =
                    snapshot.getEntityFromOid(destinationId);
                if (destinationOptional.isPresent() &&
                    destinationOptional.get().getTypeSpecificInfo().hasComputeTier()) {
                    // Calculate pre-requisites when target has VirtualMachineInfo and
                    // destination has ComputeTierInfo.
                    return prerequisiteCalculators.stream()
                        .map(calculator -> calculator.calculate(
                            target.getTypeSpecificInfo().getVirtualMachine(),
                            destinationOptional.get().getTypeSpecificInfo().getComputeTier()))
                        .filter(Optional::isPresent)
                        .map(Optional::get)
                        .collect(Collectors.toSet());
                }
            }
        }

        return Collections.emptySet();
    }

    /**
     * This functional interface defines a single pre-requisites calculator.
     */
    @FunctionalInterface
    private interface SinglePrerequisiteCalculator {
        /**
         * Calculate a specific type of pre-requisite.
         *
         * @param virtualMachineInfo virtualMachineInfo which contains pre-requisite info
         * @param computeTierInfo computeTierInfo which contains pre-requisite info
         * @return a {@link Prerequisite} if there's any
         */
        Optional<Prerequisite> calculate(VirtualMachineInfo virtualMachineInfo,
                                         ComputeTierInfo computeTierInfo);
    }

    /**
     * Calculate ENA pre-requisite. ENA (Elastic Network Adapter) driver is necessary for access to
     * Enhanced Networking on AWS EC2 instances.
     *
     * @param virtualMachineInfo virtualMachineInfo which contains pre-requisite info
     * @param computeTierInfo computeTierInfo which contains pre-requisite info
     * @return a {@link Prerequisite} if there's any
     */
    private static Optional<Prerequisite> calculatePrerequisiteEna(
            @Nonnull final VirtualMachineInfo virtualMachineInfo,
            @Nonnull final ComputeTierInfo computeTierInfo) {
        // Check if the compute tier supports only Ena vms.
        final boolean computeTierSupportsOnlyEnaVms = computeTierInfo.hasSupportedCustomerInfo() &&
            computeTierInfo.getSupportedCustomerInfo().hasSupportsOnlyEnaVms() &&
            computeTierInfo.getSupportedCustomerInfo().getSupportsOnlyEnaVms();
        // Check if the vm has an Ena driver.
        final boolean vmHasEnaDriver = virtualMachineInfo.hasDriverInfo() &&
            virtualMachineInfo.getDriverInfo().hasHasEnaDriver() &&
            virtualMachineInfo.getDriverInfo().getHasEnaDriver();

        if (computeTierSupportsOnlyEnaVms && !vmHasEnaDriver) {
            return Optional.of(Prerequisite.newBuilder()
                .setPrerequisiteType(PrerequisiteType.ENA).build());
        }

        return Optional.empty();
    }

    /**
     * Calculate NVMMe pre-requisite. NVMMe (non-volatile memory express) driver is necessary for access to
     * NVMMe block device such as EBS volumes and instance store volumes.
     *
     * @param virtualMachineInfo virtualMachineInfo which contains pre-requisite info
     * @param computeTierInfo computeTierInfo which contains pre-requisite info
     * @return a {@link Prerequisite} if there's any
     */
    private static Optional<Prerequisite> calculatePrerequisiteNVMe(
            @Nonnull final VirtualMachineInfo virtualMachineInfo,
            @Nonnull final ComputeTierInfo computeTierInfo) {
        // Check if the compute tier supports only NVMe vms.
        final boolean computeTierSupportsOnlyNVMeVms = computeTierInfo.hasSupportedCustomerInfo() &&
            computeTierInfo.getSupportedCustomerInfo().hasSupportsOnlyNVMeVms() &&
            computeTierInfo.getSupportedCustomerInfo().getSupportsOnlyNVMeVms();
        // Check if the vm has a NVMe driver.
        final boolean vmHasNvmeDriver = virtualMachineInfo.hasDriverInfo() &&
            virtualMachineInfo.getDriverInfo().hasHasNvmeDriver() &&
            virtualMachineInfo.getDriverInfo().getHasNvmeDriver();

        if (computeTierSupportsOnlyNVMeVms && !vmHasNvmeDriver) {
            return Optional.of(Prerequisite.newBuilder()
                .setPrerequisiteType(PrerequisiteType.NVME).build());
        }

        return Optional.empty();
    }

    /**
     * Calculate architecture pre-requisite.
     *
     * @param virtualMachineInfo virtualMachineInfo which contains pre-requisite info
     * @param computeTierInfo computeTierInfo which contains pre-requisite info
     * @return a {@link Prerequisite} if there's any
     */
    private static Optional<Prerequisite> calculatePrerequisiteArchitecture(
            @Nonnull final VirtualMachineInfo virtualMachineInfo,
            @Nonnull final ComputeTierInfo computeTierInfo) {
        // Check if the compute tier has supported architectures.
        final boolean computeTierHasSupportedArchitectures =
            computeTierInfo.hasSupportedCustomerInfo() &&
                !computeTierInfo.getSupportedCustomerInfo().getSupportedArchitecturesList().isEmpty();
        // Check if the vm has architecture.
        final boolean vmHasArchitecture = virtualMachineInfo.hasArchitecture();

        if (computeTierHasSupportedArchitectures &&
            vmHasArchitecture &&
            !computeTierInfo.getSupportedCustomerInfo().getSupportedArchitecturesList()
                .contains(virtualMachineInfo.getArchitecture())) {
            return Optional.of(Prerequisite.newBuilder()
                .setPrerequisiteType(PrerequisiteType.ARCHITECTURE).build());
        }

        return Optional.empty();
    }

    /**
     * Calculate virtualization type pre-requisite.
     *
     * @param virtualMachineInfo virtualMachineInfo which contains pre-requisite info
     * @param computeTierInfo computeTierInfo which contains pre-requisite info
     * @return a {@link Prerequisite} if there's any
     */
    private static Optional<Prerequisite> calculatePrerequisiteVirtualizationType(
            @Nonnull final VirtualMachineInfo virtualMachineInfo,
            @Nonnull final ComputeTierInfo computeTierInfo) {
        // Check if the compute tier has supported virtualization types.
        final boolean computeTierHasSupportedVirtualizationTypes =
            computeTierInfo.hasSupportedCustomerInfo() &&
                !computeTierInfo.getSupportedCustomerInfo().getSupportedVirtualizationTypesList().isEmpty();
        // Check if the vm has virtualization type.
        final boolean vmHasVirtualizationType = virtualMachineInfo.hasVirtualizationType();

        if (computeTierHasSupportedVirtualizationTypes &&
            vmHasVirtualizationType &&
            !computeTierInfo.getSupportedCustomerInfo().getSupportedVirtualizationTypesList()
                .contains(virtualMachineInfo.getVirtualizationType())) {
            return Optional.of(Prerequisite.newBuilder()
                .setPrerequisiteType(PrerequisiteType.VIRTUALIZATION_TYPE).build());
        }

        return Optional.empty();
    }
}
