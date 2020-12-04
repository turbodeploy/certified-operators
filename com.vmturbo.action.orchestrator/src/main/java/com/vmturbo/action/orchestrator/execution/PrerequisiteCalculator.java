package com.vmturbo.action.orchestrator.execution;

import static com.vmturbo.components.common.setting.EntitySettingSpecs.IgnoreNvmePreRequisite;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;

import com.vmturbo.action.orchestrator.action.constraint.ActionConstraintStore;
import com.vmturbo.action.orchestrator.action.constraint.ActionConstraintStoreFactory;
import com.vmturbo.action.orchestrator.action.constraint.CoreQuotaStore;
import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory.EntitiesAndSettingsSnapshot;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.Prerequisite;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.PrerequisiteType;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo.ActionTypeCase;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity.ActionEntityTypeSpecificInfo.ActionComputeTierInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity.ActionEntityTypeSpecificInfo.ActionVirtualMachineInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.EntityWithConnections;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
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
     * @param actionConstraintStoreFactory the factory which has access to all action constraint stores
     * @return a set of pre-requisites
     */
    @Nonnull
    static Set<Prerequisite> calculatePrerequisites(
            @Nonnull final Action action,
            @Nonnull final ActionPartialEntity target,
            @Nonnull final EntitiesAndSettingsSnapshot snapshot,
            @Nonnull final ProbeCategory probeCategory,
            @Nonnull final ActionConstraintStoreFactory actionConstraintStoreFactory) {
        // There is no need to generate prerequisites for migration actions.
        if (TopologyDTOUtil.isMigrationAction(action)) {
            return Collections.emptySet();
        }
        final Set<Prerequisite> generalPrerequisites = calculateGeneralPrerequisites(
            action, target, snapshot, probeCategory);
        final Set<Prerequisite> coreQuotaPrerequisites = calculateQuotaPrerequisite(
            action, target, snapshot, probeCategory, actionConstraintStoreFactory.getCoreQuotaStore());
        final Set<Prerequisite> azureScaleSetPrerequisites = calculateAzureScaleSetPrerequisite(
                action, target, snapshot);

        if (generalPrerequisites.isEmpty()
                && coreQuotaPrerequisites.isEmpty()
                && azureScaleSetPrerequisites.isEmpty()) {
            return Collections.emptySet();
        }

        final Set<Prerequisite> prerequisites =
            new HashSet<>(generalPrerequisites.size()
                    + coreQuotaPrerequisites.size()
                    + azureScaleSetPrerequisites.size());
        prerequisites.addAll(generalPrerequisites);
        prerequisites.addAll(coreQuotaPrerequisites);
        prerequisites.addAll(azureScaleSetPrerequisites);
        return prerequisites;
    }

    /**
     * This functional interface defines a single pre-requisites calculator.
     */
    @FunctionalInterface
    private interface GeneralPrerequisiteCalculator {
        /**
         * Calculate a specific type of pre-requisite.
         *
         * @param virtualMachineInfo virtualMachineInfo which contains pre-requisite info
         * @param computeTierInfo computeTierInfo which contains pre-requisite info
         * @param settingsForTargetEntity settings for the target entity
         * @return a {@link Prerequisite} if there's any
         */
        Optional<Prerequisite> calculate(ActionVirtualMachineInfo virtualMachineInfo,
                                         ActionComputeTierInfo computeTierInfo,
                                         Map<String, Setting> settingsForTargetEntity);
    }

    // A list of single calculators for different type of pre-requisite.
    private static List<GeneralPrerequisiteCalculator> generalPrerequisiteCalculators = ImmutableList.of(
            PrerequisiteCalculator::calculateEnaPrerequisite,
            PrerequisiteCalculator::calculateNVMePrerequisite,
            PrerequisiteCalculator::calculateArchitecturePrerequisite,
            PrerequisiteCalculator::calculateVirtualizationTypePrerequisite,
            PrerequisiteCalculator::calculateLockPrerequisite
    );

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
    static Set<Prerequisite> calculateGeneralPrerequisites(
            @Nonnull final Action action,
            @Nonnull final ActionPartialEntity target,
            @Nonnull final EntitiesAndSettingsSnapshot snapshot,
            @Nonnull final ProbeCategory probeCategory) {
        // Check if the category of the probe which discovers the target is CLOUD_MANAGEMENT and this
        // action is a Move action and the target of the action has virtual machine type specific info.
        // If not, there's no need to calculate pre-requisites for this action because
        // no pre-requisites will be generated for such an action.
        if (probeCategory != ProbeCategory.CLOUD_MANAGEMENT
                || action.getInfo().getActionTypeCase() != ActionTypeCase.MOVE
                || !target.getTypeSpecificInfo().hasVirtualMachine()) {
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
                    Map<String, Setting> settingsForTargetEntity = snapshot.getSettingsForEntity(target.getOid());
                    return generalPrerequisiteCalculators.stream()
                        .map(calculator -> calculator.calculate(
                            target.getTypeSpecificInfo().getVirtualMachine(),
                            destinationOptional.get().getTypeSpecificInfo().getComputeTier(),
                            settingsForTargetEntity))
                        .filter(Optional::isPresent)
                        .map(Optional::get)
                        .collect(Collectors.toSet());
                }
            }
        }

        return Collections.emptySet();
    }

    /**
     * Calculate ENA pre-requisite. ENA (Elastic Network Adapter) driver is necessary for access to
     * Enhanced Networking on AWS EC2 instances.
     *
     * @param virtualMachineInfo virtualMachineInfo which contains pre-requisite info
     * @param computeTierInfo computeTierInfo which contains pre-requisite info
     * @param settingsForTargetEntity settings for the target entity
     * @return a {@link Prerequisite} if there's any
     */
    private static Optional<Prerequisite> calculateEnaPrerequisite(
        @Nonnull final ActionVirtualMachineInfo virtualMachineInfo,
        @Nonnull final ActionComputeTierInfo computeTierInfo,
        @Nonnull final Map<String, Setting> settingsForTargetEntity) {
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
     * @param settingsForTargetEntity settings for the target entity
     * @return a {@link Prerequisite} if there's any
     */
    private static Optional<Prerequisite> calculateNVMePrerequisite(
            @Nonnull final ActionVirtualMachineInfo virtualMachineInfo,
            @Nonnull final ActionComputeTierInfo computeTierInfo,
            @Nonnull final Map<String, Setting> settingsForTargetEntity) {
        Setting ignoreNvmeSetting = settingsForTargetEntity.get(IgnoreNvmePreRequisite.getSettingName());
        boolean ignoreNvme = ignoreNvmeSetting != null &&
            ignoreNvmeSetting.hasBooleanSettingValue() &&
            ignoreNvmeSetting.getBooleanSettingValue().getValue();
        if (!ignoreNvme) {
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
        }
        return Optional.empty();
    }

    /**
     * Checks if a lock prerequisite needs to be added for the VM, based on whether we got info
     * about any read-only locks for the VM from probe.
     *
     * @param virtualMachineInfo VM info which may have optional read-only lock message set.
     * @param computeTierInfo Not used.
     * @param settingsForTargetEntity Not used.
     * @return Prerequisite if there is a valid read-only lock, or empty.
     */
    private static Optional<Prerequisite> calculateLockPrerequisite(
            @Nonnull final ActionVirtualMachineInfo virtualMachineInfo,
            @Nonnull final ActionComputeTierInfo computeTierInfo,
            @Nonnull final Map<String, Setting> settingsForTargetEntity) {
        if (!virtualMachineInfo.hasLocks() ||
                StringUtils.isBlank(virtualMachineInfo.getLocks())) {
            return Optional.empty();
        }
        return Optional.of(Prerequisite.newBuilder()
                .setPrerequisiteType(PrerequisiteType.LOCKS)
                .setLocks(virtualMachineInfo.getLocks())
                .build());
    }

    /**
     * Calculate architecture pre-requisite.
     *
     * @param virtualMachineInfo virtualMachineInfo which contains pre-requisite info
     * @param computeTierInfo computeTierInfo which contains pre-requisite info
     * @param settingsForTargetEntity settings for the target entity
     * @return a {@link Prerequisite} if there's any
     */
    private static Optional<Prerequisite> calculateArchitecturePrerequisite(
        @Nonnull final ActionVirtualMachineInfo virtualMachineInfo,
        @Nonnull final ActionComputeTierInfo computeTierInfo,
        @Nonnull final Map<String, Setting> settingsForTargetEntity) {
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
     * @param settingsForTargetEntity settings for the target entity
     * @return a {@link Prerequisite} if there's any
     */
    private static Optional<Prerequisite> calculateVirtualizationTypePrerequisite(
            @Nonnull final ActionVirtualMachineInfo virtualMachineInfo,
            @Nonnull final ActionComputeTierInfo computeTierInfo,
            @Nonnull final Map<String, Setting> settingsForTargetEntity) {
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

    /**
     * Calculate pre-requisites for a given action. We now only calculate core quota.
     *
     * @param action the action pre-requisites will be calculated for
     * @param target the target of the action
     * @param snapshot the snapshot of entities used to fetch entity information
     * @param probeCategory the category of the probe which discovers the target
     * @param actionConstraintStore a store that contains all core quota info
     * @return a set of pre-requisites
     */
    @Nonnull
    static Set<Prerequisite> calculateQuotaPrerequisite(
        @Nonnull final Action action,
        @Nonnull final ActionPartialEntity target,
        @Nonnull final EntitiesAndSettingsSnapshot snapshot,
        @Nonnull final ProbeCategory probeCategory,
        @Nonnull final ActionConstraintStore actionConstraintStore) {
        if (!(actionConstraintStore instanceof CoreQuotaStore)) {
            return Collections.emptySet();
        }

        // Check if the category of the probe which discovers the target is CLOUD_MANAGEMENT and this
        // action is a Move action. If not, there's no need to calculate pre-requisites for this
        // action because no pre-requisites will be generated for such an action.
        if (probeCategory != ProbeCategory.CLOUD_MANAGEMENT ||
            action.getInfo().getActionTypeCase() != ActionTypeCase.MOVE) {
            return Collections.emptySet();
        }

        // Get associated business account id.
        Optional<Long> businessAccountId = snapshot.getOwnerAccountOfEntity(target.getOid())
            .map(EntityWithConnections::getOid);
        if (!businessAccountId.isPresent()) {
            return Collections.emptySet();
        }

        for (ChangeProvider changeProvider : action.getInfo().getMove().getChangesList()) {
            if (changeProvider.hasSource() && changeProvider.hasDestination()) {

                long sourceId = changeProvider.getSource().getId();
                Optional<ActionPartialEntity> sourceOptional =
                    snapshot.getEntityFromOid(sourceId);
                long destinationId = changeProvider.getDestination().getId();
                Optional<ActionPartialEntity> destinationOptional =
                    snapshot.getEntityFromOid(destinationId);

                if (sourceOptional.isPresent() &&
                    sourceOptional.get().getTypeSpecificInfo().hasComputeTier() &&
                    destinationOptional.isPresent() &&
                    destinationOptional.get().getTypeSpecificInfo().hasComputeTier()) {

                    final ActionComputeTierInfo sourceComputeTierInfo =
                        sourceOptional.get().getTypeSpecificInfo().getComputeTier();
                    final String sourceFamily = sourceComputeTierInfo.getQuotaFamily();
                    final int sourceNumCores = sourceComputeTierInfo.getNumCores();

                    final ActionComputeTierInfo destinationComputeTierInfo =
                        destinationOptional.get().getTypeSpecificInfo().getComputeTier();
                    final String destinationFamily = destinationComputeTierInfo.getQuotaFamily();
                    final int destinationNumCores = destinationComputeTierInfo.getNumCores();

                    // Get the connected region id.
                    final Optional<Long> regionIdOptional = target.getConnectedEntitiesList()
                        .stream().filter(connectedEntity ->
                            connectedEntity.getConnectedEntityType() == EntityType.REGION_VALUE)
                        .map(ConnectedEntity::getConnectedEntityId).findFirst();
                    if (!regionIdOptional.isPresent()) {
                        return Collections.emptySet();
                    }

                    final Optional<Prerequisite> prerequisite =
                        calculateCoreQuotaPrerequisite(sourceFamily, sourceNumCores,
                            destinationFamily, destinationNumCores, businessAccountId.get(),
                            regionIdOptional.get(), (CoreQuotaStore)actionConstraintStore);

                    return prerequisite.map(Collections::singleton).orElse(Collections.emptySet());
                }
            }
        }

        return Collections.emptySet();
    }

    /**
     * Calculate core quota pre-requisite.
     *
     * <p>For example, suppose there's an VM move action from standard NPS family to
     * standard HBS family in region East US for a business account.
     * The remaining core quota of standard NPS family in region East US of this business account is 10.
     * The remaining core quota of standard HBS family in region East US of this business account is 6.
     * The total remaining core quota of region East US is 8.
     *
     * <p>Since the remaining core quota of the source family (standard NPS family) is larger than
     * the one of destination family (standard HBS family), we're going to generate a core quota
     * pre-requisite for this move action to request a quota increase for the destination family
     * (standard HBS family) in region East US.
     * Also, since the remaining core quota of the source family (standard NPS family) is large than
     * the total core quota of region East US, we're going to generate a core quota pre-requisite
     * to request a quota increase for the total regional vCPUs.
     * These two pre-requisites are combined in one sentence as a single {@link Prerequisite}.
     *
     * @param sourceFamily the family name of the source compute tier
     * @param sourceNumCores the number of cores of the source compute tier
     * @param destinationFamily the family name of the destination compute tier
     * @param destinationNumCores the number of cores of the destination compute tier
     * @param businessAccountId the id of the business account
     * @param regionId the id of the region
     * @param coreQuotaStore a store that contains all core quota info
     * @return an optional of pre-requisite
     */
    @Nonnull
    private static Optional<Prerequisite> calculateCoreQuotaPrerequisite(
        final String sourceFamily, final int sourceNumCores,
        final String destinationFamily, final int destinationNumCores,
        final long businessAccountId, final long regionId,
        @Nonnull final CoreQuotaStore coreQuotaStore) {
        boolean insufficientRegionalCores = false;
        boolean insufficientFamilyCores = false;

        // If resize represents an increase in cores, see if it fits within the remaining
        // regional core quota.
        if (destinationNumCores > sourceNumCores) {
            int regionalQuota = coreQuotaStore.getCoreQuota(
                businessAccountId, regionId, StringConstants.TOTAL_CORE_QUOTA);
            if ((destinationNumCores - sourceNumCores) > regionalQuota) {
                insufficientRegionalCores = true;
            }
        }

        int additionalFamilyCoresNeeded;
        if (destinationFamily.equals(sourceFamily)) {
            // If staying in the same family, we only need the difference in CPUs.
            additionalFamilyCoresNeeded = destinationNumCores - sourceNumCores;
        } else {
            // Otherwise, we need quota in the new family for all the cores, since the current
            // ones are in a different family and aren't counted against that quota.
            additionalFamilyCoresNeeded = destinationNumCores;
        }

        if (additionalFamilyCoresNeeded > 0) {
            int familyQuota = coreQuotaStore.getCoreQuota(
                businessAccountId, regionId, destinationFamily);
            if (additionalFamilyCoresNeeded > familyQuota) {
                insufficientFamilyCores = true;
            }
        }

        if (!insufficientRegionalCores && !insufficientFamilyCores) {
            return Optional.empty();
        }

        String quotaName = "";
        if (insufficientRegionalCores) {
            quotaName = StringConstants.TOTAL_REGIONAL_VCPUS_QUOTA_DISPLAYNAME;
        }
        if (insufficientRegionalCores && insufficientFamilyCores) {
            quotaName += " and ";
        }
        if (insufficientFamilyCores) {
            quotaName += destinationFamily;
        }

        return Optional.of(Prerequisite.newBuilder()
            .setPrerequisiteType(PrerequisiteType.CORE_QUOTAS)
            .setRegionId(regionId).setQuotaName(quotaName).build());
    }

    private static Set<Prerequisite> calculateAzureScaleSetPrerequisite(
            @Nonnull final Action action,
            @Nonnull final ActionPartialEntity target,
            @Nonnull final EntitiesAndSettingsSnapshot snapshot) {
        // We don't support Action execution if the target entity belongs to an Azure Scale Set.
        // I'm using the Resource Group to know if it's Azure.
        Optional<Long> resourceGroup = snapshot.getResourceGroupForEntity(target.getOid());
        if (resourceGroup.isPresent() && action.hasInfo()) {
            final ActionInfo actionInfo = action.getInfo();
            if (actionInfo.hasMove() && actionInfo.getMove().hasScalingGroupId()
                    || actionInfo.hasScale() && actionInfo.getScale().hasScalingGroupId()) {
                return Collections.singleton(Prerequisite.newBuilder()
                        .setPrerequisiteType(PrerequisiteType.SCALE_SET)
                        .build());
            }
        }
        return Sets.newHashSet();
    }
}
