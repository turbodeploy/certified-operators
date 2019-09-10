package com.vmturbo.market.runner;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.Delete;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.DeleteExplanation;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.commons.Units;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.api.SetOnce;
import com.vmturbo.cost.calculation.CostJournal;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.topology.TopologyCostCalculator;
import com.vmturbo.market.runner.Analysis.AnalysisState;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.VirtualVolumeFileDescriptor;
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;
import com.vmturbo.proactivesupport.DataMetricCounter;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;

/**
 * Perform the analysis on wasted files, generating delete actions for any files/volumes that
 * are wasted.  Wasted files/volumes are defined as those associated with a Storage or StorageTier,
 * but not a VM.  For on prem all files are associated with a virtual volume.  If the virtual volume
 * is not associated with a VM, we consider those files wasted.  For cloud, each volume is
 * represented by a virtual volume.  If the virtual volume is not associated with a VM, it
 * represents a wasted volume.
 */
public class WastedFilesAnalysis {

    private final String logPrefix;

    private AnalysisState state;

    private final Clock clock;

    private final Map<Long, TopologyEntityDTO> topologyDTOs;

    private final TopologyCostCalculator cloudCostCalculator;

    private final TopologyInfo topologyInfo;

    private final Logger logger = LogManager.getLogger();

    private final SetOnce<Instant> startTime = new SetOnce<>();

    private final SetOnce<Instant> completionTime = new SetOnce<>();

    private Collection<Action> actions;

    private final CloudTopology<TopologyEntityDTO> originalCloudTopology;

    public WastedFilesAnalysis(@Nonnull final TopologyInfo topologyInfo,
                               @Nonnull final Map<Long, TopologyEntityDTO> topologyDTOs,
                               @Nonnull final Clock clock,
                               @Nonnull final TopologyCostCalculator cloudCostCalculator,
                               @Nonnull final CloudTopology<TopologyEntityDTO> originalCloudTopology) {
        this.topologyInfo = topologyInfo;
        this.clock = clock;
        this.topologyDTOs = topologyDTOs;
        this.cloudCostCalculator = cloudCostCalculator;
        this.originalCloudTopology = originalCloudTopology;
        state = AnalysisState.INITIAL;
        logPrefix = topologyInfo.getTopologyType() + " WastedFilesAnalysis " +
            topologyInfo.getTopologyContextId() + " with topology " +
            topologyInfo.getTopologyId() + " : ";
    }

    /**
     * Get the wasted files actions.
     *
     * @return {@link Collection} of {@link Action} representing the wasted file actions generated.
     */
    public Collection<Action> getActions() {
        if (state != AnalysisState.SUCCEEDED) {
            return Collections.emptyList();
        } else {
            return actions;
        }
    }

    /**
     * Generate the wasted files actions by traversing the volumes and finding those that are not
     * associated with any VMs.
     *
     * @return True if this is the first time execute has been called, false otherwise.
     */
    public boolean execute() {
        if (!startTime.trySetValue(clock.instant())) {
            logger.error(" {} Completed or being computed", logPrefix);
            return false;
        }
        state = AnalysisState.IN_PROGRESS;
        logger.info("{} Started", logPrefix);
        try {
            try (final DataMetricTimer scopingTimer = Metrics.WASTED_FILES_SUMMARY.startTimer()) {
                // create a map by OID of all virtual volumes that have file data and are connected to
                // Storages or StorageTiers
                final Map<Long, TopologyEntityDTO> wastedFilesMap = topologyDTOs.values().stream()
                    .filter(topoEntity -> topoEntity.getEntityType() == EntityType.VIRTUAL_VOLUME_VALUE)
                    .filter(topoEntity -> topoEntity.hasTypeSpecificInfo())
                    .filter(topoEntity -> topoEntity.getTypeSpecificInfo().hasVirtualVolume())
                    .filter(topoEntity -> (topoEntity.getTypeSpecificInfo().getVirtualVolume()
                        .hasStorageAmountCapacity() &&
                        topoEntity.getTypeSpecificInfo().getVirtualVolume().getStorageAmountCapacity() > 0)
                        || topoEntity.getTypeSpecificInfo().getVirtualVolume().getFilesCount() > 0)
                    .filter(topoEntity -> topoEntity.getConnectedEntityListList().stream()
                        .anyMatch(connEntity -> connEntity.hasConnectedEntityType()
                            && (connEntity.getConnectedEntityType() == EntityType.STORAGE_VALUE
                            || connEntity.getConnectedEntityType() == EntityType.STORAGE_TIER_VALUE)))
                    .collect(Collectors.toMap(TopologyEntityDTO::getOid, Function.identity()));
                // remove any VirtualVolumes that have VMs which are connectedTo them
                topologyDTOs.values().stream()
                    .filter(topoEntity -> topoEntity.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE)
                    .forEach(virtualMachine -> virtualMachine.getConnectedEntityListList().stream()
                        .filter(connEntity -> connEntity.hasConnectedEntityType())
                        .filter(connEntity -> connEntity.getConnectedEntityType() ==
                            EntityType.VIRTUAL_VOLUME_VALUE)
                        .forEach(connEntity -> wastedFilesMap.remove(connEntity.getConnectedEntityId())));
                actions = wastedFilesMap.values().stream()
                        .flatMap(volume -> createActionsFromVolume(volume).stream())
                        .collect(Collectors.toList());

                state = AnalysisState.SUCCEEDED;

                completionTime.trySetValue(clock.instant());
            }
        } catch (RuntimeException e) {
            logger.error(logPrefix + e + " while running analysis", e);
            state = AnalysisState.FAILED;
            completionTime.trySetValue(clock.instant());
        }
        logger.info(logPrefix + "Execution time : "
            + startTime.getValue().get().until(completionTime.getValue().get(),
            ChronoUnit.SECONDS) + " seconds");
        return true;
    }

    private DeleteExplanation getOnPremWastedFilesDeleteExplanation(long sizeKb) {
        return DeleteExplanation.newBuilder().setSizeKb(sizeKb).build();
    }

    /**
     * Create a {@link Action.Builder} with a particular target and filePath.  These are the shared
     * fields between on prem and cloud.  This builder must later be refined with details specific
     * to on prem or cloud by the calling method.
     *
     * @param storageOid the id of the Storage or StorageTier hosting the file
     * @param entityType {@link EntityType} - either Storage (on prem) or StorageTier (cloud)
     * @param filePath The file path (on prem) or volume to be deleted
     * @param environmentType {@link EnvironmentType} of target
     * @return {@link Action.Builder} with the common fields for the delete action populated
     */
    private Action.Builder newActionFromVolume(final long storageOid,
                                               final EntityType entityType,
                                               final String filePath,
                                               final EnvironmentType environmentType) {
        final Action.Builder action = Action.newBuilder()
                // Assign a unique ID to each generated action.
                .setId(IdentityGenerator.next())
                .setDeprecatedImportance(0.0D)
                .setExecutable(false)
                .setInfo(ActionInfo.newBuilder().setDelete(Delete.newBuilder()
                        .setTarget(ActionEntity.newBuilder()
                                .setId(storageOid)
                                .setType(entityType.getNumber())
                                .setEnvironmentType(environmentType))
                        .setFilePath(filePath)
                ));
        Metrics.WASTED_FILES_ACTION_COUNTER.increment();
        return action;
    }

    /**
     * Create a delete action for a file.
     *
     * @param storageOid the ID of the Storage hosting the file
     * @param fileDescr {@link VirtualVolumeFileDescriptor} representing the file
     * @param environmentType {@link EnvironmentType} of Target
     * @return {@link Action.Builder} representing a delete action for the file
     */
    private Action.Builder newActionFromFile(final long storageOid,
                                             final VirtualVolumeFileDescriptor fileDescr,
                                             final EnvironmentType environmentType) {
        Action.Builder action = newActionFromVolume(storageOid, EntityType.STORAGE,
                fileDescr.getPath(), environmentType);
        action.setExplanation(Explanation.newBuilder()
                .setDelete(getOnPremWastedFilesDeleteExplanation(fileDescr.getSizeKb())));
        return action;
    }

    /**
     * Create zero or more wasted files actions from a volume DTO that represents either an ON_PREM
     * or CLOUD virtual volume.
     *
     * @param volume {@link TopologyEntityDTO} representing a wasted files virtual volume
     * @return {@link java.util.Collection}{@link Action} based on the wasted file(s) associated
     * with the volume.
     */
    private Collection<Action> createActionsFromVolume(final TopologyEntityDTO volume) {
        Optional<Long> storageOid;
        if (volume.getEnvironmentType() != EnvironmentType.ON_PREM) {
            // handle cloud case
            storageOid = TopologyDTOUtil.getOidsOfConnectedEntityOfType(volume,
                EntityType.STORAGE_TIER.getNumber()).findFirst();
            if (!storageOid.isPresent()) {
                return Collections.emptyList();
            }

            Optional<CostJournal<TopologyDTO.TopologyEntityDTO>> costJournalOpt =
                this.cloudCostCalculator.calculateCostForEntity(this.originalCloudTopology, volume);

            double costSavings = 0.0d;
            if (costJournalOpt.isPresent()) {
                // This will set the hourly saving rate to the action
                costSavings = costJournalOpt.get().getTotalHourlyCost();
            } else {
                logger.debug("Unable to get cost for volume", volume.getDisplayName());
            }

            return Collections.singletonList(newActionFromVolume(storageOid.get(),
                EntityType.STORAGE_TIER, volume.getDisplayName(), volume.getEnvironmentType())
                .setExplanation(Explanation.newBuilder().setDelete(
                    DeleteExplanation.newBuilder().build()))
                .setSavingsPerHour(CurrencyAmount.newBuilder()
                    .setAmount(costSavings)
                    .build())
                .build());
        } else {
            // handle ON_PREM
            storageOid = TopologyDTOUtil.getOidsOfConnectedEntityOfType(volume,
                EntityType.STORAGE.getNumber()).findFirst();
            if (!storageOid.isPresent()) {
                return Collections.EMPTY_LIST;
            }
            // TODO add a setting to control the minimum file size.  For now, use 1MB
            return volume.getTypeSpecificInfo().getVirtualVolume().getFilesList().stream()
                .filter(vvfd -> vvfd.getSizeKb() > Units.KBYTE)
                .map(vvfd -> newActionFromFile(storageOid.get(), vvfd,
                    volume.getEnvironmentType()).build())
                .collect(Collectors.toList());
        }
    }

    public boolean isDone() {
        return completionTime.getValue().isPresent();
    }

    public AnalysisState getState() {
        return state;
    }

    private static class Metrics {
        // Metrics for wasted files action generation

        private static final DataMetricSummary WASTED_FILES_SUMMARY = DataMetricSummary.builder()
            .withName("wasted_files_calculation_duration_seconds")
            .withHelp("Time to generate the wasted files actions.")
            .withQuantile(0.5, 0.05)   // Add 50th percentile (= median) with 5% tolerated error
            .withQuantile(0.9, 0.01)   // Add 90th percentile with 1% tolerated error
            .withQuantile(0.99, 0.001) // Add 99th percentile with 0.1% tolerated error
            .withMaxAgeSeconds(60 * 10) // 10 mins.
            .withAgeBuckets(5) // 5 buckets, so buckets get switched every 4 minutes.
            .build()
            .register();

        private static final DataMetricCounter WASTED_FILES_ACTION_COUNTER = DataMetricCounter.builder()
            .withName("wasted_files_action_count")
            .withHelp("The number of wasted file actions in each round of analysis.")
            .build()
            .register();
    }

}
