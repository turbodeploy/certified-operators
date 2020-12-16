package com.vmturbo.market.runner.wastedfiles;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Strings;
import com.google.common.collect.Streams;

import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.Delete;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.DeleteExplanation;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.commons.Units;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.journal.CostJournal;
import com.vmturbo.cost.calculation.topology.TopologyCostCalculator;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.AttachmentState;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.VirtualVolumeFileDescriptor;
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;
import com.vmturbo.proactivesupport.DataMetricCounter;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;

/**
 * Performs wasted file analysis on topologies.
 *
 * <p/>See: {@link WastedFilesAnalysisEngine#analyzeWastedFiles(TopologyInfo, Map, TopologyCostCalculator, CloudTopology)}.
 */
public class WastedFilesAnalysisEngine {
    private final Logger logger = LogManager.getLogger();

    /**
     * Perform the analysis on wasted files, generating delete actions for any files/volumes that
     * are wasted.  Wasted files/volumes are defined as those associated with a Storage or StorageTier,
     * but not a VM.  For on prem all files are associated with a virtual volume.  If the virtual volume
     * is not associated with a VM, we consider those files wasted.  For cloud, each volume is
     * represented by a virtual volume.  If the virtual volume is not associated with a VM, it
     * represents a wasted volume.
     *
     * @param topologyInfo           Information about the topology this analysis applies to.
     * @param topologyEntities       The entities in the topology.
     * @param topologyCostCalculator {@link TopologyCostCalculator} for calculating cost of cloud
     *                               volumes.
     * @param originalCloudTopology  {@link CloudTopology} for calculating potential savings from
     *                                                    deleting cloud volumes.
     * @return The {@link WastedFilesResults} object.
     */
    @Nonnull
    public WastedFilesResults analyzeWastedFiles(@Nonnull final TopologyInfo topologyInfo,
                                          @Nonnull final Map<Long, TopologyEntityDTO> topologyEntities,
                                          @Nonnull final TopologyCostCalculator topologyCostCalculator,
                                          @Nonnull final CloudTopology<TopologyEntityDTO> originalCloudTopology) {
        final String logPrefix = topologyInfo.getTopologyType() + " WastedFilesAnalysis "
            + topologyInfo.getTopologyContextId() + " with topology "
            + topologyInfo.getTopologyId() + " : ";

        logger.info("{} Started", logPrefix);
        final Long2LongMap storageToStorageAmountReleasedMap = new Long2LongOpenHashMap();
        final List<Action> actions;
        try {
            final double durationSec;
            try (DataMetricTimer scopingTimer = Metrics.WASTED_FILES_SUMMARY.startTimer()) {
                // create a map by OID of all virtual volumes that have file data and are connected to
                // Storages or StorageTiers
                final Map<Long, TopologyEntityDTO> wastedFilesMap = topologyEntities.values().stream()
                        .filter(topoEntity -> topoEntity.getEntityType() == EntityType.VIRTUAL_VOLUME_VALUE)
                        .filter(topoEntity -> topoEntity.hasTypeSpecificInfo())
                        .filter(topoEntity -> topoEntity.getTypeSpecificInfo().hasVirtualVolume())
                        .filter(topoEntity -> getStorageAmountCapacity(topoEntity) > 0
                                || topoEntity.getTypeSpecificInfo().getVirtualVolume().getFilesCount() > 0)
                        // only include those which are "deletable" from setting
                        .filter(topoEntity -> topoEntity.hasAnalysisSettings()
                                && topoEntity.getAnalysisSettings().getDeletable())
                        .collect(Collectors.toMap(TopologyEntityDTO::getOid, Function.identity()));
                // remove any VirtualVolumes that have VMs which are connectedTo them
                topologyEntities.values().stream()
                        .filter(topoEntity -> topoEntity.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE)
                        .forEach(virtualMachine -> getAttachedVolumesIds(virtualMachine)
                                .forEach(wastedFilesMap::remove));
                actions = wastedFilesMap.values().stream()
                        .flatMap(volume -> createActionsFromVolume(volume, topologyCostCalculator, originalCloudTopology, storageToStorageAmountReleasedMap).stream())
                        .collect(Collectors.toList());

                durationSec = scopingTimer.getTimeElapsedSecs();
            }
            logger.info("{} Execution time: {} seconds", logPrefix, durationSec);
            return new WastedFilesResults(actions, storageToStorageAmountReleasedMap);
        } catch (RuntimeException e) {
            logger.error(logPrefix + e + " while running analysis", e);
            return WastedFilesResults.EMPTY;
        }
    }

    private static double getStorageAmountCapacity(@Nonnull final TopologyEntityDTO entity) {
        return entity.getCommoditySoldListList().stream()
                .filter(commodity -> commodity.getCommodityType().getType()
                        == CommodityType.STORAGE_AMOUNT.getNumber())
                .map(CommoditySoldDTO::getCapacity)
                .findAny().orElse(0D);
    }

    /**
     * Get volumes attached to a virtual machine. For on prem VMs attached volumes are represented
     * using ConnectedTo relationship. For cloud VMs attached volumes are represented as commodity
     * providers. This method joins connected entities and commodity providers and returns
     * concatenated stream.
     *
     * @param virtualMachine Virtual machine.
     * @return Stream of OIDs of volumes attached to the virtual machine.
     */
    private static Stream<Long> getAttachedVolumesIds(
            @Nonnull final TopologyEntityDTO virtualMachine) {
        final int volumeType = EntityType.VIRTUAL_VOLUME_VALUE;

        // Get volumes connected to the VM (on prem case)
        final Stream<Long> connectedVolumes = virtualMachine.getConnectedEntityListList()
                .stream()
                .filter(ConnectedEntity::hasConnectedEntityType)
                .filter(connEntity -> connEntity.getConnectedEntityType() == volumeType)
                .map(ConnectedEntity::getConnectedEntityId);

        // Get volumes selling commodities to the VM (cloud case)
        final Stream<Long> consumedVolumes = virtualMachine.getCommoditiesBoughtFromProvidersList()
                .stream()
                .filter(commBought -> commBought.getProviderEntityType() == volumeType)
                .map(CommoditiesBoughtFromProvider::getProviderId);

        return Streams.concat(connectedVolumes, consumedVolumes);
    }

    private DeleteExplanation getOnPremWastedFilesDeleteExplanation(long sizeKb) {
        return DeleteExplanation.newBuilder().setSizeKb(sizeKb).build();
    }

    /**
     * Create a {@link Action.Builder} with a particular target and filePath.  These are the shared
     * fields between on prem and cloud.  This builder must later be refined with details specific
     * to on prem or cloud by the calling method.
     *
     * @param targetEntityOid id of the storage hosting the file (on-perm) or virtual volume wasted (cloud)
     * @param targetEntityType {@link EntityType} - either Storage (on prem) or Virtual Volume (cloud)
     * @param sourceEntityOid - storage tier which the Virtual Volume sits (cloud)
     * @param sourceEntityType - {@link EntityType} Storage Tier for cloud
     * @param filePath The file path to be deleted (on prem)
     * @param environmentType {@link EnvironmentType} of target
     * @return {@link Action.Builder} with the common fields for the delete action populated
     */
    private Action.Builder newActionFromVolume(final long targetEntityOid,
            final EntityType targetEntityType,
            @Nullable final Long sourceEntityOid,
            @Nullable final EntityType sourceEntityType,
            @Nullable final String filePath,
            final EnvironmentType environmentType) {
        final Delete.Builder deleteBuilder = Delete.newBuilder()
                .setTarget(ActionEntity.newBuilder()
                        .setId(targetEntityOid)
                        .setType(targetEntityType.getNumber())
                        .setEnvironmentType(environmentType));

        if (!Strings.isNullOrEmpty(filePath)) {
            deleteBuilder.setFilePath(filePath);
        }

        if (sourceEntityOid != null) {
            deleteBuilder.setSource(ActionEntity.newBuilder()
                    .setId(sourceEntityOid)
                    .setType(sourceEntityType.getNumber())
                    .setEnvironmentType(environmentType));
        }

        final Action.Builder action = Action.newBuilder()
                // Assign a unique ID to each generated action.
                .setId(IdentityGenerator.next())
                .setDeprecatedImportance(0.0D)
                .setExecutable(true)
                .setInfo(ActionInfo.newBuilder().setDelete(deleteBuilder));
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
                null, null, // TODO need to update source entity for on-perm in the future
                fileDescr.getPath(), environmentType);
        action.setExplanation(Explanation.newBuilder()
                .setDelete(getOnPremWastedFilesDeleteExplanation(fileDescr.getSizeKb())));
        return action;
    }

    /**
     * Create zero or more wasted files actions from a volume DTO that represents either an ON_PREM
     * or CLOUD virtual volume.
     *
     * @param volume {@link TopologyEntityDTO} representing a wasted files virtual volume.
     * @param topologyCostCalculator The {@link TopologyCostCalculator} for this topology.
     * @param originalCloudTopology The {@link CloudTopology} for the input topology.
     * @param storageToKbReleased Running map of storage released per by tier as part of this analysis.
     *                                Modified by the method.
     * @return {@link java.util.Collection}{@link Action} based on the wasted file(s) associated
     * with the volume.
     */
    private Collection<Action> createActionsFromVolume(final TopologyEntityDTO volume,
            TopologyCostCalculator topologyCostCalculator,
            CloudTopology<TopologyEntityDTO> originalCloudTopology,
            Long2LongMap storageToKbReleased) {
        if (volume.hasTypeSpecificInfo() && volume.getTypeSpecificInfo().hasVirtualVolume()
                && volume.getTypeSpecificInfo().getVirtualVolume().hasAttachmentState()
                && volume.getTypeSpecificInfo().getVirtualVolume().getAttachmentState()
                        == AttachmentState.ATTACHED) {
            logger.trace("Cannot generate delete action on volume {} since it is in use.",
                    volume.getDisplayName());
            return Collections.emptyList();
        }

        Optional<Long> optStorageOrStorageTierOid = TopologyDTOUtil.getVolumeProvider(volume);
        if (!optStorageOrStorageTierOid.isPresent()) {
            return Collections.emptyList();
        }
        final long storageOrStorageTierOid = optStorageOrStorageTierOid.get();

        if (volume.getEnvironmentType() != EnvironmentType.ON_PREM) {
            // handle cloud case
            Optional<CostJournal<TopologyEntityDTO>> costJournalOpt =
                    topologyCostCalculator.calculateCostForEntity(originalCloudTopology, volume);

            double costSavings = 0.0d;
            if (costJournalOpt.isPresent()) {
                // This will set the hourly saving rate to the action
                costSavings = costJournalOpt.get().getTotalHourlyCost().getValue();
            } else {
                logger.debug("Unable to get cost for volume {}", volume.getDisplayName());
            }
            return Collections.singletonList(newActionFromVolume(
                    volume.getOid(), EntityType.VIRTUAL_VOLUME, storageOrStorageTierOid,
                    EntityType.STORAGE_TIER, null, volume.getEnvironmentType())
                    .setExplanation(Explanation.newBuilder().setDelete(DeleteExplanation.newBuilder()
                            .setSizeKb((long)getStorageAmountCapacity(volume))))
                    .setSavingsPerHour(CurrencyAmount.newBuilder().setAmount(costSavings))
                    .build());
        } else {
            // handle ON_PREM
            // TODO add a setting to control the minimum file size.  For now, use 1MB
            return volume.getTypeSpecificInfo().getVirtualVolume().getFilesList().stream()
                    .filter(vvfd -> vvfd.getSizeKb() > Units.KBYTE)
                    .map(vvfd -> {
                        final long curReleased = storageToKbReleased.get(storageOrStorageTierOid);
                        storageToKbReleased.put(storageOrStorageTierOid, curReleased + vvfd.getSizeKb());
                        return newActionFromFile(storageOrStorageTierOid, vvfd, volume.getEnvironmentType())
                                .build();
                    })
                    .collect(Collectors.toList());
        }
    }

    /**
     * Wasted file analysis metrics.
     */
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
