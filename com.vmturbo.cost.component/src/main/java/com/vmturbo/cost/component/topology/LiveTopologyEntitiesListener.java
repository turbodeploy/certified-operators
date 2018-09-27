package com.vmturbo.cost.component.topology;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.cost.calculation.CostJournal;
import com.vmturbo.cost.component.entity.cost.EntityCostStore;
import com.vmturbo.cost.component.reserved.instance.ComputeTierDemandStatsWriter;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceCoverageUpdate;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.sql.utils.DbException;
import com.vmturbo.topology.processor.api.EntitiesListener;
import com.vmturbo.topology.processor.api.ProbeInfo;
import com.vmturbo.topology.processor.api.TargetInfo;
import com.vmturbo.topology.processor.api.TopologyProcessor;

/**
 * Listen for new topologies and store cloud cost entries in the DB.
 **/
public class LiveTopologyEntitiesListener implements EntitiesListener {

    private final Logger logger = LogManager.getLogger();

    private final long realtimeTopologyContextId;

    private final ComputeTierDemandStatsWriter computeTierDemandStatsWriter;

    private final TopologyProcessor topologyProcessorClient;

    private Set<SDKProbeType> cloudProbeTypes =
            ImmutableSet.of(
                    SDKProbeType.AWS,
                    SDKProbeType.AZURE
            );

    /**
     * Mapping from TargetID -> ProbeType.
     * Mapping is created/updated on-demand by querying TP when
     * the targetId is first referenced.
     */
    private final Map<Long, SDKProbeType> targetIdToTargetTypeMap = new HashMap<>();

    private final TopologyCostCalculator topologyCostCalculator;

    private final EntityCostStore entityCostStore;

    private ReservedInstanceCoverageUpdate reservedInstanceCoverageUpdate;

    public LiveTopologyEntitiesListener(long realtimeTopologyContextId,
                                        @Nonnull final ComputeTierDemandStatsWriter computeTierDemandStatsWriter,
                                        @Nonnull final TopologyProcessor topologyProcessorClient,
                                        @Nonnull final TopologyCostCalculator topologyCostCalculator,
                                        @Nonnull final EntityCostStore entityCostStore,
                                        @Nonnull final ReservedInstanceCoverageUpdate reservedInstanceCoverageUpdate) {
        this.realtimeTopologyContextId = realtimeTopologyContextId;
        this.computeTierDemandStatsWriter = Objects.requireNonNull(computeTierDemandStatsWriter);
        this.topologyProcessorClient = Objects.requireNonNull(topologyProcessorClient);
        this.topologyCostCalculator = Objects.requireNonNull(topologyCostCalculator);
        this.entityCostStore = Objects.requireNonNull(entityCostStore);
        this.reservedInstanceCoverageUpdate = Objects.requireNonNull(reservedInstanceCoverageUpdate);
    }

    @Override
    public void onTopologyNotification(@Nonnull final TopologyInfo topologyInfo,
                                       @Nonnull final RemoteIterator<TopologyEntityDTO> entityIterator) {

        final long topologyContextId = topologyInfo.getTopologyContextId();
        final long topologyId = topologyInfo.getTopologyId();
        if (topologyContextId != realtimeTopologyContextId) {
            logger.error("Received topology with wrong topologyContextId."
                    + "Expected:{}, Received:{}", realtimeTopologyContextId,
                    topologyContextId);
            return;
        }
        logger.info("Received live topology with topologyId: {}", topologyInfo.getTopologyId());
        Map<Long, TopologyEntityDTO> cloudEntities = null;
        try {
            cloudEntities = readCloudEntities(entityIterator);
            computeTierDemandStatsWriter.calculateAndStoreRIDemandStats(topologyInfo, cloudEntities, false);
        } catch (CommunicationException |TimeoutException ex) {
            logger.error("Error occurred while receiving topology:{}, topologyContext:{}",
                    topologyId, topologyContextId, ex);
            cloudEntities = Collections.emptyMap();
        } catch (InterruptedException ie) {
            logger.info("Thread interrupted while processing topology:{}, topologyContext:{}",
                    topologyId, topologyContextId, ie);
            cloudEntities = Collections.emptyMap();
        }

        Objects.requireNonNull(cloudEntities);

        final Map<Long, CostJournal<TopologyEntityDTO>> costs =
                topologyCostCalculator.calculateCosts(cloudEntities);
        try {
            entityCostStore.persistEntityCost(costs);
        } catch (DbException e) {
            logger.error("Failed to persist entity costs.", e);
        }

        // update reserved instance coverage data.
        reservedInstanceCoverageUpdate.updateAllEntityRICoverageIntoDB(topologyId, cloudEntities);

    }

    private Map<Long, TopologyEntityDTO> readCloudEntities(
            @Nonnull final RemoteIterator<TopologyEntityDTO> entityIterator)
            throws InterruptedException, TimeoutException, CommunicationException {

        final Map<Long, TopologyEntityDTO> topologyMap = new HashMap<>();
        while (entityIterator.hasNext()) {
            entityIterator.nextChunk().stream()
                    .filter(this::isCloudEntity)
                    .forEach(entity -> topologyMap.put(entity.getOid(), entity));
        }
        return topologyMap;
    }

    /**
     * Check if the entity was discovered by cloud probes(AWS/Azure etc).
     *
     * @param entityDTO DTO of the entity.
     * @return Return true if the entity was discovered by Cloud probes.
     *          Else return false.
     */
    private boolean isCloudEntity(@Nonnull final TopologyEntityDTO entityDTO) {

        Set<Long> targetIds = getTargetIds(entityDTO);
        if (targetIds.isEmpty()) {
            // Should we throw an exception?
           logger.warn("TargetId not present for entity {}",
                   entityDTO);
            return false;
        }

        if (hasCloudTarget(targetIds)) {
            return true;
        }

        // We didn't find any Cloud targets. To check if
        // it is in the missing targetList, we have to
        // reload the mapping from TP.
        logger.info("TargetIds: {} not in the targetMap. Loading from TP",
                targetIds);
        loadTargetIdToTargetTypeMapping();

        Set<Long> missingTargetIds =
                Sets.difference(targetIds,
                        targetIdToTargetTypeMap.keySet());

        if (!missingTargetIds.isEmpty()) {
            logger.warn("No targetInfo present for these targetIds in TP : {}",
                    missingTargetIds);
        }

        return hasCloudTarget(targetIds);
    }

    /**
     *  Get all the targets from the TopologyProcessor and create a mapping
     *  from TargetId -> TargetType
     */
    private synchronized void loadTargetIdToTargetTypeMapping() {
        try {
            final Set<TargetInfo> targets =
                    topologyProcessorClient.getAllTargets();
            logger.info("Loaded {} targets from TP",
                    targets.size());
            // Mapping from TargetId -> ProbeId
            Map<Long, Long> targetIdToProbeIdMap =
                    new HashMap<>();
            targets.stream()
                    .forEach(target -> {
                        targetIdToProbeIdMap.put(
                                target.getId(),
                                target.getProbeId());
                    });

            // Mapping from ProbeId -> ProbeType
            Map<Long, SDKProbeType> probeIdToProbeTypeMap =
                    new HashMap<>();
            final Set<ProbeInfo> probeInfos =
                    topologyProcessorClient.getAllProbes();
            logger.info("Loaded {} probeInfos from TP", probeInfos.size());
            probeInfos.stream()
                    .forEach(probeInfo -> {
                        probeIdToProbeTypeMap.put(
                                probeInfo.getId(),
                                SDKProbeType.create(probeInfo.getType())
                        );
                    });

            targetIdToProbeIdMap.forEach((targetId, probeId) -> {
                targetIdToTargetTypeMap.put(
                        targetId,
                        probeIdToProbeTypeMap.get(probeId)
                );
            });

        } catch (CommunicationException e) {
            logger.error("Error getting target and probe infos from TP", e);
        }
    }

    /**
     * Get the Ids of the Targets that discovered the entity.
     * @param entityDTO DTO of the entity.
     * @return The list of TargetIds.
    */
    private Set<Long> getTargetIds(TopologyEntityDTO entityDTO) {
        if (!entityDTO.hasOrigin() &&
                !entityDTO.getOrigin().hasDiscoveryOrigin()) {
            return Collections.emptySet();
        }
        return entityDTO.getOrigin().getDiscoveryOrigin()
                .getDiscoveringTargetIdsList()
                .stream()
                .collect(Collectors.toSet());
    }

    private boolean hasCloudTarget(Set<Long> targetIds) {
        return targetIds.stream()
                .anyMatch(targetId -> {
                    SDKProbeType probeType =
                            targetIdToTargetTypeMap.get(targetId);
                    if (probeType == null) {
                        return false;
                    } else {
                        return cloudProbeTypes.contains(probeType);
                    }
                });
    }
}

