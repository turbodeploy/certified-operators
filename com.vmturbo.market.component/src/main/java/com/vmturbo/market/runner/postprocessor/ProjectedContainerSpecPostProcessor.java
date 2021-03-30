package com.vmturbo.market.runner.postprocessor;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ActionTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ResizeTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Post processor to process projected ContainerSpec entities by updating commodity capacity and
 * percentile utilization to reflect after-action changes from corresponding Container resizing.
 */
public class ProjectedContainerSpecPostProcessor extends ProjectedEntityPostProcessor {

    private static final Logger logger = LogManager.getLogger();

    @Override
    public boolean appliesTo(@Nonnull final TopologyInfo topologyInfo,
                             @Nonnull final Map<Integer, List<ProjectedTopologyEntity>> entityTypeToEntitiesMap) {
        // Post process projected ContainerSpec entities only in real-time market.
        return topologyInfo.getTopologyType() == TopologyType.REALTIME
            && entityTypeToEntitiesMap.containsKey(EntityType.CONTAINER_SPEC_VALUE);
    }

    /**
     * Post process projected ContainerSpec entities by updating commodity capacity and percentile
     * utilization to reflect after-action changes from corresponding Container resizing.
     *
     * <p>A ContainerSpec entity represents shared portion of connected Containers. ContainerSpecs
     * are not directly analyzed by Market so that projected entities have the same commodity data
     * as original ones. To reflect after-action aggregated Container data on ContainerSpec, we need
     * to update commodity capacity and percentile utilization of ContainerSpec from corresponding
     * Containers with resize actions.
     *
     * @param topologyInfo                  Given {@link TopologyInfo}.
     * @param projectedEntities             Map from entity OID to {@link ProjectedTopologyEntity}.
     * @param entityTypeToProjectedEntities Map of entity types to list of {@link ProjectedTopologyEntity}.
     * @param actionsList                   List of all actions from analysis results.
     */
    @Override
    public void process(@Nonnull final TopologyInfo topologyInfo,
                        @Nonnull final Map<Long, ProjectedTopologyEntity> projectedEntities,
                        @Nonnull final Map<Integer, List<ProjectedTopologyEntity>> entityTypeToProjectedEntities,
                        @Nonnull final List<ActionTO> actionsList) {
        final String logPrefix = String.format("%s topology [ID=%d, context=%d]: ",
            topologyInfo.getTopologyType(), topologyInfo.getTopologyId(), topologyInfo.getTopologyContextId());

        // Map from ContainerSpec OID to set of commodity types to be updated.
        // A containerSpec could have multiple Containers connected. This map is used to avoid duplicate
        // update on the same commodity of the same ContainerSpec entity.
        final Map<Long, Set<Integer>> containerSpecCommodityTypeMap = new HashMap<>();
        // Map from ContainerSpec OID to ProjectedTopologyEntity builder to be updated.
        // This map is to avoid creating extra entity builder for the same ContainerSpec.
        final Map<Long, ProjectedTopologyEntity.Builder> projectedContainerSpecEntityBuilderMap = new HashMap<>();
        actionsList.stream()
                // Get all Container resize actions
                .filter(ActionTO::hasResize)
                .map(ActionTO::getResize)
                .filter(resizeTO -> projectedEntities.get(resizeTO.getSellingTrader()) != null
                        && projectedEntities.get(resizeTO.getSellingTrader()).getEntity().getEntityType() == EntityType.CONTAINER_VALUE)
                .forEach(resizeTO ->
                        updateProjectedContainerSpec(resizeTO, projectedEntities, containerSpecCommodityTypeMap, projectedContainerSpecEntityBuilderMap));
        // Set the updated projected ContainerSpec entities to projectedEntities map.
        projectedContainerSpecEntityBuilderMap.forEach((containerSpecOID, entityBuilder) ->
                projectedEntities.put(containerSpecOID, entityBuilder.build()));

        logger.info("{}Finished updating {} projected ContainerSpec entities.", logPrefix,
            projectedContainerSpecEntityBuilderMap.size());
    }

    private void updateProjectedContainerSpec(@Nonnull ResizeTO resizeTO, @Nonnull Map<Long, ProjectedTopologyEntity> projectedEntities,
            @Nonnull Map<Long, Set<Integer>> containerSpecCommodityTypeMap,
            @Nonnull Map<Long, ProjectedTopologyEntity.Builder> projectedContainerSpecEntityBuilderMap) {
        final long containerOID = resizeTO.getSellingTrader();
        final int commodityType = resizeTO.getSpecification().getBaseType();
        // ProjectedContainer is guaranteed to exist in projectedEntities map here after previous filter.
        ProjectedTopologyEntity projectedContainer = projectedEntities.get(containerOID);
        projectedContainer.getEntity().getConnectedEntityListList().stream()
                .map(ConnectedEntity::getConnectedEntityId)
                // Include the ContainerSpecs if containerSpecOID is in projectedEntities map and given
                // commodity type hasn't been updated.
                .filter(projectedEntities::containsKey)
                .filter(containerSpecOID -> !isContainerSpecCommodityUpdated(commodityType, containerSpecOID, containerSpecCommodityTypeMap))
                .forEach(containerSpecOID -> {
                    // Find the commoditySoldDTO of current action commodity type from projected
                    // Container entity.
                    projectedContainer.getEntity().getCommoditySoldListList().stream()
                            .filter(comm -> comm.getCommodityType().getType() == commodityType)
                            .findFirst()
                            .ifPresent(projectedCommSoldDTO -> {
                                double newCapacity = projectedCommSoldDTO.getCapacity();
                                ProjectedTopologyEntity.Builder projectedEntityBuilder =
                                        projectedContainerSpecEntityBuilderMap.computeIfAbsent(containerSpecOID,
                                                v -> projectedEntities.get(containerSpecOID).toBuilder());
                                // Update commodity capacity and percentile utilization of projected ContainerSpec
                                // entity with the new capacity from the connected projected Container entity.
                                projectedEntityBuilder.getEntityBuilder().getCommoditySoldListBuilderList().stream()
                                        .filter(comm -> comm.getCommodityType().getType() == commodityType)
                                        .findAny()
                                        .ifPresent(comm -> {
                                            // Update commodity capacity and percentile utilization on the projected
                                            // ContainerSpec entity.
                                            double oldCapacity = comm.getCapacity();
                                            comm.setCapacity(newCapacity);
                                            double newPercentile = comm.getHistoricalUsed().getPercentile() * oldCapacity / newCapacity;
                                            comm.getHistoricalUsedBuilder().setPercentile(newPercentile);
                                        });
                                containerSpecCommodityTypeMap.get(containerSpecOID).add(commodityType);
                            });
                });
    }

    private boolean isContainerSpecCommodityUpdated(int commodityType, long containerSpecOID,
            @Nonnull Map<Long, Set<Integer>> containerSpecCommodityTypeMap) {
        Set<Integer> updatedCommodityTypes =
                containerSpecCommodityTypeMap.computeIfAbsent(containerSpecOID, v -> new HashSet<>());
        // If current commodity of this ContainerSpec entity has been updated, no need to update again.
        return updatedCommodityTypes.contains(commodityType);
    }
}
