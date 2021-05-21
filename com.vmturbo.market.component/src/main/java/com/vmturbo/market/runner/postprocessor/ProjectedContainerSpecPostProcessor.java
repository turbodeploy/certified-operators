package com.vmturbo.market.runner.postprocessor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.commons.analysis.ByProductMap;
import com.vmturbo.commons.analysis.ByProductMap.ByProductInfo;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ActionTO;
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
     * Post process projected ContainerSpec entities by updating commodity used, capacity and percentile
     * utilization to reflect after-action changes from corresponding Container resizing.
     *
     * <p>A ContainerSpec entity represents shared portion of connected Containers. ContainerSpecs
     * are not directly analyzed by Market so that projected entities have the same commodity data
     * as original ones. To reflect after-action aggregated Container data on ContainerSpec, we need
     * to update commodity capacity and percentile utilization as well as byProduct commodity usage
     * of ContainerSpec from corresponding Containers with resize actions.
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
        final StopWatch stopWatch = StopWatch.createStarted();
        final String logPrefix = String.format("%s topology [ID=%d, context=%d]: ",
            topologyInfo.getTopologyType(), topologyInfo.getTopologyId(), topologyInfo.getTopologyContextId());

        // Map from projected ContainerSpec OID to corresponding ContainerSpecInfo.
        final Map<Long, ContainerSpecInfo> projectedContainerSpecInfoMap = new HashMap<>();

        actionsList.stream()
            // Get all Container resizing actions
            .filter(ActionTO::hasResize)
            .map(ActionTO::getResize)
            .filter(resizeTO -> projectedEntities.get(resizeTO.getSellingTrader()) != null
                && projectedEntities.get(resizeTO.getSellingTrader()).getEntity().getEntityType() == EntityType.CONTAINER_VALUE)
            .forEach(resizeTO -> {
                final long containerOID = resizeTO.getSellingTrader();
                final int resizingCommodityType = resizeTO.getSpecification().getBaseType();
                // ProjectedContainer is guaranteed to exist in projectedEntities map here after previous filter.
                ProjectedTopologyEntity projectedContainer = projectedEntities.get(containerOID);
                projectedContainer.getEntity().getConnectedEntityListList().stream()
                    .map(ConnectedEntity::getConnectedEntityId)
                    // Include the ContainerSpecs only if containerSpecOID is in projectedEntities map
                    .filter(projectedEntities::containsKey)
                    .forEach(containerSpecOID -> {
                        ContainerSpecInfo containerSpecInfo =
                            projectedContainerSpecInfoMap.computeIfAbsent(containerSpecOID,
                                k -> new ContainerSpecInfo(projectedEntities.get(containerSpecOID).toBuilder()));
                        containerSpecInfo.addInfo(containerOID, resizingCommodityType);
                    });
            });
        // Update projected ContainerSpecs with resizing actions and return list of updated projected
        // ContainerSpec topology entities.
        List<ProjectedTopologyEntity> updatedProjectedContainerSpecs =
            updateProjectedContainerSpecs(projectedEntities, projectedContainerSpecInfoMap);
        // Set the updated projected ContainerSpec entities to projectedEntities map.
        updatedProjectedContainerSpecs.forEach(projectedContainerSpec ->
                projectedEntities.put(projectedContainerSpec.getEntity().getOid(), projectedContainerSpec));
        stopWatch.stop();
        logger.info("{}Finished updating {} projected ContainerSpec entities in {} ms.", logPrefix,
            projectedContainerSpecInfoMap.size(), stopWatch.getTime(TimeUnit.MILLISECONDS));
    }

    /**
     * Update commodities of projected ContainerSpecs based on projected Containers.
     *
     * @param projectedEntities    Map of projected entity OID to projected entity.
     * @param containerSpecInfoMap Map from ContainerSpec OID to corresponding {@link ContainerSpecInfo}.
     * @return List of updated projceted ContaienrSpec topology entities.
     */
    @Nonnull
    private List<ProjectedTopologyEntity> updateProjectedContainerSpecs(
        @Nonnull final Map<Long, ProjectedTopologyEntity> projectedEntities,
        @Nonnull final Map<Long, ContainerSpecInfo> containerSpecInfoMap) {
        return containerSpecInfoMap.values().stream()
            .map(containerSpecInfo -> containerSpecInfo.updateProjectedContainerSpec(projectedEntities))
            .collect(Collectors.toList());
    }

    /**
     * Wrapper class to store ContainerSpec info, including ContainerSpec ProjectedTopologyEntity builder,
     * set of connected containers, set of commodity types being resized in current topology and
     * corresponding byProduct commodity types retrieved from resizing commodities.
     */
    private static class ContainerSpecInfo {
        /**
         * Entity builder of projected ContainerSpec.
         */
        private final ProjectedTopologyEntity.Builder entityBuilder;
        /**
         * Set of connected container OIDs from given ContainerSpec entity.
         */
        private final Set<Long> connectedContainers;
        /**
         * Set of commodity types with resizing actions from given ContainerSpec entity.
         */
        private final Set<Integer> resizingCommodityTypes;
        /**
         * Set of byProduct commodity types retrieved from set of resizing commodity types.
         */
        private final Set<Integer> byProductCommodityTypes;

        private ContainerSpecInfo(@Nonnull ProjectedTopologyEntity.Builder entityBuilder) {
            this.entityBuilder = entityBuilder;
            connectedContainers = new HashSet<>();
            resizingCommodityTypes = new HashSet<>();
            byProductCommodityTypes = new HashSet<>();
        }

        /**
         * Add connected container OID, resizing commodity type and byProduce commodities retrieved
         * from resizing commodities to current ContainerSpecInfo object.
         *
         * @param containerOID          Given container OID connected to current ContainerSpec.
         * @param resizingCommodityType Given commodity type which is being resized.
         */
        void addInfo(long containerOID, int resizingCommodityType) {
            connectedContainers.add(containerOID);
            if (!resizingCommodityTypes.add(resizingCommodityType)) {
                // Check byProduct commodities only if given resizingCommodityType doesn't
                // exist in current containerSpecInfo.
                List<ByProductInfo> byProductInfos = ByProductMap.byProductMap.get(
                    resizingCommodityType);
                if (byProductInfos != null) {
                    byProductInfos.stream()
                        .map(ByProductMap.ByProductInfo::getByProduct)
                        .forEach(byProductCommodityTypes::add);
                }
            }
        }

        /**
         * Update commodities of projected container spec based on given collected projected container
         * commodity data points and return the {@link ProjectedTopologyEntity} for this ContainerSpec.
         *
         * @param projectedEntities Map of projected entity OID to projected entity.
         * @return Updated projected ContainerSpec topology entity.
         */
        @Nonnull
        ProjectedTopologyEntity updateProjectedContainerSpec(@Nonnull final Map<Long, ProjectedTopologyEntity> projectedEntities) {
            // Collect container commodity used and capacity data points for further aggregation.
            final Map<Integer, CommodityDataPoints> collectedContainerCommDataPoints =
                collectContainerResources(projectedEntities);
            entityBuilder.getEntityBuilder().getCommoditySoldListBuilderList().stream()
                .filter(comm -> collectedContainerCommDataPoints.containsKey(comm.getCommodityType().getType()))
                .forEach(comm -> {
                    final int commodityType = comm.getCommodityType().getType();
                    CommodityDataPoints containerCommDataPoints = collectedContainerCommDataPoints.get(commodityType);
                    if (resizingCommodityTypes.contains(commodityType)) {
                        // Update percentile and capacity of resizing commodities for projectedContainerSpec
                        // based on the max capacity value of corresponding projected containers.
                        containerCommDataPoints.getCapacityList().stream()
                            .mapToDouble(Double::doubleValue)
                            .max()
                            .ifPresent(maxContainerCapacity -> {
                                double oldCapacity = comm.getCapacity();
                                comm.setCapacity(maxContainerCapacity);
                                double newPercentile =
                                    comm.getHistoricalUsed().getPercentile() * oldCapacity / maxContainerCapacity;
                                comm.getHistoricalUsedBuilder().setPercentile(newPercentile);
                            });
                    } else if (byProductCommodityTypes.contains(commodityType)) {
                        // Update used value of byProduct commodities for projectedContainerSpec based
                        // on the average used value of corresponding projected containers.
                        containerCommDataPoints.getUsedList().stream()
                            .mapToDouble(Double::doubleValue)
                            .average()
                            .ifPresent(comm::setUsed);
                    }
                });
            return entityBuilder.build();
        }

        /**
         * Collect container commodity used and capacity data points for given resizing commodity types and
         * byProduct commodity types for further aggregation.
         *
         * @param projectedEntities Map of projected entity OID to projected entity.
         * @return Map of commodity type to corresponding collected {@link CommodityDataPoints}.
         */
        @Nonnull
        private Map<Integer, CommodityDataPoints> collectContainerResources(@Nonnull final Map<Long, ProjectedTopologyEntity> projectedEntities) {
            final Map<Integer, CommodityDataPoints> collectedContainerCommDataPoints = new HashMap<>();
            connectedContainers.stream()
                .map(projectedEntities::get)
                .filter(Objects::nonNull)
                .forEach(container ->
                    container.getEntity().getCommoditySoldListList().stream()
                        .filter(comm -> resizingCommodityTypes.contains(comm.getCommodityType().getType())
                            || byProductCommodityTypes.contains(comm.getCommodityType().getType()))
                        .forEach(comm -> {
                            CommodityDataPoints commodityDataPoints =
                                collectedContainerCommDataPoints.computeIfAbsent(comm.getCommodityType().getType(),
                                    k -> new CommodityDataPoints());
                            commodityDataPoints.getUsedList().add(comm.getUsed());
                            commodityDataPoints.getCapacityList().add(comm.getCapacity());
                        }));
            return collectedContainerCommDataPoints;
        }
    }

    /**
     * Wrapper class to store commodity used and capacity data points to be aggregated.
     */
    private static class CommodityDataPoints {
        private final List<Double> capacityList;
        private final List<Double> usedList;

        CommodityDataPoints() {
            capacityList = new ArrayList<>();
            usedList = new ArrayList<>();
        }

        @Nonnull
        List<Double> getCapacityList() {
            return capacityList;
        }

        @Nonnull
        List<Double> getUsedList() {
            return usedList;
        }
    }
}
