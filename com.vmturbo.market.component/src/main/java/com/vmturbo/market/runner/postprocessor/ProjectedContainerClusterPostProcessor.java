package com.vmturbo.market.runner.postprocessor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ContainerInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ContainerPlatformClusterInfo;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ActionTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Post processor to update resources of projected ContainerPlatformCluster entities. Currently
 * ContainerPlatformCluster entities do not participate into analysis, so this post processor is to
 * reflect the after-action changes of projected containers and VMs on corresponding
 * ContainerPlatformClusters in real time and plan.
 */
public class ProjectedContainerClusterPostProcessor extends ProjectedEntityPostProcessor {

    /**
     * Default group ID to group entities by container cluster.
     */
    public static final Long DEFAULT_GROUP_ID = 0L;

    private static final Logger logger = LogManager.getLogger();

    private static final Map<Integer, Function<ContainerInfo, Boolean>> HAS_LIMIT_LOOKUP_MAP =
        ImmutableMap.of(
            CommodityType.VCPU_VALUE, info -> info.hasHasCpuLimit() && info.getHasCpuLimit(),
            CommodityType.VMEM_VALUE, info -> info.hasHasMemLimit() && info.getHasMemLimit()
        );


    private static final Set<Integer> LIMIT_COMMODITY_TYPES = ImmutableSet.of(
            CommodityType.VCPU_VALUE,
            CommodityType.VMEM_VALUE
    );

    private static final Set<Integer> REQUEST_COMMODITY_TYPES = ImmutableSet.of(
            CommodityType.VCPU_REQUEST_VALUE,
            CommodityType.VMEM_REQUEST_VALUE
    );

    @Override
    public boolean appliesTo(@Nonnull final TopologyInfo topologyInfo,
                             @Nonnull final Map<Integer, List<ProjectedTopologyEntity>> entityTypeToEntitiesMap) {
        final String logPrefix = String.format("%s topology [ID=%d, context=%d]: ",
            topologyInfo.getTopologyType(), topologyInfo.getTopologyId(), topologyInfo.getTopologyContextId());
        // Post process projected ContainerPlatformCluster entities only in "Optimize Container Cluster"
        // plan.
        if (isOptimizeContainerClusterPlan(topologyInfo)) {
            if (!entityTypeToEntitiesMap.containsKey(EntityType.CONTAINER_PLATFORM_CLUSTER_VALUE)) {
                logger.error("{}Optimize Container Cluster plan does not have any ContainerPlatformCluster entities.", logPrefix);
                return false;
            }
            return true;
        }
        // Return false if it's not "Optimize Container Cluster" plan.
        return false;
    }

    private boolean isOptimizeContainerClusterPlan(@Nonnull final TopologyInfo topologyInfo) {
        return topologyInfo.hasPlanInfo()
            && topologyInfo.getPlanInfo().hasPlanType()
            && StringConstants.OPTIMIZE_CONTAINER_CLUSTER_PLAN.equals(topologyInfo.getPlanInfo().getPlanType());
    }

    /**
     * Update resources of projected ContainerPlatformCluster entities based on projected containers
     * and VMs. The resources to update include:
     *
     * <p>1. VCPU, VMem, VCPURequest, VMemRequest and numberConsumers. These commodities will be
     * updated based on aggregated commodity usage and capacity of projected VMs in the cluster.
     *
     * <p>2. VCPUOvercommitment and VMemOvercommitment. These attributes will be updated based on
     * resource limits of projected containers and commodity capacity of projected VMs.
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
        // Map of projected container cluster to be updated.
        final Map<Long, ProjectedTopologyEntity.Builder> updatedProjectedCntClustersMap = new HashMap<>();

        // Group VMs by ContainerPlatformCluster OID. If VMs have no connected ContainerPlatformCluster,
        // leave them in the same group (with group ID as DEFAULT_GROUP_ID).
        final List<ProjectedTopologyEntity> projectedVMs = entityTypeToProjectedEntities.get(EntityType.VIRTUAL_MACHINE_VALUE);
        if (projectedVMs == null) {
            // A container cluster should have at least one master node (VM). Log an error if no
            // projected VMs.
            logger.error("{}There are no projected VMs when post processing projected ContainerPlatformClusters.",
                logPrefix);
            return;
        }
        // Cache clusterId per VM.
        final Map<Long, Long> vmToClusterIdMap = new HashMap<>();
        final Map<Long, List<ProjectedTopologyEntity>> cntClusterToVMsMap =
            groupProjectedVMs(logPrefix, projectedEntities, projectedVMs, vmToClusterIdMap);

        // Group containers by ContainerPlatformCluster OID.
        final List<ProjectedTopologyEntity> projectedContainers = entityTypeToProjectedEntities.get(EntityType.CONTAINER_VALUE);
        Map<Long, List<ProjectedTopologyEntity>> cntClusterToContainersMap = new HashMap<>();
        if (projectedContainers == null) {
            // It's a valid case when a container cluster is empty with no containers deployed.
            logger.debug("{}There are no projected containers when post processing projected ContainerPlatformClusters.",
                logPrefix);
        } else {
            cntClusterToContainersMap =
                groupProjectedContainers(logPrefix, projectedEntities, projectedContainers, vmToClusterIdMap);
        }

        List<ProjectedTopologyEntity> projectedCntClusters =
                entityTypeToProjectedEntities.get(EntityType.CONTAINER_PLATFORM_CLUSTER_VALUE);
        for (ProjectedTopologyEntity projectedCntCluster : projectedCntClusters) {
            final long containerClusterId = projectedCntCluster.getEntity().getOid();
            // ProjectedTopologyEntity.Builder to be updated.
            final ProjectedTopologyEntity.Builder projectedCntClusterBuilder = projectedCntCluster.toBuilder();
            updatedProjectedCntClustersMap.put(containerClusterId, projectedCntClusterBuilder);
            processProjectedContainerCluster(logPrefix, containerClusterId, projectedCntClusterBuilder,
                cntClusterToVMsMap, cntClusterToContainersMap);
        }
        // Set the updated projected ContainerPlatformCluster entities to projectedEntities map.
        updatedProjectedCntClustersMap.forEach((oid, projectedCntClusterBuilder) ->
                projectedEntities.put(oid, projectedCntClusterBuilder.build()));
        logger.info("{}Finished updating {} projected ContainerPlatformCluster entities.", logPrefix,
            updatedProjectedCntClustersMap.size());
    }

    @VisibleForTesting
    Map<Long, List<ProjectedTopologyEntity>> groupProjectedVMs(
        @Nonnull final String logPrefix,
        @Nonnull final Map<Long, ProjectedTopologyEntity> projectedEntities,
        @Nonnull final List<ProjectedTopologyEntity> projectedVMs,
        @Nonnull final Map<Long, Long> vmToClusterIdMap) {
        return projectedVMs.stream()
            .collect(Collectors.groupingBy(entity -> {
                    // Get container cluster from connected entities of given projected VM.
                    // If not found, the projected VM could be a cloned one without connected entities.
                    // Then look for the container cluster from the corresponding origin VM.
                    long clusterId = getContainerClusterIdFromVM(entity)
                        .orElseGet(() -> getContainerClusterIdFromOrigVM(logPrefix, entity, projectedEntities));
                    vmToClusterIdMap.put(entity.getEntity().getOid(), clusterId);
                    return clusterId;
                })
            );
    }

    private long getContainerClusterIdFromOrigVM(@Nonnull final String logPrefix,
        @Nonnull final ProjectedTopologyEntity projectedVM,
        @Nonnull final Map<Long, ProjectedTopologyEntity> projectedEntities) {
        if (projectedVM.getEntity().hasOrigin()
            && projectedVM.getEntity().getOrigin().hasAnalysisOrigin()
            && projectedVM.getEntity().getOrigin().getAnalysisOrigin().hasOriginalEntityId()) {
            long origVMId = projectedVM.getEntity().getOrigin().getAnalysisOrigin().getOriginalEntityId();
            ProjectedTopologyEntity origVM = projectedEntities.get(origVMId);
            if (origVM != null) {
                return getContainerClusterIdFromVM(origVM).orElse(DEFAULT_GROUP_ID);
            } else {
                logger.error("{}VM {} is not found from projectedEntities map.", logPrefix, origVMId);
            }
        }
        // It's a valid case when an entity has no AnalysisOrigin, we don't need to log an error here.
        return DEFAULT_GROUP_ID;
    }

    private Optional<Long> getContainerClusterIdFromVM(@Nonnull final ProjectedTopologyEntity vm) {
        return vm.getEntity().getConnectedEntityListList().stream()
            .filter(e -> e.getConnectedEntityType() == EntityType.CONTAINER_PLATFORM_CLUSTER_VALUE)
            .findFirst()
            .map((ConnectedEntity::getConnectedEntityId));
    }

    @VisibleForTesting
    Map<Long, List<ProjectedTopologyEntity>> groupProjectedContainers(
        @Nonnull final String logPrefix,
        @Nonnull final Map<Long, ProjectedTopologyEntity> projectedEntities,
        @Nonnull final List<ProjectedTopologyEntity> projectedContainers,
        @Nonnull final Map<Long, Long> vmToClusterIdMap) {
        return projectedContainers.stream()
            .collect(Collectors.groupingBy(projectedContainer -> {

                ProjectedTopologyEntity projectedContainerPod =
                    projectedContainer.getEntity().getCommoditiesBoughtFromProvidersList().stream()
                        .filter(comm -> comm.hasProviderEntityType()
                            && comm.getProviderEntityType() == EntityType.CONTAINER_POD_VALUE)
                        .map(comm -> projectedEntities.get(comm.getProviderId()))
                        .filter(Objects::nonNull)
                        .findFirst()
                        // If providerEntityType is not set, look up ContainerPod entity based on
                        // providerId.
                        .orElseGet(() ->
                            getProjectedProviderEntity(projectedContainer, projectedEntities,
                                EntityType.CONTAINER_POD_VALUE));

                if (projectedContainerPod == null) {
                    logger.error("{}Cannot find ContainerPod provider from projected Container {}",
                        logPrefix, projectedContainer.getEntity());
                    return DEFAULT_GROUP_ID;
                }

                ProjectedTopologyEntity projectedVM =
                    projectedContainerPod.getEntity().getCommoditiesBoughtFromProvidersList().stream()
                        .filter(e -> e.hasProviderEntityType()
                            && e.getProviderEntityType() == EntityType.VIRTUAL_MACHINE_VALUE)
                        .map(comm -> projectedEntities.get(comm.getProviderId()))
                        .filter(Objects::nonNull)
                        .findFirst()
                        .orElseGet(() ->
                            getProjectedProviderEntity(projectedContainerPod, projectedEntities,
                                EntityType.VIRTUAL_MACHINE_VALUE));
                if (projectedVM == null) {
                    logger.warn("{}Cannot find VM provider from projected ContainerPod. This ContainerPod is unplaced: {}",
                        logPrefix, projectedContainerPod.getEntity());
                    return DEFAULT_GROUP_ID;
                }
                return vmToClusterIdMap.getOrDefault(projectedVM.getEntity().getOid(), DEFAULT_GROUP_ID);
            }));
    }

    private ProjectedTopologyEntity getProjectedProviderEntity(@Nonnull final ProjectedTopologyEntity entity,
                                                               @Nonnull final Map<Long, ProjectedTopologyEntity> projectedEntities,
                                                               final int providerEntityType) {
        return entity.getEntity().getCommoditiesBoughtFromProvidersList().stream()
            .map(e -> projectedEntities.get(e.getProviderId()))
            .filter(provider -> provider != null
                && provider.getEntity().getEntityType() == providerEntityType)
            .findFirst()
            .orElse(null);
    }

    private void processProjectedContainerCluster(
        @Nonnull final String logPrefix,
        final long containerClusterId,
        @Nonnull final ProjectedTopologyEntity.Builder projectedContainerClusterBuilder,
        @Nonnull final Map<Long, List<ProjectedTopologyEntity>> containerClusterToVMsMap,
        @Nonnull final Map<Long, List<ProjectedTopologyEntity>> containerClusterToContainersMap) {
        // Get aggregated VM resources
        final Map<Integer, AggregatedResourceData> aggregatedVMResources =
            getAggregatedVMResources(logPrefix, containerClusterId, containerClusterToVMsMap.get(containerClusterId));

        // Get aggregated container resources
        final Map<Integer, AggregatedResourceData> aggregatedCntResources =
            getAggregatedContainerResources(logPrefix, containerClusterId, containerClusterToContainersMap.get(containerClusterId));

        // Update VCPU/VMem limit, request and numberConsumers commodities.
        updateLimitRequestAndNumConsumersCommodities(logPrefix, projectedContainerClusterBuilder, aggregatedVMResources);
        // Update VCPUOvercommitment and VMemOvercommitment attributes.
        updateOvercommitments(logPrefix, projectedContainerClusterBuilder, aggregatedVMResources, aggregatedCntResources);
    }

    private Map<Integer, AggregatedResourceData> getAggregatedVMResources(
        @Nonnull final String logPrefix,
        final long containerClusterId,
        @Nullable final List<ProjectedTopologyEntity> projectedVMs) {
        final Map<Integer, AggregatedResourceData> aggregatedResources = new HashMap<>();
        if (projectedVMs == null) {
            logger.error("{}Projected ContainerPlatformCluster {} has no VMs.", logPrefix, containerClusterId);
            return aggregatedResources;
        }
        for (ProjectedTopologyEntity projectedVM : projectedVMs) {
            // Count only active VMs as cluster capacity.
            if (projectedVM.getEntity().getEntityState() != EntityState.POWERED_ON) {
                continue;
            }
            projectedVM.getEntity().getCommoditySoldListList().stream()
                .filter(comm -> LIMIT_COMMODITY_TYPES.contains(comm.getCommodityType().getType())
                    || REQUEST_COMMODITY_TYPES.contains(comm.getCommodityType().getType())
                    || CommodityType.NUMBER_CONSUMERS_VALUE == comm.getCommodityType().getType())
                .forEach(comm -> {
                    int commodityType = comm.getCommodityType().getType();
                    AggregatedResourceData aggregatedResource =
                        aggregatedResources.computeIfAbsent(commodityType, v -> new AggregatedResourceData());
                    aggregatedResource.setUsed(aggregatedResource.getUsed() + comm.getUsed());
                    aggregatedResource.setCapacity(aggregatedResource.getCapacity() + comm.getCapacity());
                });
        }
        return aggregatedResources;
    }

    @VisibleForTesting
    Map<Integer, AggregatedResourceData> getAggregatedContainerResources(
        @Nonnull final String logPrefix,
        final long containerClusterId,
        @Nullable final List<ProjectedTopologyEntity> projectedContainers) {
        final Map<Integer, AggregatedResourceData> aggregatedResources = new HashMap<>();
        if (projectedContainers == null) {
            // It's a valid case when a container platform cluster has no resources created yet.
            logger.debug("{}Projected ContainerPlatformCluster {} has no Containers. This could be an empty cluster.",
                logPrefix, containerClusterId);
            return aggregatedResources;
        }

        for (ProjectedTopologyEntity projectedContainer : projectedContainers) {
            TopologyEntityDTO entityDTO = projectedContainer.getEntity();
            entityDTO.getCommoditySoldListList().stream()
                .filter(comm -> LIMIT_COMMODITY_TYPES.contains(comm.getCommodityType().getType()))
                .forEach(comm -> {
                    int commodityType = comm.getCommodityType().getType();
                    if (entityDTO.hasTypeSpecificInfo() && entityDTO.getTypeSpecificInfo().hasContainer()) {
                        boolean hasLimitSet = HAS_LIMIT_LOOKUP_MAP.get(commodityType)
                            .apply(entityDTO.getTypeSpecificInfo().getContainer());
                        if (hasLimitSet) {
                            AggregatedResourceData aggregatedResource =
                                aggregatedResources.computeIfAbsent(commodityType, v -> new AggregatedResourceData());
                            aggregatedResource.setCapacity(aggregatedResource.getCapacity() + comm.getCapacity());
                        }
                    }
                });
        }
        return aggregatedResources;
    }

    private void updateLimitRequestAndNumConsumersCommodities(@Nonnull final String logPrefix,
                                                  @Nonnull final ProjectedTopologyEntity.Builder cntClusterBuilder,
                                                  @Nonnull final Map<Integer, AggregatedResourceData> aggregatedVMResources) {
        cntClusterBuilder.getEntityBuilder().getCommoditySoldListBuilderList().stream()
                .filter(comm -> LIMIT_COMMODITY_TYPES.contains(comm.getCommodityType().getType())
                    || REQUEST_COMMODITY_TYPES.contains(comm.getCommodityType().getType())
                    || CommodityType.NUMBER_CONSUMERS_VALUE == comm.getCommodityType().getType())
                .forEach(comm -> {
                    int commodityType = comm.getCommodityType().getType();
                    AggregatedResourceData aggregatedResourceData =
                        aggregatedVMResources.computeIfAbsent(commodityType, v -> new AggregatedResourceData());
                    comm.setUsed(aggregatedResourceData.getUsed());
                    comm.setCapacity(aggregatedResourceData.getCapacity());
                    logger.debug("{}ContainerPlatformCluster {}: updated {} used to {} and "
                            + "capacity to {}.",
                        logPrefix, cntClusterBuilder.getEntity().getOid(), CommodityType.forNumber(commodityType),
                        comm.getUsed(), comm.getCapacity());
                });
    }

    private void updateOvercommitments(@Nonnull final String logPrefix,
                                       @Nonnull final ProjectedTopologyEntity.Builder cntClusterBuilder,
                                       @Nonnull final Map<Integer, AggregatedResourceData> aggregatedVMResources,
                                       @Nonnull final Map<Integer, AggregatedResourceData> aggregatedCntResources) {
        final double vcpuOvercommitment =
            calculateOvercommitment(logPrefix, cntClusterBuilder.getEntity().getOid(),
                aggregatedVMResources, aggregatedCntResources, CommodityType.VCPU_VALUE);
        final double vmemOvercommitment =
            calculateOvercommitment(logPrefix, cntClusterBuilder.getEntity().getOid(),
                aggregatedVMResources, aggregatedCntResources, CommodityType.VMEM_VALUE);
        if (cntClusterBuilder.getEntityBuilder().hasTypeSpecificInfo()
            && cntClusterBuilder.getEntityBuilder().getTypeSpecificInfo().hasContainerPlatformCluster()) {
            cntClusterBuilder.getEntityBuilder()
                .getTypeSpecificInfoBuilder()
                .getContainerPlatformClusterBuilder()
                .setVcpuOvercommitment(vcpuOvercommitment)
                .setVmemOvercommitment(vmemOvercommitment);
        } else {
            // Set ContainerPlatformClusterInfo if missing.
            cntClusterBuilder.getEntityBuilder().setTypeSpecificInfo(
                TypeSpecificInfo.newBuilder()
                    .setContainerPlatformCluster(ContainerPlatformClusterInfo.newBuilder()
                        .setVcpuOvercommitment(vcpuOvercommitment)
                        .setVmemOvercommitment(vmemOvercommitment)));
        }
        logger.debug("{}ContainerPlatformCluster {}: updated VCPUOvercommitment to {} and "
                + "VMemOvercommitment to {}.",
            logPrefix, cntClusterBuilder.getEntity().getOid(), vcpuOvercommitment, vmemOvercommitment);
    }

    private double calculateOvercommitment(
        @Nonnull final String logPrefix,
        final long clusterId,
        @Nonnull final Map<Integer, AggregatedResourceData> aggregatedVMResources,
        @Nonnull final Map<Integer, AggregatedResourceData> aggregatedCntResources,
        @Nonnull final Integer commodityType) {
        AggregatedResourceData vmAggResource = aggregatedVMResources.get(commodityType);
        if (vmAggResource == null) {
            logger.error("{}ContainerPlatformCluster {}: Aggregated VM {} resource is not found.",
                logPrefix, clusterId, CommodityType.forNumber(commodityType));
            return 0;
        }
        AggregatedResourceData cntAggResource = aggregatedCntResources.get(commodityType);
        if (cntAggResource == null) {
            logger.error("{}ContainerPlatformCluster {}: Aggregated container {} resource is not found.",
                logPrefix, clusterId, CommodityType.forNumber(commodityType));
            return 0;
        }
        if (vmAggResource.getCapacity() != 0) {
            return cntAggResource.getCapacity() / vmAggResource.getCapacity();
        } else {
            logger.error("{}ContainerPlatformCluster {}: Aggregated VM {} capacity is 0.",
                logPrefix, clusterId, CommodityType.forNumber(commodityType));
            return 0;
        }
    }

    /**
     * Wrapper class to store aggregated used and capacity values of a resource.
     */
    static class AggregatedResourceData {
        private double used;
        private double capacity;

        public double getUsed() {
            return used;
        }

        public void setUsed(double used) {
            this.used = used;
        }

        public double getCapacity() {
            return capacity;
        }

        public void setCapacity(double capacity) {
            this.capacity = capacity;
        }
    }
}
