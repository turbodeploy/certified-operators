package com.vmturbo.stitching.prestitching;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.platform.common.builders.SDKConstants;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.Builder;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.stitching.PreStitchingOperation;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.StitchingScope;
import com.vmturbo.stitching.StitchingScope.StitchingScopeFactory;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologicalChangelog.StitchingChangesBuilder;
import com.vmturbo.stitching.utilities.CommoditiesBought;

/**
 * This class attempts to convert VCPU, VCPURequest, VCPULimitQuota and VCPURequestQuota from
 * MHz to millicore for Container Platform Cluster entities discovered by kubeturbo older than 8.2.3.
 * Customers with large number of Kubernetes clusters are not likely to upgrade kubeturbo to the
 * version that supports millicore all at once, so it is crucial to maintain backward compatibility
 * after the server is upgraded to support millicore feature.
 *
 * <p>Once we know that all customers have upgraded all their old kubeturbos to new kubeturbos that
 * support millicores, we should remove this stage and the corresponding unit tests, so that we don't
 * have to maintain them any more. https://vmturbo.atlassian.net/browse/OM-72154 has been logged
 * to track this effort.
 */
public class ContainerClusterPreStitchingOperation implements PreStitchingOperation {

    /**
     * The logger.
     */
    private static final Logger logger = LogManager.getLogger();
    /**
     * A set to hold VCPU commodity.
     */
    private static final Set<Integer> VCPU =
            ImmutableSet.of(CommodityType.VCPU.getNumber());
    /**
     * A set to hold VCPUREQUEST commodity.
     */
    private static final Set<Integer> VCPUREQUEST =
            ImmutableSet.of(CommodityType.VCPU_REQUEST.getNumber());

    /**
     * A set to hold resource commodities.
     */
    private static final Set<Integer> VCPU_AND_VCPUREQUEST =
            ImmutableSet.of(CommodityType.VCPU.getNumber(),
                            CommodityType.VCPU_REQUEST.getNumber());
    /**
     * A set to hold quota commodities.
     */
    private static final Set<Integer> VCPU_LIMIT_REQUEST_QUOTA =
            ImmutableSet.of(CommodityType.VCPU_LIMIT_QUOTA.getNumber(),
                            CommodityType.VCPU_REQUEST_QUOTA.getNumber());
    /**
     * A set to hold all resource and quota commodities.
     */
    private static final Set<Integer> ALL_VCPU =
            ImmutableSet.of(CommodityType.VCPU.getNumber(),
                            CommodityType.VCPU_REQUEST.getNumber(),
                            CommodityType.VCPU_LIMIT_QUOTA.getNumber(),
                            CommodityType.VCPU_REQUEST_QUOTA.getNumber());

    /**
     * Implement the scoping function for this pre-stitching operation.
     *
     * @param stitchingScopeFactory The factory to use to construct the {@link StitchingScope} for this
     *                                {@link PreStitchingOperation}.
     * @return the stitching scope of this operation
     */
    @Nonnull
    @Override
    public StitchingScope<StitchingEntity> getScope(
            @Nonnull StitchingScopeFactory<StitchingEntity> stitchingScopeFactory) {
        return stitchingScopeFactory.probeCategoryEntityTypeScope(ProbeCategory.CLOUD_NATIVE,
                                                                  EntityType.CONTAINER_PLATFORM_CLUSTER);
    }

    /**
     * Implement the pre-stitching operation to perform the conversion.
     *
     * @param clusters the container platform cluster entities
     * @param resultBuilder A builder for the result containing the changes this operation wants to make
     *                      to the entities and their relationships. The calculation should use this builder
     *                      to create the result it returns.
     * @return the topological change log
     */
    @Nonnull
    @Override
    public TopologicalChangelog<StitchingEntity> performOperation(
            @Nonnull Stream<StitchingEntity> clusters,
            @Nonnull StitchingChangesBuilder<StitchingEntity> resultBuilder) {
        AtomicInteger total = new AtomicInteger();
        AtomicInteger converted = new AtomicInteger();
        clusters.filter(this::shouldConvertToMillicore)
                .forEach(cluster -> {
                    total.getAndIncrement();
                    if (convertEntitiesInCluster(cluster, resultBuilder)) {
                        converted.getAndIncrement();
                    }
                });
        if (total.get() > 0) {
            logger.info("Among {} clusters that need conversion from MHz to millicores, "
                                + "{} are converted, {} are not.",
                        total.get(), converted.get(), total.get() - converted.get());
        }
        return resultBuilder.build();
    }

    /**
     * Check if CPU needs to be converted to millicore for this cluster.
     * New kubeturbo sets a VcpuUnit. Old kubeturbo does not have this field set.
     *
     * @param cluster the cluster to check
     * @return true if CPU needs to be converted to millicore
     */
    private boolean shouldConvertToMillicore(@Nonnull final StitchingEntity cluster) {
        return !cluster.getEntityBuilder().getContainerPlatformClusterData().hasVcpuUnit();
    }

    /**
     * Convert CPU related commodities for all entities in a cluster from MHz to millicore.
     * The conversion is performed in three steps:
     * - From each VM in the cluster along the consumer relationship all the way to Application
     * - From each namespace in the cluster along the consumer/owned relationship to ContainerSpec
     * - The Cluster itself
     *
     * @param cluster the container platform cluster entity
     * @param resultBuilder the stitching change builder
     * @return true if the cluster is converted
     */
    private boolean convertEntitiesInCluster(@Nonnull StitchingEntity cluster,
                                             @Nonnull StitchingChangesBuilder<StitchingEntity> resultBuilder) {
        if (logger.isDebugEnabled()) {
            logger.debug("Converting CPU to millicores for {}.", cluster.getDisplayName());
        }
        // Compute the VM to cpu speed map for later use
        Map<StitchingEntity, Double> vmCpuSpeedMap = new HashMap<>();
        cluster.getConnectedFromByType().entrySet().stream()
                .filter(entry -> entry.getKey().equals(ConnectionType.AGGREGATED_BY_CONNECTION))
                .map(Entry::getValue)
                .flatMap(Set::stream)
                .filter(entity -> EntityType.VIRTUAL_MACHINE.equals(entity.getEntityType()))
                .forEach(vm -> {
                    final Optional<Double> cpuSpeed = getVMCPUSpeed(vm);
                    if (!cpuSpeed.isPresent()) {
                        logger.warn("Failed to get CPU speed for VM {} when converting VCPU to"
                                            + " millicores for cluster {}.",
                                    vm.getDisplayName(), cluster.getDisplayName());
                        return;
                    }
                    vmCpuSpeedMap.putIfAbsent(vm, cpuSpeed.get());
                });
        if (vmCpuSpeedMap.isEmpty()) {
            logger.error("Failed to get CPU speed for any VM in cluster {}. "
                                 + "Abort converting VCPU to millicores.", cluster.getDisplayName());
            return false;
        }
        // Convert each VM and its consumers
        vmCpuSpeedMap.forEach((vm, cpuSpeed) -> convertEntitiesFromVM(vm, cpuSpeed, resultBuilder));
        // Convert each namespace and its consumers
        cluster.getConsumers().stream()
                .filter(entity -> EntityType.NAMESPACE.equals(entity.getEntityType())
                        && entity.getEntityBuilder().hasNamespaceData())
                .forEach(ns -> getNamespaceCPUSpeed(ns).ifPresent(
                        cpuSpeed -> convertEntitiesFromNamespace(ns, cpuSpeed, resultBuilder)));
        // Convert the cluster itself
        resultBuilder.queueUpdateEntityAlone(cluster, e -> convertCluster(cluster, vmCpuSpeedMap));
        return true;
    }

    /**
     * Convert CPU related commodities for all entities from a VM (including the VM itself).
     * Only VirtualMachine, ContainerPod and Container entities are converted.
     *
     * @param entity the entity along the traversal path to convert
     * @param cpuSpeed the CPU speed of the bottom VM
     * @param resultBuilder the stitching change builder
     */
    private void convertEntitiesFromVM(@Nonnull StitchingEntity entity,
                                       double cpuSpeed,
                                       @Nonnull StitchingChangesBuilder<StitchingEntity> resultBuilder) {
        switch (entity.getEntityType()) {
            case VIRTUAL_MACHINE:
                resultBuilder.queueUpdateEntityAlone(entity, e -> convertVM(e, cpuSpeed));
                entity.getConsumers().forEach(consumer -> convertEntitiesFromVM(consumer, cpuSpeed, resultBuilder));
                return;
            case CONTAINER_POD:
                resultBuilder.queueUpdateEntityAlone(entity, e -> {
                    convertContainerOrPod(e, cpuSpeed);
                    addCPUFreqData(e, cpuSpeed);
                });
                entity.getConsumers().forEach(consumer -> convertEntitiesFromVM(consumer, cpuSpeed, resultBuilder));
                return;
            case CONTAINER:
                resultBuilder.queueUpdateEntityAlone(entity, e -> convertContainerOrPod(e, cpuSpeed));
                entity.getConsumers().forEach(consumer -> resultBuilder
                        .queueUpdateEntityAlone(consumer, c -> addCPUFreqData(c, cpuSpeed)));
            default:
        }
    }

    /**
     * Add cpuFrequency in the entity type specific data.
     * This is implemented for Application Component and Container Pod only and would return
     * the stitching entity unchanged for any other types.
     *
     * @param entity the entity to convert
     * @param cpuSpeed the CPU speed of the VM hosting this entity in Ghz
     */
    private void addCPUFreqData(@Nonnull StitchingEntity entity, double cpuSpeed) {
        // The hostingNodeCpuFrequency is expected to be set in Mhz
        if (entity.getEntityType() == EntityType.APPLICATION_COMPONENT) {
            entity.getEntityBuilder().getApplicationDataBuilder().setHostingNodeCpuFrequency(cpuSpeed * 1000);
        } else if (entity.getEntityType() == EntityType.CONTAINER_POD) {
            entity.getEntityBuilder().getContainerPodDataBuilder().setHostingNodeCpuFrequency(cpuSpeed * 1000);
        }
    }

    /**
     * Convert CPU related commodities of a VM.
     * Only VCPURequest commodities are converted. VCPU are still kept in MHz so we don't break
     * analysis in a stitched environment.
     *
     * @param vm the VM to convert
     * @param cpuSpeed the CPU speed of the VM
     */
    private void convertVM(@Nonnull StitchingEntity vm, double cpuSpeed) {
        getCommoditiesSoldBuilder(vm, VCPUREQUEST).findAny()
                .ifPresent(builder -> convertAll(builder, cpuSpeed));
    }

    /**
     * Convert CPU related commodities of a container or container pod.
     * Both containers and container pods sell and buy VCPU and VCPURequest.
     * Both containers and container pods buy VCPULimitQuota and VCPURequestQuota.
     * Container pods sell VCPULimitQuota and VCPURequestQuota. The capacity will be reset when
     * processing namespace of the pod.
     *
     * @param entity the container or container pod to convert
     * @param cpuSpeed the CPU speed of the underlying VM
     */
    private void convertContainerOrPod(@Nonnull StitchingEntity entity, double cpuSpeed) {
        getCommoditiesSoldBuilder(entity, ALL_VCPU)
                .forEach(builder -> convertAll(builder, cpuSpeed));
        getCommoditiesBoughtBuilder(entity, ALL_VCPU)
                .forEach(builder -> convertAll(builder, cpuSpeed));
    }

    /**
     * Convert CPU related commodities for all entities in a namespace from MHz to millicore,
     * including the namespace itself.
     *
     * @param namespace the namespace entity
     * @param cpuSpeed the average node CPU speed
     * @param resultBuilder the stitching change builder
     */
    private void convertEntitiesFromNamespace(@Nonnull StitchingEntity namespace,
                                              double cpuSpeed,
                                              @Nonnull StitchingChangesBuilder<StitchingEntity> resultBuilder) {
        // A list to hold all pods in the namespace in order to calculate
        // the namespace resource and quota usage
        Set<StitchingEntity> nsPods = new HashSet<>();
        // Convert entities in the namespace, and collect all pods in the namespace
        namespace.getConsumers().forEach(
                consumer -> convertEntitiesInNamespace(consumer, cpuSpeed, nsPods, resultBuilder));
        // Convert the namespace itself by aggregating usage from all pods in the namespace
        resultBuilder.queueUpdateEntityAlone(namespace, e -> convertNamespace(e, cpuSpeed, nsPods));
    }

    /**
     * Convert CPU related commodities for all entities in a namespace from MHz to millicore.
     * Also collect all pods in the namespace for later use.
     *
     * @param entity the entity along the traversal path to convert
     * @param cpuSpeed the average CPU speed of the namespace
     * @param nsPods a set to hold all pods running in the namespace for later use
     * @param resultBuilder the stitching change builder
     */
    private void convertEntitiesInNamespace(@Nonnull StitchingEntity entity,
                                            double cpuSpeed,
                                            @Nonnull Set<StitchingEntity> nsPods,
                                            @Nonnull StitchingChangesBuilder<StitchingEntity> resultBuilder) {
        switch (entity.getEntityType()) {
            case CONTAINER_POD:
                // This pod is a direct consumer of the namespace
                nsPods.add(entity);
                return;
            case WORKLOAD_CONTROLLER:
                resultBuilder.queueUpdateEntityAlone(entity, e -> convertController(e, cpuSpeed, nsPods));
                // Traverse to container specs
                entity.getConnectedToByType().entrySet().stream()
                        .filter(entry -> entry.getKey().equals(ConnectionType.OWNS_CONNECTION))
                        .map(Entry::getValue)
                        .flatMap(Set::stream)
                        .filter(e -> EntityType.CONTAINER_SPEC.equals(e.getEntityType()))
                        .forEach(owned -> convertEntitiesInNamespace(owned, cpuSpeed, nsPods, resultBuilder));
                return;
            case CONTAINER_SPEC:
                resultBuilder.queueUpdateEntityAlone(entity, e -> convertContainerSpec(e, cpuSpeed));
            default:
        }
    }

    /**
     * Convert CPU related commodities of a workload controller.
     *
     * @param controller the workload controller to convert
     * @param cpuSpeed the average node CPU speed
     * @param nsPods the pods in the controller
     */
    private void convertController(@Nonnull StitchingEntity controller,
                                   double cpuSpeed,
                                   @Nonnull Set<StitchingEntity> nsPods) {
        final Set<StitchingEntity> controllerPods = controller.getConnectedFromByType().entrySet().stream()
                .filter(entry -> entry.getKey().equals(ConnectionType.AGGREGATED_BY_CONNECTION))
                .map(Entry::getValue)
                .flatMap(Set::stream)
                .filter(e -> EntityType.CONTAINER_POD.equals(e.getEntityType()))
                .collect(Collectors.toSet());
        nsPods.addAll(controllerPods);
        convertCapacityAndSetUsedPeakForController(controller, controllerPods, cpuSpeed);
    }

    /**
     * Convert Capacity for workload controller, and aggregate the Used and Peak from all pods
     * in the controller.
     *
     * @param controller the workload controller
     * @param pods the pods in the workload controller
     * @param cpuSpeed the average node CPU speed
     */
    private void convertCapacityAndSetUsedPeakForController(@Nonnull StitchingEntity controller,
                                                            @Nonnull Set<StitchingEntity> pods,
                                                            double cpuSpeed) {
        // Compute used and peak
        final Map<Integer, Double> totalUsedMillicores = new HashMap<>();
        VCPU_LIMIT_REQUEST_QUOTA.forEach(commodityType -> totalUsedMillicores
                .put(commodityType, getTotalUsed(pods, commodityType)));
        getCommoditiesSoldBuilder(controller, VCPU_LIMIT_REQUEST_QUOTA)
                .forEach(builder -> {
                    // Convert capacity
                    convertCapacity(builder, cpuSpeed);
                    // Set used and peak
                    @Nonnull final Double totalUsedMillicore =
                            totalUsedMillicores.get(builder.getCommodityType().getNumber());
                    builder.setUsed(totalUsedMillicore).setPeak(totalUsedMillicore);
                });
        getCommoditiesBoughtBuilder(controller, VCPU_LIMIT_REQUEST_QUOTA)
                .forEach(builder -> {
                    // Set used and peak
                    @Nonnull final Double totalUsedMillicore =
                            totalUsedMillicores.get(builder.getCommodityType().getNumber());
                    builder.setUsed(totalUsedMillicore).setPeak(totalUsedMillicore);
                });
    }

    /**
     * Convert CPU related commodities for a container spec.
     *
     * @param containerSpec the container spec to convert
     * @param cpuSpeed the average node CPU speed
     */
    private void convertContainerSpec(@Nonnull StitchingEntity containerSpec,
                                      double cpuSpeed) {
        final List<StitchingEntity> containers = containerSpec.getConnectedFromByType().entrySet().stream()
                .filter(entry -> entry.getKey().equals(ConnectionType.CONTROLLED_BY_CONNECTION))
                .map(Entry::getValue)
                .flatMap(Set::stream)
                .filter(e -> EntityType.CONTAINER.equals(e.getEntityType()))
                .collect(Collectors.toList());
        setCapacityUsedAndConvertPeakForSpec(containerSpec, containers, cpuSpeed);
    }

    /**
     * Aggregate the Capacity (max) and Used (average), and convert Peak for a container spec.
     *
     * @param containerSpec the container spec to convert or set
     * @param containers the containers that share the container spec
     * @param cpuSpeed the average node CPU speed
     */
    private void setCapacityUsedAndConvertPeakForSpec(@Nonnull StitchingEntity containerSpec,
                                                      @Nonnull List<StitchingEntity> containers,
                                                      double cpuSpeed) {
        // Compute capacity and used
        final Map<Integer, Pair<OptionalDouble, OptionalDouble>> capacityAndUsedMap = new HashMap<>();
        VCPU_AND_VCPUREQUEST.forEach(commodityType -> {
            final List<CommodityDTO.Builder> soldCommodities = containers.stream()
                    .flatMap(container -> getCommoditiesSoldBuilder(container, ImmutableSet.of(commodityType)))
                    .collect(Collectors.toList());
            final OptionalDouble maxCapacityMillicore = soldCommodities.stream()
                    .mapToDouble(Builder::getCapacity)
                    .max();
            final OptionalDouble avgUsedMillicore = soldCommodities.stream()
                    .mapToDouble(Builder::getUsed)
                    .average();
            capacityAndUsedMap.put(commodityType, ImmutablePair.of(maxCapacityMillicore, avgUsedMillicore));
        });
        getCommoditiesSoldBuilder(containerSpec, VCPU_AND_VCPUREQUEST)
                .forEach(builder -> {
                    @Nonnull final Pair<OptionalDouble, OptionalDouble> capacityAndUsed =
                            capacityAndUsedMap.get(builder.getCommodityType().getNumber());
                    capacityAndUsed.getLeft().ifPresent(builder::setCapacity);
                    capacityAndUsed.getRight().ifPresent(builder::setUsed);
                    // Convert peak, approximation only
                    convertPeak(builder, cpuSpeed);
                });
    }

    /**
     * Convert CPU related commodities of a namespace.
     *
     * @param namespace the namespace to convert
     * @param cpuSpeed the average node CPU speed
     * @param nsPods all the pods in the namespace
     */
    private void convertNamespace(@Nonnull StitchingEntity namespace,
                                  double cpuSpeed,
                                  @Nonnull Set<StitchingEntity> nsPods) {
        final Map<Integer, Double> totalUsedMillicores = new HashMap<>();
        ALL_VCPU.forEach(commodityType -> totalUsedMillicores
                .put(commodityType, getTotalUsed(nsPods, commodityType)));
        // Namespaces buy resource commodities
        getCommoditiesBoughtBuilder(namespace, VCPU_AND_VCPUREQUEST)
                .forEach(builder -> {
                    @Nonnull final Double totalUsedMillicore =
                            totalUsedMillicores.get(builder.getCommodityType().getNumber());
                    builder.setUsed(totalUsedMillicore).setPeak(totalUsedMillicore);
                });
        // Namespaces sell quota commodities
        getCommoditiesSoldBuilder(namespace, VCPU_LIMIT_REQUEST_QUOTA)
                .forEach(nsCommodityBuilder -> {
                    final CommodityType commodityType = nsCommodityBuilder.getCommodityType();
                    @Nonnull final Double totalUsedMillicore = totalUsedMillicores.get(commodityType.getNumber());
                    nsCommodityBuilder.setUsed(totalUsedMillicore).setPeak(totalUsedMillicore);
                    // Capacity: convert from MHz if needed (i.e., when quota is defined for namespace)
                    convertCapacity(nsCommodityBuilder, cpuSpeed);
                    // Update the capacity of this commodity for all pods in the namespace
                    nsPods.forEach(pod -> getCommoditiesSoldBuilder(pod, ImmutableSet.of(commodityType.getNumber()))
                            .findAny()
                            .ifPresent(podCommodityBuilder -> podCommodityBuilder
                                    .setCapacity(nsCommodityBuilder.getCapacity())));
                });
    }

    /**
     * Convert CPU related commodities of a Cluster.
     * By the time this function is called, all VMs in the cluster have been successfully converted.
     * For VCPURequest on VM, it is in millicores
     * For VCPU on VM, it is still in MHz
     *
     * @param cluster the cluster to convert
     * @param cpuSpeedMap the CPU speed map
     */
    private void convertCluster(@Nonnull StitchingEntity cluster,
                                @Nonnull Map<StitchingEntity, Double> cpuSpeedMap) {
        // Cluster sells VCPU and VCPURequest commodities
        VCPU_AND_VCPUREQUEST.forEach(commodityType -> {
            // Aggregate the used and capacity from all VMs
            AtomicReference<Double> usedMillicore = new AtomicReference<>(0d);
            AtomicReference<Double> capMillicore = new AtomicReference<>(0d);
            cpuSpeedMap.forEach(
                    (vm, cpuSpeed) -> getCommoditiesSoldBuilder(vm, ImmutableSet.of(commodityType))
                            .findAny()
                            .ifPresent(builder -> {
                                if (CommodityType.VCPU.getNumber() == commodityType) {
                                    usedMillicore.updateAndGet(v -> v + builder.getUsed() / cpuSpeed);
                                    capMillicore.updateAndGet(v -> v + builder.getCapacity() / cpuSpeed);
                                } else {
                                    usedMillicore.updateAndGet(v -> v + builder.getUsed());
                                    capMillicore.updateAndGet(v -> v + builder.getCapacity());
                                }
                            }));
            // Update the used and capacity to millicore for Cluster
            getCommoditiesSoldBuilder(cluster, ImmutableSet.of(commodityType)).findAny()
                    .ifPresent(builder -> builder
                            .setCapacity(capMillicore.get())
                            .setUsed(usedMillicore.get())
                            .setPeak(usedMillicore.get()));
        });
    }

    /**
     * A helper function to sum up of the used value of a certain commodity for a set of entities.
     *
     * @param entities the entities to sum
     * @param commodityType the type of commodity
     * @return the total used value
     */
    private double getTotalUsed(@Nonnull Set<StitchingEntity> entities,
                                @Nonnull Integer commodityType) {
        return entities.stream()
                .flatMap(entity -> getCommoditiesSoldBuilder(entity, ImmutableSet.of(commodityType)))
                .mapToDouble(CommodityDTO.Builder::getUsed)
                .sum();
    }

    /**
     * A helper function to convert MHz to Millicore.
     *
     * @param builder the commodity DTO builder
     * @param cpuSpeed the CPU speed used for the conversion
     */
    private void convertAll(@Nonnull CommodityDTO.Builder builder,
                            double cpuSpeed) {
        convertCapacity(builder, cpuSpeed);
        convertUsed(builder, cpuSpeed);
        convertPeak(builder, cpuSpeed);
    }

    /**
     * A helper function to convert Capacity from MHz to Millicore.
     * If the capacity is larger than SDKConstants.INFINITE_CAPACITY, do not convert.
     *
     * @param builder the commodity DTO builder
     * @param cpuSpeed the CPU speed used for the conversion
     */
    private void convertCapacity(@Nonnull CommodityDTO.Builder builder,
                                 double cpuSpeed) {
        if (builder.hasCapacity()) {
            builder.setCapacity(builder.getCapacity() >= SDKConstants.INFINITE_CAPACITY ? builder.getCapacity()
                                            : builder.getCapacity() / cpuSpeed);
        }
    }

    /**
     * A helper function to convert Used from MHz to Millicore.
     * It is possible for used to be larger than capacity (e.g., VCPURequest).
     *
     * @param builder the commodity DTO builder
     * @param cpuSpeed the CPU speed used for the conversion
     */
    private void convertUsed(@Nonnull CommodityDTO.Builder builder,
                             double cpuSpeed) {
        if (builder.hasUsed()) {
            double used = builder.getUsed() / cpuSpeed;
            builder.setUsed(used);
        }
    }

    /**
     * A helper function to convert peak from MHz to Millicore.
     * Make sure peak is not smaller than used.
     * It is possible for peak to be larger than capacity (e.g., VCPURequest).
     *
     * @param builder the commodity DTO builder
     * @param cpuSpeed the CPU speed used for the conversion
     */
    private void convertPeak(@Nonnull CommodityDTO.Builder builder,
                             double cpuSpeed) {
        if (builder.hasPeak()) {
            double peak = builder.getPeak() / cpuSpeed;
            // Make sure peak is not smaller than used.
            if (peak < builder.getUsed()) {
                peak = builder.getUsed();
            }
            builder.setPeak(peak);
        }
    }

    /**
     * Compute the CPU speed of a VM in units of MHz/millicore.
     *
     * @param vm the VM to compute the CPU speed
     * @return the CPU speed of the VM
     */
    private Optional<Double> getVMCPUSpeed(@Nonnull StitchingEntity vm) {
        if (!vm.getEntityBuilder().hasVirtualMachineData()) {
            return Optional.empty();
        }
        return getCommoditiesSoldBuilder(vm, VCPU)
                .findAny()
                .map(Builder::getCapacity)
                .map(capacity -> capacity / vm.getEntityBuilder().getVirtualMachineData().getNumCpus())
                .filter(Double::isFinite)
                .filter(cpuSpeed -> cpuSpeed > 0)
                .map(cpuSpeed -> cpuSpeed / 1000);
    }

    /**
     * Get the average node CPU speed for a namespace in units of MHz/millicore.
     *
     * @param namespace the namespace to get the average node CPU speed
     * @return the average node CPU speed
     */
    private Optional<Double> getNamespaceCPUSpeed(@Nonnull StitchingEntity namespace) {
        final Optional<Double> namespaceCPUSpeed =
                Optional.of(namespace.getEntityBuilder().getNamespaceData().getAverageNodeCpuFrequency())
                        .filter(cpuSpeed -> cpuSpeed > 0)
                        .map(cpuSpeed -> cpuSpeed / 1000);
        if (!namespaceCPUSpeed.isPresent()) {
            logger.error("Failed to get valid CPU speed for namespace {}.", namespace.getDisplayName());
        }
        return namespaceCPUSpeed;
    }

    /**
     * A helper function to get commodity sold builders of a set of commodity types for an entity.
     *
     * @param entity the entity
     * @param commodityTypes the set of commodity types
     * @return the commodity sold builder
     */
    private Stream<CommodityDTO.Builder> getCommoditiesSoldBuilder(@Nonnull StitchingEntity entity,
                                                                   @Nonnull Set<Integer> commodityTypes) {
        return entity.getCommoditiesSold()
                .filter(soldBuilder -> commodityTypes.contains(soldBuilder.getCommodityType().getNumber()));
    }

    /**
     * A helper function to get commodity bought builders of as set of commodity types for an entity.
     *
     * @param entity the entity
     * @param commodityTypes the set of commodity types
     * @return the commodity bought builder
     */
    private Stream<CommodityDTO.Builder> getCommoditiesBoughtBuilder(@Nonnull StitchingEntity entity,
                                                                     @Nonnull Set<Integer> commodityTypes) {
        return entity.getCommodityBoughtListByProvider().values().stream()
                .flatMap(List::stream)
                .map(CommoditiesBought::getBoughtList)
                .flatMap(List::stream)
                .filter(boughtBuilder -> commodityTypes.contains(boughtBuilder.getCommodityType().getNumber()));
    }
}
