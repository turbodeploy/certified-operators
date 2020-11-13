package com.vmturbo.action.orchestrator.translation.batch.translator;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory.EntitiesAndSettingsSnapshot;
import com.vmturbo.action.orchestrator.topology.ActionGraphEntity;
import com.vmturbo.action.orchestrator.topology.ActionTopologyStore;
import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityAttribute;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity.ActionEntityTypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity.ActionEntityTypeSpecificInfo.ActionPhysicalMachineInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity.ActionEntityTypeSpecificInfo.TypeCase;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.Type;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.proactivesupport.DataMetricCounter;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.graph.util.BaseGraphEntity;
import com.vmturbo.topology.graph.util.BaseTopology;

/**
 * This class translates vCPU resize actions from MHz to number of vCPUs.
 */
public class VCpuResizeBatchTranslator implements BatchTranslator {

    private static final Logger logger = LogManager.getLogger();

    /**
     * A client for making remote calls to the Repository service to retrieve entity data.
     */
    private final RepositoryServiceBlockingStub repoService;

    /**
     * Store of minimal topology information. Used to lookup entity information during translation when
     * available. Currently only realtime topology information is available.
     */
    private final ActionTopologyStore actionTopologyStore;

    /**
     * Entity types that support CPU resize batch translation.
     */
    private static final Set<Integer> ENTITY_TYPES = ImmutableSet.of(EntityType.VIRTUAL_MACHINE_VALUE,
        EntityType.CONTAINER_VALUE);

    /**
     * CPU commodity types that support translation from MHz to number of CPUs.
     */
    private static final Set<Integer> CPU_COMMODITY_TYPES = ImmutableSet.of(CommodityType.VCPU_VALUE,
        CommodityType.VCPU_REQUEST_VALUE);

    /**
     * Constructs new instance.
     *
     * @param repoService Repository service.
     * @param actionTopologyStore Store of minimal topology information. We will lookup entities for translation
     *                            here when possible, and when not, fall back to looking up the information from
     *                            the repository.
     */
    public VCpuResizeBatchTranslator(final RepositoryServiceBlockingStub repoService,
                                     @Nonnull final ActionTopologyStore actionTopologyStore) {
        this.repoService = repoService;
        this.actionTopologyStore = Objects.requireNonNull(actionTopologyStore);
    }

    /**
     * Checks whether {@code VCpuResizeBatchTranslator} should be applied to the given action.
     * Implementation returns {@code true} for any VM Resize action if resized commodity is vCPU.
     *
     * @param actionView Action to check.
     * @return  True if {@code VCpuResizeBatchTranslator} should be applied.
     */
    @Override
    public boolean appliesTo(@Nonnull final ActionView actionView) {
        final ActionInfo actionInfo = actionView.getRecommendation().getInfo();
        return actionInfo.hasResize()
            && ENTITY_TYPES.contains(actionInfo.getResize().getTarget().getType())
            && CPU_COMMODITY_TYPES.contains(actionInfo.getResize().getCommodityType().getType());
    }

    /**
     * Translate vCPU resize actions.
     * vCPU resizes are translated from MHz to number of vCPUs.
     *
     * @param resizeActions The actions to be translated.
     * @param snapshot A snapshot of all the entities and settings involved in the actions
     *
     * @return A stream of translated vCPU actions.
     */
    @Override
    public <T extends ActionView> Stream<T> translate(@Nonnull final List<T> resizeActions,
                                                      @Nonnull final EntitiesAndSettingsSnapshot snapshot) {
        final Map<Long, List<T>> resizeActionsByEntityTargetId = resizeActions.stream()
            .collect(Collectors.groupingBy(action ->
                action.getRecommendation().getInfo().getResize().getTarget().getId()));
        Map<Long, Long> targetIdToProviderId = Maps.newHashMap();
        Set<Long> hostsToRetrieve = Sets.newHashSet();
        Set<ActionGraphEntity> vmsToRetrieve = Sets.newHashSet();

        Optional<TopologyGraph<ActionGraphEntity>> topologyGraph =
            actionTopologyStore.getSourceTopology()
            .filter(topo -> topo.topologyInfo().getTopologyContextId() == snapshot.getTopologyContextId())
            .map(BaseTopology::entityGraph);

        for (long targetId : resizeActionsByEntityTargetId.keySet()) {
            Optional<ActionPartialEntity> targetEntity = snapshot.getEntityFromOid(targetId);
            targetEntity.ifPresent(entity -> {
                if (entity.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE) {
                    targetIdToProviderId.put(entity.getOid(), entity.getPrimaryProviderId());
                    hostsToRetrieve.add(entity.getPrimaryProviderId());
                } else if (entity.getEntityType() == EntityType.CONTAINER_VALUE) {
                    getVMFromContainer(topologyGraph, targetId).ifPresent(vmEntity -> {
                        targetIdToProviderId.put(targetId, vmEntity.getOid());
                        vmsToRetrieve.add(vmEntity);
                    });
                }
            });
        }

        final Map<Long, Double> hostCPUCoreMhzMap = topologyGraph.isPresent()
            ? getHostCPUCoreMhzMapFromTopology(topologyGraph.get(), hostsToRetrieve)
            : getHostCPUCoreMhzMapFromRepo(snapshot, hostsToRetrieve);

        final Map<Long, Double> vmToCPUMillicoreMhzMap = getVMCPUMillicoreMhzMapFromTopology(vmsToRetrieve);

        final Map<Long, Double> entityToCPUSpeedMap = Maps.newHashMap();
        entityToCPUSpeedMap.putAll(hostCPUCoreMhzMap);
        entityToCPUSpeedMap.putAll(vmToCPUMillicoreMhzMap);

        return resizeActionsByEntityTargetId.entrySet().stream().flatMap(
            entry -> translateVcpuResizes(entry.getKey(), targetIdToProviderId.get(entry.getKey()),
                entry.getValue(), entityToCPUSpeedMap));
    }


    /**
     * Get VM ActionGraphEntity from the given container based on topology graph.
     *
     * @param topologyGraph A minimal topology graph for traversal.
     * @param containerId   Given container OID to retrieve CPU speed for.
     * @return Optional of VM ActionGraphEntity.
     */
    private Optional<ActionGraphEntity> getVMFromContainer(@Nonnull final Optional<TopologyGraph<ActionGraphEntity>> topologyGraph,
                                                           final long containerId) {
        if (!topologyGraph.isPresent()) {
            logger.error("Failed to apply CPU translation to container {} because topologyGraph is empty.", containerId);
            return Optional.empty();
        }
        Optional<Long> containerPod = topologyGraph.get().getProviders(containerId)
            .filter(entity -> entity.getEntityType() == EntityType.CONTAINER_POD_VALUE)
            .map(BaseGraphEntity::getOid)
            .findFirst();
        if (!containerPod.isPresent()) {
            logger.error("Failed to apply CPU translation to container {} because no provider pod.", containerId);
            return Optional.empty();
        }
        Optional<ActionGraphEntity> vm = topologyGraph.get().getProviders(containerPod.get())
            .filter(entity -> entity.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE)
            .findFirst();
        if (!vm.isPresent()) {
            logger.error("Failed to apply CPU translation to container {} because no provider VM.",
                containerId);
            return Optional.empty();
        }
        return vm;
    }

    /**
     * Lookup CPU speed in MHz/core from VMs and convert to MHz/millicore to translate container
     * VCPU/VCPURequest resize actions.
     *
     * @param vmsToRetrieve Given VMs to retrieve CPU speed in MHz/core.
     * @return Map from VM OID to CPU speed in MHz/millicore.
     */
    private Map<Long, Double>
    getVMCPUMillicoreMhzMapFromTopology(@Nonnull final Set<ActionGraphEntity> vmsToRetrieve) {
        return vmsToRetrieve.stream()
            .collect(Collectors.toMap(ActionGraphEntity::getOid, this::getVMCpuMillicoreMhz));
    }

    private double getVMCpuMillicoreMhz(@Nonnull final ActionGraphEntity vmEntity) {
        if (vmEntity.getActionEntityInfo().hasVirtualMachine()
            && vmEntity.getActionEntityInfo().getVirtualMachine().hasCpuCoreMhz()) {
            return vmEntity.getActionEntityInfo().getVirtualMachine().getCpuCoreMhz() / 1000;
        } else {
            logger.error("CPU core MHz is not found from VM {}", vmEntity.getOid());
            return 0;
        }
    }

    /**
     * Lookup CPU core MHz from host (Physical Machine) entity from repository.
     *
     * @param entityGraph The topology to use to lookup the host map.
     * @param entitiesToRetrieve The host entities to retrieve.
     * @return A map of host entities by their OID.
     */
    private Map<Long, Double>
    getHostCPUCoreMhzMapFromTopology(@Nonnull final TopologyGraph<ActionGraphEntity> entityGraph,
                                     @Nonnull final Set<Long> entitiesToRetrieve) {
        return entitiesToRetrieve.stream()
            .map(entityGraph::getEntity)
            .filter(Optional::isPresent)
            .map(e -> toHostPartialEntity(e.get()))
            .collect(Collectors.toMap(ActionPartialEntity::getOid, this::getHostCPUCoreMhz));
    }

    /**
     * Lookup host (Physical Machine) CPU speed (MHz/core) information from repository.
     *
     * @param snapshot The snapshot containing the entity information.
     * @param entitiesToRetrieve The set of entity OIDs to retrieve.
     * @return A map of host CPU core MHz by their OID.
     */
    private Map<Long, Double>
    getHostCPUCoreMhzMapFromRepo(@Nonnull final EntitiesAndSettingsSnapshot snapshot,
                                 @Nonnull final Set<Long> entitiesToRetrieve) {
        // Note: It is important to force evaluation of the gRPC stream here in order
        // to trigger any potential exceptions in this method where they can be handled
        // properly. Generating a lazy stream of gRPC results that is not evaluated until
        // after the method return causes any potential gRPC exception not to be thrown
        // until it is too late to be handled.
        return RepositoryDTOUtil.topologyEntityStream(
            repoService.retrieveTopologyEntities(
                RetrieveTopologyEntitiesRequest.newBuilder()
                    .setTopologyContextId(snapshot.getTopologyContextId())
                    .addAllEntityOids(entitiesToRetrieve)
                    .setReturnType(Type.ACTION)
                        // Look in the same topology type (source vs projected) as the one we looked
                        // in to get the rest of the entity information.
                    .setTopologyType(snapshot.getTopologyType())
                    .build()))
            .map(PartialEntity::getAction)
            .collect(Collectors.toMap(ActionPartialEntity::getOid, this::getHostCPUCoreMhz));
    }

    private double getHostCPUCoreMhz(@Nonnull final ActionPartialEntity hostEntity) {
        if (hostEntity.hasTypeSpecificInfo() && hostEntity.getTypeSpecificInfo().hasPhysicalMachine()
            && hostEntity.getTypeSpecificInfo().getPhysicalMachine().hasCpuCoreMhz()) {
            return hostEntity.getTypeSpecificInfo().getPhysicalMachine().getCpuCoreMhz();
        } else {
            logger.error("CPU core MHz is not found from host {}", hostEntity.getOid());
            return 0;
        }
    }

    /**
     * Apply CPU speed from provider to VMs and containers being resized in the actions in order to
     * translate the vCPU actions from MHz to number of vCPUs.
     *
     * @param targetId            The target id (for ex. the VM or container id)
     * @param providerId          The provider id (for ex. the host or VM id)
     * @param resizeActions       The resize actions to be translated.
     * @param entityToCPUSpeedMap Map of entity to CPU speed in MHz/core(millicore).
     * @param <T>                 Action type.
     * @return A stream of the translated resize actions.
     */
    private <T extends ActionView> Stream<T> translateVcpuResizes(final long targetId,
                                                                  @Nullable final Long providerId,
                                                                  @Nonnull List<T> resizeActions,
                                                                  @Nonnull Map<Long, Double> entityToCPUSpeedMap) {
        Double cpuSpeed = entityToCPUSpeedMap.get(providerId);
        // Set translation to failed if cpuSpeed is not found.
        if (cpuSpeed == null || cpuSpeed == 0) {
            logger.warn("CPU speed is not found from provider info for CPU resize on entity {}. Skipping translation",
                targetId);
            return resizeActions.stream()
                .peek(action -> action.getActionTranslation().setTranslationFailure());
        }
        return resizeActions.stream()
            .map(action -> {
                final Resize newResize =
                    translateVcpuResizeInfo(action.getRecommendation().getInfo().getResize(), cpuSpeed);

                // Float comparision should apply epsilon. But in this case both capacities are
                // result of Math.round and Math.ceil (see translateVcpuResizeInfo method),
                // so the values are actually integers.
                if (Float.compare(newResize.getOldCapacity(), newResize.getNewCapacity()) == 0) {
                    action.getActionTranslation().setTranslationFailure();
                    logger.debug("VCPU resize (action: {}, entity: {}) has same from and to value ({}).",
                        action.getId(), newResize.getTarget().getId(), newResize.getOldCapacity());
                    Metrics.VCPU_SAME_TO_FROM.increment();
                    return action;
                }
                // Resize explanation does not need to be translated because the explanation is in terms
                // of utilization which is normalized so translating units will not affect the values.

                action.getActionTranslation().setTranslationSuccess(
                    action.getRecommendation().toBuilder().setInfo(
                        ActionInfo.newBuilder(action.getRecommendation().getInfo())
                            .setResize(newResize).build())
                        .build());
                return action;
            });
    }

    /**
     * Apply a translation for an individual CPU resize action given its corresponding CPU speed in
     * MHz/core for VM resize actions or MHz/millicore for container resize actions.
     *
     * @param originalResize The info for the original resize action (in MHz).
     * @param cpuSpeed       CPU speed, MHz/core for VM resize actions or MHz/millicore for container
     *                       resize actions.
     * @return The translated resize information (in # of vCPU).
     */
    private Resize translateVcpuResizeInfo(@Nonnull final Resize originalResize,
                                           final double cpuSpeed) {
        // don't apply the mhz translation for limit and reserved commodity attributes
        if (originalResize.getCommodityAttribute() == CommodityAttribute.LIMIT
            || originalResize.getCommodityAttribute() == CommodityAttribute.RESERVED) {
            return originalResize;
        }
        final Resize newResize = originalResize.toBuilder()
            .setOldCapacity(Math.round(originalResize.getOldCapacity() / cpuSpeed))
            .setNewCapacity((float)Math.ceil(originalResize.getNewCapacity() / cpuSpeed))
            .build();

        logger.debug("Translated VCPU resize from {} to {} with CPU speed {} MHz/core(millicore).",
            originalResize, newResize, cpuSpeed);

        return newResize;
    }

    /**
     * Utility class with metric constants.
     */
    private static class Metrics {

        private static final DataMetricCounter VCPU_SAME_TO_FROM = DataMetricCounter.builder()
            .withName("ao_vcpu_translate_same_to_from_count")
            .withHelp("The number of VCPU translates where the to and from VCPU counts were the same.")
            .build()
            .register();

    }

    /**
     * Convert ActionGraphEntity for a host entity into an equivalent ActionPartialEntity.
     *
     * @param graphEntity The {@link ActionGraphEntity} to convert.
     * @return an equivalent ActionPartialEntity.
     */
    @VisibleForTesting
    ActionPartialEntity toHostPartialEntity(@Nonnull final ActionGraphEntity graphEntity) {
        final ActionPartialEntity.Builder builder = ActionPartialEntity.newBuilder()
            .setOid(graphEntity.getOid())
            .setEntityType(graphEntity.getEntityType())
            .setDisplayName(graphEntity.getDisplayName())
            .addAllDiscoveringTargetIds(graphEntity.getDiscoveringTargetIds().collect(Collectors.toList()));

        final ActionEntityTypeSpecificInfo entityInfo = graphEntity.getActionEntityInfo();
        if (entityInfo != null && entityInfo.getTypeCase() == TypeCase.PHYSICAL_MACHINE) {
            final ActionPhysicalMachineInfo hostInfo = entityInfo.getPhysicalMachine();
            if (hostInfo.hasCpuCoreMhz()) {
                builder.setTypeSpecificInfo(ActionEntityTypeSpecificInfo.newBuilder()
                    .setPhysicalMachine(ActionPhysicalMachineInfo.newBuilder()
                        .setCpuCoreMhz(hostInfo.getCpuCoreMhz())));
            }
        }

        return builder.build();
    }
}
