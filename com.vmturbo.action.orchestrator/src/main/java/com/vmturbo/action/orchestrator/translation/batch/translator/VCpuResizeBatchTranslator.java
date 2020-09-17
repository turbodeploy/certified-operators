package com.vmturbo.action.orchestrator.translation.batch.translator;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
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
        return actionInfo.hasResize() &&
            actionInfo.getResize().getTarget().getType() == EntityType.VIRTUAL_MACHINE_VALUE &&
            actionInfo.getResize().getCommodityType().getType() == CommodityType.VCPU_VALUE;
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
        final Map<Long, List<T>> resizeActionsByVmTargetId = resizeActions.stream()
            .collect(Collectors.groupingBy(action ->
                action.getRecommendation().getInfo().getResize().getTarget().getId()));
        Map<Long, Long> targetIdToPrimaryProviderId = Maps.newHashMap();
        Set<Long> entitiesToRetrieve = Sets.newHashSet();
        for (T action : resizeActions) {
            long targetId = action.getRecommendation().getInfo().getResize().getTarget().getId();
            Optional<ActionPartialEntity> targetEntity = snapshot.getEntityFromOid(targetId);
            targetEntity.ifPresent(entity -> {
                targetIdToPrimaryProviderId.put(entity.getOid(), entity.getPrimaryProviderId());
                entitiesToRetrieve.add(entity.getPrimaryProviderId());
            });
        }

        final Map<Long, ActionPartialEntity> hostInfoMap = actionTopologyStore.getSourceTopology()
            .filter(topo -> topo.topologyInfo().getTopologyContextId() == snapshot.getToologyContextId())
            .map(topo -> getHostInfoMapFromTopology(topo.entityGraph(), entitiesToRetrieve))
            .orElseGet(() -> getHostInfoMapFromRepo(snapshot, entitiesToRetrieve));

        return resizeActionsByVmTargetId.entrySet().stream().flatMap(
            entry -> translateVcpuResizes(
                entry.getKey(), targetIdToPrimaryProviderId.get(entry.getKey()),
                hostInfoMap, entry.getValue()));
    }

    /**
     * Lookup host (Physical Machine) entity information from repository.
     *
     * @param entityGraph The topology to use to lookup the host map.
     * @param entitiesToRetrieve The host entities to retrieve.
     * @return A map of host entities by their OID.
     */
    private Map<Long, ActionPartialEntity>
    getHostInfoMapFromTopology(@Nonnull final TopologyGraph<ActionGraphEntity> entityGraph,
                               @Nonnull final Set<Long> entitiesToRetrieve) {
        return entitiesToRetrieve.stream()
            .map(entityGraph::getEntity)
            .filter(Optional::isPresent)
            .map(e -> toHostPartialEntity(e.get()))
            .collect(Collectors.toMap(ActionPartialEntity::getOid, Function.identity()));
    }

    /**
     * Lookup host (Physical Machine) entity information from repository.
     *
     * @param snapshot The snapshot containing the entity information.
     * @param entitiesToRetrieve The set of entity OIDs to retrieve.
     * @return A map of host entities by their OID.
     */
    private Map<Long, ActionPartialEntity>
    getHostInfoMapFromRepo(@Nonnull final EntitiesAndSettingsSnapshot snapshot,
                           @Nonnull final Set<Long> entitiesToRetrieve) {
        // Note: It is important to force evaluation of the gRPC stream here in order
        // to trigger any potential exceptions in this method where they can be handled
        // properly. Generating a lazy stream of gRPC results that is not evaluated until
        // after the method return causes any potential gRPC exception not to be thrown
        // until it is too late to be handled.
        return RepositoryDTOUtil.topologyEntityStream(
            repoService.retrieveTopologyEntities(
                RetrieveTopologyEntitiesRequest.newBuilder()
                    .setTopologyContextId(snapshot.getToologyContextId())
                    .addAllEntityOids(entitiesToRetrieve)
                    .setReturnType(Type.ACTION)
                        // Look in the same topology type (source vs projected) as the one we looked
                        // in to get the rest of the entity information.
                    .setTopologyType(snapshot.getTopologyType())
                    .build()))
            .map(PartialEntity::getAction)
            .collect(Collectors.toMap(ActionPartialEntity::getOid, Function.identity()));
    }

    /**
     * Apply HostInfo about hosts of VMs hosting the VMs being resized in the actions in order
     * to translate the vCPU actions from MHz to number of vCPUs.
     *
     * @param targetId      The target id (for ex. the VM id)
     * @param providerId    The provider id (for ex. the host id)
     * @param hostInfoMap   The host info for the various resize actions.
     * @param resizeActions The resize actions to be translated.
     * @param <T>           Action type.
     * @return A stream of the translated resize actions.
     */
    private <T extends ActionView> Stream<T> translateVcpuResizes(long targetId,
                                                                  Long providerId,
                                                                  @Nonnull final Map<Long, ActionPartialEntity> hostInfoMap,
                                                                  @Nonnull List<T> resizeActions) {
        ActionPartialEntity hostInfo = hostInfoMap.get(providerId);
        if (providerId == null || hostInfo == null || !hostInfo.hasTypeSpecificInfo()
            || !hostInfo.getTypeSpecificInfo().hasPhysicalMachine()
            || !hostInfo.getTypeSpecificInfo().getPhysicalMachine().hasCpuCoreMhz()) {
            logger.warn("Host info not found for VCPU resize on entity {}. Skipping translation",
                targetId);
            // No host info found, fail the translation and return the originals.
            return resizeActions.stream()
                .map(action -> {
                    action.getActionTranslation().setTranslationFailure();
                    return action;
                });
        }
        return resizeActions.stream()
            .map(action -> {
                final Resize newResize =
                    translateVcpuResizeInfo(action.getRecommendation().getInfo().getResize(), hostInfo);

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
     * Apply a translation for an individual vCPU resize action given its corresponding host info.
     *
     * @param originalResize The info for the original resize action (in MHz).
     * @param hostInfo The host info for the host of the VM being resized.
     * @return The translated resize information (in # of vCPU).
     */
    private Resize translateVcpuResizeInfo(@Nonnull final Resize originalResize,
                                           @Nonnull final ActionPartialEntity hostInfo) {
        // don't apply the mhz translation for limit and reserved commodity attributes
        if (originalResize.getCommodityAttribute() == CommodityAttribute.LIMIT
            || originalResize.getCommodityAttribute() == CommodityAttribute.RESERVED) {
            return originalResize;
        }
        int cpuCoreMhz = hostInfo.getTypeSpecificInfo().getPhysicalMachine().getCpuCoreMhz();
        final Resize newResize = originalResize.toBuilder()
            .setOldCapacity(Math.round(originalResize.getOldCapacity() / cpuCoreMhz))
            .setNewCapacity((float)Math.ceil(originalResize.getNewCapacity() / cpuCoreMhz))
            .build();

        logger.debug("Translated VCPU resize from {} to {} for host with info {}",
            originalResize, newResize, hostInfo);

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
