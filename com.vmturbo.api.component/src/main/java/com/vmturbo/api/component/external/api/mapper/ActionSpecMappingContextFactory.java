package com.vmturbo.api.component.external.api.mapper;

import java.util.AbstractMap.SimpleEntry;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;

import com.vmturbo.api.component.external.api.mapper.aspect.CloudAspectMapper;
import com.vmturbo.api.component.external.api.mapper.aspect.VirtualMachineAspectMapper;
import com.vmturbo.api.component.external.api.mapper.aspect.VirtualVolumeAspectMapper;
import com.vmturbo.api.dto.action.ActionApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.api.dto.entityaspect.VirtualDiskApiDTO;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatFilterApiDTO;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo.ActionTypeCase;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.group.PolicyDTO;
import com.vmturbo.common.protobuf.group.PolicyServiceGrpc.PolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.search.Search.SearchPlanTopologyEntityDTOsRequest;
import com.vmturbo.common.protobuf.search.Search.SearchTopologyEntityDTOsRequest;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.components.common.ClassicEnumMapper.CommodityTypeUnits;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Map an ActionSpec returned from the ActionOrchestrator into an {@link ActionApiDTO} to be
 * returned from the API.
 */
public class ActionSpecMappingContextFactory {

    private final PolicyServiceBlockingStub policyService;

    private final ExecutorService executorService;

    private final SearchServiceBlockingStub searchServiceRpc;

    private final CloudAspectMapper cloudAspectMapper;

    private final VirtualMachineAspectMapper vmAspectMapper;

    private final VirtualVolumeAspectMapper volumeAspectMapper;

    private final long realtimeTopologyContextId;

    public ActionSpecMappingContextFactory(@Nonnull PolicyServiceBlockingStub policyService,
                                           @Nonnull ExecutorService executorService,
                                           @Nonnull SearchServiceBlockingStub searchServiceRpc,
                                           @Nonnull CloudAspectMapper cloudAspectMapper,
                                           @Nonnull VirtualMachineAspectMapper vmAspectMapper,
                                           @Nonnull VirtualVolumeAspectMapper volumeAspectMapper,
                                           final long realtimeTopologyContextId) {
        this.policyService = Objects.requireNonNull(policyService);
        this.executorService = Objects.requireNonNull(executorService);
        this.searchServiceRpc = Objects.requireNonNull(searchServiceRpc);
        this.cloudAspectMapper = Objects.requireNonNull(cloudAspectMapper);
        this.vmAspectMapper = Objects.requireNonNull(vmAspectMapper);
        this.volumeAspectMapper = Objects.requireNonNull(volumeAspectMapper);
        this.realtimeTopologyContextId = realtimeTopologyContextId;
    }

    /**
     * Create ActionSpecMappingContext for provided actions, which contains information for mapping
     * {@link ActionSpec} to {@link ActionApiDTO}.
     *
     * @param actions list of actions
     * @param topologyContextId the context id of the topology
     * @return ActionSpecMappingContext
     * @throws UnsupportedActionException
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public ActionSpecMappingContext createActionSpecMappingContext(@Nonnull List<Action> actions,
                                                                   long topologyContextId)
                throws UnsupportedActionException, ExecutionException, InterruptedException {
        final Set<Long> involvedEntities = ActionDTOUtil.getInvolvedEntityIds(actions);
        final Future<Map<Long, PolicyDTO.Policy>> policies = executorService.submit(this::getPolicies);
        final Future<List<TopologyEntityDTO>> entities = executorService
            .submit(() -> fetchTopologyEntityDTOs(topologyContextId, involvedEntities));
        List<TopologyEntityDTO> topologyEntityDTOs = entities.get();

        if (topologyContextId == realtimeTopologyContextId) {
            final Map<Long, TopologyEntityDTO> entitiesById = topologyEntityDTOs.stream()
                .collect(Collectors.toMap(TopologyEntityDTO::getOid, Function.identity()));
            return new ActionSpecMappingContext(entitiesById, policies.get(), Collections.emptyMap(),
                Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());
        }

        // fetch more info for plan actions
        // fetch related regions and create a map from zone id to region
        final List<TopologyEntityDTO> regions = fetchTopologyEntityDTOs(topologyContextId,
            collectRegionIds(topologyEntityDTOs));
        Map<Long, TopologyEntityDTO> zoneIdToRegion = regions.stream()
            .flatMap(region -> region.getConnectedEntityListList().stream()
                .filter(c -> c.getConnectedEntityType() == EntityType.AVAILABILITY_ZONE_VALUE)
                .map(zone -> new SimpleEntry<>(zone.getConnectedEntityId(), region)))
            .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

        final Map<Long, TopologyEntityDTO> entitiesById = Stream.of(topologyEntityDTOs, regions)
            .flatMap(Collection::stream)
            .collect(Collectors.toMap(TopologyEntityDTO::getOid, Function.identity()));

        // fetch all volume aspects together first rather than fetch one by one to improve performance
        Map<Long, List<VirtualDiskApiDTO>> volumesAspectsByVM = fetchVolumeAspects(actions, topologyContextId);
        // fetch cloud aspects and vm aspects
        final Map<Long, EntityAspect> cloudAspects = new HashMap<>();
        final Map<Long, EntityAspect> vmAspects = new HashMap<>();
        for (TopologyEntityDTO topologyEntityDTO : topologyEntityDTOs) {
            cloudAspects.put(topologyEntityDTO.getOid(), cloudAspectMapper.mapEntityToAspect(topologyEntityDTO));
            if (topologyEntityDTO.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE) {
                vmAspects.put(topologyEntityDTO.getOid(), vmAspectMapper.mapEntityToAspect(topologyEntityDTO));
            }
        }
        return new ActionSpecMappingContext(entitiesById, policies.get(), zoneIdToRegion,
            volumesAspectsByVM, cloudAspects, vmAspects);
    }

    @Nonnull
    private Map<Long, PolicyDTO.Policy> getPolicies() {
        final Map<Long, PolicyDTO.Policy> policies = new HashMap<>();
        policyService.getAllPolicies(PolicyDTO.PolicyRequest.newBuilder().build()).forEachRemaining(
                        response -> policies
                                        .put(response.getPolicy().getId(), response.getPolicy()));
        return policies;
    }

    /**
     * Find ids of related regions from given list of TopologyEntityDTO
     */
    private Set<Long> collectRegionIds(@Nonnull List<TopologyEntityDTO> topologyEntityDTOs) {
        // find connected regions and put into same map together with other entities
        return topologyEntityDTOs.stream()
            .flatMap(entity -> entity.getConnectedEntityListList().stream())
            .filter(connectedEntity -> connectedEntity.getConnectedEntityType() == EntityType.REGION_VALUE)
            .map(ConnectedEntity::getConnectedEntityId)
            .collect(Collectors.toSet());

    }

    /**
     * Fetch the TopologyEntityDTOs for the given entities ids from the given topology context id.
     * We always search the projected topology because the projected topology is
     * a super-set of the source topology. All involved entities that are in
     * the source topology will also be in the projected topology, but there will
     * be entities that are ONLY in the projected topology (e.g. actions involving
     * newly provisioned hosts/VMs).
     *
     * @param topologyContextId id of the topology context to fetch entities from
     * @param involvedEntities ids of entities to fetch
     * @return list of TopologyEntityDTOs
     */
    private List<TopologyEntityDTO> fetchTopologyEntityDTOs(long topologyContextId,
                                                            @Nonnull Set<Long> involvedEntities) {
        if (involvedEntities.isEmpty()) {
            return Collections.emptyList();
        }

        if (topologyContextId != realtimeTopologyContextId) {
            SearchPlanTopologyEntityDTOsRequest request = SearchPlanTopologyEntityDTOsRequest.newBuilder()
                .setTopologyContextId(topologyContextId)
                .addAllEntityOid(involvedEntities)
                .build();
            return searchServiceRpc.searchPlanTopologyEntityDTOs(request).getTopologyEntityDtosList();
        } else {
            SearchTopologyEntityDTOsRequest request = SearchTopologyEntityDTOsRequest.newBuilder()
                .addAllEntityOid(involvedEntities)
                .build();
            return searchServiceRpc.searchTopologyEntityDTOs(request).getTopologyEntityDtosList();
        }
    }

    /**
     * Fetch the volume aspects needed for given actions, and make sure the stats have both
     * beforePlan and afterPlan StorageAmount.
     */
    private Map<Long, List<VirtualDiskApiDTO>> fetchVolumeAspects(@Nonnull List<Action> actions,
                                                                  long topologyContextId) {
        Set<Long> involvedVmIds = getVMIdsToFetchVolumeAspects(actions);
        Map<Long, List<VirtualDiskApiDTO>> volumesAspectsByVM = volumeAspectMapper.mapVirtualMachines(
            involvedVmIds, topologyContextId);

        // add "beforePlan" filter to existing StorageAmount and add a new StorageAmount for after plan
        volumesAspectsByVM.values().forEach(virtualDisks ->
            virtualDisks.forEach(virtualDisk -> {
                StatApiDTO newStorageAmount = new StatApiDTO();
                for (StatApiDTO stat : virtualDisk.getStats()) {
                    if (CommodityTypeUnits.STORAGE_AMOUNT.getMixedCase().equals(stat.getName())) {
                        // beforePlan filter so ui will show this number in the correct column
                        StatFilterApiDTO beforePlanFilter = new StatFilterApiDTO();
                        beforePlanFilter.setValue(StringConstants.BEFORE_PLAN);
                        beforePlanFilter.setType(StringConstants.RESULTS_TYPE);
                        stat.getFilters().add(beforePlanFilter);
                        // todo: calculate new capacity for volume on new tier dynamically
                        // based on old volume capacity and new tier constraints, or this
                        // should be calculated from market side and returned in action?
                        newStorageAmount.setName(stat.getName());
                        newStorageAmount.setCapacity(stat.getCapacity());
                        break;
                    }
                }
                virtualDisk.getStats().add(newStorageAmount);
            })
        );
        return volumesAspectsByVM;
    }

    /**
     * Get the oids of the VMs which we need to fetch volume aspects for and set to the ActionApiDTO later.
     */
    private Set<Long> getVMIdsToFetchVolumeAspects(@Nonnull final List<Action> actions) {
        return actions.stream()
            .map(Action::getInfo)
            .filter(actionInfo -> actionInfo.getActionTypeCase() == ActionTypeCase.MOVE)
            .map(ActionInfo::getMove)
            .filter(move -> move.getChangesList().stream().anyMatch(ChangeProvider::hasResource))
            .map(Move::getTarget)
            .filter(actionEntity -> actionEntity.getEnvironmentType() == EnvironmentType.CLOUD
                && actionEntity.getType() == EntityType.VIRTUAL_MACHINE_VALUE)
            .map(ActionEntity::getId)
            .collect(Collectors.toSet());
    }

    /**
     * The context of a mapping operation from {@link ActionSpec} to a {@link ActionApiDTO}.
     *
     * <p>Caches information stored from calls to other components to allow a single set of
     * remote calls to obtain all the information required to map a set of {@link ActionSpec}s.</p>
     */
    @VisibleForTesting
    static class ActionSpecMappingContext {

        private final Map<Long, ServiceEntityApiDTO> serviceEntityApiDTOs;

        private final Map<Long, TopologyEntityDTO> topologyEntityDTOs;

        private final Map<Long, PolicyDTO.Policy> policies;

        private final Map<Long, TopologyEntityDTO> zoneIdToRegion;

        private final Map<Long, List<VirtualDiskApiDTO>> volumeAspectsByVM;

        private final Map<Long, EntityAspect> cloudAspects;

        private final Map<Long, EntityAspect> vmAspects;

        ActionSpecMappingContext(@Nonnull Map<Long, TopologyEntityDTO> topologyEntityDTOs,
                                 @Nonnull Map<Long, PolicyDTO.Policy> policies,
                                 @Nonnull Map<Long, TopologyEntityDTO> zoneIdToRegion,
                                 @Nonnull Map<Long, List<VirtualDiskApiDTO>> volumeAspectsByVM,
                                 @Nonnull Map<Long, EntityAspect> cloudAspects,
                                 @Nonnull Map<Long, EntityAspect> vmAspects) {
            this.topologyEntityDTOs = topologyEntityDTOs;
            this.serviceEntityApiDTOs = topologyEntityDTOs.entrySet().stream()
                .collect(Collectors.toMap(Entry::getKey, entry ->
                    ServiceEntityMapper.toServiceEntityApiDTO(entry.getValue(), null)));
            this.policies = Objects.requireNonNull(policies);
            this.zoneIdToRegion = Objects.requireNonNull(zoneIdToRegion);
            this.volumeAspectsByVM = Objects.requireNonNull(volumeAspectsByVM);
            this.cloudAspects = Objects.requireNonNull(cloudAspects);
            this.vmAspects = Objects.requireNonNull(vmAspects);
        }

        PolicyDTO.Policy getPolicy(long id) {
            return policies.get(id);
        }

        @Nonnull
        ServiceEntityApiDTO getEntity(final long oid) throws UnknownObjectException {
            return getOptionalEntity(oid).orElseThrow(
                () -> new UnknownObjectException("Entity: " + oid + " not found."));
        }

        Optional<ServiceEntityApiDTO> getOptionalEntity(final long oid) {
            final ServiceEntityApiDTO entity = serviceEntityApiDTOs.get(oid);
            return entity == null ? Optional.empty() : Optional.of(entity);
        }

        TopologyEntityDTO getTopologyEntityDTO(final long oid) throws UnknownObjectException {
            final TopologyEntityDTO entity = topologyEntityDTOs.get(oid);
            if (entity == null) {
                throw new UnknownObjectException("Entity: " + oid + " not found.");
            }
            return entity;
        }

        TopologyEntityDTO getRegionForVM(@Nonnull Long entityOid) throws UnknownObjectException {
            TopologyEntityDTO entityDTO = getTopologyEntityDTO(entityOid);
            Long zoneId = null;
            for (ConnectedEntity c : entityDTO.getConnectedEntityListList()) {
                if (c.getConnectedEntityType() == EntityType.REGION_VALUE) {
                    // it means azure
                    return getTopologyEntityDTO(c.getConnectedEntityId());
                } else if (c.getConnectedEntityType() == EntityType.AVAILABILITY_ZONE_VALUE) {
                    zoneId = c.getConnectedEntityId();
                }
            }
            // it means aws, find region based on zone
            return zoneIdToRegion.get(zoneId);
        }

        List<VirtualDiskApiDTO> getVolumeAspects(@Nonnull Long vmId) {
            return volumeAspectsByVM.get(vmId);
        }

        Optional<EntityAspect> getCloudAspect(@Nonnull Long entityId) {
            return Optional.ofNullable(cloudAspects.get(entityId));
        }

        Optional<EntityAspect> getVMAspect(@Nonnull Long entityId) {
            return Optional.ofNullable(vmAspects.get(entityId));
        }
    }
}
