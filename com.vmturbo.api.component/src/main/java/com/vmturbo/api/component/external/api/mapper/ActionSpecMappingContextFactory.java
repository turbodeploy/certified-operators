package com.vmturbo.api.component.external.api.mapper;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.mapper.aspect.EntityAspectMapper;
import com.vmturbo.api.component.external.api.mapper.aspect.VirtualVolumeAspectMapper;
import com.vmturbo.api.dto.action.ActionApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.api.dto.entityaspect.VirtualDiskApiDTO;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatFilterApiDTO;
import com.vmturbo.api.enums.AspectName;
import com.vmturbo.api.exceptions.ConversionException;
import com.vmturbo.auth.api.Pair;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo.ActionTypeCase;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.BuyRI;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.cost.BuyReservedInstanceServiceGrpc.BuyReservedInstanceServiceBlockingStub;
import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.Cost.GetBuyReservedInstancesByFilterRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceSpecByIdsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceSpecByIdsResponse;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.ReservedInstanceSpecServiceGrpc.ReservedInstanceSpecServiceBlockingStub;
import com.vmturbo.common.protobuf.group.PolicyDTO;
import com.vmturbo.common.protobuf.group.PolicyServiceGrpc.PolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetMultiSupplyChainsRequest;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainScope;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainSeed;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity.RelatedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.components.common.ClassicEnumMapper.CommodityTypeUnits;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Map an ActionSpec returned from the ActionOrchestrator into an {@link ActionApiDTO} to be
 * returned from the API.
 */
public class ActionSpecMappingContextFactory {

    private final PolicyServiceBlockingStub policyService;

    private final ExecutorService executorService;

    private final RepositoryApi repositoryApi;

    private final EntityAspectMapper entityAspectMapper;

    private final VirtualVolumeAspectMapper volumeAspectMapper;

    private final long realtimeTopologyContextId;

    private final BuyReservedInstanceServiceBlockingStub buyRIServiceClient;

    private final ReservedInstanceSpecServiceBlockingStub riSpecServiceClient;

    private final ServiceEntityMapper serviceEntityMapper;

    private final SupplyChainServiceBlockingStub supplyChainServiceClient;

    public ActionSpecMappingContextFactory(@Nonnull PolicyServiceBlockingStub policyService,
                                           @Nonnull ExecutorService executorService,
                                           @Nonnull RepositoryApi repositoryApi,
                                           @Nonnull EntityAspectMapper entityAspectMapper,
                                           @Nonnull VirtualVolumeAspectMapper volumeAspectMapper,
                                           final long realtimeTopologyContextId,
                                           @Nonnull BuyReservedInstanceServiceBlockingStub buyRIServiceClient,
                                           @Nonnull ReservedInstanceSpecServiceBlockingStub riSpecServiceClient,
                                           @Nonnull ServiceEntityMapper serviceEntityMapper,
                                           @Nonnull SupplyChainServiceBlockingStub supplyChainServiceClient) {
        this.policyService = Objects.requireNonNull(policyService);
        this.executorService = Objects.requireNonNull(executorService);
        this.repositoryApi = Objects.requireNonNull(repositoryApi);
        this.entityAspectMapper = Objects.requireNonNull(entityAspectMapper);
        this.volumeAspectMapper = Objects.requireNonNull(volumeAspectMapper);
        this.realtimeTopologyContextId = realtimeTopologyContextId;
        this.buyRIServiceClient = buyRIServiceClient;
        this.riSpecServiceClient = riSpecServiceClient;
        this.serviceEntityMapper = Objects.requireNonNull(serviceEntityMapper);
        this.supplyChainServiceClient = Objects.requireNonNull(supplyChainServiceClient);
    }

    /**
     * Returns a mapping of buy RI id to a pair of RI Bought and RI Spec.
     * @param buyRIActions The Buy RI actions we need to generate the mapping for.
     * @return mapping of buy RI id to a pair of RI Bought and RI Spec.
     */
    @VisibleForTesting
    public Map<Long, Pair<ReservedInstanceBought, ReservedInstanceSpec>>
                                                        getBuyRIIdToRIBoughtandRISpec(List<BuyRI> buyRIActions) {
        Map<Long, Pair<ReservedInstanceBought, ReservedInstanceSpec>> buyRIIdToRIBoughtandRISpec =
                                                                                    new HashMap<>();

        if (!buyRIActions.isEmpty()) {
            final List<Long> buyRIIds = buyRIActions.stream().map(BuyRI::getBuyRiId)
                    .collect(Collectors.toList());

            final Cost.GetBuyReservedInstancesByFilterResponse buyRIBoughtResponse =
                buyRIServiceClient.getBuyReservedInstancesByFilter(GetBuyReservedInstancesByFilterRequest
                    .newBuilder().addAllBuyRiId(buyRIIds).build());

            final List<Long> riSpecs = buyRIBoughtResponse.getReservedInstanceBoughtsList().stream()
                .map(r -> r.getReservedInstanceBoughtInfo().getReservedInstanceSpec())
                .collect(Collectors.toList());

            final GetReservedInstanceSpecByIdsResponse riSpecResponse = riSpecServiceClient
                .getReservedInstanceSpecByIds(GetReservedInstanceSpecByIdsRequest.newBuilder()
                    .addAllReservedInstanceSpecIds(riSpecs)
                    .build());

            final Map<Long, ReservedInstanceBought> buyRIIdToRIBought = buyRIBoughtResponse
                .getReservedInstanceBoughtsList().stream()
                .collect(Collectors.toMap(ReservedInstanceBought::getId, a -> a));

            final Map<Long, ReservedInstanceSpec> specIdToRISpec = riSpecResponse
                .getReservedInstanceSpecList().stream()
                .collect(Collectors.toMap(ReservedInstanceSpec::getId, a -> a));

            for (Entry<Long, ReservedInstanceBought> entry : buyRIIdToRIBought.entrySet()) {
                Long buyRIId = entry.getKey();
                ReservedInstanceBought riBought = entry.getValue();
                ReservedInstanceSpec riSpec = specIdToRISpec.get(riBought.getReservedInstanceBoughtInfo()
                                                .getReservedInstanceSpec());
                    Pair<ReservedInstanceBought, ReservedInstanceSpec> pair
                            = new Pair<>(riBought, riSpec);
                    buyRIIdToRIBoughtandRISpec.put(buyRIId, pair);
            }
        }
        return  buyRIIdToRIBoughtandRISpec;
    }

    /**
     * Create ActionSpecMappingContext for provided actions, which contains information for mapping
     * {@link ActionSpec} to {@link ActionApiDTO}.
     *
     * @param actions list of actions
     * @param topologyContextId the context id of the topology
     * @return ActionSpecMappingContext
     * @throws ExecutionException on failure getting entities
     * @throws InterruptedException if thread has been interrupted
     * @throws ConversionException if errors faced during converting data to API DTOs
     */
    public ActionSpecMappingContext createActionSpecMappingContext(@Nonnull List<Action> actions,
            long topologyContextId)
            throws ExecutionException, InterruptedException, ConversionException {

        final Future<Map<Long, PolicyDTO.Policy>> policies = executorService.submit(this::getPolicies);
        final Future<Map<Long, ApiPartialEntity>> entities = executorService.submit(() ->
            getEntities(actions, topologyContextId));
        Map<Long, ApiPartialEntity> entitiesById = entities.get();

        Map<Long, Pair<ReservedInstanceBought, ReservedInstanceSpec>> buyRIIdToRIBoughtandRISpec  =
                                        getBuyRIIdToRIBoughtandRISpec(actions.stream()
                                        .filter(a -> a.getInfo().hasBuyRi())
                                        .map(a -> a.getInfo().getBuyRi())
                                        .collect(Collectors.toList()));

        final Map<Long, ApiPartialEntity> datacenterById =
            getDatacentersByEntity(entitiesById.keySet(), topologyContextId);

        // Fetch related regions and create maps from region IDs to regions and
        // from availability zone IDs to regions.
        final Map<Long, ApiPartialEntity> entityIdToRegion = new HashMap<>();
        final Map<Long, ApiPartialEntity> regions = new HashMap<>();
        final Map<Long, ApiPartialEntity> azToRegion = new HashMap<>();
        repositoryApi.getRegion(entitiesById.keySet()).getEntities().forEach(region -> {
            Long regionOid = region.getOid();
            regions.put(regionOid, region);
            // Create the AZ -> Region mappings that we learned from this entry.
            for (RelatedEntity re : region.getConnectedToList()) {
                if (re.getEntityType() == EntityType.AVAILABILITY_ZONE_VALUE) {
                    azToRegion.put(re.getOid(), region);
                }
            }
        });

        for (ApiPartialEntity entry : entitiesById.values()) {
            // Identify the first region. Make a note of the first availability zone that we
            // encounter in case we need to derive the region from the availability zone instead.
            Long azId = null;
            ApiPartialEntity relatedRegion = null;
            for (RelatedEntity re : entry.getConnectedToList()) {
                if (re.getEntityType() == EntityType.REGION_VALUE) {
                    relatedRegion = regions.get(re.getOid());
                    break;
                } else if (azId == null &&
                           re.getEntityType() == EntityType.AVAILABILITY_ZONE_VALUE) {
                    azId = re.getOid();
                }
            }
            if (relatedRegion == null && azId != null) {
                // Get the region from the AZ instead
                relatedRegion = azToRegion.get(azId);
            }
            if (relatedRegion != null) {
                entityIdToRegion.put(entry.getOid(), relatedRegion);
            }
        }
        // Add the regions and availability zones to the entities map.
        entityIdToRegion.values().forEach(region -> entitiesById.put(region.getOid(), region));
        entitiesById.putAll(azToRegion);

        // fetch all cloud aspects
        final Map<Long, EntityAspect> cloudAspects = getEntityToAspectMapping(entitiesById.values(),
            Collections.emptySet(), Sets.newHashSet(EnvironmentType.CLOUD), AspectName.CLOUD);

        if (topologyContextId == realtimeTopologyContextId) {
            return new ActionSpecMappingContext(entitiesById, policies.get(), entityIdToRegion,
                Collections.emptyMap(), cloudAspects, Collections.emptyMap(), Collections.emptyMap(),
                buyRIIdToRIBoughtandRISpec, datacenterById, serviceEntityMapper, false);
        }

        // fetch more info for plan actions
        // fetch all volume aspects together first rather than fetch one by one to improve performance
        Map<Long, List<VirtualDiskApiDTO>> volumesAspectsByVM = fetchVolumeAspects(actions, topologyContextId);

        // fetch all vm aspects
        final Map<Long, EntityAspect> vmAspects = getEntityToAspectMapping(entitiesById.values(),
            Sets.newHashSet(ApiEntityType.VIRTUAL_MACHINE), Sets.newHashSet(EnvironmentType.CLOUD),
            AspectName.VIRTUAL_MACHINE);

        // fetch all db aspects
        final Map<Long, EntityAspect> dbAspects = getEntityToAspectMapping(entitiesById.values(),
            Sets.newHashSet(ApiEntityType.DATABASE, ApiEntityType.DATABASE_SERVER),
            Sets.newHashSet(EnvironmentType.CLOUD), AspectName.DATABASE);

        return new ActionSpecMappingContext(entitiesById, policies.get(), entityIdToRegion,
                volumesAspectsByVM, cloudAspects, vmAspects, dbAspects,
            buyRIIdToRIBoughtandRISpec, datacenterById, serviceEntityMapper, true);
    }

    /**
     * Quick test to check if entity is projected or not.  Relies on fact that Market assigns
     * negative OIDs to entities that it provisions.
     *
     * @param entityId ID of entity to check
     * @return true for projected entities.
     */
    private boolean isProjected(long entityId) {
        return entityId < 0;
    }

    /**
     * Create a Map of OID -> Aspect for a given collection of entities and the aspect desired.
     * This will make a request to repository API for some aspects as they will not be contained
     * on the {@link ApiPartialEntity}.
     *
     * @param entities the entities to get the aspects for.
     * @param entityTypes the entity types to filter by.
     * @param envTypes the environment types to filter by.
     * @param aspectName the aspect name for the aspect desired.
     * @return a map of OID -> Aspect for the requested aspect.
     * @throws InterruptedException if execution is interrupted.
     * @throws ConversionException if aspect conversion is unsuccessful.
     */
    @Nonnull
    private Map<Long, EntityAspect> getEntityToAspectMapping(
            @Nonnull Collection<ApiPartialEntity> entities,
            @Nonnull Set<ApiEntityType> entityTypes,
            @Nonnull Set<EnvironmentType> envTypes,
            @Nonnull AspectName aspectName)
            throws InterruptedException, ConversionException {

        final Map<Long, EntityAspect> oidToAspectMap = new HashMap<>();
        boolean allEntityTypes = entityTypes.isEmpty();

        // For Cloud Aspect, we can get certain information from the ApiPartialEntity
        // at this time, we don't need the full Api DTO
        if (aspectName == AspectName.CLOUD) {
            for (ApiPartialEntity entity : entities) {
                if (entity.getEnvironmentType() == EnvironmentType.ON_PREM ||
                    (allEntityTypes && entityTypes.contains(ApiEntityType.fromType(entity.getEntityType())))) {
                    continue;
                }
                final EntityAspect cloudAspect =
                    entityAspectMapper.getAspectByEntity(entity, AspectName.CLOUD);
                if (cloudAspect != null) {
                    oidToAspectMap.put(entity.getOid(), cloudAspect);
                }
            }

            return oidToAspectMap;
        }

        // For other Aspect types, we need to get the full API DTO so that it can be extracted.
        // Get the OIDs of the partial entities we need full entities of
        final Set<Long> entityIds = entities.stream()
            .filter(e -> allEntityTypes || entityTypes.contains(ApiEntityType.fromType(e.getEntityType())))
            .filter(e -> envTypes.contains(e.getEnvironmentType()))
            .map(ApiPartialEntity::getOid)
            .collect(Collectors.toSet());

        // Iterate over full entities, extract aspects, and build a mapping
        final Iterator<TopologyEntityDTO> iterator =
            repositoryApi.entitiesRequest(entityIds).getFullEntities().iterator();
        while (iterator.hasNext()) {
            final TopologyEntityDTO fullEntity = iterator.next();

            final EntityAspect aspect =
                entityAspectMapper.getAspectByEntity(fullEntity,
                    aspectName);
            oidToAspectMap.put(fullEntity.getOid(), aspect);
        }

        return oidToAspectMap;
    }

    /**
     * Take the set of entity Oids for entities related to the set of actions and find the
     * Datacenter for each Oid.  Return a map of Oids to ApiPartialEntity where each
     * ApiPartialEntity represents the Datacenter for an entity with a particular Oid.
     *
     * @param entityOids - Set of Oids for all the entities related to the set of actions.
     * @param topologyContextId - ID for the topology.
     * @return Map of Oids to ApiPartialEntity representing the Datacenter for a given Oid.
     */
    private Map<Long, ApiPartialEntity> getDatacentersByEntity(@Nonnull final Set<Long> entityOids,
                                                               final long topologyContextId) {
        final GetMultiSupplyChainsRequest.Builder requestBuilder =
            GetMultiSupplyChainsRequest.newBuilder();
        // For each OID in the set of entities, get Datacenters in its supply chain
        entityOids.forEach(oid -> {
            requestBuilder.addSeeds(SupplyChainSeed.newBuilder()
                .setSeedOid(oid)
                .setScope(SupplyChainScope.newBuilder()
                    .addStartingEntityOid(oid)
                    .addEntityTypesToInclude(ApiEntityType.DATACENTER.apiStr())));
        });
        final Map<Long, Long> dcOidMap = Maps.newHashMap();
        supplyChainServiceClient.getMultiSupplyChains(requestBuilder.build())
            .forEachRemaining(supplyChainResponse -> {
                // Here there can be only one datacenter for any given oid in the returned
                // supplychain, even though the protobufs support multiple entities being returned.
                // Thus, we take the first supplychainnode (there will only ever be one) and
                // take the first member in the memberlist (again, there will only be one).
                final long oid = supplyChainResponse.getSeedOid();
                supplyChainResponse.getSupplyChain().getSupplyChainNodesList().stream()
                    .findFirst().ifPresent(scNode -> {
                        scNode.getMembersByStateMap().values().stream()
                            .findFirst().ifPresent(memberList -> {
                                memberList.getMemberOidsList().stream()
                                    .findFirst().ifPresent(dcOid -> dcOidMap.put(oid, dcOid));

                        });
                });
            });

        final Set<Long> srcEntities = new HashSet<>();
        final Set<Long> projEntities = new HashSet<>();
        dcOidMap.values().forEach(id -> {
            if (!isProjected(id)) {
                srcEntities.add(id);
            } else {
                projEntities.add(id);
            }
        });

        final Map<Long, ApiPartialEntity> oidToPartialEntityMap = Maps.newHashMap();
        if (!srcEntities.isEmpty()) {
            repositoryApi.entitiesRequest(srcEntities)
                .contextId(topologyContextId)
                .getEntities()
                .forEach(entity -> oidToPartialEntityMap.put(entity.getOid(), entity));
        }
        if (!projEntities.isEmpty()) {
            repositoryApi.entitiesRequest(projEntities)
                .contextId(topologyContextId)
                .projectedTopology()
                .getEntities()
                .forEach(entity -> oidToPartialEntityMap.put(entity.getOid(), entity));
        }
        return dcOidMap.entrySet().stream()
            .filter(entry -> Objects.nonNull(oidToPartialEntityMap.get(entry.getValue())))
            .collect(Collectors.toMap(e -> e.getKey(),
                e -> oidToPartialEntityMap.get(e.getValue())));
    }

    @Nonnull
    private Map<Long, ApiPartialEntity> getEntities(@Nonnull final List<Action> actions, final long contextId) {
        final Set<Long> srcEntities = new HashSet<>();
        final Set<Long> projEntities = new HashSet<>();
        final Set<Long> involvedEntities = ActionDTOUtil.getInvolvedEntityIds(actions);

        // In plans, we also want to retrieve the provisioned sellers, because we will show and
        // interpret actions that interact with them (e.g. provision host X, move vm Y onto host X).
        // In realtime, we don't show those second-order moves, and the provisioned sellers are
        // not in the projected topology, so no point looking for them.
        if (contextId != realtimeTopologyContextId) {
            // getInvolvedEntityIds doesn't return the IDs of provisioned sellers (representations
            // of entities provisioned by the market), since those aren't "real" entities, and are
            // not relevant in most places. However, they ARE relevant when displaying action details
            // to the user, so we get them here.
            for (Action action : actions) {
                if (action.getInfo().getProvision().hasProvisionedSeller()) {
                    involvedEntities.add(action.getInfo().getProvision().getProvisionedSeller());
                }
            }
        }

        involvedEntities.forEach(id -> {
            // Because it is faster to retrieve realtime source entities (compared to realtime
            // projected entities), we try a shortcut:
            //
            // Right now (June 21 2019) the Market always assigns negative OIDs to entities that
            // it provisions. For example, if the market recommends provisioning a host
            // and moving VM 1 onto the host, the move will be to a host with some negative ID.
            // We can use this as a quick way to determine of an involved entity can be found
            // in the source topology (Market-recommended entities will only be
            // in the projected topology).
            if (!isProjected(id)) {
                srcEntities.add(id);
            } else {
                projEntities.add(id);
            }
        });

        final Map<Long, ApiPartialEntity> retMap = repositoryApi.entitiesRequest(srcEntities)
            .contextId(contextId)
            .getEntities()
            .collect(Collectors.toMap(ApiPartialEntity::getOid, Function.identity()));
        // Find entities we can't find in the source topology in the projected topology.
        srcEntities.stream()
            .filter(srcId -> !retMap.containsKey(srcId))
            .forEach(projEntities::add);
        if (!projEntities.isEmpty()) {
            repositoryApi.entitiesRequest(projEntities)
                .contextId(contextId)
                .projectedTopology()
                .getEntities()
                .forEach(e -> retMap.put(e.getOid(), e));
        }
        return retMap;
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
     * Fetch the volume aspects needed for given actions, and make sure the stats have both
     * beforePlan and afterPlan StorageAmount.
     *
     * @param actions  actions to analyze
     * @param topologyContextId topology context id
     * @throws InterruptedException if thread has been interrupted
     * @throws ConversionException if errors faced during converting data to API DTOs
     */
    private Map<Long, List<VirtualDiskApiDTO>> fetchVolumeAspects(@Nonnull List<Action> actions,
            long topologyContextId) throws InterruptedException, ConversionException {
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
                        newStorageAmount.setValue(stat.getValue());
                        newStorageAmount.setValues(stat.getValues());
                        newStorageAmount.setUnits(stat.getUnits());
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

        private final Map<Long, PolicyDTO.Policy> policies;

        private final Map<Long, ApiPartialEntity> entityIdToRegion;

        private final Map<Long, List<VirtualDiskApiDTO>> volumeAspectsByVM;

        private final Map<Long, EntityAspect> cloudAspects;

        private final Map<Long, EntityAspect> vmAspects;

        private final Map<Long, EntityAspect> dbAspects;

        private final Map<Long, Pair<ReservedInstanceBought, ReservedInstanceSpec>> buyRIIdToRIBoughtandRISpec;

        private final Map<Long, ApiPartialEntity> oidToDatacenter;

        private final boolean isPlan;

        ActionSpecMappingContext(@Nonnull Map<Long, ApiPartialEntity> topologyEntityDTOs,
                                 @Nonnull Map<Long, PolicyDTO.Policy> policies,
                                 @Nonnull Map<Long, ApiPartialEntity> entityIdToRegion,
                                 @Nonnull Map<Long, List<VirtualDiskApiDTO>> volumeAspectsByVM,
                                 @Nonnull Map<Long, EntityAspect> cloudAspects,
                                 @Nonnull Map<Long, EntityAspect> vmAspects,
                                 @Nonnull Map<Long, EntityAspect> dbAspects,
                                 @Nonnull Map<Long, Pair<ReservedInstanceBought, ReservedInstanceSpec>>
                                         buyRIIdToRIBoughtandRISpec,
                                 @Nonnull Map<Long, ApiPartialEntity> oidToDatacenter,
                                 @Nonnull ServiceEntityMapper serviceEntityMapper,
                                 final boolean isPlan) {
            this.serviceEntityApiDTOs = topologyEntityDTOs.entrySet().stream()
                .collect(Collectors.toMap(Entry::getKey, entry ->
                    serviceEntityMapper.toServiceEntityApiDTO(entry.getValue())));
            this.policies = Objects.requireNonNull(policies);
            this.entityIdToRegion = Objects.requireNonNull(entityIdToRegion);
            this.volumeAspectsByVM = Objects.requireNonNull(volumeAspectsByVM);
            this.cloudAspects = Objects.requireNonNull(cloudAspects);
            this.vmAspects = Objects.requireNonNull(vmAspects);
            this.dbAspects = Objects.requireNonNull(dbAspects);
            this.buyRIIdToRIBoughtandRISpec = buyRIIdToRIBoughtandRISpec;
            this.oidToDatacenter = oidToDatacenter;
            this.isPlan = isPlan;
        }

        PolicyDTO.Policy getPolicy(long id) {
            return policies.get(id);
        }

        @Nonnull
        Optional<ServiceEntityApiDTO> getEntity(final long oid) {
            final ServiceEntityApiDTO entity = serviceEntityApiDTOs.get(oid);
            return entity == null ? Optional.empty() : Optional.of(entity);
        }

        @Nonnull Optional<ApiPartialEntity> getDatacenterFromOid(@Nonnull Long entityOid) {
            return Optional.ofNullable(oidToDatacenter.get(entityOid));
        }

        @Nullable
        ApiPartialEntity getRegion(@Nonnull Long entityOid) {
            return entityIdToRegion.get(entityOid);
        }

        List<VirtualDiskApiDTO> getVolumeAspects(@Nonnull Long vmId) {
            return volumeAspectsByVM.getOrDefault(vmId, Collections.emptyList());
        }

        Optional<EntityAspect> getCloudAspect(@Nonnull Long entityId) {
            return Optional.ofNullable(cloudAspects.get(entityId));
        }

        Optional<EntityAspect> getVMAspect(@Nonnull Long entityId) {
            return Optional.ofNullable(vmAspects.get(entityId));
        }

        Optional<EntityAspect> getDBAspect(@Nonnull Long entityId) {
            return Optional.ofNullable(dbAspects.get(entityId));
        }

        public Pair<ReservedInstanceBought, ReservedInstanceSpec> getRIBoughtandRISpec(Long id) {
            return buyRIIdToRIBoughtandRISpec.get(id);
        }

        public Map<Long, ServiceEntityApiDTO> getServiceEntityApiDTOs() {
            return Collections.unmodifiableMap(serviceEntityApiDTOs);
        }

        public boolean isPlan() {
            return isPlan;
        }
    }
}
