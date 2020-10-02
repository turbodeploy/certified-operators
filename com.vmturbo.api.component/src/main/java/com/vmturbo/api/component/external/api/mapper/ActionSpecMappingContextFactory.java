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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.mapper.aspect.EntityAspectMapper;
import com.vmturbo.api.component.external.api.mapper.aspect.VirtualVolumeAspectMapper;
import com.vmturbo.api.component.external.api.service.PoliciesService;
import com.vmturbo.api.component.external.api.service.ReservedInstancesService;
import com.vmturbo.api.component.external.api.util.StatsUtils;
import com.vmturbo.api.dto.action.ActionApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.api.dto.entityaspect.VirtualDiskApiDTO;
import com.vmturbo.api.dto.entityaspect.VirtualDisksAspectApiDTO;
import com.vmturbo.api.dto.policy.PolicyApiDTO;
import com.vmturbo.api.dto.reservedinstance.ReservedInstanceApiDTO;
import com.vmturbo.api.enums.AccountFilterType;
import com.vmturbo.api.enums.AspectName;
import com.vmturbo.api.exceptions.ConversionException;
import com.vmturbo.auth.api.Pair;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo.ActionTypeCase;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.BuyRI;
import com.vmturbo.common.protobuf.action.ActionDTO.Delete;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.action.ActionDTO.Scale;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.InvolvedEntityCalculation;
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
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Map an ActionSpec returned from the ActionOrchestrator into an {@link ActionApiDTO} to be
 * returned from the API.
 */
public class ActionSpecMappingContextFactory {
    private static final Logger logger = LogManager.getLogger();

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

    private final PoliciesService policiesService;

    private final ReservedInstancesService reservedInstancesService;

    private Collection<PolicyApiDTO> policyApiDto;

    public ActionSpecMappingContextFactory(@Nonnull PolicyServiceBlockingStub policyService,
                                           @Nonnull ExecutorService executorService,
                                           @Nonnull RepositoryApi repositoryApi,
                                           @Nonnull EntityAspectMapper entityAspectMapper,
                                           @Nonnull VirtualVolumeAspectMapper volumeAspectMapper,
                                           final long realtimeTopologyContextId,
                                           @Nonnull BuyReservedInstanceServiceBlockingStub buyRIServiceClient,
                                           @Nonnull ReservedInstanceSpecServiceBlockingStub riSpecServiceClient,
                                           @Nonnull ServiceEntityMapper serviceEntityMapper,
                                           @Nonnull SupplyChainServiceBlockingStub supplyChainServiceClient,
                                           @Nonnull PoliciesService policiesService,
                                           @Nonnull ReservedInstancesService reservedInstancesService) {
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
        this.policiesService = Objects.requireNonNull(policiesService);
        this.reservedInstancesService = Objects.requireNonNull(reservedInstancesService);
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
     * @param uuidMapper Mapper to look up plan type.
     * @return ActionSpecMappingContext
     * @throws ExecutionException on failure getting entities
     * @throws InterruptedException if thread has been interrupted
     * @throws ConversionException if errors faced during converting data to API DTOs
     */
    public ActionSpecMappingContext createActionSpecMappingContext(@Nonnull List<Action> actions,
            long topologyContextId, @Nonnull final UuidMapper uuidMapper)
            throws ExecutionException, InterruptedException, ConversionException {

        // NOTE: calls made on the current thread will be made "as the user" in upstream components
        // and trigger user-scoping, authorization checks and so on. Since we decided NOT to restrict
        // the display of policy-related info in action explanations, we are making the rpcs to populate
        // the mapping context as the "System", so we don't trigger further auth checks upstream. This
        // is as simple as running the RPCS on a separate thread (and also gives us parallelization
        // of the calls, as Pengcheng originally designed for)
        final Future<Map<Long, PolicyDTO.Policy>> policies = executorService.submit(this::getPolicies);
        final Future<Collection<PolicyApiDTO>> policiesApiDto = executorService.submit(() -> policiesService.convertPolicyDTOCollection(policies.get().values()));
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

        // fetch all volume aspects
        final Map<Long, List<VirtualDiskApiDTO>> volumesAspectsByEntity = fetchVolumeAspects(actions, topologyContextId);


        datacenterById.values().forEach(e -> entitiesById.put(e.getOid(), e));

        if (topologyContextId == realtimeTopologyContextId) {
            return new ActionSpecMappingContext(entitiesById, policies.get(), entityIdToRegion,
                volumesAspectsByEntity, cloudAspects, Collections.emptyMap(), Collections.emptyMap(),
                buyRIIdToRIBoughtandRISpec, datacenterById, serviceEntityMapper, false, policiesApiDto.get());
        }

        // fetch all vm aspects
        final Map<Long, EntityAspect> vmAspects = getEntityToAspectMapping(entitiesById.values(),
            Sets.newHashSet(ApiEntityType.VIRTUAL_MACHINE), Sets.newHashSet(EnvironmentType.CLOUD),
            AspectName.VIRTUAL_MACHINE);

        final Set<Long> entityIds = getEntityIds(entitiesById.values(),
                ImmutableSet.of(ApiEntityType.VIRTUAL_MACHINE),
                ImmutableSet.of(EnvironmentType.CLOUD));

        // fetch all db aspects
        final Map<Long, EntityAspect> dbAspects = getEntityToAspectMapping(entitiesById.values(),
            Sets.newHashSet(ApiEntityType.DATABASE, ApiEntityType.DATABASE_SERVER),
            Sets.newHashSet(EnvironmentType.CLOUD), AspectName.DATABASE);

        final ActionSpecMappingContext context = new ActionSpecMappingContext(entitiesById,
                policies.get(), entityIdToRegion,
            volumesAspectsByEntity, cloudAspects, vmAspects, dbAspects,
            buyRIIdToRIBoughtandRISpec, datacenterById, serviceEntityMapper, true, policiesApiDto.get());

        if (hasMigrationActions(actions)) {
            final Map<Long, EntityAspect> vmProjectedAspects =
                    getProjectedEntityToAspectMapping(entityIds,
                            AspectName.VIRTUAL_MACHINE, topologyContextId);

            // Getting projected entity aspects and RIs for plan only for cases where there is
            // is an inter-region migration, as in cloud migration for example,
            // check added above to not affect (possible performance issues) other plans or real-time.
            context.setHasMigrationActions(true);
            context.setVMProjectedAspects(vmProjectedAspects);
            try {
                ApiId scope = uuidMapper.fromUuid(String.valueOf(topologyContextId));
                if (scope != null && StatsUtils.isValidScopeForRIBoughtQuery(scope)) {
                    context.setReservedInstances(reservedInstancesService
                            .getReservedInstances(String.valueOf(topologyContextId), true,
                                    AccountFilterType.USED_AND_PURCHASED_BY));
                }
            } catch (Exception e) {
                throw new ExecutionException("Unable to get RIs for plan " + topologyContextId, e);
            }
        }
        return context;
    }

    /**
     * Returns true if at least one action is a region migration type action, e.g cloud-to-cloud.
     *
     * @param actions List of all actions.
     * @return Whether any action has a region change.
     */
    private boolean hasMigrationActions(@Nonnull List<Action> actions) {
        for (Action action : actions) {
            if (TopologyDTOUtil.isMigrationAction(action)) {
                // Found one action that is across regions.
                return true;
            }
        }
        return false;
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
     * Convenience method to get entity ids out of partial entities.
     *
     * @param entities Partial entities.
     * @param entityTypes Types, e.g VM.
     * @param envTypes Env types, e.g CLOUD.
     * @return Set of entity ids, could be empty.
     */
    @Nonnull
    private static Set<Long> getEntityIds(@Nonnull Collection<ApiPartialEntity> entities,
            @Nonnull Set<ApiEntityType> entityTypes,
            @Nonnull Set<EnvironmentType> envTypes) {
        return entities.stream()
                .filter(e -> entityTypes.isEmpty()
                        || entityTypes.contains(ApiEntityType.fromType(e.getEntityType())))
                .filter(e -> envTypes.contains(e.getEnvironmentType()))
                .map(ApiPartialEntity::getOid)
                .collect(Collectors.toSet());
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
        final Set<Long> entityIds = getEntityIds(entities, entityTypes, envTypes);

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
     * Gets aspect mapping from projected entities specified by input entity ids. For cloud
     * migration plan, we need to get new OS type from projected entity's VM type info.
     *
     * @param entityIds Ids for which projected entities need to be fetched.
     * @param name Type of aspect.
     * @param planId Plan context id.
     * @return Map of VM aspect keyed off of the target entity (VM) id.
     * @throws InterruptedException Thrown on entity fetch error.
     * @throws ConversionException Thrown on entity conversion error.
     */
    @Nonnull
    private Map<Long, EntityAspect> getProjectedEntityToAspectMapping(
            @Nonnull Set<Long> entityIds,
            @Nonnull AspectName name,
            long planId) throws InterruptedException, ConversionException {
        final Map<Long, EntityAspect> oidToAspectMap = new HashMap<>();
        final Set<TopologyEntityDTO> entitySet = repositoryApi.entitiesRequest(entityIds)
                .contextId(planId)
                .projectedTopology()
                .getFullEntities().collect(Collectors.toSet());
        for (final TopologyEntityDTO entity : entitySet) {
            final EntityAspect aspect = entityAspectMapper.getAspectByEntity(entity, name);
            if (aspect != null) {
                oidToAspectMap.put(entity.getOid(), aspect);
            }
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
            GetMultiSupplyChainsRequest.newBuilder()
                .setContextId(topologyContextId);
        // For each OID in the set of entities, get Datacenters in its supply chain
        entityOids.forEach(oid -> {
            requestBuilder.addSeeds(SupplyChainSeed.newBuilder()
                .setSeedOid(oid)
                .setScope(SupplyChainScope.newBuilder()
                    .addStartingEntityOid(oid)
                    .addEntityTypesToInclude(ApiEntityType.DATACENTER.typeNumber())));
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
    @VisibleForTesting
    Map<Long, ApiPartialEntity> getEntities(@Nonnull final List<Action> actions, final long contextId) {
        final Set<Long> involvedEntities = ActionDTOUtil.getInvolvedEntityIds(actions,
            InvolvedEntityCalculation.INCLUDE_ALL_MERGED_INVOLVED_ENTITIES);
        boolean isPlan = contextId != realtimeTopologyContextId;
        // In plans, we also want to retrieve the provisioned sellers, because we will show and
        // interpret actions that interact with them (e.g. provision host X, move vm Y onto host X).
        // In realtime, we don't show those second-order moves, and the provisioned sellers are
        // not in the projected topology, so no point looking for them.
        if (isPlan) {
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
        // First look up in source topology (plan or real-time)
        final Map<Long, ApiPartialEntity> retMap = repositoryApi.entitiesRequest(involvedEntities)
            .contextId(contextId)
            .getEntities()
            .collect(Collectors.toMap(ApiPartialEntity::getOid, Function.identity()));

        // Check if we could not get any mappings.
        Set<Long> missingEntities = new HashSet<>(Sets.difference(involvedEntities, retMap.keySet()));

        // Find entities we can't find in the source topology, in the projected topology.
        if (!missingEntities.isEmpty()) {
            repositoryApi.entitiesRequest(missingEntities)
                .contextId(contextId)
                .projectedTopology()
                .getEntities()
                .forEach(e -> {
                    retMap.put(e.getOid(), e);
                    missingEntities.remove(e.getOid());
                });
        }
        // For plans, if we could not lookup one or more entities, then look in real-time topology.
        // Sometimes regions for BuyRI actions are missing from plan, so check in real-time.
        if (isPlan && !missingEntities.isEmpty()) {
            repositoryApi.entitiesRequest(missingEntities)
                    .contextId(realtimeTopologyContextId)
                    .getEntities()
                    .forEach(e -> {
                        retMap.put(e.getOid(), e);
                        missingEntities.remove(e.getOid());
                    });
        }
        if (!missingEntities.isEmpty()) {
            logger.warn("For context {}, got mapping for only {} entities, didn't get for: {}",
                    contextId, retMap.size(), missingEntities);
        }
        return retMap;
    }

    @Nonnull
    private Map<Long, PolicyDTO.Policy> getPolicies() {
        final Map<Long, PolicyDTO.Policy> policies = new HashMap<>();
        policyService.getPolicies(PolicyDTO.PolicyRequest.newBuilder().build()).forEachRemaining(
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
        Map<Long, List<VirtualDiskApiDTO>> volumesAspectsByEntity = new HashMap<>();
        final Set<Long> scaleVolumeIds = new HashSet<>();
        final Set<Long> deleteVolumeIds = new HashSet<>();
        final Set<Long> moveVolumeVMIds = new HashSet<>();
        getEntityIdsToFetchVolumeAspects(actions, scaleVolumeIds, moveVolumeVMIds, deleteVolumeIds);

        // retrieve volume aspects for move volume actions
        if (!moveVolumeVMIds.isEmpty()) {
            volumesAspectsByEntity.putAll(volumeAspectMapper.mapVirtualMachines(moveVolumeVMIds, topologyContextId));
        }

        // retrieve volume aspects for scale volume actions
        if (!scaleVolumeIds.isEmpty()) {
            volumesAspectsByEntity.putAll(
                    volumeAspectMapper.mapVirtualVolumes(scaleVolumeIds, topologyContextId,
                            topologyContextId != realtimeTopologyContextId));
        }

        // retrieve volume aspects for delete volume actions
        if (!deleteVolumeIds.isEmpty()) {
            volumesAspectsByEntity.putAll(volumeAspectMapper
                    .mapVirtualVolumes(deleteVolumeIds, topologyContextId, false));
        }

        return new HashMap<>(volumesAspectsByEntity);
    }

    /**
     * Get the oids of the entities which we need to fetch volume aspects for and set to the ActionApiDTO later.
     *
     * @param actions list of MOVE, SCALE or Delete Volume actions.
     * @param scaleVolumeIds set to preserve volume ids that are going to be scaled.
     * @param moveVolumeVMIds set to preserve vm ids that have move volume actions.
     * @param deleteVolumeIds set to preserve volume ids that are going to be deleted.
     */
    private void getEntityIdsToFetchVolumeAspects(@Nonnull final List<Action> actions,
                                                  @Nonnull final Set<Long> scaleVolumeIds,
                                                  @Nonnull final Set<Long> moveVolumeVMIds,
                                                  @Nonnull final Set<Long> deleteVolumeIds) {
        actions.forEach(action -> {
            ActionTypeCase actionTypeCase = action.getInfo().getActionTypeCase();
            // volume scales
            if (actionTypeCase.equals(ActionTypeCase.SCALE)) {
                Scale scaleInfo = action.getInfo().getScale();
                if (scaleInfo.getTarget().getEnvironmentType() == EnvironmentType.CLOUD
                        && scaleInfo.getTarget().getType() == EntityType.VIRTUAL_VOLUME_VALUE) {
                    scaleVolumeIds.add(scaleInfo.getTarget().getId());
                }
            }
            // volume moves
            if (actionTypeCase.equals(ActionTypeCase.MOVE)) {
                Move moveInfo = action.getInfo().getMove();
                if (moveInfo.getTarget().getEnvironmentType() == EnvironmentType.CLOUD
                        && moveInfo.getTarget().getType() == EntityType.VIRTUAL_MACHINE_VALUE) {
                    moveVolumeVMIds.add(moveInfo.getTarget().getId());
                }
            }
            // volume deletes
            if (actionTypeCase.equals(ActionTypeCase.DELETE)) {
                Delete deleteInfo = action.getInfo().getDelete();
                if (deleteInfo.getTarget().getEnvironmentType().equals(EnvironmentType.CLOUD)
                        && deleteInfo.getTarget().getType() == EntityType.VIRTUAL_VOLUME_VALUE) {
                    deleteVolumeIds.add(deleteInfo.getTarget().getId());
                }
            }
        });
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

        private final Map<Long, List<VirtualDiskApiDTO>> volumeAspectsByEntity;

        private final Map<Long, EntityAspect> cloudAspects;

        private final Map<Long, EntityAspect> vmAspects;

        private final Map<Long, EntityAspect> dbAspects;

        private final Map<Long, Pair<ReservedInstanceBought, ReservedInstanceSpec>> buyRIIdToRIBoughtandRISpec;

        private final Map<Long, ApiPartialEntity> oidToDatacenter;

        private final boolean isPlan;

        private final Map<String, PolicyApiDTO> policiesApiDto;

        private Set<ReservedInstanceApiDTO> reservedInstanceApiDTOs;

        /**
         * VM aspects from projected entities. Applicable for only some plans like cloud migration.
         */
        private Map<Long, EntityAspect> vmProjectedAspects;

        /**
         * True if this context refers to actions related to (cloud) migration.
         */
        private boolean hasMigrationActions = false;


        ActionSpecMappingContext(@Nonnull Map<Long, ApiPartialEntity> topologyEntityDTOs,
                                 @Nonnull Map<Long, PolicyDTO.Policy> policies,
                                 @Nonnull Map<Long, ApiPartialEntity> entityIdToRegion,
                                 @Nonnull Map<Long, List<VirtualDiskApiDTO>> volumeAspectsByEntity,
                                 @Nonnull Map<Long, EntityAspect> cloudAspects,
                                 @Nonnull Map<Long, EntityAspect> vmAspects,
                                 @Nonnull Map<Long, EntityAspect> dbAspects,
                                 @Nonnull Map<Long, Pair<ReservedInstanceBought, ReservedInstanceSpec>>
                                         buyRIIdToRIBoughtandRISpec,
                                 @Nonnull Map<Long, ApiPartialEntity> oidToDatacenter,
                                 @Nonnull ServiceEntityMapper serviceEntityMapper,
                                 final boolean isPlan,
                                 final Collection<PolicyApiDTO> policiesApiDto) {

            /* topologyEntityDTOs contain some ZoneId -> Region entries,
               so we can't just use serviceEntityMapper.toServiceEntityApiDTOMap() output
             */
            Map<Long, ServiceEntityApiDTO> seMap = serviceEntityMapper.toServiceEntityApiDTOMap(topologyEntityDTOs.values());
            this.serviceEntityApiDTOs = new HashMap<>();
            topologyEntityDTOs.entrySet().forEach(entry -> {
                this.serviceEntityApiDTOs.put(entry.getKey(), seMap.get(entry.getValue().getOid()));
            });
            this.policies = Objects.requireNonNull(policies);
            this.entityIdToRegion = Objects.requireNonNull(entityIdToRegion);
            this.volumeAspectsByEntity = Objects.requireNonNull(volumeAspectsByEntity);
            this.cloudAspects = Objects.requireNonNull(cloudAspects);
            this.vmAspects = Objects.requireNonNull(vmAspects);
            this.dbAspects = Objects.requireNonNull(dbAspects);
            this.buyRIIdToRIBoughtandRISpec = buyRIIdToRIBoughtandRISpec;
            this.oidToDatacenter = oidToDatacenter;
            this.isPlan = isPlan;
            this.policiesApiDto = policiesApiDto.stream()
                    .collect(Collectors.toMap(PolicyApiDTO::getUuid, Function.identity()));;
        }

        PolicyDTO.Policy getPolicy(long id) {
            return policies.get(id);
        }

        Map<String, PolicyApiDTO> getPolicyApiDtoMap() {
            return policiesApiDto;
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

        List<VirtualDiskApiDTO> getVolumeAspects(@Nonnull Long entityId) {
            return volumeAspectsByEntity.getOrDefault(entityId, Collections.emptyList());
        }

        Optional<EntityAspect> getCloudAspect(@Nonnull Long entityId) {
            return Optional.ofNullable(cloudAspects.get(entityId));
        }

        Optional<EntityAspect> getVMAspect(@Nonnull Long entityId) {
            return Optional.ofNullable(vmAspects.get(entityId));
        }

        /**
         * Sets if this context refers to a cloud migration plan.
         *
         * @param hasMigrationActions Whether a context refers to a (cloud) migration plan.
         */
        void setHasMigrationActions(boolean hasMigrationActions) {
            this.hasMigrationActions = hasMigrationActions;
        }

        /**
         * Whether context refers to a (cloud) migration plan.
         *
         * @return True if a (cloud) migration plan context.
         */
        boolean hasMigrationActions() {
            return hasMigrationActions;
        }

        /**
         * Sets VM aspects from projected entity.
         *
         * @param projectedAspects Set of projected VM aspects.
         */
        void setVMProjectedAspects(@Nonnull Map<Long, EntityAspect> projectedAspects) {
            if (vmProjectedAspects == null) {
                vmProjectedAspects = new HashMap<>();
            }
            vmProjectedAspects.clear();
            vmProjectedAspects.putAll(projectedAspects);
        }

        void setReservedInstances(@Nonnull final List<ReservedInstanceApiDTO> reservedInstances) {
            if (reservedInstanceApiDTOs == null) {
                reservedInstanceApiDTOs = new HashSet<>();
            }
            reservedInstanceApiDTOs.clear();
            reservedInstanceApiDTOs.addAll(reservedInstances);
        }

        @Nonnull
        Set<ReservedInstanceApiDTO> getReservedInstances() {
            if (reservedInstanceApiDTOs == null) {
                return Collections.emptySet();
            }
            return reservedInstanceApiDTOs;
        }

        /**
         * Gets VM aspects from projected entity with the given id.
         *
         * @param entityId Projected entity id.
         * @return Aspect if present, or else empty.
         */
        Optional<EntityAspect> getVMProjectedAspect(@Nonnull Long entityId) {
            return Optional.ofNullable(vmProjectedAspects.get(entityId));
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
