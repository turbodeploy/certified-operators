package com.vmturbo.action.orchestrator.store;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.collect.Maps;

import io.grpc.Channel;
import io.grpc.StatusRuntimeException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.util.CollectionUtils;

import com.vmturbo.action.orchestrator.action.AcceptedActionsDAO;
import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsForEntitiesRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsForEntitiesResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.Groupings;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.TopologyType;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetMultiSupplyChainsRequest;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetMultiSupplyChainsResponse;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode.MemberList;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainScope;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainSeed;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceBlockingStub;
import com.vmturbo.common.protobuf.schedule.ScheduleProto;
import com.vmturbo.common.protobuf.schedule.ScheduleServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingFilter;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingsRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.TopologySelection;
import com.vmturbo.common.protobuf.setting.SettingProto.TopologySelection.Builder;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.EntityWithConnections;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.Type;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.common.setting.SettingAndPolicies;
import com.vmturbo.components.common.setting.SettingDTOUtil;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.repository.api.RepositoryListener;
import com.vmturbo.repository.api.TopologyAvailabilityTracker;
import com.vmturbo.repository.api.TopologyAvailabilityTracker.TopologyUnavailableException;
import com.vmturbo.topology.graph.OwnershipGraph;

/**
 * The {@link EntitiesAndSettingsSnapshotFactory} is a way to create a
 * {@link EntitiesAndSettingsSnapshot} that can be used to look up setting and entity-related
 * information during {@link LiveActionStore} population.
 */
@ThreadSafe
public class EntitiesAndSettingsSnapshotFactory implements RepositoryListener {

    private static final Logger logger = LogManager.getLogger();

    private final SettingPolicyServiceBlockingStub settingPolicyService;

    private final RepositoryServiceBlockingStub repositoryService;

    private final GroupServiceBlockingStub groupService;

    private final SupplyChainServiceBlockingStub supplyChainService;

    private final ScheduleServiceGrpc.ScheduleServiceBlockingStub scheduleService;

    private final AcceptedActionsDAO acceptedActionsStore;

    private final long timeToWaitForTopology;

    private final TimeUnit timeToWaitUnit;

    private final TopologyAvailabilityTracker topologyAvailabilityTracker;

    private final long realtimeTopologyContextId;

    private final boolean settingsStrictTopologyIdMatch;

    EntitiesAndSettingsSnapshotFactory(@Nonnull final Channel groupChannel,
            @Nonnull final Channel repoChannel, final long realtimeTopologyContextId,
            @Nonnull final TopologyAvailabilityTracker topologyAvailabilityTracker,
            final long timeToWaitForTopology, @Nonnull final TimeUnit timeToWaitUnit,
            @Nonnull final AcceptedActionsDAO acceptedActionsStore,
            final boolean settingsStrictTopologyIdMatch) {
        this.settingPolicyService = SettingPolicyServiceGrpc.newBlockingStub(groupChannel);
        this.repositoryService = RepositoryServiceGrpc.newBlockingStub(repoChannel);
        this.groupService = GroupServiceGrpc.newBlockingStub(groupChannel);
        this.supplyChainService = SupplyChainServiceGrpc.newBlockingStub(repoChannel);
        this.scheduleService = ScheduleServiceGrpc.newBlockingStub(groupChannel);
        this.timeToWaitForTopology = timeToWaitForTopology;
        this.timeToWaitUnit = timeToWaitUnit;
        this.realtimeTopologyContextId = realtimeTopologyContextId;
        this.topologyAvailabilityTracker = topologyAvailabilityTracker;
        this.acceptedActionsStore = Objects.requireNonNull(acceptedActionsStore);
        this.settingsStrictTopologyIdMatch = settingsStrictTopologyIdMatch;
    }

    /**
     * The snapshot of entities and settings required to properly initialize the various
     * fields of an action (most notably the action mode, which factors in settings, flags on
     * an entity, and commodities).
     */
    public static class EntitiesAndSettingsSnapshot {
        private final Map<Long, Map<String, SettingAndPolicies>>
                settingAndPoliciesByEntityAndSpecName;
        private final Map<Long, ActionPartialEntity> oidToEntityMap;
        private final OwnershipGraph<EntityWithConnections> ownershipGraph;
        private final Map<Long, Long> entityToResourceGroupMap;
        private final Map<Long, ScheduleProto.Schedule> oidToScheduleMap;
        private final Map<Long, String> actionToAcceptorMap;
        private final long topologyContextId;
        private final TopologyType topologyType;
        private final long populationTimestamp;
        @Nullable
        private TopologyInfo topologyInfo;

        /**
         * Constructor of {@link EntitiesAndSettingsSnapshot}.
         *
         * @param settingsAndPoliciesMap mapping from entity oid to map contains information about
         * settings and associated policies
         * @param entityMap mapping from entity oid to entity info
         * @param ownershipGraph the graph contains connections between entities
         * @param entityToResourceGroupMap mapping from entity oid to related resource group
         * @param oidToScheduleMap mapping from entity oid to related schedule
         * @param actionToAcceptorMap mapping from recommendation oid to accepting user
         * @param topologyContextId the topology context id
         * @param targetTopologyType the topology type
         * @param populationTimestamp the time when snapshot is created
         */
        public EntitiesAndSettingsSnapshot(
                @Nonnull final Map<Long, Map<String, SettingAndPolicies>> settingsAndPoliciesMap,
                @Nonnull final Map<Long, ActionPartialEntity> entityMap,
                @Nonnull final OwnershipGraph<EntityWithConnections> ownershipGraph,
                @Nonnull final Map<Long, Long> entityToResourceGroupMap,
                @Nonnull final Map<Long, ScheduleProto.Schedule> oidToScheduleMap,
                @Nonnull final Map<Long, String> actionToAcceptorMap, final long topologyContextId,
                @Nonnull final TopologyType targetTopologyType, final long populationTimestamp) {
            this.settingAndPoliciesByEntityAndSpecName = settingsAndPoliciesMap;
            this.oidToEntityMap = entityMap;
            this.ownershipGraph = ownershipGraph;
            this.entityToResourceGroupMap = entityToResourceGroupMap;
            this.actionToAcceptorMap = actionToAcceptorMap;
            this.topologyContextId = topologyContextId;
            this.topologyType = targetTopologyType;
            this.oidToScheduleMap = oidToScheduleMap;
            this.populationTimestamp = populationTimestamp;
        }

        /**
         * Get the list of action-orchestrator related settings associated with an entity.
         *
         * @param entityId The ID of the entity.
         * @return A map of (setting spec name, setting) for the settings associated with the entity.
         *         This may be empty, but will not be null.
         */
        @Nonnull
        public Map<String, Setting> getSettingsForEntity(final long entityId) {
            return settingAndPoliciesByEntityAndSpecName.getOrDefault(entityId,
                    Collections.emptyMap())
                    .entrySet()
                    .stream()
                    .collect(Collectors.toMap(Entry::getKey, v -> v.getValue().getSetting()));
        }

        /**
         * Get the list of settings policies associated with an entity.
         *
         * @param entityId the id of the entity
         * @return A map of (setting spec name, settings policies ids) related to the entity.
         */
        @Nonnull
        public Map<String, Collection<Long>> getSettingPoliciesForEntity(final long entityId) {
            return settingAndPoliciesByEntityAndSpecName.getOrDefault(entityId,
                    Collections.emptyMap())
                    .entrySet()
                    .stream()
                    .collect(Collectors.toMap(Entry::getKey, v -> v.getValue().getPoliciesIds()));
        }

        /**
         * Get the set of settings defined in default policies and associated with an entity.
         * @param entityId the id of the entity
         * @return set of settings defined in default policies and associated with an entity
         */
        @Nonnull
        public Set<String> getDefaultSettingPoliciesForEntity(final long entityId) {
            return settingAndPoliciesByEntityAndSpecName.getOrDefault(entityId,
                    Collections.emptyMap())
                    .entrySet()
                    .stream()
                    .filter(v -> !CollectionUtils.isEmpty(v.getValue().getDefaultPoliciesIds()))
                    .map(Entry::getKey)
                    .collect(Collectors.toSet());
        }

        @Nonnull
        public Optional<ActionPartialEntity> getEntityFromOid(final long entityOid) {
            return Optional.ofNullable(oidToEntityMap.get(entityOid));
        }

        @Nonnull
        public  Map<Long, ActionPartialEntity> getEntityMap() {
            return oidToEntityMap;
        }

        @Nonnull
        public Map<Long, ScheduleProto.Schedule> getScheduleMap() {
            return Collections.unmodifiableMap(oidToScheduleMap);
        }

        /**
         * Returns the user that has accepted the action.
         *
         * @param recommendationId the stable id for the action for which we are querying the
         * accepting user.
         * @return the user name for the user that accepted action or empty otherwise.
         */
        @Nonnull
        public Optional<String> getAcceptingUserForAction(long recommendationId) {
            return  Optional.ofNullable(actionToAcceptorMap.get(recommendationId));
        }

        public long getToologyContextId() {
            return topologyContextId;
        }

        @Nonnull
        public TopologyType getTopologyType() {
            return topologyType;
        }

        /**
         * Sets optional topology info, for plans.
         *
         * @param topologyInfo Topology info for plans.
         */
        public void setTopologyInfo(@Nullable final TopologyInfo topologyInfo) {
            this.topologyInfo = topologyInfo;
        }

        /**
         * Gets topology info.
         *
         * @return Topology info, can be null.
         */
        @Nullable
        public TopologyInfo getTopologyInfo() {
            return topologyInfo;
        }

        /**
         * Checks if TopologyInfo is null.
         *
         * @return Whether TopologyInfo is set.
         */
        public boolean hasTopologyInfo() {
            return topologyInfo != null;
        }

        /**
         * Get the owner business account of an entity.
         *
         * @param entityId the entity which is looking for the owner
         * @return A {@link EntityWithConnections} describing the account.
         */
        @Nonnull
        public Optional<EntityWithConnections> getOwnerAccountOfEntity(final long entityId) {
            // The first owner is the immediate owner.
            return ownershipGraph.getOwners(entityId).stream().findFirst();
        }

        /**
         * Get the resource group for entity.
         *
         * @param entityId entityId which is looking for the resource group
         * @return resourceGroupId
         */
        @Nonnull
        public Optional<Long> getResourceGroupForEntity(final long entityId) {
            return Optional.ofNullable(entityToResourceGroupMap.get(entityId));
        }

        /**
         * Returns the timestamp where the snapshot is created.
         * @return the timestamp where the snapshot is created.
         */
        public long getPopulationTimestamp() {
            return populationTimestamp;
        }

    }

    /**
     * Create a new snapshot containing set of action-related settings and entities.
     * This call involves making remote calls to other components, and can take a while.
     *
     * @param entities The new set of entities to get settings for. This set should contain
     *                 the IDs of all entities involved in all actions we expose to the user.
     * @param nonProjectedEntities entities not in projected topology such as detached volume OIDs.
     * @param topologyContextId The topology context of the topology broadcast that
     *                          triggered the cache update.
     * @param topologyId The topology id of the topology, the broadcast of which triggered the
     *                   cache update. The topology id can be null as well in case we are trying to
     *                   get a new snapshot for RI Buy Actions. Because RI Buy Algorithm is not
     *                   triggered on topology broadcast.
     * @return A {@link EntitiesAndSettingsSnapshot} containing the new action-related settings and entities.
     */
    @Nonnull
    public EntitiesAndSettingsSnapshot newSnapshot(@Nonnull final Set<Long> entities,
                                                   @Nonnull final Set<Long> nonProjectedEntities,
                                                   final long topologyContextId,
                                                   final long topologyId) {
        return internalNewSnapshot(entities, nonProjectedEntities, topologyContextId, topologyId);
    }

    /**
     * A version of {@link EntitiesAndSettingsSnapshotFactory#newSnapshot(Set, Set, long, long)}
     * that waits for the latest topology in a particular context.
     *
     * @param nonProjectedEntities entities not in projected topology such as detached volume OIDs.
     * @param entities See {@link EntitiesAndSettingsSnapshot#newSnapshot(Set, Set, long, long)}
     * @param topologyContextId See {@link EntitiesAndSettingsSnapshot#newSnapshot(Set, Set, long, long)}
     * @return A {@link EntitiesAndSettingsSnapshot} containing the new action-related settings and
     * entities.
     */
    @Nonnull
    public EntitiesAndSettingsSnapshot newSnapshot(@Nonnull final Set<Long> entities,
                                                   @Nonnull final Set<Long> nonProjectedEntities,
                                                   final long topologyContextId) {
        return internalNewSnapshot(entities, nonProjectedEntities, topologyContextId, null);
    }

    /**
     * internalNewSnapshot.
     *
     * @param entities See {@link EntitiesAndSettingsSnapshot#newSnapshot(Set, Set, long, long)}
     * @param nonProjectedEntities entities not in projected topology such as detached volume OIDs.
     * @param topologyContextId topologyContextId See {@link EntitiesAndSettingsSnapshot#newSnapshot(Set, Set, long, long)}
     * @param topologyId The topology Id.
     * @return A {@link EntitiesAndSettingsSnapshot} containing the new action-related settings and entities.
     */
    @Nonnull
    private EntitiesAndSettingsSnapshot internalNewSnapshot(@Nonnull final Set<Long> entities,
                                                            @Nonnull final Set<Long> nonProjectedEntities,
                                                            final long topologyContextId,
                                                            @Nullable final Long topologyId) {
        final Map<Long, Map<String, SettingAndPolicies>> settingAndPoliciesMapByEntityAndSpecName =
                retrieveEntityToSettingAndPoliciesListMap(entities, topologyContextId, topologyId);
        final Map<Long, Long> entityToResourceGroupMap =
            retrieveResourceGroupsForEntities(entities);

        // For plans we want to look in the projected topology, because in plans we will be
        // getting actions involving provisioned entities. In realtime we get provision actions,
        // but no actions on top of the provisioned entities, so looking in the source topology
        // is safe (and more efficient).
        final TopologyType targetTopologyType = topologyContextId == realtimeTopologyContextId ?
            TopologyType.SOURCE : TopologyType.PROJECTED;

        Map<Long, ActionPartialEntity> entityMap = Collections.emptyMap();
        OwnershipGraph<EntityWithConnections> ownershipGraph = OwnershipGraph.empty();
        try {
            // Before we make calls for data that comes from the repository we wait until the
            // desired topology is available in the repository.
            if (topologyId != null) {
                // If we want a specific topology, wait for that topology to become available.
                topologyAvailabilityTracker.queueTopologyRequest(topologyContextId, topologyId)
                    .waitForTopology(timeToWaitForTopology, timeToWaitUnit);
            } else {
                // If not, wait for SOME topology of the target type to be available in the context.
                topologyAvailabilityTracker.queueAnyTopologyRequest(topologyContextId, targetTopologyType)
                    .waitForTopology(timeToWaitForTopology, timeToWaitUnit);
            }
            // This will be the case for plans with detached volume actions only.
            // We need to get the information for these entities from the real-time SOURCE topology,
            // as they're not added to the plan projected topology.
            if (!nonProjectedEntities.isEmpty()) {
                topologyAvailabilityTracker.queueAnyTopologyRequest(topologyContextId, TopologyType.SOURCE)
                .waitForTopology(timeToWaitForTopology, timeToWaitUnit);
            }

            entityMap = retrieveOidToEntityMap(entities,
                topologyContextId, topologyId, targetTopologyType);
            ownershipGraph = retrieveOwnershipGraph(entities, topologyContextId, topologyId, targetTopologyType);
        } catch (TopologyUnavailableException e) {
            logger.error("Topology not available. Entity snapshot won't have entity information." +
                " Error: {}", e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // Set the interrupt status on the thread.
            logger.error("Failed to wait for repository to return data due to exception : " + e);
        }

        if (!nonProjectedEntities.isEmpty()) {
            Map<Long, ActionPartialEntity> entityMapNonProjected;
            try {
                topologyAvailabilityTracker
                                .queueAnyTopologyRequest(topologyContextId, TopologyType.SOURCE)
                                .waitForTopology(timeToWaitForTopology, timeToWaitUnit);

                entityMapNonProjected = retrieveOidToEntityMap(nonProjectedEntities, realtimeTopologyContextId,
                                                   topologyId, TopologyType.SOURCE);
                entityMap.putAll(entityMapNonProjected);
            } catch (TopologyUnavailableException e) {
                logger.error("Topology not available. Entity snapshot won't have entity information." +
                                " Error: {}", e.getMessage());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt(); // Set the interrupt status on the thread.
                logger.error("Failed to wait for repository to return data due to exception : " + e);
            }
        }

        final Map<Long, ScheduleProto.Schedule> oidToScheduleMap = new HashMap<>();
        scheduleService.getSchedules(
            ScheduleProto.GetSchedulesRequest.newBuilder().build()).forEachRemaining(
                schedule -> oidToScheduleMap.put(schedule.getId(), schedule));

        // RecommendationId -> AcceptedBy
        final Map<Long, String> actionToAcceptorMap =
                acceptedActionsStore.getAcceptorsForAllActions();

        return new EntitiesAndSettingsSnapshot(settingAndPoliciesMapByEntityAndSpecName, entityMap,
                ownershipGraph, entityToResourceGroupMap, oidToScheduleMap, actionToAcceptorMap,
                topologyContextId, targetTopologyType, System.currentTimeMillis());
    }

    @Nonnull
    private Map<Long, Long> retrieveResourceGroupsForEntities(@Nonnull Set<Long> entities) {
        final GetGroupsForEntitiesResponse response = groupService.getGroupsForEntities(
            GetGroupsForEntitiesRequest.newBuilder()
                .addAllEntityId(entities)
                .addGroupType(GroupType.RESOURCE)
                .build());
        final Map<Long, Long> resultMap = new HashMap<>();
        for (Entry<Long, Groupings> groupingsEntry : response.getEntityGroupMap().entrySet()) {
            final long entityId = groupingsEntry.getKey();
            for (Long groupId : groupingsEntry.getValue().getGroupIdList()) {
                final Long oldGroupId = resultMap.put(entityId, groupId);
                if (oldGroupId != null) {
                    logger.warn("Found multiple resource groups for entity {}: {} and {}", entityId,
                        oldGroupId, groupId);
                }
            }
        }
        return Collections.unmodifiableMap(resultMap);
    }

    @Nonnull
    private OwnershipGraph<EntityWithConnections> retrieveOwnershipGraph(@Nonnull final Set<Long> entities,
                                                                         final long topologyContextId,
                                                                         @Nullable final Long topologyId,
                                                                         @Nonnull final TopologyType topologyType) {
        final OwnershipGraph.Builder<EntityWithConnections> graphBuilder =
            OwnershipGraph.newBuilder(EntityWithConnections::getOid);

        final RetrieveTopologyEntitiesRequest.Builder entitiesReqBuilder = RetrieveTopologyEntitiesRequest.newBuilder()
            .setReturnType(Type.WITH_CONNECTIONS)
            .setTopologyContextId(topologyContextId)
            .setTopologyType(topologyType)
            .addEntityType(ApiEntityType.BUSINESS_ACCOUNT.typeNumber());
        // Set the topologyId if its non null. Else it defaults to real time.
        if (topologyId != null) {
            entitiesReqBuilder.setTopologyId(topologyId);
        }

        final Map<Long, EntityWithConnections> businessAccounts =
                RepositoryDTOUtil.topologyEntityStream(
                        repositoryService.retrieveTopologyEntities(entitiesReqBuilder.build()))
                        .map(PartialEntity::getWithConnections)
                        .collect(Collectors.toMap(EntityWithConnections::getOid, ba -> ba));

        final GetMultiSupplyChainsRequest supplyChainRequest =
                createMultiSupplyChainRequest(businessAccounts.keySet());

        try {
            supplyChainService.getMultiSupplyChains(supplyChainRequest)
                    .forEachRemaining(
                            scResponse -> processSupplyChainResponse(entities, graphBuilder,
                                    businessAccounts, scResponse));
            return graphBuilder.build();
        } catch (StatusRuntimeException e) {
            logger.error("Failed to retrieve ownership graph entities due to repository error: {}",
                    e.getMessage());
            return OwnershipGraph.empty();
        }
    }

    /**
     * Get entities from supplyChain (filter out entities not related to current actions)
     * and add them to ownerShip graph as entities owned by certain business account.
     *
     * @param involvedEntities entities involved in current actions
     * @param graphBuilder builder for ownerShip graph
     * @param businessAccounts existed business accounts
     * @param scResponse supplyChain response for business account
     */
    private static void processSupplyChainResponse(@Nonnull Set<Long> involvedEntities,
            OwnershipGraph.Builder<EntityWithConnections> graphBuilder,
            Map<Long, EntityWithConnections> businessAccounts,
            GetMultiSupplyChainsResponse scResponse) {
        final Stream<Long> entitiesOwnedByAccount = scResponse.getSupplyChain()
                .getSupplyChainNodesList()
                .stream()
                .map(SupplyChainNode::getMembersByStateMap)
                .map(Map::values)
                .flatMap(memberList -> memberList.stream()
                        .map(MemberList::getMemberOidsList)
                        .flatMap(List::stream));
        entitiesOwnedByAccount.filter(involvedEntities::contains)
                .forEach(relevantEntity -> graphBuilder.addOwner(
                        businessAccounts.get(scResponse.getSeedOid()), relevantEntity));
    }

    /**
     * Request separate supplyChains for each business account.
     *
     * @param businessAccountIds business account oids
     * @return prepared request for supplyChain service
     */
    private static GetMultiSupplyChainsRequest createMultiSupplyChainRequest(
            Set<Long> businessAccountIds) {
        final Set<Integer> expandedEntityTypesForBusinessAccounts =
            ApiEntityType.ENTITY_TYPES_TO_EXPAND.get(ApiEntityType.BUSINESS_ACCOUNT).stream()
                .map(ApiEntityType::typeNumber)
                .collect(Collectors.toSet());
        final GetMultiSupplyChainsRequest.Builder scRequestBuilder =
                GetMultiSupplyChainsRequest.newBuilder();
        for (Long accountId : businessAccountIds) {
            final SupplyChainScope.Builder scScopeBuilder = SupplyChainScope.newBuilder()
                    .addAllEntityTypesToInclude(expandedEntityTypesForBusinessAccounts)
                    .addStartingEntityOid(accountId);
            final SupplyChainSeed.Builder scSeedBuilder = SupplyChainSeed.newBuilder()
                    .setSeedOid(accountId)
                    .setScope(scScopeBuilder.build());
            scRequestBuilder.addSeeds(scSeedBuilder.build());
        }
        return scRequestBuilder.build();
    }

    /**
     * Creates an empty snapshot. It only has a topology context id.
     *
     * @return An empty {@link EntitiesAndSettingsSnapshot}
     */
    @Nonnull
    public EntitiesAndSettingsSnapshot emptySnapshot() {
        return new EntitiesAndSettingsSnapshot(Collections.emptyMap(), Maps.newHashMap(),
                OwnershipGraph.empty(), Maps.newHashMap(), Maps.newHashMap(), Maps.newHashMap(),
                realtimeTopologyContextId, TopologyType.SOURCE, System.currentTimeMillis());
    }

    /**
     * Fetch entities from repository for given entities set.
     *
     * @param entities to fetch from repository.
     * @param topologyContextId of topology.
     * @param topologyId of topology, if we are looking for a particular topology. Null if we just
     *                   want whatever is the current latest topology (e.g. for RI Buy Actions).
     * @param targetTopologyType The topology type to get the entities from.
     * @return mapping with oid as key and {@link ActionPartialEntity} as value.
     */
    private Map<Long, ActionPartialEntity> retrieveOidToEntityMap(Set<Long> entities,
                                                                  long topologyContextId,
                                                                  @Nullable final Long topologyId,
                                                                  @Nonnull final TopologyType targetTopologyType) {
        if (entities.isEmpty()) {
            return Collections.emptyMap();
        }

        final RetrieveTopologyEntitiesRequest.Builder getEntitiesRequestBuilder =
            RetrieveTopologyEntitiesRequest.newBuilder()
                .setTopologyContextId(topologyContextId)
                .addAllEntityOids(entities)
                .setReturnType(PartialEntity.Type.ACTION)
                .setTopologyType(targetTopologyType);

        if (topologyId != null) {
            getEntitiesRequestBuilder.setTopologyId(topologyId);
        }

        try {
            final Map<Long, ActionPartialEntity> entitiesMap = RepositoryDTOUtil.topologyEntityStream(
                repositoryService.retrieveTopologyEntities(getEntitiesRequestBuilder.build()))
                .map(PartialEntity::getAction)
                .collect(Collectors.toMap(ActionPartialEntity::getOid, Function.identity()));
            return entitiesMap;
        } catch (StatusRuntimeException ex) {
            logger.error("Failed to fetch entities due to exception : " + ex);
            return Collections.emptyMap();
        }
    }

    @Nonnull
    private Map<Long, Map<String, SettingAndPolicies>> retrieveEntityToSettingAndPoliciesListMap(final Set<Long> entities,
                                                                    final long topologyContextId,
                                                                    @Nullable final Long topologyId) {
        // We don't currently upload action-relevant settings in plans,
        // so no point trying to get them.
        if (topologyContextId != realtimeTopologyContextId) {
            return Collections.emptyMap();
        }

        try {
            final Builder builder = TopologySelection.newBuilder()
                .setTopologyContextId(topologyContextId);
            // Only set the topology ID in the request if we want strict matching.
            if (topologyId != null && settingsStrictTopologyIdMatch) {
                // This should be used with caution - for the realtime case we may get an exception
                // if settings for this topology ID have been overwritten by a newer upload
                // from the topology processor.
                builder.setTopologyId(topologyId);
            }
            final GetEntitySettingsRequest request = GetEntitySettingsRequest.newBuilder()
                    .setTopologySelection(builder)
                    .setSettingFilter(EntitySettingFilter.newBuilder()
                            .addAllEntities(entities))
                    .setIncludeSettingPolicies(true)
                    .build();
            return Collections.unmodifiableMap(SettingDTOUtil.indexSettingsByEntity(
                SettingDTOUtil.flattenEntitySettings(
                    settingPolicyService.getEntitySettings(request))));
        } catch (StatusRuntimeException e) {
            logger.error("Failed to retrieve entity settings due to error: " + e.getMessage());
            return Collections.emptyMap();
        }
    }

}
