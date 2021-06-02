package com.vmturbo.action.orchestrator.store;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import io.grpc.Channel;
import io.grpc.StatusRuntimeException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.topology.ActionGraphEntity;
import com.vmturbo.action.orchestrator.topology.ActionRealtimeTopology;
import com.vmturbo.action.orchestrator.topology.ActionTopologyStore;
import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.TopologyType;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetMultiSupplyChainsRequest;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode.MemberList;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainScope;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainSeed;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.EntityWithConnections;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.Type;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.repository.api.TopologyAvailabilityTracker;
import com.vmturbo.repository.api.TopologyAvailabilityTracker.TopologyUnavailableException;
import com.vmturbo.topology.graph.OwnershipGraph;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.graph.supplychain.SupplyChainCalculator;
import com.vmturbo.topology.graph.supplychain.TraversalRulesLibrary;

/**
 * Retrieves entities required for action plan processing.
 */
public class EntitiesSnapshotFactory {
    private static final Logger logger = LogManager.getLogger();

    private final ActionTopologyStore actionTopologyStore;

    private final long realtimeTopologyContextId;

    private final long timeToWaitForTopology;

    private final RepositoryServiceBlockingStub repositoryService;

    private final SupplyChainServiceBlockingStub supplyChainService;

    private final TimeUnit timeToWaitUnit;

    private final TopologyAvailabilityTracker topologyAvailabilityTracker;

    private final SupplyChainCalculator supplyChainCalculator;

    private final TraversalRulesLibrary<ActionGraphEntity> traversalRules = new TraversalRulesLibrary<>();

    EntitiesSnapshotFactory(@Nonnull final ActionTopologyStore actionTopologyStore,
            final long realtimeTopologyContextId,
            final long timeToWaitForTopology,
            @Nonnull final TimeUnit timeToWaitUnit,
            @Nonnull final Channel repositoryChannel,
            @Nonnull final TopologyAvailabilityTracker topologyAvailabilityTracker,
            @Nonnull final SupplyChainCalculator supplyChainCalculator) {
        this.actionTopologyStore = actionTopologyStore;
        this.realtimeTopologyContextId = realtimeTopologyContextId;
        this.timeToWaitForTopology = timeToWaitForTopology;
        this.repositoryService = RepositoryServiceGrpc.newBlockingStub(repositoryChannel);
        this.supplyChainService = SupplyChainServiceGrpc.newBlockingStub(repositoryChannel);
        this.timeToWaitUnit = timeToWaitUnit;
        this.topologyAvailabilityTracker = topologyAvailabilityTracker;
        this.supplyChainCalculator = supplyChainCalculator;
    }

    @Nonnull
    private EntitiesSnapshot getRealtimeEntitiesSnapshot(@Nonnull final Set<Long> entities) {
        Map<Long, ActionPartialEntity> entityMap = Collections.emptyMap();
        OwnershipGraph<EntityWithConnections> ownershipGraph = OwnershipGraph.empty();

        try {
            Optional<ActionRealtimeTopology> topologyOpt = actionTopologyStore.getSourceTopology(timeToWaitForTopology, timeToWaitUnit);
            if (!topologyOpt.isPresent()) {
                // This should only happen at startup.
                logger.error("Realtime topology not available. Entity snapshot won't have entity information.");
            } else {
                TopologyGraph<ActionGraphEntity> entityGraph = topologyOpt.get().entityGraph();
                entityMap = entityGraph.getEntities(entities)
                        .collect(Collectors.toMap(ActionGraphEntity::getOid, ActionGraphEntity::asPartialEntity));
                ownershipGraph = retrieveRealtimeOwnershipGraph(entities, entityGraph);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // Set the interrupt status on the thread.
            logger.error("Failed to wait for realtime topology to come in due to exception.", e);
        }

        return new EntitiesSnapshot(entityMap, ownershipGraph, TopologyType.SOURCE);
    }

    @Nonnull
    private EntitiesSnapshot getPlanEntitiesSnapshot(@Nonnull final Set<Long> entities,
            @Nonnull final Set<Long> nonProjectedEntities,
            final long topologyContextId,
            @Nullable final Long topologyId) {
        Map<Long, ActionPartialEntity> entityMap = Collections.emptyMap();
        OwnershipGraph<EntityWithConnections> ownershipGraph = OwnershipGraph.empty();

        // For plans we want to look in the projected topology, because in plans we will be
        // getting actions involving provisioned entities. In realtime we get provision actions,
        // but no actions on top of the provisioned entities, so looking in the source topology
        // is safe (and more efficient).
        final TopologyType targetTopologyType = TopologyType.PROJECTED;

        try {

            // Before we make calls for data that comes from the repository we wait until the
            // desired topology is available in the repository.
            if (topologyId != null) {
                // If we want a specific topology, wait for that topology to become available.
                topologyAvailabilityTracker.queueTopologyRequest(topologyContextId, topologyId).waitForTopology(timeToWaitForTopology, timeToWaitUnit);
            } else {
                // If not, wait for SOME topology of the target type to be available in the context.
                topologyAvailabilityTracker.queueAnyTopologyRequest(topologyContextId,
                        targetTopologyType).waitForTopology(timeToWaitForTopology,
                        timeToWaitUnit);
            }

            entityMap = retrieveOidToEntityMap(entities, topologyContextId, topologyId,
                    targetTopologyType);
            ownershipGraph = retrievePlanOwnershipGraph(entities, topologyContextId, topologyId,
                    targetTopologyType);

            // This will be the case for plans with detached volume actions only.
            // We need to get the information for these entities from the real-time SOURCE topology,
            // as they're not added to the plan projected topology.
            if (!nonProjectedEntities.isEmpty()) {
                Map<Long, ActionPartialEntity> entityMapNonProjected;
                topologyAvailabilityTracker
                        .queueAnyTopologyRequest(topologyContextId, TopologyType.SOURCE)
                        .waitForTopology(timeToWaitForTopology, timeToWaitUnit);

                entityMapNonProjected = retrieveOidToEntityMap(nonProjectedEntities, realtimeTopologyContextId,
                        topologyId, TopologyType.SOURCE);
                entityMap.putAll(entityMapNonProjected);
            }
        } catch (TopologyUnavailableException e) {
            logger.error(
                    "Topology not available. Entity snapshot won't have entity information."
                            + " Error: {}", e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // Set the interrupt status on the thread.
            logger.error("Failed to wait for repository to return data due to exception : " + e);
        }

        return new EntitiesSnapshot(entityMap, ownershipGraph, targetTopologyType);
    }

    EntitiesSnapshot getEntitiesSnapshot(@Nonnull final Set<Long> entities, @Nonnull final Set<Long> nonProjectedEntities, final long topologyContextId, @Nullable final Long topologyId) {
        final boolean isRealtime = topologyContextId == realtimeTopologyContextId;

        if (isRealtime) {
            return getRealtimeEntitiesSnapshot(entities);
        } else {
            return getPlanEntitiesSnapshot(entities, nonProjectedEntities, topologyContextId, topologyId);
        }
    }


    @Nonnull
    private OwnershipGraph<EntityWithConnections> retrieveRealtimeOwnershipGraph(@Nonnull final Set<Long> entities,
            @Nonnull final TopologyGraph<ActionGraphEntity> entityGraph) {
        final OwnershipGraph.Builder<EntityWithConnections> graphBuilder = OwnershipGraph.newBuilder(EntityWithConnections::getOid);
        final Map<Long, EntityWithConnections> businessAccounts =
                entityGraph.entitiesOfType(EntityType.BUSINESS_ACCOUNT)
                        .map(ActionGraphEntity::asEntityWithConnections)
                        .collect(Collectors.toMap(EntityWithConnections::getOid, Function.identity()));

        businessAccounts.forEach((accountId, accountEntity) -> {
            Collection<SupplyChainNode> accountSupplyChain =
                    supplyChainCalculator.getSupplyChainNodes(entityGraph,
                            Collections.singleton(accountId), e -> true, traversalRules).values();
            addSupplychainToOwnershipGraph(accountEntity, entities, accountSupplyChain, graphBuilder);
        });

        return graphBuilder.build();
    }

    @Nonnull
    private OwnershipGraph<EntityWithConnections> retrievePlanOwnershipGraph(@Nonnull final Set<Long> entities,
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
                .forEachRemaining(scResponse -> addSupplychainToOwnershipGraph(businessAccounts.get(scResponse.getSeedOid()),
                        entities,
                        scResponse.getSupplyChain().getSupplyChainNodesList(),
                        graphBuilder));
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
     * @param ownerAccount The owner account whose supply chain we are processing.
     * @param involvedEntities entities involved in current actions
     * @param supplyChainNodes The supply chain of the account.
     * @param graphBuilder builder for ownerShip graph
     */
    private static void addSupplychainToOwnershipGraph(final EntityWithConnections ownerAccount,
            @Nonnull Set<Long> involvedEntities,
            Collection<SupplyChainNode> supplyChainNodes,
            OwnershipGraph.Builder<EntityWithConnections> graphBuilder) {
        final Stream<Long> entitiesOwnedByAccount = supplyChainNodes.stream()
                .map(SupplyChainNode::getMembersByStateMap)
                .map(Map::values)
                .flatMap(memberList -> memberList.stream()
                        .map(MemberList::getMemberOidsList)
                        .flatMap(List::stream));
        entitiesOwnedByAccount.filter(involvedEntities::contains)
                .forEach(relevantEntity -> graphBuilder.addOwner(ownerAccount, relevantEntity));
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

    /**
     * Snapshot of entities involved in the actions in an action plan.
     */
    public static class EntitiesSnapshot {
        private final Map<Long, ActionPartialEntity> oidToEntityMap;
        private final OwnershipGraph<EntityWithConnections> ownershipGraph;
        private final TopologyType topologyType;

        EntitiesSnapshot(@Nonnull final Map<Long, ActionPartialEntity> oidToEntityMap,
                @Nonnull final OwnershipGraph<EntityWithConnections> ownershipGraph,
                @Nonnull final TopologyType topologyType) {
            this.oidToEntityMap = oidToEntityMap;
            this.ownershipGraph = ownershipGraph;
            this.topologyType = topologyType;
        }

        public Map<Long, ActionPartialEntity> getEntityMap() {
            return oidToEntityMap;
        }

        public OwnershipGraph<EntityWithConnections> getOwnershipGraph() {
            return ownershipGraph;
        }

        public TopologyType getTopologyType() {
            return topologyType;
        }
    }
}
