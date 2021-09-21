package com.vmturbo.action.orchestrator.store;

import java.time.Clock;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import com.google.common.annotations.VisibleForTesting;

import io.opentracing.SpanContext;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.topology.ActionGraphEntity;
import com.vmturbo.action.orchestrator.topology.ActionRealtimeTopology;
import com.vmturbo.action.orchestrator.topology.ActionTopologyStore;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.TopologyType;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode.MemberList;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.EntityWithConnections;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopology;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.components.api.client.RemoteIteratorDrain;
import com.vmturbo.market.component.api.ProjectedTopologyListener;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;
import com.vmturbo.topology.graph.OwnershipGraph;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.graph.TopologyGraphCreator;
import com.vmturbo.topology.graph.supplychain.SupplyChainCalculator;
import com.vmturbo.topology.graph.supplychain.TraversalRulesLibrary;

/**
 * Retrieves entities required for action plan processing.
 *
 * <p/>In the realtime case we retrieve entities from the topology graph cached in the
 * {@link ActionTopologyStore}.
 *
 * <p/>In the plan case we listen to the projected topology broadcast by the market. When we
 * receive the projected topology we pre-construct the snapshot and make it available for the
 * thread that will be processing the action plan. We do this because the projected topology and
 * action plan are broadcast on separate topics, and can come in at different times.
 *
 * <p/>Realistically the action plan should arrive first on a large topology, and we will not
 * be caching the plan {@link EntitiesSnapshot} for long. The action plan processing thread will
 * immediately pick it up.
 */
public class EntitiesSnapshotFactory implements ProjectedTopologyListener {
    private static final Logger logger = LogManager.getLogger();

    private final Clock clock;

    private final ActionTopologyStore actionTopologyStore;

    private final long realtimeTopologyContextId;

    private final long timeToWaitForTopology;

    private final TimeUnit timeToWaitUnit;

    private final SupplyChainCalculator supplyChainCalculator;

    private final TraversalRulesLibrary<ActionGraphEntity> traversalRules = new TraversalRulesLibrary<>();

    private final Map<Long, CompletableFuture<PlanTopologySnapshot>> planSnapshotRequests = Collections.synchronizedMap(new HashMap<>());

    EntitiesSnapshotFactory(@Nonnull final ActionTopologyStore actionTopologyStore,
            final long realtimeTopologyContextId,
            final long timeToWaitForTopology,
            @Nonnull final TimeUnit timeToWaitUnit,
            @Nonnull final SupplyChainCalculator supplyChainCalculator,
            @Nonnull final Clock clock) {
        this.actionTopologyStore = actionTopologyStore;
        this.realtimeTopologyContextId = realtimeTopologyContextId;
        this.timeToWaitForTopology = timeToWaitForTopology;
        this.timeToWaitUnit = timeToWaitUnit;
        this.supplyChainCalculator = supplyChainCalculator;
        this.clock = clock;
    }

    @Nonnull
    private EntitiesSnapshot getRealtimeEntitiesSnapshot(@Nonnull final Set<Long> entities) {

        try {
            Optional<ActionRealtimeTopology> topologyOpt = actionTopologyStore.getSourceTopology(timeToWaitForTopology, timeToWaitUnit);
            if (!topologyOpt.isPresent()) {
                // This should only happen at startup.
                logger.error("Realtime topology not available. Entity snapshot won't have entity information.");
            } else {
                return snapshotFromGraph(entities, topologyOpt.get().entityGraph(), TopologyType.SOURCE);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // Set the interrupt status on the thread.
            logger.error("Failed to wait for realtime topology to come in due to exception.", e);
        }

        return EntitiesSnapshot.EMPTY;
    }

    private EntitiesSnapshot snapshotFromGraph(@Nonnull final Set<Long> entities,
                @Nonnull final TopologyGraph<ActionGraphEntity> entityGraph,
                @Nonnull final TopologyType topologyType) {
        Map<Long, ActionPartialEntity> entityMap = entityGraph.getEntities(entities)
                .collect(Collectors.toMap(ActionGraphEntity::getOid, ActionGraphEntity::asPartialEntity));
        OwnershipGraph<EntityWithConnections> ownershipGraph = retrieveRealtimeOwnershipGraph(entities, entityGraph);
        return new EntitiesSnapshot(entityMap, ownershipGraph, topologyType);
    }

    /**
     * Iterate through the map and remove any requests that have not been picked up by
     * a thread processing the action plan. This should almost never happen, unless the market
     * crashes between sending the projected topology and the action plan, or unless the action
     * plan processing thread crashed before picking up the {@link EntitiesSnapshot}.
     *
     * @return Number of cleared snapshots.
     */
    @VisibleForTesting
    int cleanupQueuedSnapshots() {
        synchronized (planSnapshotRequests) {
            final int startingSize = planSnapshotRequests.size();
            final long earliestValidTime = clock.millis() - timeToWaitUnit.toMillis(timeToWaitForTopology);
            final Iterator<Entry<Long, CompletableFuture<PlanTopologySnapshot>>> reqIt = planSnapshotRequests.entrySet().iterator();
            while (reqIt.hasNext()) {
                final Entry<Long, CompletableFuture<PlanTopologySnapshot>> nextReq = reqIt.next();
                try {
                    final CompletableFuture<PlanTopologySnapshot> planSnapshotRequest = nextReq.getValue();
                    if (planSnapshotRequest.isDone() && planSnapshotRequest.join() != null) {
                        final PlanTopologySnapshot snapshot = planSnapshotRequest.join();
                        if (snapshot.creationTime.toEpochMilli() < earliestValidTime) {
                            logger.warn("Expiring plan snapshot for id {} created at {}", nextReq.getKey(), snapshot.creationTime);
                            reqIt.remove();
                        }
                    }
                } catch (RuntimeException e) {
                    // Since we don't know when the future was cancelled/completed with an unexpected
                    // exception we don't clear it.
                }
            }
            return startingSize - planSnapshotRequests.size();
        }
    }

    @Nonnull
    private EntitiesSnapshot getPlanEntitiesSnapshot(@Nonnull final Set<Long> entities, final long planId) {
        final CompletableFuture<PlanTopologySnapshot> snapshotFuture = planSnapshotRequests.computeIfAbsent(planId,
                k -> new CompletableFuture<>());
        try (DataMetricTimer timer = Metrics.PLAN_SNAPSHOT_WAIT_SUMMARY.startTimer()) {
            final TopologyGraph<ActionGraphEntity> entityGraph =
                    snapshotFuture.get(timeToWaitForTopology, timeToWaitUnit).getTopologyGraph();
            logger.info("Got plan entities graph for plan {} with {} entities ({} requested) after waiting {} seconds",
                    planId, entityGraph.size(), entities.size(), timer.getTimeElapsedSecs());

            final EntitiesSnapshot snapshot = snapshotFromGraph(entities,
                                                                entityGraph, TopologyType.PROJECTED);

            logger.info("Got plan entities snapshot for plan {} with {} entities ({} requested) after waiting {} seconds",
                    planId, snapshot.getEntityMap().size(), entities.size(), timer.getTimeElapsedSecs());
            return snapshot;
        } catch (ExecutionException | PlanSnapshotConstructionException e) {
            logger.error("Projected for plan {} topology not available due to error."
                    + "Actions may be missing description details", planId, e);
            return EntitiesSnapshot.EMPTY;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return EntitiesSnapshot.EMPTY;
        } catch (TimeoutException e) {
            logger.error("Timed out waiting for projected topology for plan {} to become available."
                    + "Actions may be missing description details", planId, e);
            return EntitiesSnapshot.EMPTY;
        } finally {
            // Regardless of what happens, once we're done waiting for it no one else is going
            // to wait for it anymore.
            planSnapshotRequests.remove(planId);
        }
    }

    @Nonnull
    EntitiesSnapshot getEntitiesSnapshot(@Nonnull final Set<Long> entities, final long topologyContextId) {
        final boolean isRealtime = topologyContextId == realtimeTopologyContextId;

        // Light maintenance work to keep the plan snapshot requests map clean and avoid wasting
        // memory.
        cleanupQueuedSnapshots();

        if (isRealtime) {
            return getRealtimeEntitiesSnapshot(entities);
        } else {
            return getPlanEntitiesSnapshot(entities, topologyContextId);
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

    @Override
    public void onProjectedTopologyReceived(final ProjectedTopology.Metadata metadata,
            @Nonnull final RemoteIterator<ProjectedTopologyEntity> topology,
            @Nonnull final SpanContext tracingContext) {
        final TopologyInfo sourceTopologyInfo = metadata.getSourceTopologyInfo();
        // We don't care about realtime projected topologies.
        if (sourceTopologyInfo.getTopologyContextId() == realtimeTopologyContextId) {
            RemoteIteratorDrain.drainIterator(topology,
                    TopologyDTOUtil.getProjectedTopologyLabel(sourceTopologyInfo),
                    // Do not expect empty - we know there are entities there!
                    false);
            return;
        }

        final CompletableFuture<PlanTopologySnapshot> requestedSnapshot =
                planSnapshotRequests.computeIfAbsent(sourceTopologyInfo.getTopologyContextId(),
                        k -> new CompletableFuture<>());

        TopologyGraphCreator<ActionGraphEntity.Builder, ActionGraphEntity> topologyGraphCreator =
                new TopologyGraphCreator<>();
        try {
            while (topology.hasNext()) {
                topology.nextChunk().forEach(projectedEntity -> topologyGraphCreator.addEntity(new ActionGraphEntity.Builder(projectedEntity.getEntity())));
            }

            requestedSnapshot.complete(new PlanTopologySnapshot(topologyGraphCreator.build(), clock, null));
        } catch (CommunicationException | TimeoutException | RuntimeException e) {
            requestedSnapshot.complete(new PlanTopologySnapshot(null, clock,
                new PlanSnapshotConstructionException("Failed to construct snapshot from graph.", e)));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            requestedSnapshot.complete(new PlanTopologySnapshot(null, clock,
                    new PlanSnapshotConstructionException("Thread interrupted while constructing snapshot.")));
        } finally {
            RemoteIteratorDrain.drainIterator(topology,
                    TopologyDTOUtil.getProjectedTopologyLabel(sourceTopologyInfo),
                    // Expect empty - should have processed the full topology.
                    true);
        }
    }

    /**
     * Snapshot of entities involved in the actions in an action plan.
     */
    public static class EntitiesSnapshot {

        private static final EntitiesSnapshot EMPTY = new EntitiesSnapshot(Collections.emptyMap(), OwnershipGraph.empty(), TopologyType.SOURCE);

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

    /**
     * Wrapper around an {@link TopologyGraph} or a {@link PlanSnapshotConstructionException} encountered
     * trying to construct the snapshot, and the time it was created.
     */
    @Immutable
    private static class PlanTopologySnapshot {
        private final Instant creationTime;
        private final TopologyGraph<ActionGraphEntity> topologyGraph;
        private final PlanSnapshotConstructionException exception;

        PlanTopologySnapshot(@Nullable final TopologyGraph<ActionGraphEntity> topologyGraph,
                             @Nonnull final Clock clock,
                             @Nullable final PlanSnapshotConstructionException exception) {
            this.creationTime = clock.instant();
            this.topologyGraph = topologyGraph;
            this.exception = exception;
        }

        @Nonnull
        public TopologyGraph<ActionGraphEntity> getTopologyGraph() throws PlanSnapshotConstructionException {
            if (exception != null) {
                throw exception;
            } else if (topologyGraph == null) {
                throw new PlanSnapshotConstructionException("Unexpected null snapshot.");
            }
            return topologyGraph;
        }
    }

    /**
     * Exception encountered when constructing the {@link EntitiesSnapshot} for plans.
     */
    private static class PlanSnapshotConstructionException extends Exception {
        PlanSnapshotConstructionException(String message) {
            super(message);
        }

        PlanSnapshotConstructionException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * Metrics.
     */
    private static class Metrics {

        private static final DataMetricSummary PLAN_SNAPSHOT_WAIT_SUMMARY = DataMetricSummary.builder()
                .withName("ao_plan_entities_snapshot_wait_seconds")
                .withHelp("Amount of time a plan action plan had to wait for the entities snapshot.")
                .build()
                .register();
    }
}
