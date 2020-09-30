package com.vmturbo.action.orchestrator.stats.aggregator;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;

import io.grpc.Channel;
import io.grpc.StatusRuntimeException;

import com.vmturbo.action.orchestrator.stats.ActionStat;
import com.vmturbo.action.orchestrator.stats.LiveActionsStatistician.PreviousBroadcastActions;
import com.vmturbo.action.orchestrator.stats.ManagementUnitType;
import com.vmturbo.action.orchestrator.stats.StatsActionViewFactory.StatsActionView;
import com.vmturbo.action.orchestrator.stats.aggregator.ActionAggregatorFactory.ActionAggregator;
import com.vmturbo.action.orchestrator.stats.groups.ImmutableMgmtUnitSubgroupKey;
import com.vmturbo.action.orchestrator.stats.groups.MgmtUnitSubgroup.MgmtUnitSubgroupKey;
import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetMultiSupplyChainsRequest;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChain;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainScope;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainSeed;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceBlockingStub;
import com.vmturbo.proactivesupport.DataMetricCounter;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;

/**
 * An {@link ActionAggregator} for clusters.
 */
public class ClusterActionAggregator extends ActionAggregator {

    /**
     * The list of entity types to look for in the supply chain of the members of the cluster.
     * <p>
     * All matching entities will be considered "members" of this cluster for aggregation purposes.
     * Each entity type will have its own {@link MgmtUnitSubgroupKey} - e.g. the VMs in the cluster
     * will have action stats tracked separately from the hosts in the cluster.
     */
    private static final Set<String> EXPANDED_SCOPE_ENTITY_TYPES = ImmutableSet.<String>builder()
            .add("VirtualMachine")
            .build();

    private final GroupServiceBlockingStub groupService;

    private final SupplyChainServiceBlockingStub supplyChainService;

    /**
     * entityId -> clusters the entity is in
     *
     * This includes the actual members of the cluster - i.e. the hosts/storages in the cluster
     * definition - as well as entities in the cluster scope with entity type in
     * {@link ClusterActionAggregator#EXPANDED_SCOPE_ENTITY_TYPES}.
     */
    private final Multimap<Long, Long> clustersOfEntity = HashMultimap.create();

    private ClusterActionAggregator(@Nonnull final GroupServiceBlockingStub groupService,
                            @Nonnull final SupplyChainServiceBlockingStub supplyChainService,
                            @Nonnull final LocalDateTime snapshotTime) {
        super(snapshotTime);
        this.groupService = Objects.requireNonNull(groupService);
        this.supplyChainService = Objects.requireNonNull(supplyChainService);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void start() {
        try (DataMetricTimer ignored = Metrics.CLUSTER_AGGREGATOR_INIT_TIME.startTimer()) {
            final Multimap<Long, Long> entitiesInCluster = HashMultimap.create();
            GroupProtoUtil.CLUSTER_GROUP_TYPES
                .forEach(type -> groupService.getGroups(GetGroupsRequest.newBuilder()
                                .setGroupFilter(GroupFilter.newBuilder()
                                        .setGroupType(type)).build())
                                .forEachRemaining(group ->
                                    GroupProtoUtil.getAllStaticMembers(group.getDefinition()).forEach(memberId -> {
                                        entitiesInCluster.put(group.getId(), memberId);
                                        clustersOfEntity.put(memberId, group.getId());
                                    })));

            // Short-circuit if there are no clusters with members.
            if (entitiesInCluster.isEmpty()) {
                return;
            }

            final GetMultiSupplyChainsRequest.Builder requestBuilder = GetMultiSupplyChainsRequest.newBuilder();
            entitiesInCluster.asMap().forEach((clusterId, clusterEntities) -> {
                // For clusters we don't expect any hy
                requestBuilder.addSeeds(SupplyChainSeed.newBuilder()
                    .setSeedOid(clusterId)
                    .setScope(SupplyChainScope.newBuilder()
                        .addAllStartingEntityOid(clusterEntities)
                        .addAllEntityTypesToInclude(EXPANDED_SCOPE_ENTITY_TYPES)));
            });

            try {
                supplyChainService.getMultiSupplyChains(requestBuilder.build())
                    .forEachRemaining(supplyChainResponse -> {
                        final long clusterId = supplyChainResponse.getSeedOid();
                        if (supplyChainResponse.hasError()) {
                            logger.error("Limited action stats will be aggregated for the" +
                                    " cluster {}. Failed to retireve supply chain due to error: {}",
                                clusterId, supplyChainResponse.getError());
                        }
                        final SupplyChain supplyChain = supplyChainResponse.getSupplyChain();
                        final int missingEntitiesCount = supplyChain.getMissingStartingEntitiesCount();
                        if (missingEntitiesCount > 0) {
                            Metrics.MISSING_CLUSTER_ENTITIES_COUNTER.increment(
                                (double)missingEntitiesCount);
                            logger.warn("Supply chains of {} cluster members of cluster {} not found." +
                                    " Missing members: {}", missingEntitiesCount,
                                clusterId, supplyChain.getMissingStartingEntitiesList());
                        }

                        supplyChain.getSupplyChainNodesList().stream()
                            .filter(node -> EXPANDED_SCOPE_ENTITY_TYPES.contains(node.getEntityType()))
                            .flatMap(vmNode -> vmNode.getMembersByStateMap().values().stream())
                            .flatMap(memberList -> memberList.getMemberOidsList().stream())
                            .forEach(memberId -> clustersOfEntity.put(memberId, clusterId));
                    });
            } catch (StatusRuntimeException e) {
                // We can proceed - we just won't have stats for any "related" entities in the
                // cluster.
                logger.error("Failed to retrieve supply chains of clusters. Will only " +
                    "aggregate stats for hosts in the clusters. Error: {}", e.getMessage());
            }

            logger.info("Got {} clusters, and {} entities in scope.",
                    entitiesInCluster.keySet().size(), clustersOfEntity.keySet().size());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void processAction(@Nonnull final StatsActionView actionSnapshot,
                              @Nonnull final PreviousBroadcastActions previousBroadcastActions) {
        // Collect the entities involved in the action by cluster, and entity type.
        //
        // Because we "expand" the scope to the VMs related to the clusters, entities will
        // often be in the scope of several clusters at once.
        final Map<Long, Multimap<Integer, ActionEntity>> involvedEntitiesByClusterAndType = new HashMap<>();
        actionSnapshot.involvedEntities().forEach(entity ->
            clustersOfEntity.get(entity.getId()).forEach(clusterId -> {
                final Multimap<Integer, ActionEntity> entitiesByType =
                    involvedEntitiesByClusterAndType.computeIfAbsent(clusterId,
                        k -> HashMultimap.create());
                entitiesByType.put(entity.getType(), entity);
            }));

        if (logger.isTraceEnabled() && !involvedEntitiesByClusterAndType.isEmpty()) {
            logger.trace("Action {} involved entities by cluster and type: {}",
                actionSnapshot.recommendation().getId(), involvedEntitiesByClusterAndType);
        }

        involvedEntitiesByClusterAndType.forEach((clusterId, entitiesByType) -> {
            entitiesByType.asMap().forEach((entityType, entities) -> {
                // Note - we may end up creating a lot of these objects as we process an
                // action plan. If it becomes a problem we can keep them saved somewhere -
                // we only need 2 per cluster (one for PMs, and one for VMs).
                final MgmtUnitSubgroupKey muKey = ImmutableMgmtUnitSubgroupKey.builder()
                    .mgmtUnitId(clusterId)
                    .entityType(entityType)
                    .mgmtUnitType(getManagementUnitType())
                    // Clusters are always on prem.
                    .environmentType(EnvironmentType.ON_PREM)
                    .build();
                final ActionStat stat = getStat(muKey, actionSnapshot.actionGroupKey());
                stat.recordAction(actionSnapshot.recommendation(), entities,
                    actionIsNew(actionSnapshot, previousBroadcastActions));
            });

            // Add the "global" action stats record - all entities in this cluster that are
            // involved in this action.
            //
            // We can't reliably re-construct the global records from the per-entity-type records
            // because a single action may appear in a variable number of entity type records.
            final MgmtUnitSubgroupKey globalSubgroupKey = ImmutableMgmtUnitSubgroupKey.builder()
                .mgmtUnitId(clusterId)
                .mgmtUnitType(getManagementUnitType())
                // Clusters are always on prem.
                .environmentType(EnvironmentType.ON_PREM)
                .build();
            final ActionStat stat = getStat(globalSubgroupKey, actionSnapshot.actionGroupKey());
            // Not using all entities involved in the snapshot, because some of them may be out
            // of the cluster.
            stat.recordAction(actionSnapshot.recommendation(), entitiesByType.values(),
                actionIsNew(actionSnapshot, previousBroadcastActions));
        });
    }

    @Nonnull
    @Override
    protected ManagementUnitType getManagementUnitType() {
        return ManagementUnitType.CLUSTER;
    }

    /**
     * Metrics for {@link ClusterActionAggregator}.
     */
    private static class Metrics {

        private static final DataMetricSummary CLUSTER_AGGREGATOR_INIT_TIME = DataMetricSummary.builder()
            .withName("ao_action_cluster_agg_init_seconds")
            .withHelp("Information about how long it took to initialize the cluster aggregator.")
            .build()
            .register();

        private static final DataMetricCounter MISSING_CLUSTER_ENTITIES_COUNTER = DataMetricCounter.builder()
            .withName("ao_action_cluster_agg_missing_supply_chain_entities_count")
            .withHelp("Count of cluster members with missing supply chains (because they're not in the repository).")
            .build()
            .register();

    }

    /**
     * Factory for {@link ClusterActionAggregator}s.
     */
    public static class ClusterActionAggregatorFactory implements ActionAggregatorFactory<ClusterActionAggregator> {

        private final GroupServiceBlockingStub groupServiceStub;

        private final SupplyChainServiceBlockingStub supplyChainServiceStub;

        public ClusterActionAggregatorFactory(@Nonnull final Channel groupChannel,
                                              @Nonnull final Channel repositoryChannel) {
            this.groupServiceStub =
                GroupServiceGrpc.newBlockingStub(Objects.requireNonNull(groupChannel));
            this.supplyChainServiceStub =
                SupplyChainServiceGrpc.newBlockingStub(repositoryChannel);
        }

        @Override
        public ClusterActionAggregator newAggregator(@Nonnull final LocalDateTime snapshotTime) {
            return new ClusterActionAggregator(groupServiceStub, supplyChainServiceStub, snapshotTime);
        }
    }
}
