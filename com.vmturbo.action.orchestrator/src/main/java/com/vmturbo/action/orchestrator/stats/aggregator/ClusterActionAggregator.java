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

import com.vmturbo.action.orchestrator.stats.ActionStat;
import com.vmturbo.action.orchestrator.stats.ManagementUnitType;
import com.vmturbo.action.orchestrator.stats.SingleActionSnapshotFactory.SingleActionSnapshot;
import com.vmturbo.action.orchestrator.stats.aggregator.ActionAggregatorFactory.ActionAggregator;
import com.vmturbo.action.orchestrator.stats.groups.ImmutableMgmtUnitSubgroupKey;
import com.vmturbo.action.orchestrator.stats.groups.MgmtUnitSubgroup.MgmtUnitSubgroupKey;
import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.Group.Type;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.SupplyChain.MultiSupplyChainsRequest;
import com.vmturbo.common.protobuf.repository.SupplyChain.SupplyChainSeed;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceBlockingStub;
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
     * {@link ClusterActionAggregator.EXPANDED_SCOPE_ENTITY_TYPES}.
     */
    private final Multimap<Long, Long> clustersOfEntity = HashMultimap.create();

    ClusterActionAggregator(@Nonnull final GroupServiceBlockingStub groupService,
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
        try (DataMetricTimer timer = Metrics.CLUSTER_AGGREGATOR_INIT_TIME.startTimer()) {
            final Multimap<Long, Long> entitiesInCluster = HashMultimap.create();
            groupService.getGroups(GetGroupsRequest.newBuilder()
                    .setTypeFilter(Type.CLUSTER)
                    .build())
                    .forEachRemaining(group -> {
                        GroupProtoUtil.getClusterMembers(group).forEach(memberId -> {
                            entitiesInCluster.put(group.getId(), memberId);
                            clustersOfEntity.put(memberId, group.getId());
                        });
                    });

            // Short-circuit if there are no clusters with members.
            if (entitiesInCluster.isEmpty()) {
                return;
            }

            final MultiSupplyChainsRequest.Builder requestBuilder = MultiSupplyChainsRequest.newBuilder();
            entitiesInCluster.asMap().forEach((clusterId, clusterEntities) -> {
                // For clusters we don't expect any hy
                requestBuilder.addSeeds(SupplyChainSeed.newBuilder()
                    .setSeedOid(clusterId)
                    .addAllStartingEntityOid(clusterEntities)
                    .addAllEntityTypesToInclude(EXPANDED_SCOPE_ENTITY_TYPES));
            });

            supplyChainService.getMultiSupplyChains(requestBuilder.build())
                .forEachRemaining(supplyChainResponse -> {
                    final long clusterId = supplyChainResponse.getSeedOid();
                    supplyChainResponse.getSupplyChainNodesList().stream()
                        .filter(node -> EXPANDED_SCOPE_ENTITY_TYPES.contains(node.getEntityType()))
                        .flatMap(vmNode -> vmNode.getMembersByStateMap().values().stream())
                        .flatMap(memberList -> memberList.getMemberOidsList().stream())
                        .forEach(memberId -> clustersOfEntity.put(memberId, clusterId));
                });

            logger.info("Got {} clusters, and {} entities in scope.",
                    entitiesInCluster.keySet().size(), clustersOfEntity.keySet().size());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void processAction(@Nonnull final SingleActionSnapshot actionSnapshot) {
        // Collect the entities involved in the action by cluster, and entity type.
        //
        // Because we "expand" the scope to the VMs related to the clusters, entities will
        // often be in the scope of several clusters at once.
        final Map<Long, Multimap<Integer, ActionEntity>> involvedEntitiesByClusterAndType = new HashMap<>();
        actionSnapshot.involvedEntities().forEach(entity -> {
            clustersOfEntity.get(entity.getId()).forEach(clusterId -> {
                final Multimap<Integer, ActionEntity> entitiesByType =
                    involvedEntitiesByClusterAndType.computeIfAbsent(clusterId,
                        k -> HashMultimap.create());
                entitiesByType.put(entity.getType(), entity);
            });
        });

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
                        .build();
                final ActionStat stat = getStat(muKey, actionSnapshot.actionGroupKey());
                stat.recordAction(actionSnapshot.recommendation(), entities);
            });
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
