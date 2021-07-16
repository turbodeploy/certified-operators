package com.vmturbo.action.orchestrator.stats.aggregator;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nonnull;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableSet;

import io.grpc.Channel;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.longs.LongSets;

import org.apache.commons.lang3.mutable.MutableInt;

import com.vmturbo.action.orchestrator.stats.ActionStat;
import com.vmturbo.action.orchestrator.stats.LiveActionsStatistician.PreviousBroadcastActions;
import com.vmturbo.action.orchestrator.stats.ManagementUnitType;
import com.vmturbo.action.orchestrator.stats.StatsActionViewFactory.StatsActionView;
import com.vmturbo.action.orchestrator.stats.aggregator.ActionAggregatorFactory.ActionAggregator;
import com.vmturbo.action.orchestrator.stats.groups.ImmutableMgmtUnitSubgroupKey;
import com.vmturbo.action.orchestrator.stats.groups.MgmtUnitSubgroup.MgmtUnitSubgroupKey;
import com.vmturbo.action.orchestrator.topology.ActionGraphEntity;
import com.vmturbo.action.orchestrator.topology.ActionRealtimeTopology;
import com.vmturbo.action.orchestrator.topology.ActionTopologyStore;
import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.components.api.FormattedString;
import com.vmturbo.proactivesupport.DataMetricCounter;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.graph.supplychain.SupplyChainCalculator;
import com.vmturbo.topology.graph.supplychain.TraversalRulesLibrary;

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
    private static final Set<Integer> EXPANDED_SCOPE_ENTITY_TYPES = ImmutableSet.<Integer>builder()
            .add(ApiEntityType.VIRTUAL_MACHINE.typeNumber())
            .build();

    private final GroupServiceBlockingStub groupService;

    private final ActionTopologyStore actionTopologyStore;

    /**
     * entityId -> clusters the entity is in
     *
     * This includes the actual members of the cluster - i.e. the hosts/storages in the cluster
     * definition - as well as entities in the cluster scope with entity type in
     * {@link ClusterActionAggregator#EXPANDED_SCOPE_ENTITY_TYPES}.
     */
    private final Long2ObjectMap<LongSet> clustersOfEntity = new Long2ObjectOpenHashMap<>();

    private final SupplyChainCalculator supplyChainCalculator;
    private final TraversalRulesLibrary<ActionGraphEntity> traversalRules = new TraversalRulesLibrary<>();

    private ClusterActionAggregator(@Nonnull final GroupServiceBlockingStub groupService,
                            @Nonnull final ActionTopologyStore actionTopologyStore,
                            @Nonnull final LocalDateTime snapshotTime,
                            @Nonnull final SupplyChainCalculator supplyChainCalculator) {
        super(snapshotTime);
        this.groupService = Objects.requireNonNull(groupService);
        this.actionTopologyStore = Objects.requireNonNull(actionTopologyStore);
        this.supplyChainCalculator = Objects.requireNonNull(supplyChainCalculator);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void start() {
        // We expect the topology to be there, because we waited for it to become available
        // earlier in the action pipeline when constructing the entities and settings snapshot.
        Optional<TopologyGraph<ActionGraphEntity>> topologyGraphOpt =
                actionTopologyStore.getSourceTopology().map(ActionRealtimeTopology::entityGraph);
        if (!topologyGraphOpt.isPresent()) {
            logger.error("No topology graph found for cluster aggregation. Actions on related"
                    + "entities will not count.");
        }

        try (DataMetricTimer ignored = Metrics.CLUSTER_AGGREGATOR_INIT_TIME.startTimer()) {
            final MutableInt clusterCount = new MutableInt(0);
            final Stopwatch supplychainTimer = Stopwatch.createUnstarted();
            final AtomicReference<Exception> firstError = new AtomicReference<>(null);
            final Set<String> failedClusterDescriptors = new HashSet<>();

            GroupProtoUtil.CLUSTER_GROUP_TYPES.forEach(type -> groupService.getGroups(GetGroupsRequest.newBuilder()
                .setGroupFilter(GroupFilter.newBuilder()
                        .setGroupType(type)).build())
                .forEachRemaining(group -> {
                    final long clusterId = group.getId();
                    clusterCount.increment();
                    final Set<Long> clusterMembers = GroupProtoUtil.getAllStaticMembers(group.getDefinition());
                    clusterMembers.forEach(directMemberId -> {
                        clustersOfEntity.computeIfAbsent(directMemberId, k -> new LongOpenHashSet())
                                .add(clusterId);
                    });
                    topologyGraphOpt.ifPresent(topologyGraph -> {
                        supplychainTimer.start();
                        try {
                            final Map<Integer, SupplyChainNode> nodes = supplyChainCalculator.getSupplyChainNodes(topologyGraph,
                                    clusterMembers,
                                    e -> true,
                                    traversalRules);
                            EXPANDED_SCOPE_ENTITY_TYPES.forEach(entityType -> {
                                final SupplyChainNode node = nodes.getOrDefault(entityType,
                                        SupplyChainNode.getDefaultInstance());
                                RepositoryDTOUtil.getAllMemberOids(node).forEach(memberOid -> {
                                    clustersOfEntity.computeIfAbsent(memberOid, k -> new LongOpenHashSet()).add(clusterId);
                                });
                            });
                        } catch (RuntimeException e) {
                            final String clusterDescriptor = FormattedString.format("{} (id: {})",
                                    group.getDefinition().getDisplayName(),
                                    group.getId());
                            logger.debug("Failed to calculate supply chain for cluster {}.",
                                    clusterDescriptor, e);
                            firstError.compareAndSet(null, e);
                            failedClusterDescriptors.add(clusterDescriptor);
                        }
                        supplychainTimer.stop();
                    });
                }));

            if (!failedClusterDescriptors.isEmpty()) {
                logger.error("Failed to calculate supply chains for clusters: {}."
                    + " Turn on debug logging for all stack traces. First error:",
                    failedClusterDescriptors, firstError.get());
            }

            logger.info("Got {} clusters, and {} entities in scope. Supply chain calculation took {}s",
                    clusterCount.getValue(), clustersOfEntity.keySet().size(),
                    supplychainTimer.elapsed(TimeUnit.SECONDS));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void processAction(@Nonnull final StatsActionView actionSnapshot,
                              @Nonnull final PreviousBroadcastActions previousBroadcastActions) {
        final ActionEntity primaryEntity = actionSnapshot.primaryEntity();
        final int entityType = primaryEntity.getType();

        // Probably not going to be in multiple clusters, but generically speaking can't be sure.
        final LongSet clusterIds = clustersOfEntity.getOrDefault(primaryEntity.getId(), LongSets.EMPTY_SET);
        final boolean newAction = actionIsNew(actionSnapshot, previousBroadcastActions);
        clusterIds.forEach((long clusterId) -> {
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
            final ActionStat typeSpecificStat = getStat(muKey, actionSnapshot.actionGroupKey());
            typeSpecificStat.recordAction(actionSnapshot.recommendation(), primaryEntity, newAction);

            // Add the "global" action stats record - all entities in this cluster that are
            // involved in this action.
            //
            // We can't reliably re-construct the global records from the per-entity-type records
            // because a single action may appear in a variable number of entity type records.
            //
            // Note - this is no longer the case for clusters.
            // TODO (roman, Jun 14 2021): Check whether it's safe to remove the global subgroup.
            final MgmtUnitSubgroupKey globalSubgroupKey = ImmutableMgmtUnitSubgroupKey.builder()
                    .mgmtUnitId(clusterId)
                    .mgmtUnitType(getManagementUnitType())
                    // Clusters are always on prem.
                    .environmentType(EnvironmentType.ON_PREM)
                    .build();
            final ActionStat globalStat = getStat(globalSubgroupKey, actionSnapshot.actionGroupKey());
            globalStat.recordAction(actionSnapshot.recommendation(), primaryEntity, newAction);
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

        private final ActionTopologyStore actionTopologyStore;

        private final SupplyChainCalculator supplyChainCalculator;

        public ClusterActionAggregatorFactory(@Nonnull final Channel groupChannel,
                @Nonnull final ActionTopologyStore actionTopologyStore,
                @Nonnull final SupplyChainCalculator supplyChainCalculator) {
            this.groupServiceStub =
                GroupServiceGrpc.newBlockingStub(Objects.requireNonNull(groupChannel));
            this.actionTopologyStore = Objects.requireNonNull(actionTopologyStore);
            this.supplyChainCalculator = Objects.requireNonNull(supplyChainCalculator);
        }

        @Override
        public ClusterActionAggregator newAggregator(@Nonnull final LocalDateTime snapshotTime) {
            return new ClusterActionAggregator(groupServiceStub, actionTopologyStore,
                    snapshotTime, supplyChainCalculator);
        }
    }
}
