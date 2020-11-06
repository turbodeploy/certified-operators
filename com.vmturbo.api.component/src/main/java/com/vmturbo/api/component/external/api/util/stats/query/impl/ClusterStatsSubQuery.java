package com.vmturbo.api.component.external.api.util.stats.query.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;

import com.vmturbo.api.component.external.api.mapper.StatsMapper;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryContextFactory.StatsQueryContext;
import com.vmturbo.api.component.external.api.util.stats.query.StatsSubQuery;
import com.vmturbo.api.component.external.api.util.stats.query.SubQuerySupportedStats;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.stats.Stats.ClusterStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.ClusterStatsResponse;
import com.vmturbo.common.protobuf.stats.Stats.EntityStats;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.utils.StringConstants;

/**
 * Sub-query responsible for getting pre aggregated cluster-level stats from the history
 * component. This query is meant for stats that are calculated daily, during the nightly plan.
 * Stats such as mem and cpu, should be fetched by HistoricalCommodityStatsSubQuery since they
 * are calculated every 10 minutes.
 */
public class ClusterStatsSubQuery implements StatsSubQuery {

    // TODO make sure this is consistent with CLUSTER property lists in StatsService, and see if
    // there's an opporutnity to consolidate
    private static final Set<String> PRE_AGGREGATED_CLUSTER_STATS =
            ImmutableSet.of(StringConstants.CPU_HEADROOM,
                    StringConstants.MEM_HEADROOM,
                    StringConstants.STORAGE_HEADROOM,
                    StringConstants.TOTAL_HEADROOM,
                    StringConstants.CPU_EXHAUSTION,
                    StringConstants.MEM_EXHAUSTION,
                    StringConstants.STORAGE_EXHAUSTION,
                    StringConstants.VM_GROWTH,
                    StringConstants.HEADROOM_VMS,
                    StringConstants.NUM_VMS,
                    StringConstants.NUM_HOSTS,
                    StringConstants.NUM_STORAGES,
                    StringConstants.NUM_SOCKETS,
                    StringConstants.NUM_CPUS,
                    StringConstants.HOST, // TODO why is this here?
                    // these aren't stored in cluster-stats directly but can be synthesized
                    // by ComputedPropertiesProccessor, so we treat them as if they were
                    StringConstants.NUM_VMS_PER_HOST,
                    StringConstants.NUM_VMS_PER_STORAGE);

    private final StatsMapper statsMapper;
    private final StatsHistoryServiceBlockingStub statsServiceRpc;

    /**
     * Create a new instance.
     *
     * @param statsMapper     stats mapper
     * @param statsServiceRpc stats RPC service endpoint
     */
    public ClusterStatsSubQuery(@Nonnull final StatsMapper statsMapper,
            @Nonnull final StatsHistoryServiceBlockingStub statsServiceRpc) {
        this.statsMapper = statsMapper;
        this.statsServiceRpc = statsServiceRpc;
    }

    @Override
    public boolean applicableInContext(@Nonnull final StatsQueryContext context) {
        // Supports queries on clusters. Applicable only if the input scope is a cluster and stats requested
        // are empty or in  the request there is at least one cluster commodity. If not, we assume the request is
        // for Physical Machines commodities.
        return context.getInputScope().getGroupType()
                .filter(GroupProtoUtil.CLUSTER_GROUP_TYPES::contains)
                .isPresent()
                && (context.getRequestedStats().isEmpty() || context.getRequestedStats().stream()
                .map(StatApiInputDTO::getName)
                .anyMatch(PRE_AGGREGATED_CLUSTER_STATS::contains));
    }

    @Override
    public SubQuerySupportedStats getHandledStats(@Nonnull final StatsQueryContext context) {
        return SubQuerySupportedStats.some(context.findStats(PRE_AGGREGATED_CLUSTER_STATS));
    }

    @Nonnull
    @Override
    public List<StatSnapshotApiDTO> getAggregateStats(@Nonnull final Set<StatApiInputDTO> requestedStats,
                                                      @Nonnull final StatsQueryContext context) throws OperationFailedException {
        final ClusterStatsRequest clusterStatsRequest = statsMapper.toClusterStatsRequest(
            context.getInputScope().uuid(),
            context.newPeriodInputDto(requestedStats));
        final List<EntityStats> entityStatsList = new ArrayList<>();

        Iterator<ClusterStatsResponse> response =
            statsServiceRpc.getClusterStats(clusterStatsRequest);
        while (response.hasNext()) {
            ClusterStatsResponse chunk = response.next();
            if (chunk.hasSnapshotsChunk()) {
               entityStatsList.addAll(chunk.getSnapshotsChunk().getSnapshotsList());
            }
        }

        return entityStatsList.stream()
                    .flatMap(e -> e.getStatSnapshotsList().stream())
                    .map(statsMapper::toStatSnapshotApiDTO)
                    .collect(Collectors.toList());
    }
}
