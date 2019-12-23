package com.vmturbo.api.component.external.api.util.stats.query.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;

import com.vmturbo.api.component.external.api.mapper.StatsMapper;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryContextFactory.StatsQueryContext;
import com.vmturbo.api.component.external.api.util.stats.query.StatsSubQuery;
import com.vmturbo.api.component.external.api.util.stats.query.SubQuerySupportedStats;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.stats.Stats.ClusterStatsRequest;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.components.common.utils.StringConstants;

/**
 * Sub-query responsible for getting pre aggregated cluster-level stats from the history
 * component. This query is meant for stats that are calculated daily, during the nightly plan.
 * Stats such as mem and cpu, should be fetched by HistoricalCommodityStatsSubQuery since they
 * are calculated every 10 minutes.
 */
public class ClusterStatsSubQuery implements StatsSubQuery {

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
            StringConstants.HOST);

    private final StatsMapper statsMapper;
    private final StatsHistoryServiceBlockingStub statsServiceRpc;

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
            .isPresent() &&
            (context.getRequestedStats().isEmpty() ||
                context.getRequestedStats().stream()
                    .map(StatApiInputDTO::getName)
                    .anyMatch(PRE_AGGREGATED_CLUSTER_STATS::contains));
    }

    @Override
    public SubQuerySupportedStats getHandledStats(@Nonnull final StatsQueryContext context) {
        return SubQuerySupportedStats.some(context.findStats(PRE_AGGREGATED_CLUSTER_STATS));
    }

    @Nonnull
    @Override
    public Map<Long, List<StatApiDTO>> getAggregateStats(@Nonnull final Set<StatApiInputDTO> requestedStats,
                                                         @Nonnull final StatsQueryContext context) throws OperationFailedException {
        final ClusterStatsRequest clusterStatsRequest = statsMapper.toClusterStatsRequest(
            context.getInputScope().uuid(),
            context.newPeriodInputDto(requestedStats));

        Map<Long, List<StatApiDTO>> stats = new HashMap<>();
        statsServiceRpc.getClusterStats(clusterStatsRequest).forEachRemaining(snapshot -> {
            final StatSnapshotApiDTO apiSnapshot = statsMapper.toStatSnapshotApiDTO(snapshot);
            stats.put(DateTimeUtil.parseTime(apiSnapshot.getDate()), apiSnapshot.getStatistics());
        });
        return stats;
    }
}
